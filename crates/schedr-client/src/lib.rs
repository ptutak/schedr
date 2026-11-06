//! A client of the webserver's REST API (`/api/v1`), for the user CLI and the web UI.
//!
//! Requests go through a [`Transport`]: HTTP in the real CLI ([`HttpTransport`]), something in
//! process in tests. Every request carries `Schedr-Client-Version`. Actions return a command;
//! [`Client::wait`] reads it back until the master has decided it (see PROJECT.md, Web UI and
//! CLI: the CLI waits for the decision).

use std::fmt::Write as _;
use std::time::Duration;

use jiff::Timestamp;
use schedr_api::views::{
    AllRunsQuery, AttemptList, CheckConnectionsRequest, CommandList, CommandView, ConnectionList,
    DagList, DagView, DeploymentView, ImportErrorList, LogChunk, OutputView, PauseRequest,
    ResourceList, RunDetail, RunList, SensorList, TestConnectionRequest, TriggerRequest,
    WorkerList,
};
use schedr_model::commands::ConnectionsResult;
use serde::de::DeserializeOwned;

/// This client's version (the workspace's), sent in [`CLIENT_VERSION_HEADER`].
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod stream;
pub mod times;

pub use stream::{Chunks, EventReader, ServerEvent};

#[cfg(test)]
mod tests;

/// The header carrying this client's version.
pub const CLIENT_VERSION_HEADER: &str = "schedr-client-version";

/// How often [`Client::wait`] reads a command back.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// How long [`Client::wait`] waits for the master's decision.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long connecting to the server may take.
///
/// A handshake that never completes (a black-holed address) stops here.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one request may take, from the start of the connection until the body ends.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// An HTTP method the client uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
}

/// A request to the API.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiRequest {
    /// The method.
    pub method: Method,
    /// The path and query, e.g. `/api/v1/dags/sync/runs?for=...`.
    pub path: String,
    /// The JSON body, if any.
    pub body: Option<serde_json::Value>,
    /// A command's `Idempotency-Key` (see [`COMMAND_KEY`]).
    pub key: Option<String>,
}

/// The API's answer.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiResponse {
    /// The HTTP status.
    pub status: u16,
    /// The JSON body (`null` when empty or not JSON).
    pub body: serde_json::Value,
}

/// Why a request failed.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The server can't be reached.
    #[error("can't reach the schedr server at {server}: {reason}")]
    Connect {
        /// The server's URL.
        server: String,
        /// Why.
        reason: String,
    },
    /// The server answered with an error.
    #[error("{message}")]
    Server {
        /// The HTTP status.
        status: u16,
        /// The server's message.
        message: String,
    },
    /// The answer isn't what the API describes.
    #[error("unexpected answer from the server: {0}")]
    Unexpected(String),
    /// The master rejected the command.
    #[error("rejected: {0}")]
    Rejected(String),
    /// A command was sent [`COMMAND_TRIES`] times and no answer came (or the server failed):
    /// it may have been stored, so it is not "not sent" (B140).
    #[error(
        "no answer from the schedr server ({reason}): the command may have arrived. Look \
         before sending it again (`schedr runs list`, `schedr dags list`, `schedr workers \
         list`): sent again, it is a new command"
    )]
    Unanswered {
        /// The last try's error.
        reason: String,
    },
    /// The master didn't decide in time.
    #[error(
        "the master hasn't applied the command yet (command {command_id}); is the master \
         running? `schedr` will apply it when it is"
    )]
    Timeout {
        /// The command.
        command_id: String,
    },
}

/// Sends requests to the API.
pub trait Transport: Sync {
    /// Sends one request.
    fn send(
        &self,
        request: ApiRequest,
    ) -> impl Future<Output = Result<ApiResponse, ClientError>> + Send;

    /// Opens `GET <path>` and reads its body as it comes (server-sent events); a transport
    /// that can't stream says so.
    fn open(&self, path: String) -> impl Future<Output = Result<Chunks, ClientError>> + Send {
        let _ = path;
        std::future::ready(Err(ClientError::Unexpected(
            "this transport can't stream".to_owned(),
        )))
    }
}

tokio::task_local! {
    /// The person a request is made for (the web UI sets it for each page it serves): sent
    /// as `X-Schedr-User`, which a webserver takes only from a caller proven to be the UI
    /// (see `schedr_api::auth`).
    pub static USER: String;
}

/// `work` made for the same person as the current request: [`USER`] doesn't follow a spawned
/// task by itself, so a page asking several deployments at once carries it over.
pub fn for_same_user<F: Future>(work: F) -> impl Future<Output = F::Output> {
    let user = current_user();
    async move {
        match user {
            Some(user) => USER.scope(user, work).await,
            None => work.await,
        }
    }
}

tokio::task_local! {
    /// The key of the command sent in this scope (B140): sent as `Idempotency-Key` with each
    /// `POST`, so a command sent again with it after its answer was lost (the webserver may
    /// have stored it) is the one stored then, not a second.
    pub static COMMAND_KEY: String;
}

/// `send` made with `key` as its command's `Idempotency-Key` ([`COMMAND_KEY`]).
pub async fn with_command_key<F: Future>(key: String, send: F) -> F::Output {
    COMMAND_KEY.scope(key, send).await
}

/// How many times [`Client::submit`] sends a command whose answer doesn't come.
pub const COMMAND_TRIES: u32 = 3;

/// A new command key: unique, for one command (see [`Client::submit`]).
#[must_use]
pub fn new_command_key() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// The person the current request is made for, if [`USER`] is set.
#[must_use]
pub fn current_user() -> Option<String> {
    USER.try_with(Clone::clone).ok()
}

/// How long a stream may stay open before the client gives up on it (and opens another).
pub const STREAM_TIMEOUT: Duration = Duration::from_hours(24);

/// Credentials the CLI sends (never validated here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// None (`auth.mode = "none"`, from inside the namespace).
    None,
    /// A bearer token (`--token` / `SCHEDR_TOKEN`).
    Bearer(String),
    /// HTTP Basic (`--user` / `SCHEDR_USER`, password from `SCHEDR_PASSWORD`).
    Basic {
        /// The user name.
        user: String,
        /// The password.
        password: Option<String>,
    },
}

impl Auth {
    /// Bearer when `token` is set (it wins over a user name), else HTTP Basic, else none.
    #[must_use]
    pub fn from_cli(token: Option<&str>, user: Option<&str>, password: Option<&str>) -> Self {
        match (token, user) {
            (Some(token), _) => Self::Bearer(token.to_owned()),
            (None, Some(user)) => Self::Basic {
                user: user.to_owned(),
                password: password.map(str::to_owned),
            },
            (None, None) => Self::None,
        }
    }
}

/// An mTLS client and the material generation it was built from.
type CachedClient = std::sync::Arc<std::sync::Mutex<(u64, reqwest::Client)>>;

/// The API over HTTP.
#[derive(Debug, Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    server: String,
    auth: Auth,
    /// The token presented to prove the calls are the web UI's (`X-Schedr-Ui-Token`).
    ui_token: Option<schedr_api::auth::UiToken>,
    /// mTLS with this role's certificate (a webserver's UI port), and the client built from
    /// it, by the material's generation (rebuilt when the files change).
    tls: Option<(schedr_tls::Tls, CachedClient)>,
}

impl HttpTransport {
    /// A transport to `server` (e.g. `http://localhost:8080`) sending `auth`.
    ///
    /// Connecting gives up after [`CONNECT_TIMEOUT`]; a request gives up after
    /// [`REQUEST_TIMEOUT`], a stream ([`Transport::open`]) after [`STREAM_TIMEOUT`].
    ///
    /// # Panics
    ///
    /// If the HTTP client cannot be built. The builder here sets only the two timeouts, so
    /// this does not happen in practice.
    #[must_use]
    pub fn new(server: &str, auth: Auth) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("the HTTP client builds with connect and request timeouts");
        Self {
            client,
            server: server.trim_end_matches('/').to_owned(),
            auth,
            ui_token: None,
            tls: None,
        }
    }

    /// This transport reaching its server over mTLS with `tls`'s certificate (`schedr ui` to
    /// a webserver's UI port, decision 36); the server's certificate must name the URL's host.
    ///
    /// # Panics
    ///
    /// As [`Self::new`].
    #[must_use]
    pub fn with_tls(self, tls: schedr_tls::Tls) -> Self {
        let client = mtls_client(&tls);
        let cached = std::sync::Arc::new(std::sync::Mutex::new((tls.generation(), client)));
        Self {
            tls: Some((tls, cached)),
            ..self
        }
    }

    /// The HTTP client to use now: the plain one, or the mTLS one of the material in use.
    fn http(&self) -> reqwest::Client {
        let Some((tls, cached)) = &self.tls else {
            return self.client.clone();
        };
        let generation = tls.generation();
        let mut cached = cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cached.0 != generation {
            *cached = (generation, mtls_client(tls));
        }
        cached.1.clone()
    }

    /// This transport presenting `token` as the web UI's (where there is no mTLS: decision 36),
    /// as it is at each request (a token file followed).
    #[must_use]
    pub fn with_ui_token(self, token: schedr_api::auth::UiToken) -> Self {
        Self {
            ui_token: Some(token),
            ..self
        }
    }

    /// The headers every request carries: the version, the credentials, the UI's token and
    /// the person it is made for.
    fn headers(&self, mut builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder = builder.header(CLIENT_VERSION_HEADER, VERSION);
        builder = match &self.auth {
            Auth::None => builder,
            Auth::Bearer(token) => builder.bearer_auth(token),
            Auth::Basic { user, password } => builder.basic_auth(user, password.as_ref()),
        };
        if let Some(token) = &self.ui_token {
            builder = builder.header(
                schedr_api::auth::UI_TOKEN_HEADER,
                token.current().0.as_str(),
            );
        }
        if let Some(user) = current_user() {
            builder = builder.header(schedr_api::auth::USER_HEADER, user);
        }
        builder
    }
}

impl HttpTransport {
    /// The server's URL, without a trailing slash.
    #[must_use]
    pub fn server(&self) -> &str {
        &self.server
    }

    /// The credentials it sends.
    #[must_use]
    pub const fn auth(&self) -> &Auth {
        &self.auth
    }
}

impl Transport for HttpTransport {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        let url = format!("{}{}", self.server, request.path);
        let client = self.http();
        let mut builder = self.headers(match request.method {
            Method::Get => client.get(&url),
            Method::Post => client.post(&url),
        });
        if let Some(body) = &request.body {
            builder = builder.json(body);
        }
        if let Some(key) = &request.key {
            builder = builder.header("idempotency-key", key);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| connect_error(&self.server, e))?;
        refuse_redirect(&self.server, &response)?;
        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| connect_error(&self.server, e))?;
        Ok(ApiResponse {
            status,
            body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        })
    }

    async fn open(&self, path: String) -> Result<Chunks, ClientError> {
        let url = format!("{}{path}", self.server);
        let builder = self.headers(
            self.http()
                .get(&url)
                .header("accept", "text/event-stream")
                .timeout(STREAM_TIMEOUT),
        );
        let connect = |e: reqwest::Error| connect_error(&self.server, e);
        let mut response = builder.send().await.map_err(connect)?;
        refuse_redirect(&self.server, &response)?;
        if !response.status().is_success() {
            return Err(ClientError::Server {
                status: response.status().as_u16(),
                message: format!("the server answered {}", response.status()),
            });
        }
        let (sender, chunks) = Chunks::channel();
        let server = self.server.clone();
        tokio::spawn(async move {
            loop {
                // A reader gone is noticed at once, not at the next chunk: a stalled stream
                // would otherwise stay open (its socket too) until its timeout (B138).
                let next = tokio::select! {
                    () = sender.closed() => return,
                    next = response.chunk() => next,
                };
                let chunk = match next {
                    Ok(Some(chunk)) => Ok(chunk.to_vec()),
                    Ok(None) => return,
                    Err(e) => Err(connect_error(&server, e)),
                };
                let failed = chunk.is_err();
                // The reader went away, or the stream failed: done either way.
                if sender.send(chunk).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(chunks)
    }
}

/// The server couldn't be reached: with its URL and the reason shown masked, so a password
/// in the URL (`[ui] deployments`, `--server`) or in what the HTTP library says never shows
/// (Codex's review of M4.4b).
fn connect_error(server: &str, reason: impl std::fmt::Display) -> ClientError {
    ClientError::Connect {
        server: schedr_config::masked::mask_text(server),
        reason: schedr_config::masked::mask_text(&reason.to_string()),
    }
}

/// A redirect is an error: it is never followed (B133), since the request carries
/// credentials (the UI's token, a password, the person's name) that would go wherever it
/// points. The message names that address, to be configured instead.
fn refuse_redirect(server: &str, response: &reqwest::Response) -> Result<(), ClientError> {
    if !response.status().is_redirection() {
        return Ok(());
    }
    let to = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|l| l.to_str().ok())
        .unwrap_or("an address it didn't give");
    Err(connect_error(
        server,
        format!(
            "it answered {} redirecting to {to}; schedr follows no redirect (the request's \
             credentials would go there too): configure the address it should reach",
            response.status()
        ),
    ))
}

/// An HTTP client presenting `tls`'s certificate (its `api_client` configuration).
fn mtls_client(tls: &schedr_tls::Tls) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .tls_backend_preconfigured((*tls.api_client()).clone())
        .build()
        .expect("the HTTP client builds with its timeouts and TLS configuration")
}

/// The API in the same process: requests go straight to its router, with no network (the
/// web UI of `schedr standalone`, and tests).
#[derive(Debug, Clone)]
pub struct RouterTransport {
    app: axum::Router,
}

impl RouterTransport {
    /// A transport to the API served by `app` (the webserver's router).
    #[must_use]
    pub const fn new(app: axum::Router) -> Self {
        Self { app }
    }
}

impl Transport for RouterTransport {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;
        let in_process = |reason: String| ClientError::Connect {
            server: "the API in this process".to_owned(),
            reason,
        };
        let method = match request.method {
            Method::Get => axum::http::Method::GET,
            Method::Post => axum::http::Method::POST,
        };
        let body = request.body.map_or_else(axum::body::Body::empty, |b| {
            axum::body::Body::from(b.to_string())
        });
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(&request.path)
            .header("content-type", "application/json");
        if let Some(key) = &request.key {
            builder = builder.header("idempotency-key", key);
        }
        let http = in_process_request(builder)
            .body(body)
            .map_err(|e| in_process(e.to_string()))?;
        let response = self
            .app
            .clone()
            .oneshot(http)
            .await
            .map_err(|e| in_process(e.to_string()))?;
        let status = response.status().as_u16();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| in_process(e.to_string()))?
            .to_bytes();
        Ok(ApiResponse {
            status,
            body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        })
    }

    async fn open(&self, path: String) -> Result<Chunks, ClientError> {
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;
        let in_process = |reason: String| ClientError::Connect {
            server: "the API in this process".to_owned(),
            reason,
        };
        let http = in_process_request(
            axum::http::Request::builder()
                .uri(&path)
                .header("accept", "text/event-stream"),
        )
        .body(axum::body::Body::empty())
        .map_err(|e| in_process(e.to_string()))?;
        let response = self
            .app
            .clone()
            .oneshot(http)
            .await
            .map_err(|e| in_process(e.to_string()))?;
        if !response.status().is_success() {
            return Err(ClientError::Server {
                status: response.status().as_u16(),
                message: format!("the server answered {}", response.status()),
            });
        }
        let mut body = response.into_body();
        let (sender, chunks) = Chunks::channel();
        tokio::spawn(async move {
            loop {
                // As over HTTP: a reader gone is noticed at once (B138).
                let frame = tokio::select! {
                    () = sender.closed() => return,
                    frame = body.frame() => frame,
                };
                let Some(frame) = frame else { return };
                let chunk = match frame {
                    Ok(frame) => match frame.into_data() {
                        Ok(data) => Ok(data.to_vec()),
                        Err(_) => continue,
                    },
                    Err(e) => Err(in_process(e.to_string())),
                };
                let failed = chunk.is_err();
                if sender.send(chunk).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(chunks)
    }
}

/// A request to the API in this process: the version, the person it is made for, and marked
/// as the UI's, proven by coming from inside the process (an extension: nothing from the
/// network can set it; see `schedr_api::auth`).
fn in_process_request(builder: axum::http::request::Builder) -> axum::http::request::Builder {
    let mut builder = builder
        .header(CLIENT_VERSION_HEADER, VERSION)
        .extension(schedr_api::auth::ProvenUi);
    if let Some(user) = current_user() {
        builder = builder.header(schedr_api::auth::USER_HEADER, user);
    }
    builder
}

/// Typed calls to the API.
#[derive(Debug, Clone)]
pub struct Client<T> {
    transport: T,
    poll: Duration,
    timeout: Duration,
}

impl<T: Transport> Client<T> {
    /// A client over `transport`, waiting for decisions every `poll` for at most `timeout`.
    pub const fn new(transport: T, poll: Duration, timeout: Duration) -> Self {
        Self {
            transport,
            poll,
            timeout,
        }
    }

    /// The transport.
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    /// How often it reads again while waiting (a decision, a growing log).
    pub const fn poll(&self) -> Duration {
        self.poll
    }

    /// `GET /api/v1/workers`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn workers(&self) -> Result<WorkerList, ClientError> {
        self.call(Method::Get, "/api/v1/workers".to_owned(), None)
            .await
    }

    /// `GET /api/v1/sensors/switched-off`: the wait steps switched off.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn switched_off_sensors(&self) -> Result<SensorList, ClientError> {
        self.call(Method::Get, "/api/v1/sensors/switched-off".to_owned(), None)
            .await
    }

    /// `POST /api/v1/dags/{dag}/sensors/{sensor}/switch-on`: the command.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn switch_on_sensor(
        &self,
        dag: &str,
        sensor: &str,
    ) -> Result<CommandView, ClientError> {
        self.call(
            Method::Post,
            format!(
                "/api/v1/dags/{}/sensors/{}/switch-on",
                path(dag),
                path(sensor)
            ),
            None,
        )
        .await
    }

    /// `GET /api/v1/runs/{run}/resources`: a run's resources.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn run_resources(&self, run: &str) -> Result<ResourceList, ClientError> {
        self.call(
            Method::Get,
            format!("/api/v1/runs/{}/resources", path(run)),
            None,
        )
        .await
    }

    /// `GET /api/v1/deployment`: the deployment's nodes and their configurations (masked).
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn deployment(&self) -> Result<DeploymentView, ClientError> {
        self.call(Method::Get, "/api/v1/deployment".to_owned(), None)
            .await
    }

    /// `GET /api/v1/import-errors`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn import_errors(&self) -> Result<ImportErrorList, ClientError> {
        self.call(Method::Get, "/api/v1/import-errors".to_owned(), None)
            .await
    }

    /// `GET /api/v1/connections`: each root's connections and the tasks declaring them.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn connections(&self) -> Result<ConnectionList, ClientError> {
        self.call(Method::Get, "/api/v1/connections".to_owned(), None)
            .await
    }

    /// `GET /api/v1/dags`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn dags(&self) -> Result<DagList, ClientError> {
        self.call(Method::Get, "/api/v1/dags".to_owned(), None)
            .await
    }

    /// `GET /api/v1/dags/{dag}`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails (a DAG that doesn't exist: the server's 404) or
    /// the answer doesn't read.
    pub async fn dag(&self, dag: &str) -> Result<DagView, ClientError> {
        self.call(Method::Get, format!("/api/v1/dags/{}", path(dag)), None)
            .await
    }

    /// `GET /api/v1/dags/{dag}/runs`, optionally for one moment.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn runs(
        &self,
        dag: &str,
        planned_for: Option<Timestamp>,
    ) -> Result<RunList, ClientError> {
        let query = planned_for.map_or_else(String::new, |t| format!("?for={t}"));
        self.call(
            Method::Get,
            format!("/api/v1/dags/{}/runs{query}", path(dag)),
            None,
        )
        .await
    }

    /// `GET /api/v1/runs?dag=...&state=...&before=...&before_run=...&limit=...`: runs of every DAG (or one),
    /// newest triggered first.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn all_runs(&self, query: &AllRunsQuery) -> Result<RunList, ClientError> {
        let mut params = Vec::new();
        if let Some(dag) = &query.dag {
            params.push(format!("dag={}", path(dag.as_str())));
        }
        if let Some(state) = query.state {
            params.push(format!("state={state}"));
        }
        if let Some(before) = query.before {
            params.push(format!("before={}", path(&before.to_string())));
        }
        if let Some(run_id) = &query.before_run {
            params.push(format!("before_run={}", path(run_id)));
        }
        if let Some(limit) = query.limit {
            params.push(format!("limit={limit}"));
        }
        let query = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params.join("&"))
        };
        self.call(Method::Get, format!("/api/v1/runs{query}"), None)
            .await
    }

    /// `GET /api/v1/runs/{run}/dag`: the DAG JSON of the version the run ran with.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn run_dag(&self, run_id: &str) -> Result<serde_json::Value, ClientError> {
        self.call(
            Method::Get,
            format!("/api/v1/runs/{}/dag", path(run_id)),
            None,
        )
        .await
    }

    /// `GET /api/v1/runs/{run}/commands`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn run_commands(&self, run_id: &str) -> Result<CommandList, ClientError> {
        self.call(
            Method::Get,
            format!("/api/v1/runs/{}/commands", path(run_id)),
            None,
        )
        .await
    }

    /// `GET /api/v1/runs/{run}/tasks/{task}/output` (an error with status 404 while it has
    /// none).
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn output(&self, run_id: &str, task_id: &str) -> Result<OutputView, ClientError> {
        self.instance_output(run_id, task_id, None).await
    }

    /// As [`Self::output`], of one instance of a mapped task (`None`: a mapped task's whole
    /// list, decision 81).
    ///
    /// # Errors
    ///
    /// As [`Self::output`].
    pub async fn instance_output(
        &self,
        run_id: &str,
        task_id: &str,
        map_index: Option<u32>,
    ) -> Result<OutputView, ClientError> {
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/output{}",
                path(run_id),
                path(task_id),
                instance(map_index, '?')
            ),
            None,
        )
        .await
    }

    /// `GET /api/v1/events`: what changed, as it happens (`run`, `dag`, `workers` and
    /// `refresh` events).
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the stream can't be opened.
    pub async fn events(&self) -> Result<EventReader, ClientError> {
        let chunks = self.transport.open("/api/v1/events".to_owned()).await?;
        Ok(EventReader::new(chunks))
    }

    /// `GET /api/v1/commands/{command}`: a command and the master's decision.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn command(&self, command_id: &str) -> Result<CommandView, ClientError> {
        self.call(
            Method::Get,
            format!("/api/v1/commands/{}", path(command_id)),
            None,
        )
        .await
    }

    /// `GET /api/v1/runs/{run}`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn run(&self, run_id: &str) -> Result<RunDetail, ClientError> {
        self.call(Method::Get, format!("/api/v1/runs/{}", path(run_id)), None)
            .await
    }

    /// `GET /api/v1/runs/{run}/tasks/{task}/attempts`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn attempts(&self, run_id: &str, task_id: &str) -> Result<AttemptList, ClientError> {
        self.instance_attempts(run_id, task_id, None).await
    }

    /// As [`Self::attempts`], of one instance of a mapped task (decision 82).
    ///
    /// # Errors
    ///
    /// As [`Self::attempts`].
    pub async fn instance_attempts(
        &self,
        run_id: &str,
        task_id: &str,
        map_index: Option<u32>,
    ) -> Result<AttemptList, ClientError> {
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/attempts{}",
                path(run_id),
                path(task_id),
                instance(map_index, '?')
            ),
            None,
        )
        .await
    }

    /// `GET /api/v1/runs/{run}/tasks/{task}/attempts/{try}/log?from=...`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails (the worker is gone, the log deleted) or the
    /// answer doesn't read.
    pub async fn log(
        &self,
        run_id: &str,
        task_id: &str,
        try_number: u32,
        from: u64,
    ) -> Result<LogChunk, ClientError> {
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/attempts/{try_number}/log?from={from}",
                path(run_id),
                path(task_id)
            ),
            None,
        )
        .await
    }

    /// As [`Self::log`], at most `limit` bytes (the API's limits apply).
    ///
    /// # Errors
    ///
    /// As [`Self::log`].
    pub async fn log_part(
        &self,
        run_id: &str,
        task_id: &str,
        try_number: u32,
        from: u64,
        limit: u64,
    ) -> Result<LogChunk, ClientError> {
        self.instance_log_part(run_id, (task_id, None), try_number, from, limit)
            .await
    }

    /// As [`Self::log_part`], of one instance of a mapped task (decision 82).
    ///
    /// # Errors
    ///
    /// As [`Self::log`].
    pub async fn instance_log_part(
        &self,
        run_id: &str,
        (task_id, map_index): (&str, Option<u32>),
        try_number: u32,
        from: u64,
        limit: u64,
    ) -> Result<LogChunk, ClientError> {
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/attempts/{try_number}/log?from={from}&limit={limit}{}",
                path(run_id),
                path(task_id),
                instance(map_index, '&')
            ),
            None,
        )
        .await
    }

    /// `POST /api/v1/dags/{dag}/pause` or `.../unpause`: the command.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn set_paused(
        &self,
        dag: &str,
        paused: bool,
        cancel_runs: bool,
    ) -> Result<CommandView, ClientError> {
        let (action, body) = if paused {
            let request = PauseRequest { cancel_runs };
            ("pause", Some(to_body(&request)?))
        } else {
            ("unpause", None)
        };
        self.call(
            Method::Post,
            format!("/api/v1/dags/{}/{action}", path(dag)),
            body,
        )
        .await
    }

    /// `POST /api/v1/dags/{dag}/runs`: the command; `params`, values for the run's
    /// parameters over the DAG's defaults.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn trigger(
        &self,
        dag: &str,
        planned_for: Option<Timestamp>,
        params: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Result<CommandView, ClientError> {
        let body = to_body(&TriggerRequest {
            planned_for,
            params: params.cloned(),
        })?;
        self.call(
            Method::Post,
            format!("/api/v1/dags/{}/runs", path(dag)),
            Some(body),
        )
        .await
    }

    /// `POST /api/v1/connections/check`: the command (`schedr connections check`).
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn check_connections(
        &self,
        request: &CheckConnectionsRequest,
    ) -> Result<CommandView, ClientError> {
        let body = to_body(request)?;
        self.call(
            Method::Post,
            "/api/v1/connections/check".to_owned(),
            Some(body),
        )
        .await
    }

    /// `POST /api/v1/connections/{name}/test`: the command (`schedr connections test`).
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn test_connection(
        &self,
        name: &str,
        request: &TestConnectionRequest,
    ) -> Result<CommandView, ClientError> {
        let body = to_body(request)?;
        self.call(
            Method::Post,
            format!("/api/v1/connections/{}/test", path(name)),
            Some(body),
        )
        .await
    }

    /// Reads an applied connections command back until every job has its answer (or was
    /// lost), for at most `timeout`: its result.
    ///
    /// # Errors
    ///
    /// [`ClientError::Timeout`] if a worker hasn't answered by then, a result that doesn't
    /// read ([`ClientError::Unexpected`]), or a request failure.
    pub async fn wait_connections(
        &self,
        command: CommandView,
        timeout: Duration,
    ) -> Result<ConnectionsResult, ClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut current = command;
        loop {
            let result: ConnectionsResult = current
                .result
                .clone()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| ClientError::Unexpected(format!("a connections result: {e}")))?
                .unwrap_or_default();
            if result.finished() {
                return Ok(result);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClientError::Timeout {
                    command_id: current.command_id,
                });
            }
            tokio::time::sleep(self.poll).await;
            current = self.command(&current.command_id).await?;
        }
    }

    /// `POST /api/v1/runs/{run}/cancel`: the command.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn cancel(&self, run_id: &str) -> Result<CommandView, ClientError> {
        self.call(
            Method::Post,
            format!("/api/v1/runs/{}/cancel", path(run_id)),
            None,
        )
        .await
    }

    /// `POST /api/v1/runs/{run}/tasks/{task}/{action}` (`cancel` or `retry`): the command.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn task_action(
        &self,
        run_id: &str,
        (task_id, map_index): (&str, Option<u32>),
        action: TaskAction,
    ) -> Result<CommandView, ClientError> {
        let action = match action {
            TaskAction::Cancel => "cancel",
            TaskAction::Retry => "retry",
        };
        self.call(
            Method::Post,
            format!(
                "/api/v1/runs/{}/tasks/{}/{action}{}",
                path(run_id),
                path(task_id),
                instance(map_index, '?')
            ),
            None,
        )
        .await
    }

    /// `POST /api/v1/workers/{worker}/{action}` (`drain` or `resume`): the command.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn worker_action(
        &self,
        worker_id: &str,
        action: WorkerAction,
    ) -> Result<CommandView, ClientError> {
        let action = match action {
            WorkerAction::Drain => "drain",
            WorkerAction::Resume => "resume",
        };
        self.call(
            Method::Post,
            format!("/api/v1/workers/{}/{action}", path(worker_id)),
            None,
        )
        .await
    }

    /// [`Self::submit`] under a new key: one command, stored once however often it is sent
    /// (boxed: its future stays small in the caller's). For one-shot callers (the CLI), whose
    /// key is gone once they return.
    ///
    /// # Errors
    ///
    /// As [`Self::submit`], but [`ClientError::Unanswered`] when no answer came: the command
    /// may have arrived.
    pub async fn send_command<F>(&self, request: impl Fn() -> F) -> Result<CommandView, ClientError>
    where
        F: Future<Output = Result<CommandView, ClientError>>,
    {
        match Box::pin(self.submit(new_command_key(), request)).await {
            Err(
                error @ (ClientError::Connect { .. } | ClientError::Server { status: 500.., .. }),
            ) => Err(ClientError::Unanswered {
                reason: error.to_string(),
            }),
            other => other,
        }
    }

    /// Sends a command with `request`, stored once (B140): under `key` (the client's
    /// `Idempotency-Key`; [`new_command_key`] for a new one), and sent again with the same key,
    /// at most [`COMMAND_TRIES`] times in all, while no answer comes or the server fails (it may
    /// have stored it): the webserver gives the command stored the first time.
    ///
    /// # Errors
    ///
    /// The last try's [`ClientError`], or the first that isn't worth another try (the server
    /// refused the command).
    pub async fn submit<F>(
        &self,
        key: String,
        request: impl Fn() -> F,
    ) -> Result<CommandView, ClientError>
    where
        F: Future<Output = Result<CommandView, ClientError>>,
    {
        let mut delay = self.poll;
        let mut tries = 1;
        loop {
            let sent = with_command_key(key.clone(), request()).await;
            let again = match &sent {
                Err(ClientError::Connect { .. }) => true,
                Err(ClientError::Server { status, .. }) => *status >= 500,
                _ => false,
            };
            if !again || tries >= COMMAND_TRIES {
                return sent;
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2);
            tries += 1;
        }
    }

    /// Reads `command` back until the master decides it.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rejected`] with the master's reason, [`ClientError::Timeout`] if it
    /// stays pending, or a request failure.
    pub async fn wait(&self, command: CommandView) -> Result<CommandView, ClientError> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let mut current = command;
        loop {
            match current.status.as_str() {
                "applied" => return Ok(current),
                "rejected" => {
                    return Err(ClientError::Rejected(current.reason.unwrap_or_default()));
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClientError::Timeout {
                    command_id: current.command_id,
                });
            }
            tokio::time::sleep(self.poll).await;
            current = self
                .call(
                    Method::Get,
                    format!("/api/v1/commands/{}", path(&current.command_id)),
                    None,
                )
                .await?;
        }
    }

    async fn call<R: DeserializeOwned>(
        &self,
        method: Method,
        path: String,
        body: Option<serde_json::Value>,
    ) -> Result<R, ClientError> {
        let key = match method {
            Method::Post => COMMAND_KEY.try_with(Clone::clone).ok(),
            Method::Get => None,
        };
        let response = self
            .transport
            .send(ApiRequest {
                method,
                path,
                body,
                key,
            })
            .await?;
        if !(200..300).contains(&response.status) {
            // Masked: an error may quote an address with its password (as `connect_error`).
            let message = response.body["error"].as_str().map_or_else(
                || format!("the server answered {}", response.status),
                schedr_config::masked::mask_text,
            );
            return Err(ClientError::Server {
                status: response.status,
                message,
            });
        }
        serde_json::from_value(response.body).map_err(|e| ClientError::Unexpected(e.to_string()))
    }
}

fn to_body(value: &impl serde::Serialize) -> Result<serde_json::Value, ClientError> {
    serde_json::to_value(value).map_err(|e| ClientError::Unexpected(e.to_string()))
}

/// `segment` as one URL path segment: everything but letters, digits and `-._~`
/// percent-encoded (a DAG ID may hold `/`, `:` and `#`).
#[must_use]
pub fn path_segment(segment: &str) -> String {
    path(segment)
}

fn path(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// What `schedr workers drain` / `resume` asks of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerAction {
    /// Drain it for maintenance.
    Drain,
    /// Let it take tasks again.
    Resume,
}

/// What `schedr tasks` asks of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAction {
    /// Cancel it.
    Cancel,
    /// Retry it (an `unschedulable` one).
    Retry,
}

/// The query parameter naming a mapped task's instance (`?map_index=N`, or with `&` after
/// other parameters); empty for none.
fn instance(map_index: Option<u32>, separator: char) -> String {
    map_index.map_or_else(String::new, |i| format!("{separator}map_index={i}"))
}
