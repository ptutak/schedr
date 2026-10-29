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
    AllRunsQuery, AttemptList, CommandList, CommandView, DagList, DagView, ImportErrorList,
    LogChunk, OutputView, PauseRequest, RunDetail, RunList, TriggerRequest, WorkerList,
};
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

/// The person the current request is made for, if [`USER`] is set.
fn current_user() -> Option<String> {
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
        let response = builder.send().await.map_err(|e| ClientError::Connect {
            server: self.server.clone(),
            reason: e.to_string(),
        })?;
        let status = response.status().as_u16();
        let bytes = response.bytes().await.map_err(|e| ClientError::Connect {
            server: self.server.clone(),
            reason: e.to_string(),
        })?;
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
        let connect = |e: reqwest::Error| ClientError::Connect {
            server: self.server.clone(),
            reason: e.to_string(),
        };
        let mut response = builder.send().await.map_err(connect)?;
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
                let chunk = match response.chunk().await {
                    Ok(Some(chunk)) => Ok(chunk.to_vec()),
                    Ok(None) => return,
                    Err(e) => Err(ClientError::Connect {
                        server: server.clone(),
                        reason: e.to_string(),
                    }),
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

/// An HTTP client presenting `tls`'s certificate (its `api_client` configuration).
fn mtls_client(tls: &schedr_tls::Tls) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
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
        let http = in_process_request(
            axum::http::Request::builder()
                .method(method)
                .uri(&request.path)
                .header("content-type", "application/json"),
        )
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
            while let Some(frame) = body.frame().await {
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

    /// `GET /api/v1/import-errors`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn import_errors(&self) -> Result<ImportErrorList, ClientError> {
        self.call(Method::Get, "/api/v1/import-errors".to_owned(), None)
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
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/output",
                path(run_id),
                path(task_id)
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
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/attempts",
                path(run_id),
                path(task_id)
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
        self.call(
            Method::Get,
            format!(
                "/api/v1/runs/{}/tasks/{}/attempts/{try_number}/log?from={from}&limit={limit}",
                path(run_id),
                path(task_id)
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

    /// `POST /api/v1/dags/{dag}/runs`: the command.
    ///
    /// # Errors
    ///
    /// [`ClientError`] if the request fails or the answer doesn't read.
    pub async fn trigger(
        &self,
        dag: &str,
        planned_for: Option<Timestamp>,
    ) -> Result<CommandView, ClientError> {
        let body = to_body(&TriggerRequest { planned_for })?;
        self.call(
            Method::Post,
            format!("/api/v1/dags/{}/runs", path(dag)),
            Some(body),
        )
        .await
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
        task_id: &str,
        action: TaskAction,
    ) -> Result<CommandView, ClientError> {
        let action = match action {
            TaskAction::Cancel => "cancel",
            TaskAction::Retry => "retry",
        };
        self.call(
            Method::Post,
            format!(
                "/api/v1/runs/{}/tasks/{}/{action}",
                path(run_id),
                path(task_id)
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
        let response = self
            .transport
            .send(ApiRequest { method, path, body })
            .await?;
        if !(200..300).contains(&response.status) {
            let message = response.body["error"].as_str().map_or_else(
                || format!("the server answered {}", response.status),
                str::to_owned,
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
