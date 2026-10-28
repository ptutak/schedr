//! Who the user is (M3.5; see PROJECT.md, Authentication: as little as possible in schedr).
//! The name is recorded on commands, never used to allow or deny anything; there are no roles.
//!
//! - **A caller proven to be `schedr ui`** passes the person's name on in [`USER_HEADER`]
//!   (decision 36): proven by its mTLS certificate on the webserver's UI port (that listener
//!   marks its requests [`ProvenUi`]), by the token it presents in [`UI_TOKEN_HEADER`] where
//!   there is no mTLS (a mesh, a standalone), or by calling in-process (standalone's own UI).
//!   Only so proven is the name taken; anyone else's [`USER_HEADER`] is ignored.
//! - **Anyone else**, by the mode: `none` takes the identity header's value if there is one
//!   (else `anonymous`); `proxy_header` requires the SSO proxy's header; `basic` checks HTTP
//!   Basic credentials against bcrypt hashes. A request refused is 401 (with a Basic challenge
//!   in `basic` mode).
//!
//! The same rules serve the UI's own pages (`[ui.auth]`), with no UI in front of it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use axum::http::{HeaderMap, HeaderName, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use schedr_config::{AuthMode, AuthSection};

#[cfg(test)]
mod tests;

/// The header `schedr ui` names the person in (taken only from a proven UI).
pub const USER_HEADER: &str = "x-schedr-user";

/// The header `schedr ui` presents its token in, where there is no mTLS.
pub const UI_TOKEN_HEADER: &str = "x-schedr-ui-token";

/// The name recorded when no one is named.
pub const ANONYMOUS: &str = "anonymous";

/// The header an SSO proxy names the user in, when `identity_header` isn't set.
pub const DEFAULT_PROXY_HEADER: &str = "x-forwarded-user";

/// The longest name taken (a longer one is refused, not cut).
const MAX_NAME: usize = 256;

/// A request from `schedr ui`, proven by how it arrived (the UI port's mTLS, or in-process):
/// a request extension, which no client can set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvenUi;

/// Who a request is from (a request extension, set by [`Auth::user`]'s middleware).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User(pub String);

/// A value read from a file, and read again when the file changes: the file is looked at at
/// most every [`RECHECK`] (its modification time and length), and read again when they
/// changed. A file that can't be read or doesn't parse leaves the value as it was (logged as an
/// error: a mistake in the deployment), and is tried again at the next look. Or a fixed value.
#[derive(Debug)]
pub struct Followed<T> {
    file: Option<FollowedFile<T>>,
    /// The value, and how many times it was read again.
    current: RwLock<(Arc<T>, u64)>,
}

#[derive(Debug)]
struct FollowedFile<T> {
    path: PathBuf,
    /// What it holds, for the logs (`users`, `the UI's token`).
    what: &'static str,
    parse: fn(&str) -> Result<T, String>,
    /// When it was last looked at, and how it was when last read.
    looked: Mutex<(Instant, Option<Stamp>)>,
}

/// How often a followed file is looked at for a change, at most.
pub const RECHECK: std::time::Duration = std::time::Duration::from_secs(5);

/// What tells a file changed: its modification time and length.
type Stamp = (Option<std::time::SystemTime>, u64);

fn stamp(path: &Path) -> Option<Stamp> {
    std::fs::metadata(path)
        .ok()
        .map(|meta| (meta.modified().ok(), meta.len()))
}

impl<T: Default> Default for Followed<T> {
    fn default() -> Self {
        Self::fixed(T::default())
    }
}

impl<T> Followed<T> {
    /// A value that doesn't change.
    pub fn fixed(value: T) -> Self {
        Self {
            file: None,
            current: RwLock::new((Arc::new(value), 0)),
        }
    }

    /// The value `parse` makes of the file at `path` (`what` it holds, for the logs), followed.
    ///
    /// # Errors
    ///
    /// Why it can't be read or doesn't parse (the path named).
    pub fn read(
        path: &Path,
        what: &'static str,
        parse: fn(&str) -> Result<T, String>,
    ) -> Result<Self, String> {
        let looked = stamp(path);
        let value = read_file(path)
            .and_then(|text| parse(&text).map_err(|e| format!("{}: {e}", path.display())))?;
        Ok(Self {
            file: Some(FollowedFile {
                path: path.to_owned(),
                what,
                parse,
                looked: Mutex::new((Instant::now(), looked)),
            }),
            current: RwLock::new((Arc::new(value), 0)),
        })
    }

    /// The value as last read, and how many times it was read again (after reading the file
    /// again if it changed).
    pub fn current(&self) -> (Arc<T>, u64) {
        self.follow();
        let current = read(&self.current);
        (Arc::clone(&current.0), current.1)
    }

    /// Reads the file again if it changed since it was last read, at most every [`RECHECK`].
    fn follow(&self) {
        let Some(file) = &self.file else {
            return;
        };
        let now_stamp = {
            let mut looked = lock(&file.looked);
            let now = Instant::now();
            if now.duration_since(looked.0) < RECHECK {
                return;
            }
            // Others don't look meanwhile.
            looked.0 = now;
            let now_stamp = stamp(&file.path);
            if now_stamp == looked.1 {
                return;
            }
            now_stamp
        };
        let path = file.path.display();
        match read_file(&file.path).and_then(|text| (file.parse)(&text)) {
            Ok(value) => {
                {
                    let mut current = self
                        .current
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    *current = (Arc::new(value), current.1 + 1);
                }
                // Only once read: a failed read is tried again at the next look.
                lock(&file.looked).1 = now_stamp;
                tracing::info!("{path} changed: {} read again", file.what);
            }
            // A deployment's mistake, not a way to change anything (an empty users file is).
            Err(error) => tracing::error!(
                %error,
                "{path} changed but doesn't read: {} stay as they were",
                file.what
            ),
        }
    }
}

fn read_file(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("can't read {}: {e}", path.display()))
}

/// The token a UI presents (`[webserver] ui_token_file`, `[[ui.deployments]] token_file`),
/// followed: a new token takes effect without a restart.
pub type UiToken = Arc<Followed<String>>;

/// The shortest token taken.
pub const MIN_UI_TOKEN: usize = 16;

/// The UI's token in the file at `path`, followed (its text, trimmed).
///
/// # Errors
///
/// Why it can't be read, or a token shorter than [`MIN_UI_TOKEN`].
pub fn read_ui_token(path: &Path) -> Result<UiToken, String> {
    Followed::read(path, "the UI's token", |text| {
        let token = text.trim();
        if token.len() < MIN_UI_TOKEN {
            return Err(format!(
                "the token is shorter than {MIN_UI_TOKEN} characters"
            ));
        }
        Ok(token.to_owned())
    })
    .map(Arc::new)
}

/// Users of `basic` mode: names and their bcrypt hashes.
///
/// bcrypt is slow on purpose (about 0.1-0.3 s a check), and without sessions (24A) every
/// request carries the credentials: a check that passed is remembered for [`REMEMBERED`]
/// (live pages make several requests a second), by a keyed hash of the name and password
/// (its key random for each process: never the password itself). A failed check isn't
/// remembered, so guessing pays the full price each time.
///
/// Read from a file, the users follow it ([`Followed`]): a password changed or a user removed
/// takes effect, what was remembered forgotten; an empty file signs no one in.
#[derive(Debug, Clone, Default)]
pub struct Users {
    loaded: Arc<Followed<Loaded>>,
    passed: Arc<Mutex<Passed>>,
}

/// How long a check that passed is remembered.
pub const REMEMBERED: std::time::Duration = std::time::Duration::from_mins(5);

/// At most this many remembered at once (the oldest go first).
const MAX_REMEMBERED: usize = 1_000;

/// The users as read.
#[derive(Debug, Default)]
struct Loaded {
    hashes: HashMap<String, String>,
    /// A real hash at the file's highest cost, verified for an unknown name, so the time taken
    /// doesn't tell which names exist (a malformed one would be refused at once).
    dummy: String,
}

/// Checks that passed: the credentials' keyed hash, and until when.
#[derive(Debug, Default)]
struct Passed {
    key: std::collections::hash_map::RandomState,
    until: HashMap<u64, Instant>,
    /// The users' read these are of: once read again, forgotten.
    of: u64,
}

impl Passed {
    fn digest(&self, name: &str, password: &str) -> u64 {
        use std::hash::BuildHasher as _;
        self.key.hash_one((name, password))
    }

    /// Forgets what was remembered of an earlier read of the users.
    fn of(&mut self, read: u64) -> &mut Self {
        if self.of != read {
            self.until.clear();
            self.of = read;
        }
        self
    }
}

impl PartialEq for Users {
    /// The same users (what was remembered aside).
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.loaded, &other.loaded)
            || self.loaded.current().0.hashes == other.loaded.current().0.hashes
    }
}

impl Eq for Users {}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The users of a text of `name:hash` lines.
fn parse_users(text: &str) -> Result<Loaded, String> {
    let mut hashes = HashMap::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let at = number + 1;
        let Some((name, hash)) = line.split_once(':') else {
            return Err(format!("line {at} isn't `name:bcrypt-hash`"));
        };
        if name.is_empty() || name.len() > MAX_NAME || !printable(name) {
            return Err(format!("line {at}: the name isn't usable"));
        }
        if cost(hash).is_none() {
            return Err(format!("line {at}: `{name}`'s hash isn't bcrypt's"));
        }
        if hashes.insert(name.to_owned(), hash.to_owned()).is_some() {
            return Err(format!("line {at}: `{name}` is there twice"));
        }
    }
    Ok(Loaded::new(hashes))
}

/// A bcrypt hash's cost (`$2b$12$...`: 12; bcrypt takes 4 to 31), if it is one.
fn cost(hash: &str) -> Option<u32> {
    let rest = ["$2a$", "$2b$", "$2y$"]
        .iter()
        .find_map(|prefix| hash.strip_prefix(prefix))?;
    let (cost, _) = rest.split_once('$')?;
    cost.parse().ok().filter(|cost| (4..=31).contains(cost))
}

impl Loaded {
    fn new(hashes: HashMap<String, String>) -> Self {
        // No users: no name to tell apart, every check refused at once.
        let dummy = hashes
            .values()
            .filter_map(|hash| cost(hash))
            .max()
            .and_then(|cost| bcrypt::hash("schedr: no such user", cost).ok())
            .unwrap_or_default();
        Self { hashes, dummy }
    }
}

impl Users {
    /// The users of a text of `name:hash` lines (blank lines and `#` comments skipped).
    ///
    /// # Errors
    ///
    /// Why the text doesn't read: a line without a name or a hash, a hash bcrypt doesn't know,
    /// a name twice.
    pub fn parse(text: &str) -> Result<Self, String> {
        Ok(Self {
            loaded: Arc::new(Followed::fixed(parse_users(text)?)),
            ..Self::default()
        })
    }

    /// The users of the file at `path`, following it (see [`Users`]).
    ///
    /// # Errors
    ///
    /// Why it can't be read or doesn't read.
    pub fn read(path: &Path) -> Result<Self, String> {
        Ok(Self {
            loaded: Arc::new(Followed::read(path, "its users", parse_users)?),
            ..Self::default()
        })
    }

    /// Whether `password` is `name`'s (a check that passed within [`REMEMBERED`] isn't run
    /// again).
    #[must_use]
    pub fn verify(&self, name: &str, password: &str) -> bool {
        let (loaded, read) = self.loaded.current();
        let now = Instant::now();
        let digest = {
            let mut passed = lock(&self.passed);
            let passed = passed.of(read);
            let digest = passed.digest(name, password);
            if passed.until.get(&digest).is_some_and(|until| *until > now) {
                return true;
            }
            digest
        };
        let valid = if let Some(hash) = loaded.hashes.get(name) {
            bcrypt::verify(password, hash).unwrap_or(false)
        } else {
            let _ = bcrypt::verify(password, &loaded.dummy);
            false
        };
        if valid {
            let mut passed = lock(&self.passed);
            // Read again while this was checked: not remembered.
            if passed.of != read {
                return valid;
            }
            passed.until.retain(|_, until| *until > now);
            if passed.until.len() >= MAX_REMEMBERED
                && let Some(oldest) = passed
                    .until
                    .iter()
                    .min_by_key(|(_, until)| **until)
                    .map(|(digest, _)| *digest)
            {
                passed.until.remove(&oldest);
            }
            passed.until.insert(digest, now + REMEMBERED);
        }
        valid
    }

    /// How many there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.loaded.current().0.hashes.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// How a role tells who the user is.
#[derive(Debug, Clone)]
pub struct Auth {
    mode: AuthMode,
    /// `none`: the header recorded if present; `proxy_header`: the required one.
    identity_header: Option<HeaderName>,
    users: Users,
    /// The token a UI may present (`[webserver] ui_token_file`'s).
    ui_token: Option<UiToken>,
}

impl Default for Auth {
    /// `none`, no identity header, no UI token.
    fn default() -> Self {
        Self {
            mode: AuthMode::None,
            identity_header: None,
            users: Users::default(),
            ui_token: None,
        }
    }
}

/// Why a request is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// `proxy_header`: the proxy's header is missing (the request didn't come through it).
    NoProxyHeader(String),
    /// `basic`: no credentials, or wrong ones.
    Credentials,
}

impl Auth {
    /// The rules of `section`, a UI trusted by `ui_token` (empty: none), the users read from
    /// `section.users_file` in `basic` mode.
    ///
    /// # Errors
    ///
    /// Why they can't be used: `basic` without a readable users file, or with no users; a
    /// header name that isn't one.
    pub fn from_config(section: &AuthSection, ui_token: Option<UiToken>) -> Result<Self, String> {
        let header = |name: &str| {
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| format!("`{name}` isn't a header name"))
        };
        let identity_header = match (section.mode, section.identity_header.as_deref()) {
            (AuthMode::ProxyHeader, None) => Some(header(DEFAULT_PROXY_HEADER)?),
            (_, Some(name)) => Some(header(name)?),
            (_, None) => None,
        };
        let users = if section.mode == AuthMode::Basic {
            let path = section
                .users_file
                .as_deref()
                .ok_or("`basic` needs `users_file` (`name:bcrypt-hash` lines)")?;
            let users = Users::read(path)?;
            if users.is_empty() {
                return Err(format!("{}: no users", path.display()));
            }
            users
        } else {
            Users::default()
        };
        Ok(Self {
            mode: section.mode,
            identity_header,
            users,
            ui_token,
        })
    }

    /// `none` mode naming the user by `identity_header` if given (what a webserver did before
    /// M3.5).
    ///
    /// # Panics
    ///
    /// If `identity_header` isn't a header name.
    #[must_use]
    pub fn identity(identity_header: Option<String>) -> Self {
        Self {
            identity_header: identity_header
                .map(|name| HeaderName::try_from(name).expect("a header name")),
            ..Self::default()
        }
    }

    /// The same rules without the UI's token: for standalone's own pages, which a token
    /// mustn't open without the person's sign-in.
    #[must_use]
    pub fn without_ui_token(&self) -> Self {
        Self {
            ui_token: None,
            ..self.clone()
        }
    }

    /// The mode.
    #[must_use]
    pub const fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Who a request with `headers` (and `proven`: arrived as `schedr ui`'s) is from, or why
    /// it is refused.
    ///
    /// # Errors
    ///
    /// [`Refused`] in `proxy_header` and `basic` modes.
    pub fn user(&self, headers: &HeaderMap, proven: bool) -> Result<String, Refused> {
        let value = |name: &str| {
            single(headers, name)
                .map(str::trim)
                .filter(|v| !v.is_empty() && v.len() <= MAX_NAME && printable(v))
                .map(str::to_owned)
        };
        if proven || self.token_matches(headers) {
            return Ok(value(USER_HEADER).unwrap_or_else(|| ANONYMOUS.to_owned()));
        }
        match self.mode {
            AuthMode::None => Ok(self
                .identity_header
                .as_ref()
                .and_then(|h| value(h.as_str()))
                .unwrap_or_else(|| ANONYMOUS.to_owned())),
            AuthMode::ProxyHeader => {
                let header = self
                    .identity_header
                    .as_ref()
                    .map_or(DEFAULT_PROXY_HEADER, HeaderName::as_str);
                value(header).ok_or_else(|| Refused::NoProxyHeader(header.to_owned()))
            }
            AuthMode::Basic => {
                let (name, password) = basic_credentials(headers).ok_or(Refused::Credentials)?;
                if self.users.verify(&name, &password) {
                    Ok(name)
                } else {
                    Err(Refused::Credentials)
                }
            }
        }
    }

    /// Whether the request presents the UI's token (compared in constant time).
    fn token_matches(&self, headers: &HeaderMap) -> bool {
        let Some(token) = &self.ui_token else {
            return false;
        };
        let (token, _) = token.current();
        // An empty token proves nothing.
        !token.is_empty()
            && single(headers, UI_TOKEN_HEADER)
                .is_some_and(|given| constant_time_eq(given.as_bytes(), token.as_bytes()))
    }

    /// The answer to a refused request: 401, a Basic challenge in `basic` mode; its body
    /// `body(message)`.
    pub fn refusal(&self, refused: &Refused, body: impl FnOnce(&str) -> Response) -> Response {
        let message = match refused {
            Refused::NoProxyHeader(header) => format!(
                "not signed in: the `{header}` header the sign-in proxy sets is missing (reach \
                 schedr through the proxy)"
            ),
            Refused::Credentials => "not signed in: user name and password needed".to_owned(),
        };
        let mut response = body(&message);
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        if self.mode == AuthMode::Basic {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                header::HeaderValue::from_static("Basic realm=\"schedr\", charset=\"UTF-8\""),
            );
        }
        response
    }
}

/// The user name and password of an `Authorization: Basic` header.
#[must_use]
pub fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = single(headers, header::AUTHORIZATION.as_str())?;
    let (scheme, encoded) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (name, password) = text.split_once(':')?;
    Some((name.to_owned(), password.to_owned()))
}

/// A header's value when it is there once (sent twice, say a client's and a proxy's after it,
/// neither is taken).
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    value.to_str().ok()
}

/// Whether `given` is `expected`, in time depending only on `expected`'s length (every byte of
/// it compared, whatever `given`'s length).
fn constant_time_eq(given: &[u8], expected: &[u8]) -> bool {
    let differs = expected.iter().enumerate().fold(
        u8::from(given.len() != expected.len()),
        |acc, (at, byte)| acc | (byte ^ given.get(at).copied().unwrap_or(0)),
    );
    differs == 0
}

/// Whether a browser says a request comes from the page's own origin: `Sec-Fetch-Site`
/// (`same-origin`, or `none`: typed by the user), else an `Origin` whose host is the request's
/// (`X-Forwarded-Host` behind a proxy; default ports aside). `None` when neither header is
/// there: not a browser's request (a CLI, `schedr ui`), or an old browser's GET.
#[must_use]
pub fn browser_same_origin(headers: &HeaderMap) -> Option<bool> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(site) = header("sec-fetch-site") {
        return Some(site == "same-origin" || site == "none");
    }
    let origin = header(header::ORIGIN.as_str())?;
    let host = header("x-forwarded-host").or_else(|| header(header::HOST.as_str()));
    let (scheme, origin_host) = origin.split_once("://").unwrap_or(("", origin));
    let without_default_port = |host: &str| {
        let default = match scheme {
            "https" => ":443",
            "http" => ":80",
            _ => return host.to_ascii_lowercase(),
        };
        host.strip_suffix(default)
            .unwrap_or(host)
            .to_ascii_lowercase()
    };
    Some(host.is_some_and(|host| {
        without_default_port(host) == without_default_port(origin_host.trim_end_matches('/'))
    }))
}

/// The webserver's middleware refusing (403) a write a browser sends from another site: a
/// browser attaches Basic credentials it remembers, or a proxy's cookie, to any site's form,
/// so a page elsewhere could act as the person signed in. Requests no browser header marks
/// (the CLI, `schedr ui`) pass.
pub async fn refuse_cross_site(
    request: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    if !request.method().is_safe() && browser_same_origin(request.headers()) == Some(false) {
        return crate::error::ApiError::Forbidden(
            "refused: a write from another site's page (`Sec-Fetch-Site` or `Origin`)".to_owned(),
        )
        .into_response();
    }
    next.run(request).await
}

/// Only printable characters (no control characters to forge a log line or a cell).
fn printable(text: &str) -> bool {
    !text.chars().any(char::is_control)
}

/// Whether a request for `path` is open to anyone: the API's description and Swagger UI
/// (they show what the API is, nothing of its data; decision 2.11a).
#[must_use]
pub fn is_open(path: &str) -> bool {
    path == crate::OPENAPI_PATH || path == crate::DOCS_PATH || path.starts_with("/api/v1/docs/")
}

/// The webserver's middleware: the request's [`User`] set, or a 401.
pub async fn require(
    axum::extract::State(state): axum::extract::State<crate::ApiState>,
    mut request: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    if is_open(request.uri().path()) {
        return next.run(request).await;
    }
    let proven = request.extensions().get::<ProvenUi>().is_some();
    match state.auth.user(request.headers(), proven) {
        Ok(user) => {
            request.extensions_mut().insert(User(user));
            next.run(request).await
        }
        Err(refused) => state.auth.refusal(&refused, |message| {
            crate::error::ApiError::Unauthorized(message.to_owned()).into_response()
        }),
    }
}
