//! Versions between clients and the server (see PROJECT.md, Web UI and CLI: compatibility).
//!
//! Every response carries `Schedr-Server-Version`. A client sending `Schedr-Client-Version`
//! with another **major** version is refused, with what to install; any minor version works
//! (the API changes only additively within a major). Clients that send no version (scripts,
//! generated clients) are served.

use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use schedr_model::SchedrVersion;

use crate::error::ApiError;
use crate::state::ApiState;

#[cfg(test)]
mod tests;

/// The header a client sends its version in.
pub const CLIENT_VERSION_HEADER: &str = "schedr-client-version";

/// The header every response carries the server's version in.
pub const SERVER_VERSION_HEADER: &str = "schedr-server-version";

/// Checks a client's version against the server's.
///
/// # Errors
///
/// A message for a version that doesn't parse or has another major version.
pub fn check_client_version(server: SchedrVersion, client: Option<&str>) -> Result<(), String> {
    let Some(text) = client else {
        return Ok(());
    };
    let client: SchedrVersion = text
        .parse()
        .map_err(|_| format!("`{CLIENT_VERSION_HEADER}: {text}` isn't a schedr version"))?;
    if client.major != server.major {
        let major = server.major;
        return Err(format!(
            "the server is schedr {server}, this client is schedr {client}: \
             `pip install 'schedr>={major},<{}'`",
            major + 1
        ));
    }
    Ok(())
}

/// Middleware: refuses clients of another major version and adds the server's version.
pub async fn versions(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    let client = request
        .headers()
        .get(CLIENT_VERSION_HEADER)
        .and_then(|v| v.to_str().ok().map(str::to_owned));
    let mut response = match check_client_version(state.version, client.as_deref()) {
        Ok(()) => next.run(request).await,
        Err(message) => ApiError::BadRequest(message).into_response(),
    };
    if let Ok(value) = HeaderValue::from_str(state.version_text) {
        response.headers_mut().insert(SERVER_VERSION_HEADER, value);
    }
    response
}
