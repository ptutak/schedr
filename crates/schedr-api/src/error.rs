//! Errors of the REST API, answered as JSON (`{"error": "..."}`).

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use schedr_db::DbError;

use crate::views::ErrorBody;

#[cfg(test)]
mod tests;

/// Why a request failed.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The request is malformed: a bad ID, body, query or version.
    #[error("{0}")]
    BadRequest(String),
    /// No such DAG, run or command.
    #[error("{0}")]
    NotFound(String),
    /// Not signed in (see [`crate::auth`]).
    #[error("{0}")]
    Unauthorized(String),
    /// Refused whoever asks (a write from another site's page).
    #[error("{0}")]
    Forbidden(String),
    /// Well-formed, but not to be done: an `Idempotency-Key` sent before with another command.
    #[error("{0}")]
    Unprocessable(String),
    /// Something the request needs can't be reached now (a worker holding a log).
    #[error("{0}")]
    Unavailable(String),
    /// The request's content is over what is taken (a posted event's data).
    #[error("{0}")]
    TooLarge(String),
    /// It was there and was deleted for good (an output `[retention] outputs` removed).
    #[error("{0}")]
    Gone(String),
    /// Something stored doesn't read (a run's DAG version a model change left behind): a 500
    /// that says what, logged too (B89).
    #[error("{0}")]
    Unreadable(String),
    /// The database failed (logged; the client gets a generic message).
    #[error(transparent)]
    Db(#[from] DbError),
}

impl ApiError {
    /// The HTTP status for this error.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Gone(_) => StatusCode::GONE,
            Self::Unreadable(_) | Self::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let error = match &self {
            Self::Db(error) => {
                tracing::error!(%error, "API request failed");
                "the database failed; see the webserver's log".to_owned()
            }
            Self::Unreadable(what) => what.clone(),
            _ => self.to_string(),
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}
