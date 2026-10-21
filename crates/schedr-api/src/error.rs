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
            Self::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let error = if let Self::Db(error) = &self {
            tracing::error!(%error, "API request failed");
            "the database failed; see the webserver's log".to_owned()
        } else {
            self.to_string()
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}
