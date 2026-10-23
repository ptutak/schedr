//! The webserver's metrics (see PROJECT.md, Observability), registered in the process's
//! registry and served on the ops port: requests by route template (never a raw path, which
//! holds IDs) and status, and why the log proxy couldn't serve a log.

use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;

use crate::state::ApiState;

#[cfg(test)]
mod tests;

/// A request's route template and its answer's status.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    /// The route template, e.g. `/api/v1/runs/{run_id}`; `unmatched` for a path no route has.
    pub route: String,
    /// The HTTP status.
    pub status: u16,
}

/// Why a log couldn't be served.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct LogErrorLabels {
    /// `missing`, `stale_address`, `unreachable`, `worker_gone` or `no_endpoint`.
    pub kind: String,
}

/// The webserver's metrics (cheap to clone; clones share them).
#[derive(Debug, Clone, Default)]
pub struct ApiMetrics {
    /// Requests by route template and status.
    pub requests: Family<RequestLabels, Counter>,
    /// Logs the proxy couldn't serve, by why.
    pub log_errors: Family<LogErrorLabels, Counter>,
}

impl ApiMetrics {
    /// Registers every metric in `registry` (names get the `schedr_` prefix there).
    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "http_requests",
            "Requests to the REST API, by route template and status",
            self.requests.clone(),
        );
        registry.register(
            "log_proxy_errors",
            "Logs the webserver couldn't read from a worker, by why",
            self.log_errors.clone(),
        );
    }

    /// Counts a log the proxy couldn't serve.
    pub fn log_error(&self, kind: &str) {
        self.log_errors
            .get_or_create(&LogErrorLabels {
                kind: kind.to_owned(),
            })
            .inc();
    }
}

/// Middleware counting each request by its route template and status, those the version check
/// refuses included.
pub async fn count_requests(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let response = next.run(request).await;
    state
        .metrics
        .requests
        .get_or_create(&RequestLabels {
            route,
            status: response.status().as_u16(),
        })
        .inc();
    response
}
