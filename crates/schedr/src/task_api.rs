//! The REST API as the master runs it for tasks (M4.11h, the user's B2): the API's own router,
//! in this process, each request marked proven and named after the task that makes it.

use std::future::Future;
use std::pin::Pin;

use axum::body::Body;
use schedr_api::auth::{MAX_NAME, ProvenUi, USER_HEADER};
use schedr_api::{ApiState, router};
use schedr_db::Database;
use schedr_master::core::task_api::{TaskApi, TaskApiRequest, TaskApiResponse};
use sha2::Digest as _;
use tower::ServiceExt as _;

/// The router a master calls for its tasks.
#[derive(Debug, Clone)]
pub struct RouterTaskApi {
    router: axum::Router,
    /// The largest answer read back, larger refused: it goes to the worker in one message.
    max_answer: usize,
}

impl RouterTaskApi {
    /// The API over `db`, taking events of up to `max_event_size` bytes (`[events] max_size`),
    /// giving answers of up to `max_answer` bytes.
    #[must_use]
    pub fn new(db: Database, max_event_size: usize, max_answer: usize) -> Self {
        let state = ApiState::new(db, None).with_max_event_size(max_event_size);
        Self {
            router: router(state),
            max_answer,
        }
    }
}

/// The name a task's requests are made as: its caller, or, past what the API takes as a name
/// (a long DAG and task ID), its start and a hash of the whole, so it is still its own and not
/// `anonymous` (Codex's review of M4.11h).
fn user_name(caller: &str) -> String {
    if caller.len() <= MAX_NAME {
        return caller.to_owned();
    }
    let digest = sha2::Sha256::digest(caller.as_bytes());
    let hash = digest[..12]
        .iter()
        .fold(String::new(), |hash, byte| hash + &format!("{byte:02x}"));
    // IDs are ASCII: any byte is a boundary; room for `~` and the hash.
    let start = caller.get(..MAX_NAME - 1 - hash.len()).unwrap_or_default();
    format!("{start}~{hash}")
}

fn failed(status: u16, why: &str) -> TaskApiResponse {
    TaskApiResponse {
        status,
        body: serde_json::to_vec(&serde_json::json!({ "error": why })).unwrap_or_default(),
    }
}

impl TaskApi for RouterTaskApi {
    fn call(
        &self,
        request: TaskApiRequest,
    ) -> Pin<Box<dyn Future<Output = TaskApiResponse> + Send + '_>> {
        Box::pin(async move {
            let mut builder = axum::http::Request::builder()
                .method(request.method.as_str())
                .uri(request.path.as_str())
                .header(USER_HEADER, user_name(&request.caller));
            if !request.body.is_empty() {
                builder = builder.header("content-type", "application/json");
            }
            if let Some(key) = &request.idempotency_key {
                builder = builder.header("idempotency-key", key.as_str());
            }
            let mut http = match builder.body(Body::from(request.body)) {
                Ok(http) => http,
                Err(error) => return failed(400, &format!("the request doesn't read: {error}")),
            };
            // As `schedr ui`'s port marks its requests: the user is the one named.
            http.extensions_mut().insert(ProvenUi);
            let response = match self.router.clone().oneshot(http).await {
                Ok(response) => response,
                Err(never) => match never {},
            };
            let status = response.status().as_u16();
            match axum::body::to_bytes(response.into_body(), self.max_answer).await {
                Ok(body) => TaskApiResponse {
                    status,
                    body: body.to_vec(),
                },
                Err(_) => failed(
                    502,
                    &format!(
                        "the API's answer is over the {} bytes a worker's message takes ([grpc] \
                         max_message_size): ask for less (`limit=`)",
                        self.max_answer
                    ),
                ),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_caller_keeps_a_name_of_its_own_within_the_limit() {
        let long = |task: &str| format!("task:{}/r1/{task}", "d".repeat(250));
        let (a, b) = (user_name(&long("a")), user_name(&long("b")));
        assert_eq!(a.len(), MAX_NAME);
        assert_ne!(a, b);
        assert_eq!(user_name("task:d/r1/t"), "task:d/r1/t");
    }
}
