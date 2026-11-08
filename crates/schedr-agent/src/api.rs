//! A task's requests to schedr's REST API (M4.11h, the user's B2): sent to the master over the
//! agent's stream, which runs them as the task; resent on a new stream until answered, for at
//! most [`API_CALL_LIMIT`].

use std::sync::Arc;
use std::time::Duration;

use schedr_proto::pb;
use tokio::sync::oneshot;

/// How long a call waits for its answer, over reconnects, before it is given up.
pub const API_CALL_LIMIT: Duration = Duration::from_mins(1);

/// Sends a call on to the master (through the agent's loop); the answer comes on the receiver.
pub type ApiSend = Arc<dyn Fn(pb::ApiCall) -> oneshot::Receiver<pb::ApiAnswer> + Send + Sync>;

/// What an attempt or a trigger calls the API with.
#[derive(Clone)]
pub struct ApiCaller {
    attempt_id: String,
    trigger_dag_id: String,
    send: ApiSend,
}

impl std::fmt::Debug for ApiCaller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiCaller")
            .field("attempt_id", &self.attempt_id)
            .field("trigger_dag_id", &self.trigger_dag_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ApiCaller {
    fn eq(&self, other: &Self) -> bool {
        self.attempt_id == other.attempt_id && self.trigger_dag_id == other.trigger_dag_id
    }
}

/// An API call's request, as a task makes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRequest {
    /// `GET`, `POST`, ...
    pub method: String,
    /// The path and its query.
    pub path: String,
    /// The body, JSON.
    pub body: Option<serde_json::Value>,
    /// Its `Idempotency-Key`.
    pub idempotency_key: Option<String>,
    /// Whether [`ApiRequest::masked_by`] changed it: the task is told (B189's rule).
    pub masked: bool,
}

impl ApiRequest {
    /// The request with what the task read of its secrets masked in its body, as an output is:
    /// what the API stores is shown to people (Codex's review of M4.11h).
    #[must_use]
    pub fn masked_by(mut self, secrets: Option<&crate::secrets::AttemptSecrets>) -> Self {
        let Some(mask) = secrets
            .map(|secrets| &secrets.mask)
            .filter(|mask| !mask.is_empty())
        else {
            return self;
        };
        if let Some(body) = &self.body {
            let masked = mask.mask_json(body);
            self.masked |= &masked != body;
            self.body = Some(masked);
        }
        // Stored too, as `request_key` (Grok's review).
        if let Some(key) = &self.idempotency_key {
            let masked = mask.mask_text(key);
            self.masked |= &masked != key;
            self.idempotency_key = Some(masked);
        }
        self
    }

    /// What the task is told of its request, if anything: that a secret in it was masked.
    #[must_use]
    pub fn note(&self) -> Option<String> {
        self.masked.then(|| MASKED_REQUEST.to_owned())
    }
}

impl ApiCaller {
    /// The caller of attempt `attempt_id`.
    #[must_use]
    pub fn of_attempt(attempt_id: &str, send: ApiSend) -> Self {
        Self {
            attempt_id: attempt_id.to_owned(),
            trigger_dag_id: String::new(),
            send,
        }
    }

    /// The caller of trigger instance `instance_id` of DAG `dag_id`.
    #[must_use]
    pub fn of_trigger(instance_id: &str, dag_id: &str, send: ApiSend) -> Self {
        Self {
            attempt_id: instance_id.to_owned(),
            trigger_dag_id: dag_id.to_owned(),
            send,
        }
    }

    /// Makes the call: the status and body the API answered, or 503 when no answer came in
    /// time (the agent away from its master that long).
    pub async fn call(&self, request: ApiRequest) -> (u16, serde_json::Value) {
        self.call_within(request, API_CALL_LIMIT).await
    }

    async fn call_within(&self, request: ApiRequest, limit: Duration) -> (u16, serde_json::Value) {
        // Sent again after a reconnect, or by the task after a 503 with the key it is told, a
        // call is still one (Codex's and Grok's reviews): the caller's own key, or this one.
        let key = request.idempotency_key.unwrap_or_else(new_call_key);
        let call = pb::ApiCall {
            request_id: 0,
            attempt_id: self.attempt_id.clone(),
            trigger_dag_id: self.trigger_dag_id.clone(),
            method: request.method,
            path: request.path,
            body_json: request
                .body
                .map(|body| serde_json::to_vec(&body).unwrap_or_default())
                .unwrap_or_default(),
            idempotency_key: key.clone(),
        };
        let answer = (self.send)(call);
        match tokio::time::timeout(limit, answer).await {
            Ok(Ok(answer)) => {
                let body = if answer.body_json.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&answer.body_json).unwrap_or_else(
                        |_| serde_json::json!({"error": "the API's answer doesn't read"}),
                    )
                };
                (u16::try_from(answer.status).unwrap_or(500), body)
            }
            Ok(Err(_)) => {
                unavailable_with_key("the agent stopped before the master answered", &key)
            }
            Err(_) => unavailable_with_key(
                &format!(
                    "no answer from the master within {} s: the call may still be made; send it \
                     again with Idempotency-Key `{key}`",
                    limit.as_secs()
                ),
                &key,
            ),
        }
    }
}

/// A runner's [`schedr_model::runner::RunnerMessage::ApiCall`] answered: through `api`, or
/// refused where there is none.
pub async fn runner_answer(
    api: Option<&ApiCaller>,
    request: ApiRequest,
) -> schedr_model::runner::AgentMessage {
    let note = request.note();
    let (status, body) = match api {
        Some(api) => api.call(request).await,
        None => unavailable(NO_MASTER),
    };
    schedr_model::runner::AgentMessage::ApiAnswer { status, body, note }
}

/// What a task is told when its request held a secret it read (as a trigger's log is, B189).
pub const MASKED_REQUEST: &str = "schedr: a request to the API held a secret this task read: it \
                                  was sent masked (****), as an output is; event keys differing \
                                  only in secrets count as one key";

/// Why a task run with no master (`schedr dags test`) can't call the API.
pub const NO_MASTER: &str = "this task runs without a master (`schedr dags test`, a local run): \
                             schedr's API isn't reachable from it; in a test, \
                             `schedr.testing.task_context` posts to a fake";

/// A key of a call's own, for one made without: unique in this process (the clock and a count).
fn new_call_key() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    format!("schedr-call-{now:x}-{n}")
}

/// A 503 naming the key the call was made with, to send it again with.
fn unavailable_with_key(why: &str, key: &str) -> (u16, serde_json::Value) {
    (
        503,
        serde_json::json!({ "error": why, "idempotency_key": key }),
    )
}

fn unavailable(why: &str) -> (u16, serde_json::Value) {
    (503, serde_json::json!({ "error": why }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller(answer: Option<pb::ApiAnswer>) -> ApiCaller {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = Arc::new(std::sync::Mutex::new(Vec::new()));
        ApiCaller::of_attempt(
            "a1",
            Arc::new(move |call| {
                sent.lock().unwrap().push(call);
                let (tx, rx) = oneshot::channel();
                match &answer {
                    Some(answer) => {
                        let _ = tx.send(answer.clone());
                    }
                    // Kept unanswered, as a master away.
                    None => kept.lock().unwrap().push(tx),
                }
                rx
            }),
        )
    }

    fn post() -> ApiRequest {
        ApiRequest {
            method: "POST".to_owned(),
            path: "/api/v1/events".to_owned(),
            body: Some(serde_json::json!({"name": "paid", "key": "o-1"})),
            idempotency_key: Some("k".to_owned()),
            masked: false,
        }
    }

    #[tokio::test]
    async fn a_call_gives_what_the_master_answered() {
        let answer = pb::ApiAnswer {
            request_id: 1,
            status: 201,
            body_json: br#"{"id": "e1", "new": true}"#.to_vec(),
        };
        let (status, body) = caller(Some(answer)).call(post()).await;
        assert_eq!(status, 201);
        assert_eq!(body["id"], "e1");
    }

    #[tokio::test]
    async fn a_call_unanswered_in_time_is_unavailable() {
        let (status, body) = caller(None)
            .call_within(post(), Duration::from_millis(20))
            .await;
        assert_eq!(status, 503);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("Idempotency-Key `k`")
        );
        assert_eq!(body["idempotency_key"], "k");
        // One made without a key is given one, and told it (Grok's review).
        let keyless = ApiRequest {
            idempotency_key: None,
            ..post()
        };
        let (_, body) = caller(None)
            .call_within(keyless, Duration::from_millis(20))
            .await;
        assert!(
            body["idempotency_key"]
                .as_str()
                .unwrap()
                .starts_with("schedr-call-"),
            "{body}"
        );
    }
}
