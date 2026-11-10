//! A task's REST requests through its agent (M4.11h, the user's B2): the master runs them
//! through the API's own router as the task's identity, on an allow-list of routes, so a task
//! needs no URL or token and reaches nothing else.

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use schedr_proto::pb::{self, master_message};

use super::{CoreError, MasterCore, Outgoing};

/// The routes a task may call, by method and path (a query is allowed after it): the events
/// (the user's, 2026-11-08), whose code is the API's own, and its own notifications (decision
/// 421), which the master answers itself.
pub const TASK_ROUTES: &[(&str, &str)] = &[
    ("POST", "/api/v1/events"),
    ("GET", "/api/v1/events"),
    ("POST", NOTIFICATIONS),
];

/// A task's own notification: `{"channel": ..., "text": ..., "data": ...}`, answered 202 once
/// queued.
pub const NOTIFICATIONS: &str = "/api/v1/notifications";

/// A request a task made, as the router gets it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskApiRequest {
    /// `GET`, `POST`, ...
    pub method: String,
    /// The path and its query.
    pub path: String,
    /// The body, JSON; empty for none.
    pub body: Vec<u8>,
    /// Its `Idempotency-Key`, if any.
    pub idempotency_key: Option<String>,
    /// Who makes it: `task:<dag>/<run>/<task>` or `trigger:<dag>`, recorded as the user.
    pub caller: String,
}

/// What the router answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskApiResponse {
    /// The HTTP status.
    pub status: u16,
    /// The body, JSON.
    pub body: Vec<u8>,
}

/// The API, as the master calls it for tasks: the `schedr` binary gives it the REST router.
pub trait TaskApi: Debug + Send + Sync {
    /// Runs `request` through the API.
    fn call(
        &self,
        request: TaskApiRequest,
    ) -> Pin<Box<dyn Future<Output = TaskApiResponse> + Send + '_>>;
}

/// Whether a task may call `method` on `path` (its query aside).
#[must_use]
pub fn allowed(method: &str, path: &str) -> bool {
    let bare = path.split_once('?').map_or(path, |(bare, _)| bare);
    TASK_ROUTES
        .iter()
        .any(|(m, p)| m.eq_ignore_ascii_case(method) && *p == bare)
}

/// A call checked and ready to run, away from the core's lock (a request may take a while).
#[derive(Debug)]
pub struct ApiJob {
    api: std::sync::Arc<dyn TaskApi>,
    request: TaskApiRequest,
    worker_id: String,
    request_id: u64,
}

impl ApiJob {
    /// Runs the request: the answer for the worker.
    pub async fn run(self) -> Outgoing {
        let response = self.api.call(self.request).await;
        answer(
            &self.worker_id,
            self.request_id,
            response.status,
            response.body,
        )
    }
}

/// What to do with a task's call: answered already (refused), or to run.
#[derive(Debug)]
pub enum ApiCallPlan {
    /// Refused: the answer.
    Answered(Box<Outgoing>),
    /// To run.
    Run(ApiJob),
}

fn answer(worker_id: &str, request_id: u64, status: u16, body: Vec<u8>) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(master_message::Message::ApiAnswer(pb::ApiAnswer {
                request_id,
                status: u32::from(status),
                body_json: body,
            })),
        },
    }
}

/// Why a task's own notification wasn't queued.
#[derive(Debug)]
enum Unsent {
    /// Past `[notifications] max_per_attempt`.
    OverQuota,
    /// The database failed.
    Db(schedr_db::DbError),
}

impl From<schedr_db::DbError> for Unsent {
    fn from(error: schedr_db::DbError) -> Self {
        Self::Db(error)
    }
}

/// The body of a task's own notification.
#[derive(Debug)]
struct NotifyRequest {
    channel: String,
    text: Option<String>,
    data: serde_json::Value,
}

impl NotifyRequest {
    /// `{"channel": "...", "text": "..." | null, "data": ...}`, nothing else.
    fn read(body: &[u8]) -> Result<Self, String> {
        let value: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
        let serde_json::Value::Object(mut map) = value else {
            return Err("it isn't an object".to_owned());
        };
        if let Some(key) = map
            .keys()
            .find(|k| !matches!(k.as_str(), "channel" | "text" | "data"))
        {
            return Err(format!("unknown field `{key}`"));
        }
        let Some(serde_json::Value::String(channel)) = map.remove("channel") else {
            return Err("`channel` is a channel's name".to_owned());
        };
        let text = match map.remove("text") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(text)) => Some(text),
            Some(_) => return Err("`text` is a string".to_owned()),
        };
        let data = map.remove("data").unwrap_or(serde_json::Value::Null);
        Ok(Self {
            channel,
            text,
            data,
        })
    }
}

fn refusal(why: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "error": why })).unwrap_or_default()
}

impl MasterCore {
    /// A task's request through its worker, run here (the core's callers that hold no lock
    /// across it; the hub runs [`Self::plan_api_call`]'s job outside its lock).
    pub(crate) async fn api_call(
        &mut self,
        worker_id: &str,
        call: pb::ApiCall,
        now: jiff::Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        Ok(vec![
            match self.plan_api_call(worker_id, call, now).await? {
                ApiCallPlan::Answered(answer) => *answer,
                ApiCallPlan::Run(job) => job.run().await,
            },
        ])
    }

    /// A task's request through its worker: to run as the task, if the worker holds that
    /// attempt (or trigger) open and the route is on the list; refused otherwise.
    ///
    /// # Errors
    ///
    /// A database failure.
    pub async fn plan_api_call(
        &mut self,
        worker_id: &str,
        call: pb::ApiCall,
        now: jiff::Timestamp,
    ) -> Result<ApiCallPlan, CoreError> {
        let refuse = |status: u16, why: &str| {
            Ok(ApiCallPlan::Answered(Box::new(answer(
                worker_id,
                call.request_id,
                status,
                refusal(why),
            ))))
        };
        if !allowed(&call.method, &call.path) {
            return refuse(
                403,
                &format!(
                    "`{} {}` isn't open to tasks: they may call {}",
                    call.method,
                    call.path,
                    TASK_ROUTES
                        .iter()
                        .map(|(m, p)| format!("`{m} {p}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
        }
        if call
            .path
            .split_once('?')
            .map_or(call.path.as_str(), |(bare, _)| bare)
            == NOTIFICATIONS
        {
            let (status, body) = self.notify_call(worker_id, &call, now).await?;
            return Ok(ApiCallPlan::Answered(Box::new(answer(
                worker_id,
                call.request_id,
                status,
                body,
            ))));
        }
        let Some(caller) = self.api_caller(worker_id, &call).await? else {
            return refuse(403, "no open attempt or trigger of this worker makes it");
        };
        let Some(api) = self.task_api.clone() else {
            return refuse(503, "this master serves no API to tasks");
        };
        let request = TaskApiRequest {
            method: call.method.to_ascii_uppercase(),
            path: call.path.clone(),
            body: call.body_json.clone(),
            idempotency_key: Some(call.idempotency_key.clone()).filter(|key| !key.is_empty()),
            caller,
        };
        Ok(ApiCallPlan::Run(ApiJob {
            api,
            request,
            worker_id: worker_id.to_owned(),
            request_id: call.request_id,
        }))
    }

    /// A task's own notification (decision 421): to a channel of its DAG, or a server one,
    /// queued with its count, at most `[notifications] max_per_attempt` an attempt.
    async fn notify_call(
        &self,
        worker_id: &str,
        call: &pb::ApiCall,
        now: jiff::Timestamp,
    ) -> Result<(u16, Vec<u8>), CoreError> {
        if !call.trigger_dag_id.is_empty() {
            return Ok((
                403,
                refusal("a trigger can't send notifications; a task can"),
            ));
        }
        let attempt = self.db.attempt(&call.attempt_id).await?;
        let Some(attempt) = attempt.filter(|a| a.worker_id == worker_id && a.status.is_none())
        else {
            return Ok((403, refusal("no open attempt of this worker makes it")));
        };
        let request = match NotifyRequest::read(&call.body_json) {
            Ok(request) => request,
            Err(error) => {
                return Ok((
                    400,
                    refusal(&format!(
                        "the body is {{\"channel\": ..., \"text\": ..., \"data\": ...}}: {error}"
                    )),
                ));
            }
        };
        let Some(run) = self.db.run_tasks(&attempt.run_id).await? else {
            return Ok((404, refusal("the attempt's run is gone")));
        };
        let task = if attempt.map_index >= 0 {
            format!("{}[{}]", attempt.task_id, attempt.map_index)
        } else {
            attempt.task_id.to_string()
        };
        let mut event = crate::notifications::Event::new(&run.dag, now);
        event.run_id = Some(&attempt.run_id);
        event.task = Some(&task);
        event.attempt = Some(attempt.try_number);
        event.state = Some("running");
        event.text = request.text;
        event.data = request.data;
        event.planned_for = self
            .db
            .run_record(&attempt.run_id)
            .await?
            .map(|r| r.planned_for);
        let queued = match self.notifications.message(&event, &request.channel) {
            Ok(queued) => queued,
            Err(why) => return Ok((400, refusal(&why))),
        };
        let max = self.notifications.max_per_attempt;
        let db = &self.db;
        let key = call.idempotency_key.as_str();
        // Over the quota rolls the whole write back, the call's key with it: sent again, it is
        // refused again (not taken for one that was queued).
        let sent = Box::pin(db.atomically(async {
            // Sent again (its agent reconnected before the answer): queued already.
            if !key.is_empty() && !db.first_notification_request(&attempt.id, key).await? {
                return Ok(());
            }
            if !db.count_attempt_notification(&attempt.id, max).await? {
                return Err(Unsent::OverQuota);
            }
            db.queue_notifications(std::slice::from_ref(&queued), now)
                .await?;
            Ok(())
        }))
        .await;
        let counted = match sent {
            Ok(()) => true,
            Err(Unsent::OverQuota) => false,
            Err(Unsent::Db(error)) => return Err(error.into()),
        };
        Ok(if counted {
            (
                202,
                serde_json::to_vec(&serde_json::json!({"queued": true})).unwrap_or_default(),
            )
        } else {
            (
                429,
                refusal(&format!(
                    "this attempt has sent {max} notifications, the most it may \
                     ([notifications] max_per_attempt)"
                )),
            )
        })
    }

    /// Who makes `call`: the attempt it names, open on this worker, or its trigger instance.
    async fn api_caller(
        &self,
        worker_id: &str,
        call: &pb::ApiCall,
    ) -> Result<Option<String>, CoreError> {
        if !call.trigger_dag_id.is_empty() {
            let record = self.db.trigger(&call.trigger_dag_id).await?;
            let holds = record.is_some_and(|record| {
                record.instance_id.as_deref() == Some(call.attempt_id.as_str())
                    && record.worker_id.as_deref() == Some(worker_id)
            });
            return Ok(holds.then(|| format!("trigger:{}", call.trigger_dag_id)));
        }
        let Some(attempt) = self.db.attempt(&call.attempt_id).await? else {
            return Ok(None);
        };
        if attempt.worker_id != worker_id || attempt.status.is_some() {
            return Ok(None);
        }
        let dag = self
            .db
            .run_record(&attempt.run_id)
            .await?
            .map(|run| run.dag_id.to_string())
            .unwrap_or_default();
        let instance = if attempt.map_index < 0 {
            String::new()
        } else {
            format!("[{}]", attempt.map_index)
        };
        Ok(Some(format!(
            "task:{dag}/{}/{}{instance}",
            attempt.run_id, attempt.task_id
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::allowed;

    #[test]
    fn only_the_listed_routes_are_open_to_tasks() {
        assert!(allowed("POST", "/api/v1/events"));
        assert!(allowed("get", "/api/v1/events?name=paid&limit=5"));
        assert!(!allowed("DELETE", "/api/v1/events"));
        assert!(!allowed("POST", "/api/v1/dags/sync/pause"));
        assert!(!allowed("GET", "/api/v1/events/../dags"));
        assert!(!allowed("GET", "/api/v1/events/"));
        assert!(allowed("POST", "/api/v1/notifications"));
        assert!(!allowed("GET", "/api/v1/notifications"));
    }
}
