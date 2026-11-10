//! `POST /api/v1/events` and `GET /api/v1/events` (decision 354): events posted by outside
//! systems, which `EventNamed` triggers and waiting steps act on. Posting needs what triggering a
//! run needs: a signed-in user or token (schedr has no permissions of its own).
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Extension, body::Bytes};
use schedr_db::named_events::{NamedEvent, NewNamedEvent, Posted};
use schedr_model::EventName;
use schedr_model::trigger::check_event_key;

use crate::auth::User;
use crate::error::ApiError;
use crate::routes::ApiResult;
use crate::routes::RequestKey;
use crate::state::ApiState;
use crate::views::{NamedEventList, NamedEventView, PostEventRequest};

/// `[events] max_size` unless set: 1 MiB.
pub const DEFAULT_MAX_EVENT_SIZE: usize = 1024 * 1024;

/// How far ahead of now an event's `at` may be, for clock skew: as the master's `--for`.
const AT_TOLERANCE: jiff::SignedDuration = jiff::SignedDuration::from_mins(1);

/// What a post's body may hold besides its data: the name, the key (1 KiB), the time, the JSON.
pub const EVENT_BODY_ROOM: usize = 16 * 1024;

/// How many events one list gives, by default and at most.
const DEFAULT_EVENTS: u32 = 50;
const MAX_EVENTS: u32 = 500;

impl From<&NamedEvent> for NamedEventView {
    fn from(event: &NamedEvent) -> Self {
        let mut data = event.data.clone();
        // Masked when read, as an output is (decision 131): what a run reads is the stored one.
        schedr_config::masked::mask_data(&mut data);
        Self {
            id: event.id.clone(),
            name: event.name.clone(),
            key: schedr_config::masked::mask_text(&event.key),
            data,
            at: event.at,
            posted_at: event.posted_at,
            posted_by: event.posted_by.clone(),
        }
    }
}

/// Post an event: each post is one, a repeat of a name and key too; a trigger starts one run per
/// key, a waiting step matches one posted since its run started. Sent again with the same
/// `Idempotency-Key`, it is the event stored the first time.
#[utoipa::path(post, path = "/api/v1/events", tag = "events",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the event stored the first time")),
    request_body = PostEventRequest,
    responses((status = 201, body = NamedEventView), (status = 200, body = NamedEventView,
        description = "Sent again with its key: the event stored then"),
        (status = 400, body = crate::views::ErrorBody), (status = 413, body = crate::views::ErrorBody),
        (status = 422, body = crate::views::ErrorBody)))]
pub async fn post_event(
    State(state): State<ApiState>,
    Extension(user): Extension<User>,
    key: RequestKey,
    body: Bytes,
) -> Result<(StatusCode, Json<NamedEventView>), ApiError> {
    let request: PostEventRequest = crate::routes::read_body(&body)
        .map_err(|why| ApiError::BadRequest(format!("the event: {why}")))?;
    let name = EventName::new(&request.name)
        .map_err(|error| ApiError::BadRequest(format!("name: {error}")))?;
    check_event_key(&request.key).map_err(|why| ApiError::BadRequest(format!("key: {why}")))?;
    if schedr_model::params::nul_at(&request.data).is_some() {
        return Err(ApiError::BadRequest(
            "data: text containing NUL (\\u0000) can't be stored".to_owned(),
        ));
    }
    let size = serde_json::to_vec(&request.data).map_or(usize::MAX, |bytes| bytes.len());
    if size > state.max_event_size {
        return Err(ApiError::TooLarge(format!(
            "the event's data is {size} bytes, over the {} taken ([events] max_size)",
            state.max_event_size
        )));
    }
    let now = (state.clock)();
    // As `--for`: a run planned years ahead would start now (Grok's review).
    let ahead = now.checked_add(AT_TOLERANCE);
    if let (Some(at), Ok(ahead)) = (request.at, ahead)
        && at > ahead
    {
        return Err(ApiError::BadRequest(format!(
            "at: {at} is in the future: an event is for when it happened"
        )));
    }
    let event = NewNamedEvent {
        name: name.as_str(),
        key: &request.key,
        data: &request.data,
        at: request.at.unwrap_or(now),
    };
    let posted = state
        .db
        .post_named_event(&event, &user.0, key.0.as_deref(), now)
        .await?;
    let taken = || {
        ApiError::Unprocessable(
            "this `Idempotency-Key` was sent before with another event: use a new key for each \
             event"
                .to_owned(),
        )
    };
    let (status, id) = match posted {
        Posted::New(id) => (StatusCode::CREATED, id),
        Posted::Again(id) => (StatusCode::OK, id),
        Posted::KeyTaken => return Err(taken()),
    };
    let stored = state
        .db
        .named_events_by_id(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no event `{id}`")))?;
    // Sent again with another `at` of its own is another event (Codex's review); without one,
    // a retry's "now" differs and is the first's.
    if status == StatusCode::OK && request.at.is_some_and(|at| at != stored.at) {
        return Err(taken());
    }
    Ok((status, Json(NamedEventView::from(&stored))))
}

/// Which posted events.
#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct EventsQuery {
    /// Only those of this name.
    pub name: Option<String>,
    /// How many, 50 by default and 500 at most.
    pub limit: Option<u32>,
}

/// The events posted, newest first, their data masked.
#[utoipa::path(get, path = "/api/v1/events", tag = "events", params(EventsQuery),
    responses((status = 200, body = NamedEventList)))]
pub async fn list_events(
    State(state): State<ApiState>,
    Query(query): Query<EventsQuery>,
) -> ApiResult<NamedEventList> {
    let limit = query.limit.unwrap_or(DEFAULT_EVENTS).clamp(1, MAX_EVENTS);
    let events = state
        .db
        .named_events(query.name.as_deref(), limit)
        .await?
        .iter()
        .map(NamedEventView::from)
        .collect();
    Ok(Json(NamedEventList { events }))
}

#[cfg(test)]
mod tests;
