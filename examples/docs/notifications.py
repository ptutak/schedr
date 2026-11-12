"""Notification bodies for a relay and an email service."""

from schedr import Dag, Notify, WebhookChannel, notify, task

relay = WebhookChannel(
    "relay",
    url="https://relay.example.com/schedr",
    body={"text": t"{notify.dag}: {notify.event}", "failures": notify.failed_tasks},
)
plain_email = WebhookChannel(
    "plain-email",
    url="https://relay.example.com/mail/send",
    body={
        "to": ["ops@example.com"],
        "subject": t"{notify.dag}: {notify.event}",
        "text": notify.text,
    },
)
template_email = WebhookChannel(
    "template-email",
    url="https://relay.example.com/mail/template",
    body={
        "to": ["ops@example.com"],
        "template_id": "run-alert",
        "variables": {"dag": notify.dag, "message": notify.text},
    },
)


@task
def prepare() -> str:
    return "ready"


with Dag(
    "notifications_demo",
    description="Sends notifications to Slack and email when a run fails or recovers.",
    notify={
        "ops-slack": [Notify.RUN_FAILED, Notify.RUN_RECOVERED],
        relay: [Notify.TASK_FAILED],
        plain_email: [Notify.RUN_FAILED],
        template_email: [Notify.RUN_FAILED],
    },
):
    prepare()
