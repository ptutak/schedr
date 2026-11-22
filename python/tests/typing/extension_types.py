"""What a plugin author writes, checked by mypy --strict and basedpyright (`make lint`): plain
dict literals and typed declarations alike pass `main` (agy's review of M5.2i7: a list of plain
dicts infers `dict[str, object]` and was refused)."""

from schedr.extension import (
    ExtensionChannel,
    ExtensionJson,
    ExtensionRequest,
    MetricDeclaration,
    PanelDeclaration,
    main,
)

PLAIN_METRICS = [
    {"name": "jobs", "kind": "counter", "help": "Jobs", "labels": [{"name": "outcome"}]},
]
PLAIN_PANELS = [{"title": "Jobs", "metric": "jobs", "chart": "rate", "row": 1, "cell": "full"}]

TYPED_METRICS: list[MetricDeclaration] = [
    {"name": "jobs", "kind": "counter", "help": "Jobs", "labels": [{"name": "outcome"}]},
]
TYPED_PANELS: list[PanelDeclaration] = [
    {
        "title": "Jobs",
        "metric": "jobs",
        "chart": "rate",
        "row": 1,
        "cell": "full",
        "combine": "max",
    },
]


async def run(request: ExtensionRequest, channel: ExtensionChannel) -> ExtensionJson:
    """Reads its payload with `isinstance` alone: `ExtensionJson` narrows."""
    payload = request.payload
    if isinstance(payload, dict):
        image = payload.get("image")
        if isinstance(image, str):
            await channel.status("pulling", image)
            return {"image": image}
    return None


def plain() -> int:
    """Plain mappings."""
    return main(run, name="x", versions=["1"], metrics=PLAIN_METRICS, panels=PLAIN_PANELS)


def typed() -> int:
    """Typed declarations."""
    return main(run, name="x", versions=["1"], metrics=TYPED_METRICS, panels=TYPED_PANELS)
