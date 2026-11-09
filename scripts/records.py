#!/usr/bin/env python3
"""The project's records: the plan, the decisions, the open questions, and the views made from them.

Each record is a TOML file; every list or state that would otherwise be copied by hand is
generated from them (decision 368):

- `plan/<milestone>.toml`: a milestone and its steps, in order; renders `PLAN.md`.
- `decisions/D<n>.toml`: one decision; renders `DECISIONS.md`.
- `questions/Q<n>.toml`: one question not decided yet (or since decided); shown in
  `DECISIONS.md` and the topic's design page.
- Generated blocks, between marker lines, in hand-written pages: each `design/<topic>.md`'s
  decisions, code and open questions; `STATUS.md`'s milestone, next step and open bugs.

A milestone, `plan/M4.toml`:

    title = "Native API complete"
    order = 70                  # milestones sorted by it
    done_when = "the native API's whole design is built ..."
    notes = '''
    Prose: how it was planned, what it builds on (Markdown).
    '''

    [[steps]]
    id = "M4.12a"
    title = "TriggerDagOperator"
    scope = '''What the step builds (Markdown).'''
    done_when = "a parent waits on a chained run and routes its Failed; ..."
    status = "planned"          # planned, done, dropped
    done = 2026-11-09           # with status "done": when
    commits = ["1f06df5"]       # optional
    part_of = "M4.12"           # optional: the step this one is a part of

The next step is the first `planned` one without planned parts, in milestone order, then step
order. `plan/README.md` (the plan's rules and lasting notes) opens `PLAN.md`.

A decision, `decisions/D<n>.toml`:

    title = "TriggerDagOperator gives the target run's ID"
    date = 2026-11-08
    by = "user"                 # user, claude
    status = "active"           # active, superseded, dropped, untraced
    superseded_by = 0           # the decision replacing it (status "superseded" only)
    topic = "chained-runs"      # a topic of design/README.md
    also = ["runs"]             # optional: other topics whose design page lists it
    section = "Waiting"         # optional: the subheading of its topic's page it goes under
    unverified = ["..."]        # optional: claims of the summary nobody found in the code yet
    summary = '''
    What holds, in two to four lines: the result, not the reasons.
    '''
    history = "JOURNAL.md#D372"  # its anchor: <a id="D372"></a> (archive/PROJECT.md up to D366)
    step = "M4.12a"             # the plan step that builds it; a milestone ("M6": planned there,
                                # no step yet); a bug ("B160": built by its fix, once bugs/ has no
                                # file for it); "later"; "n/a" (a rule or a process, not code).
                                # Required while active.
    code = ["crates/schedr-master/src/chained.rs::chained_run"]  # optional: where it lives, a
                                # path or path::symbol (checked: the file has the symbol)
    checked = 2026-11-09        # optional: when a second reader compared it with the full text

Built or planned is never written: it is the step's status. `untraced` is a number with no text
found; its `history` may name a commit instead of an anchor.

A question, `questions/Q<n>.toml`:

    title = "Should a non-leading master name the leader?"
    asked = 2026-11-02
    topic = "protocol"
    status = "open"             # open, decided, dropped
    decided_by = 0              # with status "decided": the decision
    proposal = "What is proposed now, one or two lines."
    history = "archive/PROJECT.md: Open questions"   # where it was raised

`decisions/next-id` and `questions/next-id` hold the next free numbers; every number below has a
file, and every "decision N" written in the repository is below it.

Usage:
    python3 scripts/records.py render              # write every generated file and block
    python3 scripts/records.py check               # validate; fail if anything generated is stale
    python3 scripts/records.py decision "<title>"  # a new decision's file; prints its path
    python3 scripts/records.py question "<title>"  # a new question's file; prints its path
    python3 scripts/records.py affected [<since>]  # decisions whose code changed since <since>
                                                   # (default HEAD: the uncommitted changes)
    python3 scripts/records.py stale               # decisions whose code changed after `checked`
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import re
import subprocess
import sys
import textwrap
import tomllib
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parent.parent
BY = ("user", "claude")
STATUS = ("active", "superseded", "dropped", "untraced")
STEP_STATUS = ("planned", "done", "dropped")
QUESTION_STATUS = ("open", "decided", "dropped")
NO_STEP = ("later", "n/a")
BUG = re.compile(r"B[1-9][0-9]*")
NAME = re.compile(r"D([1-9][0-9]*)\.toml")
QUESTION_NAME = re.compile(r"Q([1-9][0-9]*)\.toml")
TOPIC_COVERS = re.compile(r"^\| `([a-z0-9-]+)` \|([^|\n]*)\|", re.MULTILINE)
ANCHOR = re.compile(r'<a id="(D[1-9][0-9]*)"></a>')
HISTORY_ANCHOR = re.compile(r"((?:archive/PROJECT|JOURNAL)\.md)#(D[1-9][0-9]*)")
# The history up to decision 366, and the journal from 2026-11-08 on (decision 370).
ANCHOR_ID = re.compile(r"D[1-9][0-9]*")
PAGE_NOTE = (
    "Generated by `make records` from the decisions whose `topic` (or `also`) is this one; edit"
    " the decisions, not this page."
)
HISTORIES = ("archive/PROJECT.md", "JOURNAL.md")
# "decision 12", "decisions 326-333", "decisions 87, 88 and 90"; a year (2026) isn't one.
DASH = "[-\u2013]"  # a hyphen or an en dash
MENTION = re.compile(rf"[Dd]ecisions? ((?:\d+[A-Za-z']*(?:\s*{DASH}\s*\d+)?(?:,\s*|\s+and\s+)?)+)")
PART = re.compile(rf"(\d+)[A-Za-z']*(?:\s*{DASH}\s*(\d+))?")
# A range wider than this is a typo or two numbers, not a list of decisions.
WIDEST_RANGE = 40
# From here a number is a year ("decision 2026" in a date), not a decision.
FIRST_YEAR = 1900
OWN_FILES = ("records.py", "test_records.py")
# What the mention check reads: text the project writes (not generated files nor history).
SKIPPED = ("target/", "site/", "docs/_generated/", "archive/", "decisions/", "DECISIONS.md")
SUFFIXES = (".md", ".rs", ".py", ".toml", ".sql", ".proto", ".yml", ".yaml", ".sh", ".html")
# A file the mention check can't read is skipped (a tuple: older Pythons run this script).
UNREADABLE = (OSError, UnicodeDecodeError)


def generated(source: str) -> str:
    """The first line of a generated file."""
    return f"<!-- Generated by `scripts/records.py render` from {source}: edit those files. -->\n"


def begin(block: str) -> str:
    """The marker line opening a generated block."""
    return f"<!-- generated by scripts/records.py: {block} -->\n"


def end(block: str) -> str:
    """The marker line closing a generated block."""
    return f"<!-- end of {block} -->\n"


class RecordError(Exception):
    """A record, a mention or a page that doesn't follow the rules."""


@dataclass(frozen=True)
class Paths:
    """Where the records and their views are; the repository's by default."""

    root: Path = ROOT

    @property
    def plan(self) -> Path:
        """`plan/`."""
        return self.root / "plan"

    @property
    def decisions(self) -> Path:
        """`decisions/`."""
        return self.root / "decisions"

    @property
    def questions(self) -> Path:
        """`questions/`."""
        return self.root / "questions"

    @property
    def design(self) -> Path:
        """`design/`."""
        return self.root / "design"


@dataclass(frozen=True)
class Step:
    """One step of the plan."""

    id: str
    milestone: str
    title: str
    scope: str
    done_when: str
    status: str
    done: datetime.date | None
    commits: tuple[str, ...]
    part_of: str = ""


@dataclass(frozen=True)
class Milestone:
    """A milestone and its steps."""

    id: str
    title: str
    order: int
    done_when: str
    notes: str
    steps: tuple[Step, ...]


@dataclass(frozen=True)
class Decision:
    """One decision, as its file says."""

    number: int
    title: str
    date: datetime.date
    by: str
    status: str
    superseded_by: int
    topic: str
    also: tuple[str, ...]
    summary: str
    history: str
    step: str
    code: tuple[str, ...]
    checked: datetime.date | None = None
    section: str = ""
    unverified: tuple[str, ...] = ()

    @property
    def id(self) -> str:
        """`D357`."""
        return f"D{self.number}"

    @property
    def shown_status(self) -> str:
        """The status, with what replaced it."""
        if self.status == "superseded":
            return f"superseded by D{self.superseded_by}"
        return self.status


@dataclass(frozen=True)
class Question:
    """One question, open or since decided."""

    number: int
    title: str
    asked: datetime.date
    topic: str
    status: str
    decided_by: int
    proposal: str
    history: str

    @property
    def id(self) -> str:
        """`Q12`."""
        return f"Q{self.number}"


@dataclass(frozen=True)
class Records:
    """Everything loaded and checked against each other."""

    milestones: tuple[Milestone, ...]
    decisions: tuple[Decision, ...]
    questions: tuple[Question, ...]
    open_bugs: frozenset[str] = frozenset()

    @property
    def milestone_ids(self) -> set[str]:
        """Every milestone's ID."""
        return {m.id for m in self.milestones}

    @property
    def steps(self) -> dict[str, Step]:
        """Every step by ID."""
        return {s.id: s for m in self.milestones for s in m.steps}

    @property
    def next_step(self) -> Step | None:
        """The first planned step without planned parts, in plan order."""
        return next(iter(self.planned_steps), None)

    @property
    def planned_steps(self) -> list[Step]:
        """The planned steps without planned parts, in plan order."""
        steps = [s for m in self.milestones for s in m.steps]
        wholes = {s.part_of for s in steps if s.status == "planned"}
        return [s for s in steps if s.status == "planned" and s.id not in wholes]

    def state(self, decision: Decision) -> str:
        """`built M4.9c`, `planned (M4.15)`, `later`, `rule`, or the decision's status."""
        if decision.status != "active":
            return decision.shown_status
        if decision.step == "n/a":
            return "rule"
        if decision.step == "later":
            return "planned (later)"
        if decision.step in self.milestone_ids:
            return f"planned ({decision.step})"
        if BUG.fullmatch(decision.step):
            fixed = decision.step not in self.open_bugs
            return f"built ({decision.step} fixed)" if fixed else f"planned ({decision.step})"
        step = self.steps[decision.step]
        return f"built {step.id}" if step.status == "done" else f"planned ({step.id})"


def date_field(
    name: str, data: dict[str, object], field: str, *, required: bool
) -> datetime.date | None:
    """A TOML date (not a date-time, not a string), or None when optional and absent."""
    value = data.get(field)
    if value is None and not required:
        return None
    if not isinstance(value, datetime.date) or isinstance(value, datetime.datetime):
        raise RecordError(f"{name}: `{field}` is a date, e.g. 2026-11-08 (no quotes)")
    return value


def text_field(name: str, data: dict[str, object], field: str) -> str:
    """A required non-empty string."""
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise RecordError(f"{name}: `{field}` is a non-empty string")
    return value.strip()


def string_list(name: str, data: dict[str, object], field: str) -> list[str]:
    """An optional list of non-empty strings."""
    value = data.get(field, [])
    if not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
        raise RecordError(f"{name}: `{field}` is a list of strings")
    return [str(v) for v in value]


def fields(name: str, data: dict[str, object], required: set[str], optional: set[str]) -> None:
    """Refuse missing and unknown fields."""
    missing, extra = sorted(required - set(data)), sorted(set(data) - required - optional)
    if missing or extra:
        raise RecordError(f"{name}: missing {missing}, unknown {extra}")


def parse(path: Path) -> dict[str, object]:
    """A TOML file, its syntax errors named."""
    try:
        return tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as error:
        raise RecordError(f"{path.name}: {error}") from error


# The plan.


def read_milestone(path: Path) -> Milestone:
    """A milestone's file, checked."""
    name = path.name
    data = parse(path)
    fields(name, data, {"title", "order", "done_when"}, {"notes", "steps"})
    order = data["order"]
    if not isinstance(order, int) or isinstance(order, bool):
        raise RecordError(f"{name}: `order` is a number")
    raw = data.get("steps", [])
    if not isinstance(raw, list) or not all(isinstance(s, dict) for s in raw):
        raise RecordError(f"{name}: `steps` is a list of [[steps]] tables")
    steps = []
    for entry in raw:
        where = f"{name}: step {entry.get('id', '?')}"
        fields(
            where,
            entry,
            {"id", "title", "scope", "done_when", "status"},
            {"done", "commits", "part_of"},
        )
        status = entry["status"]
        if status not in STEP_STATUS:
            raise RecordError(f"{where}: `status` is one of {', '.join(STEP_STATUS)}")
        done = date_field(where, entry, "done", required=False)
        if (status == "done") != (done is not None):
            raise RecordError(f"{where}: `done` (a date) is given exactly when the status is done")
        steps.append(
            Step(
                id=text_field(where, entry, "id"),
                milestone=path.stem,
                title=text_field(where, entry, "title"),
                scope=text_field(where, entry, "scope"),
                done_when=text_field(where, entry, "done_when"),
                status=status,
                done=done,
                commits=tuple(string_list(where, entry, "commits")),
                part_of=str(entry.get("part_of", "")),
            )
        )
    notes = data.get("notes", "")
    if not isinstance(notes, str):
        raise RecordError(f"{name}: `notes` is a string")
    return Milestone(
        id=path.stem,
        title=text_field(name, data, "title"),
        order=order,
        done_when=text_field(name, data, "done_when"),
        notes=notes.strip(),
        steps=tuple(steps),
    )


def load_plan(folder: Path) -> list[Milestone]:
    """Every milestone, in order; step IDs unique."""
    milestones = sorted((read_milestone(p) for p in folder.glob("*.toml")), key=lambda m: m.order)
    orders = [m.order for m in milestones]
    if len(set(orders)) != len(orders):
        raise RecordError("plan/: two milestones share an `order`")
    seen: set[str] = set()
    for step in (s for m in milestones for s in m.steps):
        if step.id in seen or step.id in NO_STEP:
            raise RecordError(f"plan/: step ID {step.id} is used twice or reserved")
        seen.add(step.id)
    clash = seen & {m.id for m in milestones}
    if clash:
        raise RecordError(f"plan/: {', '.join(sorted(clash))} names both a milestone and a step")
    for step in (s for m in milestones for s in m.steps):
        if step.part_of and step.part_of not in seen:
            raise RecordError(f"plan/: step {step.id} is part of {step.part_of}, unknown")
    return milestones


# The decisions.


def topics(readme: Path) -> set[str]:
    """The topics `design/README.md` lists."""
    return set(covers(readme))


def covers(readme: Path) -> dict[str, str]:
    """Each topic `design/README.md` lists, with what it covers (the row's second cell)."""
    return {topic: covered.strip() for topic, covered in TOPIC_COVERS.findall(readme.read_text())}


def generated_page(topic: str, covered: str, blocks: dict[str, str]) -> str:
    """A whole design page: for the agent, an index into the decisions and the code."""
    parts = [
        f"# {topic}\n\n{covered[:1].upper() + covered[1:]}.\n\n",
        f"{PAGE_NOTE}\n\n",
        f"## Current\n\n{blocks['current']}\n",
        f"## Replaced\n\n{blocks['decisions']}\n",
        f"## Where in code\n\n{blocks['code']}\n",
        f"## Open\n\n{blocks['questions']}\n",
        blocks["details"],
    ]
    return "".join(parts)


def read_decision(path: Path, known_topics: set[str]) -> Decision:
    """A decision's file, checked on its own."""
    match = NAME.fullmatch(path.name)
    if match is None:
        raise RecordError(f"{path.name}: a decision's file is named `D<number>.toml`")
    name = path.name
    data = parse(path)
    fields(
        name,
        data,
        {"title", "date", "by", "status", "topic", "summary", "history"},
        {"superseded_by", "step", "also", "section", "code", "checked", "unverified"},
    )
    if data["by"] not in BY:
        raise RecordError(f"{name}: `by` is one of {', '.join(BY)}")
    status = data["status"]
    if status not in STATUS:
        raise RecordError(f"{name}: `status` is one of {', '.join(STATUS)}")
    superseded_by = data.get("superseded_by", 0)
    if not isinstance(superseded_by, int) or isinstance(superseded_by, bool):
        raise RecordError(f"{name}: `superseded_by` is a decision's number")
    if (status == "superseded") != (superseded_by > 0):
        raise RecordError(
            f"{name}: `superseded_by` is given exactly when the status is `superseded`"
        )
    topic = data["topic"]
    if topic not in known_topics:
        raise RecordError(f"{name}: topic `{topic}` isn't one of design/README.md's topics")
    also = string_list(name, data, "also")
    wrong = [t for t in also if t not in known_topics or t == topic]
    if wrong:
        raise RecordError(f"{name}: `also` names {wrong}: other topics of design/README.md")
    step = data.get("step", "")
    if not isinstance(step, str):
        raise RecordError(f'{name}: `step` is a plan step ("M4.12a"), "later" or "n/a"')
    if status == "active" and not step:
        raise RecordError(f'{name}: an active decision names its `step` (or "later", "n/a")')
    return Decision(
        number=int(match[1]),
        title=text_field(name, data, "title"),
        date=date_field(name, data, "date", required=True) or datetime.date.min,
        by=str(data["by"]),
        status=str(status),
        superseded_by=superseded_by,
        topic=str(topic),
        also=tuple(also),
        summary=" ".join(text_field(name, data, "summary").split()),
        history=text_field(name, data, "history"),
        step=step.strip(),
        code=tuple(string_list(name, data, "code")),
        checked=date_field(name, data, "checked", required=False),
        section=section_of(name, data),
        unverified=tuple(string_list(name, data, "unverified")),
    )


def section_of(name: str, data: dict[str, object]) -> str:
    """A decision's optional `section`: the subheading of its topic's page it is listed under."""
    section = data.get("section", "")
    if not isinstance(section, str) or "\n" in section:
        raise RecordError(f"{name}: `section` is one line of text")
    return section.strip()


def next_number(folder: Path, letter: str) -> int:
    """The next free number, from `next-id` (`D367`, `Q12`)."""
    text = (folder / "next-id").read_text().strip()
    if not re.fullmatch(rf"{letter}[1-9][0-9]*", text):
        raise RecordError(f"{folder.name}/next-id: `{text}` isn't an ID like {letter}12")
    return int(text[1:])


def numbered(folder: Path, letter: str, numbers: list[int]) -> None:
    """No gap below `next-id`, nothing at or past it, no stray file."""
    stray = sorted(
        p.name
        for p in folder.iterdir()
        if p.name not in {"README.md", "next-id"} and not p.name.endswith(".toml")
    )
    if stray:
        raise RecordError(f"{folder.name}/: not a record: {', '.join(stray)}")
    upto = next_number(folder, letter)
    gaps = [n for n in range(1, upto) if n not in set(numbers)]
    if gaps:
        shown = ", ".join(f"{letter}{n}" for n in gaps[:10])
        raise RecordError(f"no file for {shown}: a number not used keeps a file (`untraced`)")
    if numbers and max(numbers) >= upto:
        raise RecordError(
            f"{folder.name}/next-id says {letter}{upto}, but {letter}{max(numbers)} exists"
        )


def load_decisions(folder: Path, known_topics: set[str]) -> list[Decision]:
    """Every decision, by number; numbers and replacements checked."""
    decisions = sorted(
        (read_decision(p, known_topics) for p in folder.glob("*.toml")), key=lambda d: d.number
    )
    numbers = [d.number for d in decisions]
    numbered(folder, "D", numbers)
    for d in decisions:
        if d.superseded_by == d.number:
            raise RecordError(f"{d.id}: can't supersede itself")
        if d.status == "superseded" and d.superseded_by not in numbers:
            raise RecordError(f"{d.id}: superseded by D{d.superseded_by}, unknown")
    return decisions


# The questions.


def read_question(path: Path, known_topics: set[str]) -> Question:
    """A question's file, checked on its own."""
    match = QUESTION_NAME.fullmatch(path.name)
    if match is None:
        raise RecordError(f"{path.name}: a question's file is named `Q<number>.toml`")
    name = path.name
    data = parse(path)
    fields(name, data, {"title", "asked", "topic", "status", "proposal", "history"}, {"decided_by"})
    if data["status"] not in QUESTION_STATUS:
        raise RecordError(f"{name}: `status` is one of {', '.join(QUESTION_STATUS)}")
    if data["topic"] not in known_topics:
        raise RecordError(f"{name}: topic `{data['topic']}` isn't one of design/README.md's topics")
    decided_by = data.get("decided_by", 0)
    if not isinstance(decided_by, int) or isinstance(decided_by, bool):
        raise RecordError(f"{name}: `decided_by` is a decision's number")
    if (data["status"] == "decided") != (decided_by > 0):
        raise RecordError(f"{name}: `decided_by` is given exactly when the status is `decided`")
    return Question(
        number=int(match[1]),
        title=text_field(name, data, "title"),
        asked=date_field(name, data, "asked", required=True) or datetime.date.min,
        topic=str(data["topic"]),
        status=str(data["status"]),
        decided_by=decided_by,
        proposal=" ".join(text_field(name, data, "proposal").split()),
        history=text_field(name, data, "history"),
    )


def load_questions(folder: Path, known_topics: set[str]) -> list[Question]:
    """Every question, by number."""
    if not folder.exists():
        return []
    questions = sorted(
        (read_question(p, known_topics) for p in folder.glob("*.toml")), key=lambda q: q.number
    )
    numbered(folder, "Q", [q.number for q in questions])
    return questions


# Everything together.


def load(paths: Paths) -> Records:
    """The plan, decisions and questions, checked against each other."""
    known = topics(paths.design / "README.md")
    milestones = load_plan(paths.plan) if paths.plan.exists() else []
    bugs = paths.root / "bugs"
    records = Records(
        milestones=tuple(milestones),
        decisions=tuple(load_decisions(paths.decisions, known)),
        questions=tuple(load_questions(paths.questions, known)),
        open_bugs=frozenset(p.stem for p in bugs.glob("B*.toml")) if bugs.exists() else frozenset(),
    )
    known_steps = set(records.steps) | records.milestone_ids
    for d in records.decisions:
        if (
            d.step
            and d.step not in NO_STEP
            and d.step not in known_steps
            and not BUG.fullmatch(d.step)
        ):
            raise RecordError(
                f"{d.id}: step {d.step} isn't a step or milestone in plan/, nor a bug"
            )
    numbers = {d.number for d in records.decisions}
    for q in records.questions:
        if q.decided_by and q.decided_by not in numbers:
            raise RecordError(f"{q.id}: decided by D{q.decided_by}, unknown")
    return records


def problems(records: Records, root: Path, files: list[Path]) -> list[str]:
    """What the files can't show alone: missing code paths, anchors, mentions in `files`."""
    found = [
        problem
        for d in records.decisions
        if d.status == "active"
        for entry in d.code
        if (problem := code_problem(d.id, entry, root))
    ]
    anchors = {
        name: set(ANCHOR.findall((root / name).read_text())) if (root / name).exists() else set()
        for name in HISTORIES
    }
    for d in records.decisions:
        target = HISTORY_ANCHOR.fullmatch(d.history)
        if target and target[2] not in anchors[target[1]]:
            found.append(f'{d.id}: {target[1]} has no anchor <a id="{target[2]}"></a>')
    upto = max((d.number for d in records.decisions), default=0) + 1
    found += unknown_mentions(files, upto)
    found += unknown_names(records, files, root)
    return found


# Where the documents are, not the code: a name found only there isn't in the code.
DOCUMENTS = ("decisions/", "design/", "docs/", "plan/", "questions/", "bugs/", "archive/")
CODE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")


def unknown_names(records: Records, files: list[Path], root: Path) -> list[str]:
    """Names in code font in a built or rule decision's summary that no code file has.

    A summary's `names` are the identifiers inside its backticks; each must be a word of some
    tracked file outside the documents (decisions, design, docs, plan, ...) and the root's
    Markdown. One listed in the decision's `unverified` is let through.
    """
    words: set[str] = set()
    # The script itself names the records' fields.
    for path in [*files, *(root / "scripts" / name for name in OWN_FILES)]:
        if not path.is_file():
            continue
        name = path.relative_to(root).as_posix()
        if name.startswith(DOCUMENTS) or ("/" not in name and name.endswith(".md")):
            continue
        try:
            words.update(CODE_NAME.findall(path.read_text(encoding="utf-8")))
        except UNREADABLE:
            continue
    found = []
    for d in records.decisions:
        state = records.state(d)
        if d.status != "active" or not (state.startswith("built") or state == "rule"):
            continue
        allowed = set(CODE_NAME.findall(" ".join(d.unverified)))
        quoted = " ".join(re.findall(r"`([^`]+)`", d.summary))
        missing = sorted({w for w in CODE_NAME.findall(quoted) if not known(w, words)} - allowed)
        if missing:
            found.append(
                f"{d.id}: its summary names {', '.join(f'`{w}`' for w in missing)}, not in the"
                " code: correct it, or list the claim in `unverified`"
            )
    return found


def known(name: str, words: set[str]) -> bool:
    """`name` is a word of the code, or an environment variable built from words of it.

    `SCHEDR_HIDDEN__WEBSERVER__REFRESH_INTERVAL` is assembled from its sections' names, so each
    part after the prefix is looked for in lower case.
    """
    if name in words:
        return True
    prefix, *parts = name.split("__")
    return (
        bool(parts)
        and prefix in words | {"SCHEDR_HIDDEN", "SCHEDR_CONFIG"}
        and all(part.lower() in words for part in parts)
    )


def changed_files(root: Path, since: str) -> set[str]:
    """The files changed since `since` (a commit or range), the working tree included."""
    listed = subprocess.run(
        ["git", "diff", "--name-only", since], cwd=root, capture_output=True, text=True, check=True
    ).stdout.split()
    return set(listed)


def affected(records: Records, changed: set[str]) -> list[str]:
    """The active decisions whose `code` names a changed file: their summaries to re-read."""
    lines = []
    for d in records.decisions:
        hits = [e for e in d.code if e.partition("::")[0] in changed]
        if d.status == "active" and hits:
            lines.append(f"{d.id} ({records.state(d)}): {d.title} [{', '.join(hits)}]")
    return lines


def last_change(root: Path, path: str) -> datetime.date | None:
    """The date of the last commit that changed `path`."""
    out = subprocess.run(
        ["git", "log", "-1", "--format=%cs", "--", path],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return datetime.date.fromisoformat(out) if out else None


def stale(records: Records, root: Path) -> list[str]:
    """Active decisions whose code changed after they were `checked`, or never checked."""
    lines = []
    for d in records.decisions:
        if d.status != "active" or records.state(d) in ("rule",) or not d.code:
            continue
        paths = sorted({e.partition("::")[0] for e in d.code})
        if d.checked is None:
            lines.append(f"{d.id}: never checked [{', '.join(paths)}]")
            continue
        later = [p for p in paths if (when := last_change(root, p)) and when > d.checked]
        if later:
            lines.append(f"{d.id}: checked {d.checked}, changed since [{', '.join(later)}]")
    return lines


def code_problem(decision: str, entry: str, root: Path) -> str:
    """Why a `code` entry (`path` or `path::symbol`) doesn't hold, or nothing."""
    path, _, symbol = entry.partition("::")
    file = root / path
    if not file.exists():
        return f"{decision}: {path} doesn't exist"
    if symbol and file.is_file():
        try:
            text = file.read_text(encoding="utf-8")
        except UNREADABLE:
            return f"{decision}: {path} can't be read for `{symbol}`"
        if not re.search(rf"\b{re.escape(symbol)}\b", text):
            return f"{decision}: {path} has no `{symbol}`"
    return ""


def by_number(item: str) -> tuple[int, str]:
    """Sorts decision IDs by number (D9 before D10) and headings by name."""
    return (int(item[1:]) if ANCHOR_ID.fullmatch(item) else 0, item)


def anchor_places(root: Path) -> dict[str, tuple[str, str]]:
    """Where each anchor is: `D213` -> (`archive/PROJECT.md`, `Task assignment`), its heading."""
    places: dict[str, tuple[str, str]] = {}
    for name in HISTORIES:
        path = root / name
        if not path.exists():
            continue
        heading = ""
        for line in path.read_text().splitlines():
            level = len(line) - len(line.lstrip("#"))
            if level in (2, 3) and line[level : level + 1] == " ":
                heading = ANCHOR.sub("", line[level + 1 :]).strip()
            for anchor in ANCHOR.findall(line):
                places[anchor] = (name, heading)
    return places


def mentions(text: str) -> set[int]:
    """The decision numbers `text` names ("decision 12", "decisions 3-5")."""
    found: set[int] = set()
    for match in MENTION.finditer(text):
        for part in PART.finditer(match.group(1)):
            first = int(part.group(1))
            last = int(part.group(2)) if part.group(2) else first
            # A year or a wide range isn't a list of decisions.
            if last < first or last - first > WIDEST_RANGE:
                last = first
            found.update(range(first, last + 1))
    return found


def tracked(root: Path) -> list[Path]:
    """The repository's tracked text files the mention check reads."""
    listed = subprocess.run(
        ["git", "ls-files"], cwd=root, capture_output=True, text=True, check=True
    ).stdout.split()
    return [
        root / name
        for name in listed
        if name.endswith(SUFFIXES)
        and not name.startswith(SKIPPED)
        # The script and its tests name decisions as examples.
        and Path(name).name not in OWN_FILES
    ]


def unknown_mentions(files: list[Path], upto: int) -> list[str]:
    """Mentions of a decision with no file yet (at or past `upto`); years aside."""
    found: list[str] = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except UNREADABLE:
            continue
        found.extend(
            f"{path.name}: decision {n} has no file"
            for n in sorted(mentions(text))
            if upto <= n < FIRST_YEAR
        )
    return found


# The views.


def cell(text: str) -> str:
    """Text fit for one Markdown table cell."""
    return " ".join(text.split()).replace("|", "\\|")


def render_plan(records: Records, readme: Path) -> str:
    """PLAN.md, an index: `plan/README.md`, then one line per step; details in `plan/<id>.toml`."""
    next_step = records.next_step
    parts = [generated("plan/"), readme.read_text().rstrip() + "\n\n"]
    if next_step:
        parts.append(f"**Next:** {next_step.id}, {next_step.title}.\n")
    for m in records.milestones:
        parts.append(
            f"\n## {m.id}: {m.title}\n\n**Done when:** {m.done_when} Scope, criteria and notes: "
            f"`plan/{m.id}.toml`.\n"
        )
        if not m.steps:
            continue
        parts.append("\n| Step | Title | State | Decisions |\n|---|---|---|---|\n")
        for s in m.steps:
            state = f"done {s.done.isoformat()}" if s.done else s.status
            if s is next_step:
                state = "**next**"
            built = [d.id for d in records.decisions if d.step == s.id and d.status == "active"]
            step = f"{s.id} (part of {s.part_of})" if s.part_of else s.id
            parts.append(f"| {step} | {cell(s.title)} | {state} | {', '.join(built)} |\n")
    return "".join(parts)


def render_decisions(records: Records) -> str:
    """DECISIONS.md: the open questions, every decision by number, the active ones by topic."""
    parts = [
        generated("decisions/ and questions/"),
        "# Decisions\n\n",
        "One line per decision; the file `decisions/D<n>.toml` has its summary, and its `history` "
        "is its anchor in archive/PROJECT.md (to D366) or JOURNAL.md, where its reasons are. "
        "How each topic works now: "
        "`design/<topic>.md`. Built or planned: the step's state in `PLAN.md`.\n",
    ]
    open_questions = [q for q in records.questions if q.status == "open"]
    parts.append("\n## Open questions\n\n")
    if open_questions:
        parts.append("| # | Asked | Topic | Question | Proposal |\n|---|---|---|---|---|\n")
        parts.extend(
            f"| {q.id} | {q.asked.isoformat()} | {q.topic} | {cell(q.title)} "
            f"| {cell(q.proposal)} |\n"
            for q in reversed(open_questions)
        )
    else:
        parts.append("None.\n")
    doubted = [d for d in records.decisions if d.status == "active" and d.unverified]
    parts.append("\n## Unverified claims\n\n")
    parts.append(
        "Active decisions with a claim nobody found in the code yet (`unverified`): "
        + ", ".join(d.id for d in doubted)
        + ".\n"
        if doubted
        else "None.\n"
    )
    parts.append(
        "\n## Every decision\n\n"
        "| # | Date | By | State | Topic | Title |\n|---|---|---|---|---|---|\n"
    )
    for d in reversed(records.decisions):
        row = (d.id, d.date.isoformat(), d.by, records.state(d), d.topic, cell(d.title))
        parts.append("| " + " | ".join(row) + " |\n")
    parts.append("\n## Active, by topic\n")
    for topic in sorted({d.topic for d in records.decisions}):
        active = [d for d in records.decisions if d.topic == topic and d.status == "active"]
        if active:
            parts.append(f"\n### {topic}\n\n")
            parts.extend(f"- {d.id}: {d.title}\n" for d in reversed(active))
    return "".join(parts)


def with_block(page: str, block: str, content: str, name: str) -> str:
    """`page` with its generated `block` replaced by `content`."""
    first, last = begin(block), end(block)
    if page.count(first) != 1 or page.count(last) != 1:
        raise RecordError(
            f"{name}: needs the marker lines of `{block}` once each:\n  {first}  {last}"
        )
    head, rest = page.split(first)
    _, tail = rest.split(last)
    return head + first + content + last + tail


def design_blocks(
    records: Records, topic: str, places: dict[str, tuple[str, str]] | None = None
) -> dict[str, str]:
    """A design page's generated blocks.

    `current`: the topic's active decisions' summaries, grouped by `section` (sections in the
    order of their oldest decision; none last), each naming its ID and, unless built, its state;
    then the other topics' decisions naming this one in `also`, one line each. `decisions`: the
    replaced, dropped and untraced ones. `code`: the active decisions' `code` entries.
    `questions`: the open ones. `details`: where the reasons are (the anchors' sections).
    """
    own = [d for d in records.decisions if d.topic == topic]
    active = [d for d in own if d.status == "active"]
    sections: dict[str, list[Decision]] = {}
    for d in sorted(active, key=lambda d: (d.section == "", d.number)):
        sections.setdefault(d.section, []).append(d)
    current = []
    for name, decisions in sections.items():
        current.append(f"### {name or 'Other'}\n\n")
        for d in decisions:
            state = records.state(d)
            note = "" if state.startswith("built") or state == "rule" else f", {state}"
            text = f"- **{d.title}** ({d.id}{note}): {' '.join(d.summary.split())}"
            if d.unverified:
                text += " *Not found in the code yet:* " + "; ".join(d.unverified) + "."
            current.append(textwrap.fill(text, 100, subsequent_indent="  ") + "\n")
        current.append("\n")
    others = [d for d in records.decisions if d.topic != topic and topic in d.also]
    if others:
        current.append("### From other topics\n\n")
        current.extend(
            f"- {d.title} ({d.id}, `{d.topic}`; {records.state(d)})\n"
            for d in others
            if d.status == "active"
        )
        current.append("\n")
    replaced = "".join(
        f"- {d.id} ({records.state(d)}): {d.title}\n" for d in reversed(own) if d.status != "active"
    )
    paths: dict[str, list[str]] = {}
    for d in [*active, *(d for d in others if d.status == "active")]:
        for entry in d.code:
            paths.setdefault(entry, []).append(d.id)
    code = "".join(
        f"- `{p}` ({', '.join(sorted(set(ids), key=lambda i: int(i[1:])))})\n"
        for p, ids in sorted(paths.items())
    )
    questions = "".join(
        f"- {q.id}: {q.title} Proposal: {q.proposal}\n"
        for q in records.questions
        if q.topic == topic and q.status == "open"
    )
    where: dict[str, set[str]] = {}
    for d in own:
        anchor = d.history.partition("#")[2]
        if anchor in (places or {}):
            name, heading = (places or {})[anchor]
            # A journal entry is one decision; a long document's section holds many.
            where.setdefault(name, set()).add(
                anchor if name == "JOURNAL.md" or not heading else heading
            )
    details = "".join(
        f"- {name}: {', '.join(sorted(items, key=by_number))}\n"
        for name, items in sorted(where.items())
    )
    details = (
        "Details and edge cases, with the reasons; read them before changing this:\n\n" + details
        if details
        else "Details: the decisions' anchors.\n"
    )
    return {
        "current": "".join(current).rstrip("\n") + "\n" if current else "None yet.\n",
        "decisions": replaced or "None.\n",
        "code": code or "None named by the decisions yet.\n",
        "questions": questions or "None.\n",
        "details": details,
    }


def status_block(records: Records, root: Path) -> str:
    """STATUS.md's generated block: the next step, its decisions, open questions, open bugs."""
    lines = []
    step = records.next_step
    if step:
        milestone = next(m for m in records.milestones if m.id == step.milestone)
        planned = [d.id for d in records.decisions if d.step == step.id and d.status == "active"]
        lines.append(f"- **Milestone:** {milestone.id}, {milestone.title}.\n")
        lines.append(
            f"- **Next step:** {step.id}, {step.title}"
            + (f" (decisions {', '.join(planned)})" if planned else "")
            + f". Done when: {step.done_when}\n"
        )
        later = [s.id for s in records.planned_steps][1:6]
        if later:
            lines.append(f"- **Then:** {', '.join(later)}.\n")
    open_questions = [q.id for q in records.questions if q.status == "open"]
    lines.append(f"- **Open questions:** {len(open_questions)} (`DECISIONS.md`).\n")
    bugs = bugs_module(root)
    if bugs is not None:
        ranked = bugs.rank(bugs.load(root / "bugs"))
        shown = ", ".join(f"{b.id} ({b.severity}/{b.probability})" for b in ranked[:5])
        lines.append(
            f"- **Open bugs:** {len(ranked)}"
            + (f"; first: {shown}" if shown else "")
            + " (`BUGS.md`).\n"
        )
    return "".join(lines)


@dataclass(frozen=True)
class StatusNotes:
    """`status.toml`: what only the agent knows; the rest of STATUS.md comes from the records."""

    updated: datetime.date
    in_progress: tuple[str, ...]
    waiting: tuple[str, ...]
    rules: tuple[str, ...]
    colleagues: str
    resume: str


def read_status(path: Path) -> StatusNotes:
    """`status.toml`, checked."""
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as error:
        raise RecordError(f"status.toml: {error}") from error
    allowed = {"updated", "in_progress", "waiting", "rules", "colleagues", "resume"}
    if unknown := sorted(set(data) - allowed):
        raise RecordError(f"status.toml: unknown fields {unknown}")
    colleagues, resume = data.get("colleagues", ""), data.get("resume", "")
    if not isinstance(colleagues, str) or not isinstance(resume, str):
        raise RecordError("status.toml: `colleagues` and `resume` are text")
    updated = date_field("status.toml", data, "updated", required=True)
    assert updated is not None  # required
    return StatusNotes(
        updated=updated,
        in_progress=tuple(string_list("status.toml", data, "in_progress")),
        waiting=tuple(string_list("status.toml", data, "waiting")),
        rules=tuple(string_list("status.toml", data, "rules")),
        colleagues=colleagues.strip(),
        resume=resume.strip(),
    )


JOURNAL_ENTRY = re.compile(
    r'^### <a id="(D[1-9][0-9]*)"></a>(\d{4}-\d{2}-\d{2}): (.*?)(?: \(decision \d+\))?$'
)


def recent(records: Records, root: Path, count: int = 5) -> list[str]:
    """The latest journal entries and the latest steps done, one line each."""
    journal = root / "JOURNAL.md"
    entries = [
        f"- {m[1]} ({m[2]}): {m[3]}"
        for line in (journal.read_text().splitlines() if journal.exists() else [])
        if (m := JOURNAL_ENTRY.match(line))
    ]
    done = sorted(
        (s for m in records.milestones for s in m.steps if s.status == "done" and s.done),
        key=lambda s: (s.done, s.id),
    )
    steps = [f"- {s.id} ({s.done}): {s.title}" for s in done[-3:]]
    return [*reversed(entries[-count:]), *reversed(steps)]


def bullets(items: tuple[str, ...]) -> str:
    """Markdown items, or "Nothing."."""
    return "".join(f"- {item}\n" for item in items) if items else "Nothing.\n"


def render_status(records: Records, root: Path, notes: StatusNotes) -> str:
    """STATUS.md, whole: the records' state, then `status.toml`'s notes."""
    parts = [
        generated("plan/, decisions/, questions/, bugs/, JOURNAL.md and status.toml"),
        "# Status\n\n",
        "Start here, then read only what the step needs: `PLAN.md` (the step's row), `DECISIONS.md`"
        " and `design/<topic>.md`, `BUGS.md`. Reasons are in `JOURNAL.md` (`archive/PROJECT.md`"
        " before D367). The notes below come from `status.toml`: edit that, then `make records`.\n",
        f"\n## Now ({notes.updated})\n\n",
        status_block(records, root),
        "\n## Recent\n\n",
        "".join(f"{line}\n" for line in recent(records, root)) or "Nothing yet.\n",
        "\n## In progress\n\n",
        bullets(notes.in_progress),
        "\n## Waiting for the user\n\n",
        bullets(notes.waiting),
        "\n## Rules in force\n\n",
        bullets(notes.rules),
    ]
    if notes.colleagues:
        parts.append(f"\n## Colleagues\n\n{notes.colleagues}\n")
    if notes.resume:
        parts.append(f"\nResume this session with:\n\n{notes.resume}\n")
    return "".join(parts)


def bugs_module(root: Path) -> ModuleType | None:
    """`scripts/bugs.py`, if the project has one."""
    path = root / "scripts" / "bugs.py"
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location("bugs", path)
    if spec is None or spec.loader is None:
        raise RecordError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered first: its dataclass looks its module up while it is created.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def views(records: Records, paths: Paths) -> dict[Path, str]:
    """Every generated file and every page with generated blocks, as `render` writes them."""
    out = {
        paths.root / "DECISIONS.md": render_decisions(records),
    }
    if records.milestones:
        out[paths.root / "PLAN.md"] = render_plan(records, paths.plan / "README.md")
    places = anchor_places(paths.root)
    for topic, covered in sorted(covers(paths.design / "README.md").items()):
        page = paths.design / f"{topic}.md"
        blocks = design_blocks(records, topic, places)
        if page.exists() and begin("current") in page.read_text():
            # A page with hand-written parts (development.md): only its blocks are generated.
            text = page.read_text()
            for block, content in blocks.items():
                text = with_block(text, block, content, f"design/{page.name}")
            out[page] = text
        else:
            out[page] = generated_page(topic, covered, blocks)
    status = paths.root / "STATUS.md"
    hand = paths.root / "status.toml"
    if hand.exists() and records.milestones:
        out[status] = render_status(records, paths.root, read_status(hand))
    elif status.exists() and records.milestones:
        out[status] = with_block(
            status.read_text(), "status", status_block(records, paths.root), "STATUS.md"
        )
    return out


# New records.


def new_decision(folder: Path, title: str) -> Path:
    """A new decision's file with the next number, to fill in; `next-id` moves on."""
    number = next_number(folder, "D")
    path = folder / f"D{number}.toml"
    today = datetime.date.today().isoformat()
    path.write_text(
        f"title = {json.dumps(title, ensure_ascii=False)}\n"
        f"date = {today}\n"
        'by = "user"  # user, claude\n'
        'status = "active"  # active, superseded, dropped, untraced\n'
        'topic = ""  # a topic of design/README.md\n'
        'section = ""  # the subheading of the topic\'s page it goes under\n'
        "summary = '''\n'''\n"
        f'history = "JOURNAL.md#D{number}"  # <a id="D{number}"></a> starts its JOURNAL.md entry\n'
        'step = ""  # the plan step that builds it; "later"; "n/a" for a rule\n'
        "code = []\n"
    )
    (folder / "next-id").write_text(f"D{number + 1}\n")
    return path


def new_question(folder: Path, title: str) -> Path:
    """A new question's file with the next number, to fill in."""
    number = next_number(folder, "Q")
    path = folder / f"Q{number}.toml"
    path.write_text(
        f"title = {json.dumps(title, ensure_ascii=False)}\n"
        f"asked = {datetime.date.today().isoformat()}\n"
        'topic = ""\n'
        'status = "open"  # open, decided (with decided_by), dropped\n'
        'proposal = ""\n'
        'history = "JOURNAL.md"\n'
    )
    (folder / "next-id").write_text(f"Q{number + 1}\n")
    return path


REPOSITORY = Paths()


def check(paths: Paths) -> None:
    """Fail on any problem in the records or a stale generated view."""
    records = load(paths)
    found = problems(records, paths.root, tracked(paths.root))
    found += [
        f"{p.relative_to(paths.root)} is out of date: run `make records`"
        for p, text in views(records, paths).items()
        if not p.exists() or p.read_text() != text
    ]
    if found:
        raise RecordError("\n  ".join(["records:", *found]))


def run(argv: list[str], paths: Paths) -> bool:
    """Run the command `argv` names; whether it was one."""
    command, args = (argv[1], argv[2:]) if len(argv) > 1 else ("", [])
    if command == "render" and not args:
        for path, text in views(load(paths), paths).items():
            path.write_text(text)
    elif command == "check" and not args:
        check(paths)
    elif command == "affected" and len(args) <= 1:
        lines = affected(load(paths), changed_files(paths.root, *args or ["HEAD"]))
        if lines:
            print("\n".join(lines))
    elif command == "stale" and not args:
        lines = stale(load(paths), paths.root)
        if lines:
            print("\n".join(lines))
    elif command in ("decision", "question") and len(args) == 1 and args[0].strip():
        make = new_decision if command == "decision" else new_question
        folder = paths.decisions if command == "decision" else paths.questions
        print(make(folder, args[0].strip()).relative_to(paths.root))
    else:
        return False
    return True


def main(argv: list[str], paths: Paths = REPOSITORY) -> int:
    """`render`, `check`, `affected`, `stale`, `decision <title>` or `question <title>`."""
    try:
        if run(argv, paths):
            return 0
    except RecordError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(__doc__.split("Usage:")[1], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
