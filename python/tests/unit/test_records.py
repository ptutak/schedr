"""The records (plan, decisions, questions) are checked and their views generated."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]
README = "| Topic | Covers |\n|---|---|\n| `runs` | runs |\n| `chained-runs` | chains |\n"


@lru_cache(maxsize=1)
def records() -> ModuleType:
    """Load scripts/records.py."""
    path = ROOT / "scripts" / "records.py"
    spec = importlib.util.spec_from_file_location("records", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered first: its dataclass looks its module up while it is created.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def decision(status: str = "active", topic: str = "runs", **extra: str) -> str:
    """A decision's file."""
    fields = {
        "title": '"a title"',
        "date": "2026-11-08",
        "by": '"user"',
        "status": f'"{status}"',
        "topic": f'"{topic}"',
        "summary": "'''\nwhat holds.\n'''",
        "history": '"archive/PROJECT.md: Runs"',
        "step": '"S1"',
        **extra,
    }
    return "".join(f"{key} = {value}\n" for key, value in fields.items())


def question(status: str = "open", **extra: str) -> str:
    """A question's file."""
    fields = {
        "title": '"Which way?"',
        "asked": "2026-11-08",
        "topic": '"runs"',
        "status": f'"{status}"',
        "proposal": '"this way"',
        "history": '"archive/PROJECT.md: Open questions"',
        **extra,
    }
    return "".join(f"{key} = {value}\n" for key, value in fields.items())


PLAN = """title = "First"
order = 1
done_when = "it runs"
notes = "How it was planned."

[[steps]]
id = "S1"
title = "Built one"
scope = "the first part"
done_when = "a test passes"
status = "done"
done = 2026-11-01
commits = ["abc1234"]

[[steps]]
id = "S2"
title = "Planned one"
scope = "the second part"
done_when = "another passes"
status = "planned"

[[steps]]
id = "S3"
title = "After it"
scope = "the third"
done_when = "a third passes"
status = "planned"
"""


def project(
    tmp_path: Path,
    decisions: dict[int, str],
    questions: dict[int, str] | None = None,
    plan: str = PLAN,
    next_decision: int | None = None,
) -> ModuleType:
    """A project folder at `tmp_path`; returns the module, `tmp_path` holds the files."""
    for folder in ("plan", "decisions", "questions", "design"):
        (tmp_path / folder).mkdir()
    (tmp_path / "design" / "README.md").write_text(README)
    (tmp_path / "plan" / "M1.toml").write_text(plan)
    (tmp_path / "plan" / "README.md").write_text("# The plan\n\nIts rules.\n")
    for number, text in decisions.items():
        (tmp_path / "decisions" / f"D{number}.toml").write_text(text)
    upto = next_decision if next_decision is not None else max(decisions, default=0) + 1
    (tmp_path / "decisions" / "next-id").write_text(f"D{upto}\n")
    for number, text in (questions or {}).items():
        (tmp_path / "questions" / f"Q{number}.toml").write_text(text)
    (tmp_path / "questions" / "next-id").write_text(f"Q{max(questions or {0: ''}) + 1}\n")
    return records()


def load(tmp_path: Path) -> object:
    """The records at `tmp_path`."""
    return records().load(records().Paths(tmp_path))


def test_decisions_load_by_number_and_their_state_comes_from_the_plan(tmp_path: Path) -> None:
    project(
        tmp_path,
        {
            1: decision(),
            2: decision(status="superseded", superseded_by="3"),
            3: decision(topic="chained-runs", step='"S2"'),
            4: decision(step='"n/a"'),
            5: decision(step='"later"'),
        },
    )
    loaded = load(tmp_path)
    assert [d.number for d in loaded.decisions] == [1, 2, 3, 4, 5]
    states = [loaded.state(d) for d in loaded.decisions]
    assert states == [
        "built S1",
        "superseded by D3",
        "planned (S2)",
        "rule",
        "planned (later)",
    ]
    text = records().render_decisions(loaded)
    assert text.index("| D3 |") < text.index("| D1 |")
    assert "### chained-runs\n\n- D3: a title" in text
    assert "- D2:" not in text


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({1: decision(status="maybe")}, "`status` is one of"),
        ({1: decision(topic="nowhere")}, "topic `nowhere`"),
        ({1: decision(status="superseded")}, "exactly when the status is `superseded`"),
        ({1: decision(superseded_by="1")}, "exactly when"),
        ({1: decision(status="superseded", superseded_by="9")}, "superseded by D9, unknown"),
        ({1: decision(by='"someone"')}, "`by` is one of"),
        ({1: decision(date='"2026-11-08"')}, "`date` is a date"),
        ({1: decision(colour='"red"')}, "unknown ['colour']"),
        ({1: decision(code='"a/path"')}, "`code` is a list"),
        ({1: decision(checked='"2026-11-09"')}, "`checked` is a date"),
        ({1: decision(step='"S9"')}, "step S9 isn't a step or milestone"),
        ({1: decision(step='""')}, "names its `step`"),
        ({1: decision(also='["nowhere"]')}, "`also` names"),
        ({1: decision(also='["runs"]')}, "`also` names"),
        ({1: decision(), 3: decision()}, "no file for D2"),
    ],
)
def test_a_wrong_decision_is_refused_saying_why(
    tmp_path: Path, files: dict[int, str], message: str
) -> None:
    module = project(tmp_path, files)
    with pytest.raises(module.RecordError, match=message.replace("[", r"\[")):
        load(tmp_path)


def test_next_id_must_be_past_every_file(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()}, next_decision=1)
    with pytest.raises(module.RecordError, match="next-id says D1"):
        load(tmp_path)


def test_a_dropped_decision_needs_no_step(tmp_path: Path) -> None:
    project(tmp_path, {1: decision(status="dropped").replace('step = "S1"\n', "")})
    assert load(tmp_path).decisions[0].step == ""


@pytest.mark.parametrize(
    ("plan", "message"),
    [
        (PLAN.replace('status = "done"', 'status = "half"'), "`status` is one of"),
        (PLAN.replace("done = 2026-11-01\n", ""), "`done` .* exactly when"),
        (PLAN.replace('id = "S3"', 'id = "S1"'), "S1 is used twice"),
        (PLAN.replace('id = "S3"', 'id = "n/a"'), "reserved"),
        (PLAN.replace('scope = "the third"\n', ""), "missing \\['scope'\\]"),
    ],
)
def test_a_wrong_plan_is_refused_saying_why(tmp_path: Path, plan: str, message: str) -> None:
    module = project(tmp_path, {1: decision()}, plan=plan)
    with pytest.raises(module.RecordError, match=message):
        load(tmp_path)


def test_the_plan_renders_with_the_next_step_and_each_steps_decisions(tmp_path: Path) -> None:
    project(tmp_path, {1: decision(), 2: decision(step='"S2"')})
    loaded = load(tmp_path)
    assert loaded.next_step.id == "S2"
    text = records().render_plan(loaded, tmp_path / "plan" / "README.md")
    assert "# The plan\n\nIts rules.\n\n**Next:** S2, Planned one." in text
    assert (
        "## M1: First\n\n**Done when:** it runs Scope, criteria and notes: `plan/M1.toml`." in text
    )
    assert "| S1 | Built one | done 2026-11-01 | D1 |" in text
    assert "| S2 | Planned one | **next** | D2 |" in text
    assert "How it was planned." not in text


def test_the_next_step_skips_a_whole_whose_parts_are_planned(tmp_path: Path) -> None:
    plan = PLAN.replace('id = "S3"', 'id = "S2a"\npart_of = "S2"')
    project(tmp_path, {1: decision()}, plan=plan)
    loaded = load(tmp_path)
    assert loaded.next_step.id == "S2a"
    assert [s.id for s in loaded.planned_steps] == ["S2a"]


def test_a_part_of_an_unknown_step_is_refused(tmp_path: Path) -> None:
    module = project(
        tmp_path, {1: decision()}, plan=PLAN.replace('id = "S3"', 'id = "S3"\npart_of = "S9"')
    )
    with pytest.raises(module.RecordError, match="part of S9, unknown"):
        load(tmp_path)


@pytest.mark.parametrize(
    ("file", "message"),
    [
        (question(status="decided"), "exactly when the status is `decided`"),
        (question(status="decided", decided_by="7"), "decided by D7, unknown"),
        (question(topic='"nowhere"'), "topic `nowhere`"),
    ],
)
def test_a_wrong_question_is_refused(tmp_path: Path, file: str, message: str) -> None:
    module = project(tmp_path, {1: decision()}, {1: file})
    with pytest.raises(module.RecordError, match=message):
        load(tmp_path)


def test_open_questions_lead_decisions_md(tmp_path: Path) -> None:
    project(
        tmp_path, {1: decision()}, {1: question(), 2: question(status="decided", decided_by="1")}
    )
    text = records().render_decisions(load(tmp_path))
    assert "## Open questions\n\n| # |" in text
    assert "| Q1 | 2026-11-08 | runs | Which way? | this way |" in text
    assert "| Q2 |" not in text


BLOCKS = ("current", "decisions", "code", "questions", "details")


def page(module: ModuleType, title: str) -> str:
    """A design page with the generated blocks, holding stale text."""
    blocks = "".join(
        f"## {block}\n\n{module.begin(block)}stale\n{module.end(block)}\n" for block in BLOCKS
    )
    return f"# {title}\n\n## Overview\n\nHand-written.\n\n{blocks}"


def test_a_design_pages_blocks_are_generated_between_markers(tmp_path: Path) -> None:
    module = project(
        tmp_path,
        {
            1: decision(code='["a.rs"]', section='"Waiting"', history='"JOURNAL.md#D1"'),
            2: decision(status="superseded", superseded_by="3", code='["old.rs"]'),
            3: decision(topic="chained-runs", also='["runs"]', step='"S2"', code='["a.rs"]'),
            4: decision(step='"n/a"'),
            5: decision(step='"S2"', section='"Waiting"', summary="'''\nwaits\nlonger.\n'''"),
        },
        {1: question()},
    )
    (tmp_path / "JOURNAL.md").write_text('# Journal\n\n## 2026-11-08: runs\n\n<a id="D1"></a>x\n')
    (tmp_path / "design" / "runs.md").write_text(page(module, "Runs"))
    out = module.views(load(tmp_path), module.Paths(tmp_path))
    text = out[tmp_path / "design" / "runs.md"]
    # Active decisions by section (oldest first, unsectioned last); built and rules unmarked.
    assert (
        module.begin("current") + "### Waiting\n\n"
        "- **a title** (D1): what holds.\n"
        "- **a title** (D5, planned (S2)): waits longer.\n\n"
        "### Other\n\n- **a title** (D4): what holds.\n\n"
        "### From other topics\n\n- a title (D3, `chained-runs`; planned (S2))\n"
        + module.end("current")
    ) in text
    # Only what no longer holds is listed by state.
    assert (
        module.begin("decisions") + "- D2 (superseded by D3): a title\n" + module.end("decisions")
    ) in text
    # Only active decisions' code, each path once with its decisions.
    assert module.begin("code") + "- `a.rs` (D1, D3)\n" + module.end("code") in text
    assert "- Q1: Which way? Proposal: this way\n" in text
    assert "Hand-written." in text
    assert "changing this:\n\n- JOURNAL.md: D1\n" + module.end("details") in text


def test_a_long_summary_is_wrapped_inside_its_item(tmp_path: Path) -> None:
    long = " ".join(["word"] * 40)
    module = project(tmp_path, {1: decision(summary=f"'''\n{long}\n'''")})
    current = module.design_blocks(load(tmp_path), "runs")["current"]
    lines = current.splitlines()
    assert all(len(line) <= 100 for line in lines)
    assert lines[3].startswith("  word")


def test_a_topic_without_a_hand_page_gets_a_whole_generated_page(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision(section='"Waiting"')})
    out = module.views(load(tmp_path), module.Paths(tmp_path))
    text = out[tmp_path / "design" / "runs.md"]
    assert text.startswith("# runs\n\nRuns.\n\nGenerated by `make records`")
    assert "## Current\n\n### Waiting\n\n- **a title** (D1): what holds.\n" in text
    assert "## Replaced\n\nNone.\n" in text
    # Every topic of the README gets one.
    assert (tmp_path / "design" / "chained-runs.md") in out


def test_a_hand_page_missing_a_block_is_refused(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    marker = module.begin("current") + module.end("current")
    (tmp_path / "design" / "runs.md").write_text(f"# Runs\n\n{marker}")
    with pytest.raises(module.RecordError, match="marker lines of `decisions`"):
        module.views(load(tmp_path), module.Paths(tmp_path))


def test_unverified_claims_are_shown_on_the_page_and_counted(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision(unverified='["it retries"]'), 2: decision()})
    loaded = load(tmp_path)
    current = module.design_blocks(loaded, "runs")["current"]
    assert "what holds. *Not found in the code yet:* it retries." in " ".join(current.split())
    assert "## Unverified claims\n\nActive decisions with a claim" in module.render_decisions(
        loaded
    )
    assert ": D1.\n" in module.render_decisions(loaded)


def test_a_summary_naming_something_not_in_the_code_is_refused(tmp_path: Path) -> None:
    module = project(
        tmp_path,
        {
            1: decision(summary="'''\nUses `run_dag` and `gone_name`.\n'''"),
            # Planned: not built yet, so its names needn't exist.
            2: decision(step='"S2"', summary="'''\nAdds `later_name`.\n'''"),
            3: decision(summary="'''\nSays `odd_name`.\n'''", unverified='["odd_name works"]'),
            4: decision(summary="'''\nSet `SCHEDR_HIDDEN__WEBSERVER__REFRESH`.\n'''"),
        },
    )
    code = tmp_path / "a.py"
    code.write_text("def run_dag(): ...\nwebserver = refresh = 1\n")
    (tmp_path / "design" / "notes.md").write_text("gone_name\n")
    found = module.unknown_names(load(tmp_path), [code, tmp_path / "design" / "notes.md"], tmp_path)
    assert found == [
        "D1: its summary names `gone_name`, not in the code: correct it, or list the claim in"
        " `unverified`"
    ]


def git_repository(tmp_path: Path) -> None:
    """`tmp_path` as a git repository with one commit of what it holds."""
    for command in (
        ["init", "-q"],
        ["add", "-A"],
        ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "first"],
    ):
        subprocess.run(["git", *command], cwd=tmp_path, check=True)


def test_affected_lists_the_decisions_whose_code_changed(tmp_path: Path) -> None:
    module = project(
        tmp_path,
        {
            1: decision(code='["a.rs::run"]'),
            2: decision(code='["b.rs"]'),
            3: decision(status="dropped", code='["a.rs"]'),
        },
    )
    (tmp_path / "a.rs").write_text("fn run() {}\n")
    (tmp_path / "b.rs").write_text("\n")
    git_repository(tmp_path)
    (tmp_path / "a.rs").write_text("fn run() { todo!() }\n")
    changed = module.changed_files(tmp_path, "HEAD")
    assert changed == {"a.rs"}
    assert module.affected(load(tmp_path), changed) == ["D1 (built S1): a title [a.rs::run]"]


def test_stale_lists_decisions_changed_after_their_check(tmp_path: Path) -> None:
    module = project(
        tmp_path,
        {
            1: decision(code='["a.rs"]', checked="2000-01-01"),
            2: decision(code='["a.rs"]', checked="2999-01-01"),
            3: decision(code='["a.rs"]'),
            4: decision(),
        },
    )
    (tmp_path / "a.rs").write_text("\n")
    git_repository(tmp_path)
    assert module.stale(load(tmp_path), tmp_path) == [
        "D1: checked 2000-01-01, changed since [a.rs]",
        "D3: never checked [a.rs]",
    ]


STATUS_NOTES = """updated = 2026-11-09
in_progress = ["half of S2"]
rules = ["ask first"]
colleagues = "Codex 10%."
resume = "claude --resume x"
"""


def test_status_md_is_written_whole_from_the_records_and_status_toml(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision(step='"S2"')}, {1: question()})
    (tmp_path / "status.toml").write_text(STATUS_NOTES)
    (tmp_path / "JOURNAL.md").write_text(
        '# Journal\n\n### <a id="D1"></a>2026-11-08: the first choice (decision 1)\n'
    )
    text = module.views(load(tmp_path), module.Paths(tmp_path))[tmp_path / "STATUS.md"]
    assert "## Now (2026-11-09)\n\n- **Milestone:** M1, First.\n" in text
    assert (
        "## Recent\n\n- D1 (2026-11-08): the first choice\n- S1 (2026-11-01): Built one\n" in text
    )
    assert "## In progress\n\n- half of S2\n" in text
    assert "## Waiting for the user\n\nNothing.\n" in text
    assert "## Rules in force\n\n- ask first\n" in text
    assert text.endswith("Codex 10%.\n\nResume this session with:\n\nclaude --resume x\n")


def test_status_toml_refuses_unknown_fields(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    (tmp_path / "status.toml").write_text(STATUS_NOTES + 'mood = "good"\n')
    with pytest.raises(module.RecordError, match="unknown fields"):
        module.views(load(tmp_path), module.Paths(tmp_path))


def test_a_note_is_numbered_on_and_shown_on_the_decision_it_corrects(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    (tmp_path / "JOURNAL.md").write_text(
        '# Journal\n\n### <a id="D1"></a>2026-11-08: a choice (decision 1)\n\nWhy.\n'
    )
    heading = module.new_note(tmp_path, "the branch deleted", "D1")
    assert heading.endswith(": the branch deleted (note 1; corrects D1)")
    assert module.new_note(tmp_path, "a finding").endswith(": a finding (note 2)")
    entries = module.journal(tmp_path)
    assert [(e.id, e.corrects) for e in entries] == [("D1", ""), ("N1", "D1"), ("N2", "")]
    loaded = load(tmp_path)
    current = " ".join(module.design_blocks(loaded, "runs", notes=entries)["current"].split())
    assert "what holds. *Corrected by N1:* the branch deleted." in current
    assert module.note_problems(loaded, tmp_path) == []
    with pytest.raises(module.RecordError, match="corrects a decision"):
        module.new_note(tmp_path, "x", "B5")


def test_notes_out_of_order_or_correcting_nothing_are_refused(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    (tmp_path / "JOURNAL.md").write_text(
        '### <a id="N2"></a>2026-11-09: skipped one (note 2)\n\n'
        '### <a id="N3"></a>2026-11-09: wrong target (note 3; corrects D9)\n'
    )
    assert module.note_problems(load(tmp_path), tmp_path) == [
        "JOURNAL.md: note N2 is out of order (expected N1)",
        "JOURNAL.md: note N3 is out of order (expected N2)",
        "JOURNAL.md: N3 corrects D9, which isn't a decision",
    ]


def test_a_deferred_question_waits_for_a_step_and_is_listed_apart(tmp_path: Path) -> None:
    module = project(
        tmp_path,
        {1: decision()},
        {
            1: question(),
            2: question(status="deferred", until='"S2"', title='"Later?"'),
            3: question(status="deferred", until='"someday"'),
        },
    )
    loaded = load(tmp_path)
    text = module.render_decisions(loaded)
    assert "## Deferred questions\n\n- Q2 (until S2, `runs`): Later?\n" in text
    assert "| Q2 |" not in text
    found = module.problems(loaded, tmp_path, [])
    assert any("Q3: `until` is a step, a milestone" in p for p in found), found
    assert "- Q2 (deferred until S2): Later?\n" in module.design_blocks(loaded, "runs")["questions"]
    (tmp_path / "questions" / "Q2.toml").write_text(question(status="deferred"))
    with pytest.raises(module.RecordError, match="`until` is given exactly"):
        load(tmp_path)


def test_a_section_is_one_line(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision(section="'''\ntwo\nlines\n'''")})
    with pytest.raises(module.RecordError, match="`section` is one line"):
        load(tmp_path)


def test_a_code_symbol_must_be_in_its_file(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    (tmp_path / "a.rs").write_text("fn chained_run() {}\n")
    assert module.code_problem("D1", "a.rs::chained_run", tmp_path) == ""
    assert module.code_problem("D1", "a.rs", tmp_path) == ""
    assert module.code_problem("D1", "a.rs::chained", tmp_path) == "D1: a.rs has no `chained`"
    assert module.code_problem("D1", "b.rs::x", tmp_path) == "D1: b.rs doesn't exist"


def test_anchors_are_placed_under_their_headings(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    (tmp_path / "archive").mkdir()
    (tmp_path / "archive" / "PROJECT.md").write_text(
        '# P\n\n<a id="D1"></a>\n## Runs <a id="D2"></a>\n\n#### Deep\n\n<a id="D3"></a>\n'
    )
    assert module.anchor_places(tmp_path) == {
        "D1": ("archive/PROJECT.md", ""),
        "D2": ("archive/PROJECT.md", "Runs"),
        "D3": ("archive/PROJECT.md", "Runs"),
    }


def test_status_md_gets_the_next_step_and_open_questions(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision(step='"S2"')}, {1: question()})
    status = tmp_path / "STATUS.md"
    status.write_text(f"# Status\n\n{module.begin('status')}{module.end('status')}\nBy hand.\n")
    text = module.views(load(tmp_path), module.Paths(tmp_path))[status]
    assert "- **Milestone:** M1, First.\n" in text
    assert "- **Next step:** S2, Planned one (decisions D1). Done when: another passes\n" in text
    assert "- **Then:** S3.\n" in text
    assert "- **Open questions:** 1 (`DECISIONS.md`).\n" in text
    assert text.endswith("By hand.\n")


def test_check_finds_missing_code_and_anchors(tmp_path: Path) -> None:
    module = project(
        tmp_path,
        {
            1: decision(code='["here.rs", "gone.rs"]', history='"archive/PROJECT.md#D1"'),
            # A dropped decision's code may be gone.
            2: decision(status="dropped", code='["gone.rs"]', history='"archive/PROJECT.md#D2"'),
            3: decision(history='"JOURNAL.md#D3"'),
            4: decision(history='"JOURNAL.md#D4"'),
        },
    )
    (tmp_path / "here.rs").write_text("")
    (tmp_path / "archive").mkdir()
    (tmp_path / "archive/PROJECT.md").write_text('- <a id="D2"></a>**Two:** text.\n')
    (tmp_path / "JOURNAL.md").write_text('### <a id="D3"></a>2026-11-08: three\n')
    module_problems = module.problems(load(tmp_path), tmp_path, [])
    assert "D1: gone.rs doesn't exist" in module_problems
    assert 'D1: archive/PROJECT.md has no anchor <a id="D1"></a>' in module_problems
    assert 'D4: JOURNAL.md has no anchor <a id="D4"></a>' in module_problems
    assert not any(p.startswith(("D2", "D3")) for p in module_problems)


def test_mentions_read_lists_and_ranges_but_not_years() -> None:
    found = records().mentions(
        "decision 12; decisions 326-328 and 330, decision 38A'; decision 2026 was a year"
    )
    assert found == {12, 326, 327, 328, 330, 38, 2026}


def test_a_mention_past_the_last_decision_is_reported(tmp_path: Path) -> None:
    text = tmp_path / "PLAN.md"
    text.write_text("built in decision 9 and decision 12; the year 2026 (decision 2026)\n")
    assert records().unknown_mentions([text], 10) == ["PLAN.md: decision 12 has no file"]


def test_new_records_take_the_next_number(tmp_path: Path) -> None:
    module = project(tmp_path, {1: decision()})
    created = module.new_decision(tmp_path / "decisions", "a new one")
    assert created.name == "D2.toml"
    assert (tmp_path / "decisions" / "next-id").read_text() == "D3\n"
    assert 'history = "JOURNAL.md#D2"' in created.read_text()
    asked = module.new_question(tmp_path / "questions", "why?")
    assert asked.name == "Q1.toml"
    assert (tmp_path / "questions" / "next-id").read_text() == "Q2\n"


def test_the_topics_are_read_from_the_design_readme(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(README)
    assert records().topics(readme) == {"runs", "chained-runs"}


def test_this_repositorys_records_are_current() -> None:
    module = records()
    paths = module.Paths(ROOT)
    loaded = module.load(paths)
    stale = [
        str(p.relative_to(ROOT))
        for p, text in module.views(loaded, paths).items()
        if p.read_text() != text
    ]
    assert stale == [], "out of date: run `make records`"


def test_a_step_may_be_a_milestone_or_a_bug(tmp_path: Path) -> None:
    project(
        tmp_path,
        {1: decision(step='"M1"'), 2: decision(step='"B5"'), 3: decision(step='"B6"')},
    )
    (tmp_path / "bugs").mkdir()
    (tmp_path / "bugs" / "B5.toml").write_text("")
    loaded = load(tmp_path)
    assert [loaded.state(d) for d in loaded.decisions] == [
        "planned (M1)",
        "planned (B5)",
        "built (B6 fixed)",
    ]


def test_a_step_named_like_a_milestone_is_refused(tmp_path: Path) -> None:
    # schedr's release 0.1.0 was once milestone R0.1 with a first step R0.1: ambiguous.
    module = project(tmp_path, {1: decision()}, plan=PLAN.replace('id = "S3"', 'id = "M1"'))
    with pytest.raises(module.RecordError, match="M1 names both a milestone and a step"):
        load(tmp_path)
