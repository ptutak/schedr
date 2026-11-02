# Run parameters

A DAG can declare a class of values a run is given. Every task of that run sees the same
values, and they are known before any task starts. A scheduled run uses the defaults. A
manual run, `schedr dags test`, or `POST /api/v1/dags/{dag_id}/runs` can give other values.
Those are checked before the run exists. A refusal says why, and there is no run.

```{literalinclude} ../../examples/docs/params.py
:language: python
:caption: examples/docs/params.py
```

## Declaring them

`params=` takes a dataclass or a Pydantic model **class** (`pip install
schedr[pydantic]`, Pydantic 2.11 or newer, before 3). An instance, or anything else, is a
`TypeError`: ``DAG `report`: params must be a dataclass or a Pydantic model class``.

Importing the file writes three things into the DAG JSON: the class's type, a JSON Schema,
and the defaults. `schedr dags check` and the master both check that schema
([What the master checks](#what-the-master-checks)). A DAG that leaves `params=` off has
no parameters. Giving it some is refused: ``the DAG takes no parameters (it declares no
`params=`)``.

A DAG with a trigger needs a default on every field. `Cron`, `Every` and `Once` all count:
their scheduled runs use the defaults. The import fails with

```
DAG `report`: params: `ReportParams.name` has no default, but the DAG has a trigger: its scheduled runs use the defaults; give every field a default
```

The master's wording of the same rule is ``params: `name` has no default, but the DAG has
a schedule (its runs use the defaults)``. A DAG with `trigger=None` may leave fields
required. A manual run then has to supply them.

`field(default_factory=...)` runs once, when the file is imported, and that value is the
default. A Pydantic default is read with `model_construct`, so a default that breaks a
constraint such as `min_length` still imports. The master checks each default against its
field's schema. One that does not fit fails as `InvalidDag` (`params: the defaults: ...`),
and on a server the file's DAGs are held
([Parsing](../03-guide/06-parsing.md#a-held-dag)).

The defaults, as compact JSON, may be at most 64 KiB. Past that the import fails: ``the
defaults are N bytes as JSON, over the limit of 64 KiB (they travel with every task)``.
The same limit applies to the values of one run, defaults included.

## Field types

A field has the types a task may return
([What a task may return](02-tasks.md#what-a-task-may-return)), except `ProcessResult`.
`set` and `bytes` are refused the same way. A dataclass that refers to itself is refused.
`Literal` is an `enum`. `tuple[A, B]` is a JSON array of that length. A nested dataclass
refuses extra keys. A nested Pydantic model uses the schema Pydantic wrote.

| Field | How a trigger gives it |
|---|---|
| `str`, `bool`, `int`, `float` | JSON of that type. `-p name=text` is always text, so a bool or a number uses `name:=json` (`full:=true`, `retries:=2`). |
| `date` | Text, `YYYY-MM-DD`: `-p day=2026-10-01`. |
| `datetime` | Text, ISO 8601 with an offset: `-p start=2026-10-01T03:00:00Z`. |
| `timedelta` on a dataclass | Text, ISO 8601 in weeks, days, hours, minutes and seconds, fractions included (`PT1H`, `PT0.5S`, `P1DT2H`). Years and months are refused (their lengths vary). One hour is shown later as `P0DT3600.000000S`. |
| `timedelta` on a Pydantic model | Text with `format: duration` (`PT1H0.5S`), checked by the same rule: ISO 8601, not in years or months. The text is kept as given. The model parses it when a task reads the model. |
| `date \| None`, and the same for the others | The value, or `null` (`day:=null`). |
| `list[int]`, `dict[str, str]` | A JSON array or object (`limits:=[1,2]`). |

A dataclass's dates, datetimes, durations and tuples are stored tagged, the way a task
output is (`{"$date": "2026-10-01"}`, `{"$timedelta": 3600000000}`). A Pydantic model is
stored as the JSON its schema accepts, so a date stays the text Pydantic uses. A task that
reads the model gets Pydantic's parsing
([Pydantic models](03-data-flow.md#pydantic-models)).

Constraints that the schema carries are checked before the run exists: `minLength`,
`minimum`, `pattern`, `format`, `enum`, `required`, and the other keywords
[below](#what-the-master-checks). A Pydantic `field_validator` or `model_validator` is not
in that schema. It runs when a task decodes the model: `dag_ctx.params(ReportParams)`, a
field read inside the task (that decodes the whole model), or an argument whose type is
the model. A validator that refuses the value fails that task. The run already exists.
Passing one field into an argument of a plain type (`region: str`) does not run the
model's validators.

## Reading them

| | In the DAG body | Inside a running task |
|---|---|---|
| `dag_ctx.params.region` | A reference. Pass it to a task, or put it in a t-string. The path is checked against the class when the file loads. | The value. `dag_ctx.params["region"]` is the same. |
| `dag_ctx.params.limits[0]` | A reference to that element. A tuple index past its length fails the import. A list index is the element's type; the length is the run's. | Ordinary indexing of the value. `dag_ctx.params[0]` raises `TypeError` (`dag_ctx.params is indexed by field name`): index a field, not the parameters. |
| `dag_ctx.params(ReportParams)` | A reference to all of them, typed by that class. | The instance, decoded into the class. |
| `if dag_ctx.params.full`, `str(...)`, an f-string, `for` | `TypeError`: ``a reference can't be used as a value in the DAG file (it is known only at run time); pass it to a task, or decide in a task``. | The value. |
| Anywhere else, including a test with no setup | `RuntimeError` naming `schedr.testing.task_context(...)`. | |

A missing field fails the import: ``DAG `report`: `dag_ctx.params.zone` doesn't exist: the DAG's params are `ReportParams` ``.
A DAG that declares none fails the import and names `Dag(..., params=SomeDataclass)`.
Another class fails with `` `dag_ctx.params(Other)`: the DAG's params are `ReportParams`; pass that class ``.
A part of the parameters isn't callable (`` `dag_ctx.params.region` isn't callable ``).

For type checkers a field is `Any`. `dag_ctx.params(ReportParams)` is that class
([Type checkers](09-type-checkers.md#how-a-dag-body-reads)). The example annotates the
locals it passes on.

A command reads a field with a t-string: `t"{dag_ctx.params.region}"`, when the field is
`str`, `bool`, `int`, `float`, `date`, `datetime`, `timedelta` or `Any`. A `Literal` is
refused at import (``a value of type `Literal['eu', 'us']` can't be rendered into text``):
pass it to a task, or declare the field `str`. A dataclass date or datetime takes a format
spec (`t"{dag_ctx.params.start:%Y-%m-%d}"`). A dataclass duration renders as seconds
(`3600` for one hour), and a format spec does not apply to it. `None`, a list and an
object can't be rendered. A date a Pydantic model holds is text, so a format spec does not
apply to it ([Pydantic models](03-data-flow.md#pydantic-models)). A t-string fills the
command before it starts. While the command runs, `schedr dag-ctx params get` prints
these values in the JSON form `--params` takes, and `from schedr.script import dag_ctx`
decodes them in a Python script the command starts
([Reading the run from a command](06-context.md#reading-the-run-from-a-command)).

The list `.expand()` maps over cannot be a parameter. The number of instances would depend
on the run:

```
a task can't be mapped over the run's params (the number of instances can't depend on them); map over a task's output instead
```

One element of a list written in the file may be a parameter:
`.expand(name=[dag_ctx.params.region, "us"])`
([Mapping](10-mapping.md#mapping-a-task)).

## Giving values to a run

A scheduled run is stored with the defaults. It has no command.

A manual run overlays what you give onto the defaults:

```
schedr runs trigger report -p region=us -p full:=true -p 'limits:=[1,2]' -p day=2026-10-01 -p window=PT1H
schedr runs trigger report --params '{"region": "us", "full": true}'
```

| Form | What it is |
|---|---|
| `-p` / `--param`, repeatable | One field. `name=text` is always a JSON string (`full=true` is the text `true`, not a bool). `name:=json` is any JSON value (`full:=true`, `limits:=[1,2]`, `day:=null`). |
| `--params '{...}'` | One JSON object, in place of `-p`. The two can't be combined. |
| Neither | The defaults. `--params '{}'` is the same. |

A field with no `=`, an empty name, a name given twice, or JSON that doesn't read is a
usage error, before the master. The error names the field by its name, or by its number
when it has no name, and never quotes the value (it may be a secret, mistyped):
``error: -p #2: give `name=text` or `name:=json` ``. `--params` that isn't an object says
so the same way.

`POST /api/v1/dags/{dag_id}/runs` takes the same values in the body, next to the moment:

```json
{"planned_for": "2026-10-15T03:00:00Z", "params": {"region": "us", "full": true}}
```

Leave `params` out for the defaults. `schedr dags test` takes the same `-p` and
`--params` ([Testing a DAG](07-testing.md#schedr-dags-test)).

The webserver stores the command and does not check the values. The master checks them
while it applies the command, fills in every default you left out, and inserts the run
only when they fit. A refusal marks the command `rejected`. The CLI prints it and exits 1:

```
error: rejected: DAG `report`: unknown parameter `nope` (the DAG takes `day`, `full`, `limits`, `region`, `start`, `window`)
```

There is no run. The same check refuses:

- values for a DAG that declares no `params=`
- a name that isn't a field, including when the model's schema would allow extra keys. A nested object follows its schema
- a value that doesn't fit the schema or the type (a bool where an int is required, a date that isn't one, a duration in months)
- text containing NUL (`\u0000`), in a value or a key: no store keeps it
- more than 64 KiB of compact JSON, defaults included: ``the parameters are N bytes as JSON, over the limit of 64 KiB``

A reason never quotes a value, nor a value the schema holds (a `const`, an `enum`): it
says where, and what failed: a missing field (`` `name` is required ``), unknown fields,
the expected type (`` `full`: not of type `boolean` ``), the format
(`` `day`: not of the format `date` ``), or the keyword (`` `region`: doesn't fit the
schema's `enum` ``). Names in it are masked as texts (an address keeps its password
hidden). Under a field whose name has a secret word, not even the keyword is given:
`` `api_key`: doesn't fit its schema (the value isn't shown) ``
([Secrets](../03-guide/12-ui.md#secrets)). A request body that isn't JSON of the right shape
is refused without quoting its text (`invalid type: string "…"`).

Masking covers the values a run is given. Text written in the DAG's code (a default, a
`Literal`'s values, a field's name) is code: it is shown as written where code is shown,
such as a task's output type, an import error or `schedr dags check`. Don't write a secret
in a DAG file; give it when the run is triggered.

### The Trigger dialog

The [Trigger dialog](../03-guide/12-ui.md#actions) on the DAG's page has an input for each
field, filled in with its default. A DAG that takes none has no inputs there, and the run
uses the defaults. The inputs sit under **Parameters**. Under them the dialog says "Fields
left as they are keep their defaults." A dataclass's fields are in the order they are
declared. A model's are by name.

| Field | The input |
|---|---|
| `bool` | A check box. Unchecked is false. |
| `int` | A number. A fraction is refused, `1.5` and `1.0` alike: `` `name` isn't a whole number ``. The CLI and the API may send `1.0` (`count:=1.0`); the master stores it as `1`. |
| `float` | A number. Text that is not a number is refused: `` `name` isn't a number ``. |
| `str` | Text. |
| `date` | A date. |
| `datetime` | Text with an offset (`2026-10-15T03:00:00+02:00`). |
| `timedelta` | Text, an ISO 8601 duration (`PT1H30M`). |
| `Literal`, or a model's `enum` or `const` | A list of the choices. When a choice mixes text with a number or a bool, each is labelled as JSON, so `1` and `"1"` stay apart. With no default the list starts on "choose". A value that is not one of them is refused: `` `name` isn't one of its choices ``. |
| a nested dataclass or model, a list, a tuple, a dict, or a union such as `int \| str` | A JSON box, filled in with the default as JSON, the form `--params` takes. JSON that does not parse is refused. The reason names the line and the column: `` `name` isn't JSON (line N, column N) ``. |

`date | None` has that field's input and a box labelled "none" beside it, and the same for
`bool`, `int`, `float`, `str`, `datetime`, `timedelta` and `Literal`. The box starts
checked when the default is `null`. Checked, the value is `null` and the input beside it
is ignored. A `Literal` that includes `None` works this way: `null` is not one of the
choices. A JSON field that may be `null` (`list[int] | None`, a nested model or `None`)
keeps the JSON box and has no "none" box. A `null` default leaves the box empty, and
leaving it empty keeps that default. To send `null`, write `null` in the box.

A secret field is an empty password box, in place of the input in the table. A field is
secret when a word of its name names a secret, or when its default or a choice holds one
(a password in an address, or a nested field with such a name). The words are the ones a
task output uses ([Secrets](../03-guide/12-ui.md#secrets)): `password`, `api_key`,
`apiKey`, `clientSecret` and `access_token` are secret, and `partition_key` is shown. The
default and the choices never reach the page. An optional box says "leave empty for the
default"; left empty, the master uses the default. An empty required one is left out, and
the master says the field is required. Text in the box is sent. For a secret `int`,
`float`, `bool`, choice or JSON value, text that reads as JSON is that JSON (`5432` a
number, `true` a bool); other text is sent as text and the master checks it. A secret
`str`, `date`, `datetime` or duration is the text as typed. When the field has a "none"
box, the secret one does too. It starts unchecked: the default is not on the page.
Checking it sends `null`.

Confirm sends a field only when its value differs from the default the page was given. The
page has no default for a secret, so any text in that box is sent. An empty number, date,
datetime, duration, choice or JSON box is left out. The master fills in the rest and
checks the values as it checks any trigger, earlier in this section. Its refusal is the
result at the top of the page: **rejected**, with the master's reason
([Actions](../03-guide/12-ui.md#actions)). A value the form refuses is not sent. The result
says "not sent", and why. The reason names the field and what is wrong, and never quotes
what was typed.

The browser asks for a required field that is not a `bool` and has no "none" box. A
required check box can stay unchecked: that is false. Checking "none" answers a field that
has that box.

A required text can't be given as empty. It has no default, so an empty box is left out
and the master says the field is required. An empty text box on a field that has a default
is the empty string, when "none" is unchecked. The CLI (`-p name=`) and the API
(`"name": ""`) can send an empty string for a required text.

A page that showed parameters, loaded before those fields changed, is refused. The result
says "not sent", with this reason:

```
the DAG's parameters changed since this page was loaded: reload it and trigger again
```

The fields have changed when one was added or dropped, or its kind changed, or whether it
is required, whether it may be `null`, whether it is secret, its choices, or its default:
a box still showing the old default would otherwise send it as if it were chosen. A secret
field's default isn't on the page, so a change of it alone doesn't refuse the page: its
empty box sends nothing, and the master uses the current default. A page that showed no
parameters sends none. The master fills in the defaults, and refuses a field that is now
required.

`GET /api/v1/dags/{dag_id}` carries these fields as `params`. `GET /api/v1/dags` leaves
`params` out. The key is absent when the DAG takes none. Each field has:

| Key | What it is |
|---|---|
| `name` | The field's name, as a trigger's values name it. |
| `kind` | `bool`, `int`, `float`, `text`, `date`, `datetime`, `duration`, `choice` or `json`. |
| `required` | Whether a trigger must give it: it has no default, and the schema lists it as required. |
| `nullable` | Whether it may be `null` (the "none" box). False for a JSON field, even when the type allows `null`. |
| `secret` | Whether the default and the choices are withheld. |
| `default` | The default as JSON. Present as `null` when `null` is the default. Absent when there is no default, and when `secret` is true. |
| `choices` | A `choice`'s values, as JSON. Absent when there are none, and when `secret` is true. `null` is not an entry: the field is `nullable`. |

The values that were accepted stay with the run. A queued run keeps the ones stored when
it was created. A later version of the DAG does not replace them. Every task of the run is
sent those values.

## What the master checks

`schedr dags check` and the master, after a file has imported, check the schema with JSON
Schema 2020-12. A problem is `InvalidDag`, prefixed with the DAG's ID (`error  report.py:
InvalidDag: DAG `report`: params: ...`). On a server that holds the file's DAGs
([Parsing](../03-guide/06-parsing.md#a-held-dag)).

A keyword the checker doesn't evaluate is refused:

```
params: the schema's keyword `contentMediaType` (at `/properties/name`) isn't one the master checks; remove it, or check it in the task
```

The keywords it checks are `type`, `enum`, `const`, `multipleOf`, `maximum`,
`exclusiveMaximum`, `minimum`, `exclusiveMinimum`, `maxLength`, `minLength`, `pattern`,
`maxItems`, `minItems`, `uniqueItems`, `maxContains`, `minContains`, `maxProperties`,
`minProperties`, `required`, `dependentRequired`, `allOf`, `anyOf`, `oneOf`, `not`, `if`,
`then`, `else`, `dependentSchemas`, `prefixItems`, `items`, `contains`, `properties`,
`patternProperties`, `additionalProperties`, `propertyNames`, `unevaluatedItems`,
`unevaluatedProperties`, `format`, `$ref`, `$defs` and `definitions`.

These only describe, and are allowed: `$schema`, `$comment`, `title`, `description`,
`default`, `examples`, `deprecated`, `readOnly`, `writeOnly`, `discriminator`. `$schema`,
when present, has to name draft 2020-12.

A `format` has to be one of `date`, `date-time`, `time`, `duration`, `email`, `hostname`,
`ipv4`, `ipv6`, `uri`, `uri-reference`, `iri`, `iri-reference`, `uri-template`, `uuid`,
`json-pointer`, `relative-json-pointer`, `regex`. There is no `idn-*` format. An unknown
format names itself and that list. `duration` accepts fractional seconds (`PT0.5S`).

A `$ref` has to start with `#`. The master fetches nothing.

The same pass checks a scheduled DAG's missing default, each default against its field,
every `dag_ctx.params` path a task uses, and a task mapped over the parameters.

## How a run shows them

`schedr runs show` prints a line when the run has parameters. A string is `name=text`.
Any other JSON value is `name:=json`:

```
params: full:=true limits:=[1,2] region=us
```

An empty object is left out. `GET /api/v1/runs/{run_id}` returns `params` as the JSON a
trigger would give (tags turned back into text), and `{}` when the DAG takes none.
`GET /api/v1/runs/{run_id}/dag`, the DAG the run started with, holds only the
declaration's defaults, masked as the values are: its schema and type, which can hold
copies of values (a `Literal`'s, a schema's `default`), are left out.

The [run page](../03-guide/12-ui.md#a-run) lists **Params** when there are any: `name =
value`, text as itself, any other value as JSON. A run with none leaves the line out.

What is shown is masked the same way as a task output: a field whose name has a whole word
naming a secret (`password`, `api_key`, `apiKey`, `clientSecret`, `access_token`) is
`****`, and an address shows its password as `****`. An empty string or null is left as it
is. `partition_key` is shown. The rules are [Secrets](../03-guide/12-ui.md#secrets). The
tasks receive the values as stored.
