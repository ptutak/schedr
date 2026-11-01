use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::Field;

fn field(name: &str, ty: TypeDesc, required: bool) -> Field {
    Field {
        name: name.to_owned(),
        ty,
        required,
    }
}

/// `ReportParams` of PROJECT.md: region, full, day, limits, plus a window, a pair and a map.
fn report() -> TypeDesc {
    TypeDesc::Dataclass {
        name: "ReportParams".to_owned(),
        fields: vec![
            field(
                "region",
                TypeDesc::Literal {
                    values: vec![json!("eu"), json!("us")],
                },
                false,
            ),
            field("full", TypeDesc::Bool, false),
            field(
                "day",
                TypeDesc::Union {
                    variants: vec![TypeDesc::Date, TypeDesc::None],
                },
                false,
            ),
            field(
                "limits",
                TypeDesc::List {
                    item: Box::new(TypeDesc::Int),
                },
                false,
            ),
            field("window", TypeDesc::Timedelta, false),
            field("at", TypeDesc::Datetime, false),
            field(
                "pair",
                TypeDesc::Tuple {
                    items: vec![TypeDesc::Str, TypeDesc::Float],
                },
                false,
            ),
            field(
                "extra",
                TypeDesc::Dict {
                    value: Box::new(TypeDesc::Any),
                },
                false,
            ),
            field("name", TypeDesc::Str, true),
        ],
    }
}

#[test]
fn values_given_as_json_are_stored_as_outputs_are() {
    let given = json!({
        "region": "us", "full": true, "day": "2026-10-01", "limits": [1, 2],
        "window": "P1DT2H0.5S", "at": "2026-10-01T03:00:00+02:00", "pair": ["a", 1],
        "extra": {"x": {"$date": "not a tag"}}, "name": "n",
    });
    assert_eq!(
        to_stored(&given, &report()).unwrap(),
        json!({
            "region": "us", "full": true, "day": {"$date": "2026-10-01"}, "limits": [1, 2],
            "window": {"$timedelta": 93_600_500_000_i64},
            "at": {"$datetime": "2026-10-01T03:00:00+02:00"},
            "pair": {"$tuple": ["a", 1]},
            "extra": {"x": {"$dict": {"$date": "not a tag"}}}, "name": "n",
        })
    );
}

#[test]
fn shown_values_are_json_again() {
    let stored = json!({
        "day": {"$date": "2026-10-01"}, "window": {"$timedelta": -93_600_500_000_i64},
        "at": {"$datetime": "2026-10-01T03:00:00Z"}, "pair": {"$tuple": ["a", {"$date": "2026-01-02"}]},
        "extra": {"$dict": {"$date": "kept"}}, "plain": {"a": 1},
    });
    assert_eq!(
        shown(&stored),
        json!({
            "day": "2026-10-01", "window": "-P1DT7200.500000S", "at": "2026-10-01T03:00:00Z",
            "pair": ["a", "2026-01-02"], "extra": {"$date": "kept"}, "plain": {"a": 1},
        })
    );
}

#[test]
fn a_shown_duration_is_read_back_to_the_same_microseconds() {
    for micros in [0_i64, 1, -1, 93_600_500_000, -86_400_000_001] {
        let text = shown(&json!({"$timedelta": micros}));
        assert_eq!(
            to_stored(&text, &TypeDesc::Timedelta).unwrap(),
            json!({"$timedelta": micros}),
            "{text}"
        );
    }
}

#[rstest]
#[case::bad_date(json!({"name": "n", "day": "2026-13-01"}), "`day`: text fits none of its types")]
#[case::naive_time(json!({"name": "n", "at": "2026-10-01T03:00:00"}), "`at`: not a date and time with an offset")]
#[case::months(json!({"name": "n", "window": "P1M"}), "`window`: a duration in years or months")]
#[case::list_item(json!({"name": "n", "limits": [1, "2"]}), "`limits[1]`: expected an integer, got text")]
#[case::tuple_length(json!({"name": "n", "pair": ["a"]}), "`pair`: expected 2 elements, got 1")]
#[case::unknown(json!({"name": "n", "nope": 1}), "unknown field `nope`")]
#[case::required(json!({"region": "eu"}), "`name` is required")]
#[case::literal(json!({"name": "n", "region": "asia"}), "`region`: expected one of its values, got text")]
fn values_that_dont_fit_are_refused_saying_where(#[case] given: Value, #[case] message: &str) {
    let error = to_stored(&given, &report()).unwrap_err().to_string();
    assert!(error.starts_with(message), "{error}");
}

#[test]
fn an_int_is_not_a_bool_nor_a_float_an_int_param() {
    assert!(to_stored(&json!(true), &TypeDesc::Int).is_err());
    assert!(to_stored(&json!(1.5), &TypeDesc::Int).is_err());
    assert_eq!(to_stored(&json!(2), &TypeDesc::Float).unwrap(), json!(2));
}

#[test]
fn a_models_values_are_kept_as_json_tags_escaped() {
    let model = TypeDesc::Model {
        name: "M".to_owned(),
        schema: json!({}),
    };
    assert_eq!(
        to_stored(&json!({"day": "2026-10-01", "d": {"$tuple": 1}}), &model).unwrap(),
        json!({"day": "2026-10-01", "d": {"$dict": {"$tuple": 1}}})
    );
}

#[test]
fn a_whole_number_written_with_a_fraction_is_an_int() {
    assert_eq!(to_stored(&json!(3.0), &TypeDesc::Int).unwrap(), json!(3));
    assert!(to_stored(&json!(3.5), &TypeDesc::Int).is_err());
}

#[test]
fn a_week_is_seven_days() {
    assert_eq!(
        to_stored(&json!("P1W"), &TypeDesc::Timedelta).unwrap(),
        json!({"$timedelta": 7 * 86_400_000_000_i64})
    );
}

#[test]
fn a_misfit_names_the_fields_along_its_path() {
    let misfit = Misfit {
        path: vec![
            Step::Key("config".to_owned()),
            Step::Key("api.key".to_owned()),
            Step::Index(1),
        ],
        why: "x".to_owned(),
    };
    assert_eq!(misfit.fields().collect::<Vec<_>>(), ["config", "api.key"]);
    assert_eq!(misfit.to_string(), "`config.api.key[1]`: x");
}

#[rstest]
#[case::text(json!("a\u{0}b"), &[][..])]
#[case::nested(json!({"x": [1, "a\u{0}"]}), &[Step::Key("x".to_owned()), Step::Index(1)][..])]
#[case::key(json!({"a\u{0}": 1}), &[][..])]
fn text_containing_nul_is_refused_saying_where(#[case] value: Value, #[case] path: &[Step]) {
    let misfit = to_stored(&value, &TypeDesc::Any).unwrap_err();
    assert_eq!(misfit.path, path);
    assert!(misfit.why.contains("NUL"), "{misfit}");
}

#[rstest]
#[case::bad("an hour")]
#[case::friendly("1h")]
#[case::months("P1M")]
fn a_refused_duration_isnt_quoted(#[case] text: &str) {
    let misfit = to_stored(&json!(text), &TypeDesc::Timedelta).unwrap_err();
    assert!(!misfit.to_string().contains(text), "{misfit}");
}
