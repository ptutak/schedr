use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn command(resource: Option<serde_json::Value>, output_type: &serde_json::Value) -> Task {
    let mut task = json!({
        "id": "cluster",
        "kind": "command",
        "command": {"argv": [[{"text": "./create.sh"}]]},
        "settings": serde_json::to_value(schedr_model::TaskSettings::default()).unwrap(),
        "output_type": output_type.clone(),
    });
    if let Some(role) = resource {
        task["resource"] = role;
    }
    serde_json::from_value(task).unwrap()
}

fn info_type() -> serde_json::Value {
    json!({"type": "typed_dict", "name": "Info", "fields": [
        {"name": "cluster_id", "type": {"type": "str"}, "required": true},
        {"name": "made", "type": {"type": "datetime"}, "required": false},
    ]})
}

fn setup(mask: Arc<MaskSet>) -> AttemptResource {
    let task = command(Some(json!({"role": "setup"})), &info_type());
    AttemptResource::of(&task, &BTreeMap::new(), mask).unwrap()
}

#[test]
fn only_a_commands_resource_part_has_one() {
    let plain = command(None, &json!({"type": "process_result"}));
    assert!(AttemptResource::of(&plain, &BTreeMap::new(), Arc::new(MaskSet::new(4))).is_none());
}

#[test]
fn a_setup_reports_its_value_once_stored_tagged_and_masked() {
    let mask = Arc::new(MaskSet::new(4));
    mask.add("token", "s3cr3t-id");
    let resource = setup(mask);
    assert_eq!(resource.outcome(), Some(Err(())), "nothing reported yet");
    resource
        .set(&json!({"cluster_id": "s3cr3t-id", "made": "2026-11-05T01:00:00Z"}))
        .unwrap();
    let Some(Ok(value)) = resource.outcome() else {
        panic!("reported");
    };
    assert_eq!(value["made"], json!({"$datetime": "2026-11-05T01:00:00Z"}));
    assert_ne!(value["cluster_id"], json!("s3cr3t-id"), "masked");
    assert_eq!(
        resource.set(&json!({"cluster_id": "b"})),
        Err(ResourceError::Again)
    );
    assert_eq!(resource.get(), Err(ResourceError::NotCleanup));
}

#[test]
fn a_value_of_another_type_is_refused_and_nothing_is_reported() {
    let resource = setup(Arc::new(MaskSet::new(4)));
    let misfit = resource.set(&json!({"cluster_id": 8})).unwrap_err();
    assert!(
        matches!(&misfit, ResourceError::Misfit(why) if why.contains("cluster_id")),
        "{misfit}"
    );
    assert!(matches!(
        resource.set(&json!({"made": "2026-11-05T01:00:00Z"})),
        Err(ResourceError::Misfit(_))
    ));
    assert_eq!(resource.outcome(), Some(Err(())));
}

#[test]
fn a_setup_without_a_value_reports_nothing_and_ends_with_null() {
    let task = command(Some(json!({"role": "setup"})), &json!({"type": "none"}));
    let resource = AttemptResource::of(&task, &BTreeMap::new(), Arc::new(MaskSet::new(4))).unwrap();
    assert_eq!(resource.set(&json!({})), Err(ResourceError::NoValue));
    assert_eq!(resource.outcome(), Some(Ok(serde_json::Value::Null)));
}

#[test]
fn a_cleanup_reads_its_setups_value_and_cant_report_one() {
    let task = command(
        Some(json!({"role": "cleanup", "setup": "cluster"})),
        &json!({"type": "process_result"}),
    );
    let value = json!({"cluster_id": "c-1"});
    let inputs = BTreeMap::from([("cluster".to_owned(), value.clone())]);
    let resource = AttemptResource::of(&task, &inputs, Arc::new(MaskSet::new(4))).unwrap();
    assert_eq!(resource.get(), Ok(value));
    assert_eq!(resource.set(&json!({})), Err(ResourceError::NotSetup));
    assert_eq!(resource.outcome(), None);

    let missing = AttemptResource::of(&task, &BTreeMap::new(), Arc::new(MaskSet::new(4))).unwrap();
    assert_eq!(missing.get(), Err(ResourceError::Missing));
    // A resource without a value, or an optional one that reported null: both read as null.
    let none = BTreeMap::from([("cluster".to_owned(), serde_json::Value::Null)]);
    let valueless = AttemptResource::of(&task, &none, Arc::new(MaskSet::new(4))).unwrap();
    assert_eq!(valueless.get(), Ok(serde_json::Value::Null));
}

#[test]
fn a_secret_read_after_the_report_is_masked_in_the_output() {
    let mask = Arc::new(MaskSet::new(4));
    let resource = setup(Arc::clone(&mask));
    resource.set(&json!({"cluster_id": "hunter22-pw"})).unwrap();
    mask.add("db#password", "hunter22-pw");
    let Some(Ok(value)) = resource.outcome() else {
        panic!("reported");
    };
    assert_ne!(value["cluster_id"], json!("hunter22-pw"));
}

#[test]
fn a_value_too_large_for_the_cleanup_to_read_is_refused() {
    let task = command(
        Some(json!({"role": "setup"})),
        &json!({"type": "list", "item": {"type": "date"}}),
    );
    let resource = AttemptResource::of(&task, &BTreeMap::new(), Arc::new(MaskSet::new(4))).unwrap();
    // Each date grows by its tag when stored: it fits the request, not the answer.
    let dates = vec![json!("2026-11-05"); 70_000];
    let request = serde_json::to_string(&dates).unwrap().len();
    assert!(request < MAX_VALUE_SIZE, "{request}");
    assert!(matches!(
        resource.set(&serde_json::Value::Array(dates)),
        Err(ResourceError::TooLarge(_))
    ));
    assert_eq!(resource.outcome(), Some(Err(())));
}
