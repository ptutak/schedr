use pretty_assertions::assert_eq;

use super::*;

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn a_new_data_directory_and_database_are_the_users_alone() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path().join("a/data");
    assert_eq!(data_dir(&data).unwrap(), None);
    assert_eq!(mode(&data), 0o700);
    let file = data.join("schedr.db");
    let url = format!("sqlite://{}", file.display());
    assert_eq!(database(&url).unwrap(), None);
    assert_eq!(mode(&file), 0o600);
    assert_eq!(std::fs::metadata(&file).unwrap().len(), 0);
}

#[test]
fn existing_ones_open_to_others_are_warned_of_and_left_as_they_are() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    chmod(&data, 0o755);
    let warned = data_dir(&data).unwrap().unwrap();
    assert!(
        warned.contains("mode 755") && warned.contains("chmod o-rwx"),
        "{warned}"
    );
    assert_eq!(mode(&data), 0o755);
    let file = data.join("schedr.db");
    std::fs::write(&file, b"").unwrap();
    chmod(&file, 0o644);
    let warned = database(&format!("sqlite:{}?mode=rwc", file.display()))
        .unwrap()
        .unwrap();
    assert!(warned.starts_with("the database"), "{warned}");
    assert_eq!(mode(&file), 0o644);
    // Group access is the deployment's (a volume's fsGroup): not warned of.
    chmod(&data, 0o770);
    assert_eq!(data_dir(&data).unwrap(), None);
}

#[test]
fn a_database_linked_to_nothing_is_warned_of() {
    let dir = tempfile::TempDir::new().unwrap();
    let link = dir.path().join("schedr.db");
    std::os::unix::fs::symlink(dir.path().join("missing.db"), &link).unwrap();
    let warned = database(&format!("sqlite://{}", link.display()))
        .unwrap()
        .unwrap();
    assert!(warned.contains("symbolic link to nothing"), "{warned}");
}

#[test]
fn only_a_sqlite_file_is_a_file() {
    assert_eq!(
        sqlite_file("sqlite:///d/schedr.db"),
        Some("/d/schedr.db".into())
    );
    assert_eq!(sqlite_file("sqlite:rel.db?mode=rwc"), Some("rel.db".into()));
    assert_eq!(sqlite_file("sqlite::memory:"), None);
    assert_eq!(sqlite_file("sqlite://x.db?mode=memory&cache=shared"), None);
    assert_eq!(sqlite_file("postgres://u@h/db"), None);
    assert_eq!(database("postgres://u@h/db").unwrap(), None);
}

#[test]
fn a_secret_file_open_to_others_is_named() {
    let dir = tempfile::TempDir::new().unwrap();
    let users = dir.path().join("users");
    std::fs::write(&users, b"ana:$2b$").unwrap();
    chmod(&users, 0o604);
    let token = dir.path().join("token");
    std::fs::write(&token, b"t").unwrap();
    chmod(&token, 0o600);
    let warned = secret_files(&[
        ("[webserver.auth] users_file", Some(&users)),
        ("[webserver] ui_token_file", Some(&token)),
        ("[webserver] ui_token_file", None),
    ]);
    assert_eq!(warned.len(), 1);
    assert!(
        warned[0].starts_with("[webserver.auth] users_file"),
        "{}",
        warned[0]
    );
}
