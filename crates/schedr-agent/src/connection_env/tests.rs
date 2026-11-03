use std::io::Read as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use pretty_assertions::assert_eq;
use serde_json::{Map, Value, json};

use super::*;

const DIR: &str = "/run/c/DB";

fn values(json: Value) -> Map<String, Value> {
    match json {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

fn of(kind: ConnectionType, json: Value) -> View {
    view(Some(kind), &values(json), Path::new(DIR)).unwrap()
}

fn refused(kind: Option<ConnectionType>, json: Value) -> String {
    view(kind, &values(json), Path::new(DIR)).unwrap_err()
}

#[test]
fn a_postgres_url_gets_no_password_verified_tls_and_a_pgpass_file() {
    let view = of(
        ConnectionType::Sql,
        json!({"url": "postgresql://db.example.com:5432/etl", "user": "etl",
               "password": "p@ss:w\\rd"}),
    );
    assert_eq!(
        view.variables["URL"],
        "postgresql://etl@db.example.com:5432/etl?sslmode=verify-full&sslrootcert=system&\
         passfile=%2Frun%2Fc%2FDB%2Fpgpass"
    );
    assert_eq!(view.variables["USER"], "etl");
    assert_eq!(view.variables["PASSWORD"], "p@ss:w\\rd");
    assert_eq!(view.variables["PGPASSFILE"], "/run/c/DB/pgpass");
    assert_eq!(view.files["pgpass"], "*:*:*:*:p@ss\\:w\\\\rd\n");
    assert!(!view.variables["URL"].contains("p@ss"));
}

#[test]
fn a_password_in_the_url_moves_to_the_file_and_the_urls_own_tls_is_kept() {
    let view = of(
        ConnectionType::Sql,
        json!({"url": "postgres://etl:p%40ss%2Fw@db/etl?sslmode=disable",
               "ca_cert": "PEM"}),
    );
    assert_eq!(
        view.variables["URL"],
        "postgres://etl@db/etl?sslmode=disable&sslrootcert=%2Frun%2Fc%2FDB%2Fca.pem&\
         passfile=%2Frun%2Fc%2FDB%2Fpgpass"
    );
    assert_eq!(view.variables["PASSWORD"], "p@ss/w");
    assert_eq!(view.files["pgpass"], "*:*:*:*:p@ss/w\n");
    assert_eq!(view.files["ca.pem"], "PEM");
    assert_eq!(view.variables["CA_FILE"], "/run/c/DB/ca.pem");
}

#[test]
fn verify_ca_without_a_ca_names_no_root_cert() {
    let view = of(
        ConnectionType::Sql,
        json!({"url": "postgresql://db/etl?sslmode=verify-ca"}),
    );
    assert_eq!(
        view.variables["URL"],
        "postgresql://db/etl?sslmode=verify-ca"
    );
    assert!(!view.files.contains_key("pgpass"));
}

#[test]
fn a_mysql_connection_gets_an_option_file() {
    let view = of(
        ConnectionType::Sql,
        json!({"url": "mysql://[::1]:3307/shop?sslmode=verify-ca", "user": "app",
               "password": "a\"b\\c", "ca_cert": "PEM", "timeout": 2.5}),
    );
    assert_eq!(
        view.files["my.cnf"],
        "[client]\nloose-ssl-mode = VERIFY_CA\nssl-ca = \"/run/c/DB/ca.pem\"\nprotocol = TCP\n\
         host = \"::1\"\nport = 3307\nuser = \"app\"\npassword = \"a\\\"b\\\\c\"\n\
         connect-timeout = 3\n\
         \n[client-mariadb]\nssl\nssl-verify-server-cert\n\
         \n[mysql]\ndatabase = \"shop\"\n"
    );
    assert_eq!(view.variables["HOST"], "::1");
    assert_eq!(view.variables["PORT"], "3307");
    assert_eq!(view.variables["DATABASE"], "shop");
    assert_eq!(view.variables["MYSQL_CNF"], "/run/c/DB/my.cnf");
    assert_eq!(
        view.variables["URL"],
        "mysql://app@[::1]:3307/shop?sslmode=verify-ca"
    );
}

#[test]
fn mysql_verifies_against_the_systems_cas_by_default() {
    let view = of(ConnectionType::Sql, json!({"url": "mysql://db"}));
    let cnf = &view.files["my.cnf"];
    assert!(cnf.contains("port = 3306\n"), "{cnf}");
    assert!(cnf.contains("loose-ssl-mode = VERIFY_IDENTITY\nssl-capath = \"/etc/ssl/certs\"\n"));
    assert!(
        cnf.ends_with("[client-mariadb]\nssl\nssl-verify-server-cert\n"),
        "{cnf}"
    );
    assert!(!cnf.contains("[mysql]"));
    let view = of(
        ConnectionType::Sql,
        json!({"url": "mariadb://db?sslmode=prefer"}),
    );
    let cnf = &view.files["my.cnf"];
    assert!(cnf.contains("loose-ssl-mode = PREFERRED\n"), "{cnf}");
    assert!(
        cnf.ends_with("[client-mariadb]\nssl\nskip-ssl-verify-server-cert\n"),
        "{cnf}"
    );
    assert!(!cnf.contains("ssl-ca"));
    let view = of(
        ConnectionType::Sql,
        json!({"url": "mysql://db?sslmode=disable"}),
    );
    let cnf = &view.files["my.cnf"];
    assert!(cnf.contains("loose-ssl-mode = DISABLED\n"), "{cnf}");
    assert!(cnf.ends_with("[client-mariadb]\nskip-ssl\n"), "{cnf}");
}

#[test]
fn a_user_or_password_given_twice_or_another_scheme_is_refused() {
    let twice = refused(
        Some(ConnectionType::Sql),
        json!({"url": "postgresql://a:pw@db/x", "password": "pw"}),
    );
    assert!(twice.contains("password"), "{twice}");
    let twice = refused(
        Some(ConnectionType::Mongo),
        json!({"uri": "mongodb://a@db", "user": "b", "database": "x"}),
    );
    assert!(twice.contains("user"), "{twice}");
    let other = refused(Some(ConnectionType::Sql), json!({"url": "sqlite:///x.db"}));
    assert!(other.contains("sqlite"), "{other}");
}

#[test]
fn mongo_gets_tls_a_uri_without_credentials_and_a_tools_config() {
    let view = of(
        ConnectionType::Mongo,
        json!({"uri": "mongodb://etl:s3%3Acret@h1:27017,h2:27017/?replicaSet=rs0",
               "database": "orders", "ca_cert": "PEM"}),
    );
    assert_eq!(
        view.variables["URI"],
        "mongodb://h1:27017,h2:27017/orders?replicaSet=rs0&authSource=admin&tls=true&\
         tlsCAFile=%2Frun%2Fc%2FDB%2Fca.pem"
    );
    assert_eq!(view.variables["USER"], "etl");
    assert_eq!(view.variables["PASSWORD"], "s3:cret");
    assert_eq!(view.variables["DATABASE"], "orders");
    assert_eq!(
        view.files["mongo-tools.yaml"],
        "uri: \"mongodb://etl@h1:27017,h2:27017/orders?replicaSet=rs0&authSource=admin&\
         tls=true&tlsCAFile=%2Frun%2Fc%2FDB%2Fca.pem\"\npassword: \"s3:cret\"\n"
    );
    assert_eq!(view.variables["TOOLS_CONFIG"], "/run/c/DB/mongo-tools.yaml");
}

#[test]
fn mongos_own_tls_setting_is_kept() {
    let view = of(
        ConnectionType::Mongo,
        json!({"uri": "mongodb://localhost:27017", "database": "x", "user": "u",
               "password": "p\"w\n"}),
    );
    assert_eq!(
        view.variables["URI"],
        "mongodb://localhost:27017/x?authSource=admin&tls=true"
    );
    assert_eq!(
        view.files["mongo-tools.yaml"],
        "uri: \"mongodb://u@localhost:27017/x?authSource=admin&tls=true\"\n\
         password: \"p\\\"w\\n\"\n"
    );
    let view = of(
        ConnectionType::Mongo,
        json!({"uri": "mongodb+srv://c.example.net/?TLS=false", "database": "x"}),
    );
    assert_eq!(
        view.variables["URI"],
        "mongodb+srv://c.example.net/x?TLS=false"
    );
    assert!(!view.variables.contains_key("PASSWORD"));
}

#[test]
fn http_gets_a_curl_config() {
    let view = of(
        ConnectionType::Http,
        json!({"base_url": "https://api.example.com", "token": "t\"k",
               "headers": {"X-Team": "etl"}, "ca_cert": "CA", "client_cert": "C",
               "client_key": "K", "timeout": 0.2}),
    );
    assert_eq!(
        view.files["curl.config"],
        "header = \"Authorization: Bearer t\\\"k\"\nheader = \"X-Team: etl\"\n\
         cacert = \"/run/c/DB/ca.pem\"\ncert = \"/run/c/DB/client-cert.pem\"\n\
         key = \"/run/c/DB/client-key.pem\"\nconnect-timeout = 1\n"
    );
    assert_eq!(view.variables["BASE_URL"], "https://api.example.com");
    assert_eq!(view.variables["TOKEN"], "t\"k");
    assert_eq!(view.variables["CURL_CONFIG"], "/run/c/DB/curl.config");
    assert_eq!(view.variables["KEY_FILE"], "/run/c/DB/client-key.pem");
    let basic = of(
        ConnectionType::Http,
        json!({"base_url": "http://x", "user": "u", "password": "p:w"}),
    );
    assert_eq!(basic.files["curl.config"], "user = \"u:p:w\"\n");
    assert_eq!(basic.variables["PASSWORD"], "p:w");
}

#[test]
fn ssh_gets_a_config_that_trusts_only_its_known_hosts() {
    let view = of(
        ConnectionType::Ssh,
        json!({"host": "bastion%1", "user": "deploy", "port": 2222,
               "private_key": "KEY", "known_hosts": "[bastion]:2222 ssh-ed25519 AAA",
               "timeout": 10}),
    );
    assert_eq!(
        view.files["ssh_config"],
        "Host target\n  HostName \"bastion%%1\"\n  Port \"2222\"\n  User \"deploy\"\n\
         \x20 IdentityFile \"/run/c/DB/id_key\"\n  IdentitiesOnly yes\n\
         \x20 UserKnownHostsFile \"/run/c/DB/known_hosts\"\n  GlobalKnownHostsFile /dev/null\n\
         \x20 StrictHostKeyChecking yes\n  BatchMode yes\n  ConnectTimeout 10\n"
    );
    assert_eq!(view.files["id_key"], "KEY\n");
    assert_eq!(view.variables["HOST"], "bastion%1");
    assert_eq!(view.variables["PORT"], "2222");
    assert_eq!(view.variables["SSH_CONFIG"], "/run/c/DB/ssh_config");
}

#[test]
fn ssh_with_a_password_may_prompt_and_insecure_takes_any_key() {
    let view = of(
        ConnectionType::Ssh,
        json!({"host": "h", "user": "u", "password": "pw",
               "insecure_skip_host_check": true}),
    );
    let config = &view.files["ssh_config"];
    assert!(config.contains("Port \"22\""), "{config}");
    assert!(config.contains("StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n"));
    assert!(!config.contains("BatchMode"));
    assert!(!view.files.contains_key("known_hosts"));
    assert_eq!(view.variables["PASSWORD"], "pw");
}

#[test]
fn ssh_values_an_ssh_config_cant_hold_are_refused() {
    let quote = refused(
        Some(ConnectionType::Ssh),
        json!({"host": "h\"x", "user": "u", "known_hosts": "k"}),
    );
    assert!(quote.contains("`host`"), "{quote}");
    let line = refused(
        Some(ConnectionType::Ssh),
        json!({"host": "h", "user": "u\nProxyCommand x", "known_hosts": "k"}),
    );
    assert!(line.contains("`user`"), "{line}");
}

#[test]
fn own_connections_get_one_variable_per_value_and_all_as_json() {
    let view = view(
        None,
        &values(json!({"dsn": "x://y", "pool": 3, "tags": ["a"], "none": null})),
        Path::new(DIR),
    )
    .unwrap();
    assert_eq!(view.variables["DSN"], "x://y");
    assert_eq!(view.variables["POOL"], "3");
    assert_eq!(view.variables["TAGS"], "[\"a\"]");
    assert_eq!(view.variables["NONE"], "null");
    assert_eq!(view.variables["JSON"], "/run/c/DB/connection.json");
    assert_eq!(
        serde_json::from_str::<Value>(&view.files["connection.json"]).unwrap(),
        json!({"dsn": "x://y", "pool": 3, "tags": ["a"], "none": null})
    );
    let clash = refused(Some(ConnectionType::Generic), json!({"a": 1, "A": 2}));
    assert!(clash.contains("`A`") || clash.contains("`a`"), "{clash}");
    let json = refused(None, json!({"json": 1}));
    assert!(json.contains("`json`"), "{json}");
    let name = refused(None, json!({"a-b": 1}));
    assert!(name.contains("`a-b`"), "{name}");
}

#[test]
fn written_files_are_the_owners_only_and_the_variables_prefixed() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = folder(tmp.path(), "DB");
    let view = view(
        Some(ConnectionType::Sql),
        &values(json!({"url": "postgresql://db/etl", "password": "pw"})),
        &dir,
    )
    .unwrap();
    let variables = write(&view, "DB", &dir).unwrap();
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&dir.join("pgpass")), 0o600);
    assert_eq!(
        std::fs::read_to_string(&variables["DB_PGPASSFILE"]).unwrap(),
        "*:*:*:*:pw\n"
    );
    assert_eq!(variables["DB_PASSWORD"], "pw");
    assert!(variables.keys().all(|name| name.starts_with("DB_")));
}

#[test]
fn percent_decoding_keeps_what_isnt_an_escape() {
    assert_eq!(decoded("a%40b%2"), "a@b%2");
    assert_eq!(decoded("%zz%4"), "%zz%4");
    assert_eq!(decoded("%E2%82%AC"), "€");
}

#[test]
fn the_files_go_in_memory_by_default_in_a_folder_per_data_directory() {
    let shm = tempfile::tempdir().unwrap();
    let one = default_dir(Path::new("/srv/a"), shm.path(), 1000);
    let other = default_dir(Path::new("/srv/b"), shm.path(), 1000);
    assert_eq!(one.parent().unwrap(), shm.path().join("schedr-1000"));
    assert_ne!(one, other, "two data directories share a folder");
    assert_eq!(one, default_dir(Path::new("/srv/a"), shm.path(), 1000));
    // No memory file system: the data directory.
    assert_eq!(
        default_dir(Path::new("/srv/a"), &shm.path().join("none"), 1000),
        Path::new("/srv/a/connections")
    );
}

#[test]
fn prepare_makes_the_folders_the_users_own_and_refuses_others() {
    let shm = tempfile::tempdir().unwrap();
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    let dir = shm.path().join("schedr-x/abc/worker");
    prepare(&dir).unwrap();
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(dir.parent().unwrap()), 0o700);
    // Again: what exists is fine.
    prepare(&dir).unwrap();
    // A folder others can read, made first.
    let open = shm.path().join("open/worker");
    std::fs::create_dir_all(&open).unwrap();
    std::fs::set_permissions(
        shm.path().join("open"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let error = prepare(&open).unwrap_err();
    assert!(
        error.contains("mode 0700") && error.contains("755"),
        "{error}"
    );
    // A link to a folder elsewhere, made first.
    let elsewhere = shm.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    std::fs::create_dir(shm.path().join("linked")).unwrap();
    std::fs::set_permissions(
        shm.path().join("linked"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    std::os::unix::fs::symlink(&elsewhere, shm.path().join("linked/worker")).unwrap();
    let error = prepare(&shm.path().join("linked/worker")).unwrap_err();
    assert!(error.contains("is a link"), "{error}");
}

#[test]
fn prepare_refuses_a_folder_above_others_can_write_in() {
    // Codex's finding 1: `/dev/shm/schedr-<uid>` made first by another user (here: left
    // open to all), its private child swapped afterwards.
    let shm = tempfile::tempdir().unwrap();
    let top = shm.path().join("schedr-x");
    std::fs::create_dir(&top).unwrap();
    std::fs::set_permissions(&top, std::fs::Permissions::from_mode(0o777)).unwrap();
    let error = prepare(&top.join("abc/worker")).unwrap_err();
    assert!(
        error.contains("another user") && error.contains("777"),
        "{error}"
    );
    // Sticky, as `/dev/shm` itself: fine.
    std::fs::set_permissions(&top, std::fs::Permissions::from_mode(0o1777)).unwrap();
    prepare(&top.join("abc/worker")).unwrap();
    // A link on the way that isn't root's.
    let real = shm.path().join("real");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, shm.path().join("via")).unwrap();
    let error = prepare(&shm.path().join("via/abc/worker")).unwrap_err();
    assert!(error.contains("is a link"), "{error}");
}

#[test]
fn a_file_made_first_is_replaced_never_reused() {
    // Codex's findings 1 and 7: a file someone made there (mode 0644) keeps its mode when
    // reopened, and a reader of a file being rewritten sees it empty.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("c");
    let planted = dir.join("DB/curl.config");
    std::fs::create_dir_all(planted.parent().unwrap()).unwrap();
    std::fs::write(&planted, "").unwrap();
    std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o644)).unwrap();
    let held = std::fs::File::open(&planted).unwrap();
    let view = of(
        ConnectionType::Http,
        json!({"base_url": "https://h", "token": "tk"}),
    );
    write(&view, "DB", &dir.join("DB")).unwrap();
    let mode = std::fs::metadata(&planted).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    assert!(
        std::fs::read_to_string(&planted)
            .unwrap()
            .contains("Bearer tk")
    );
    // The one opened before still reads the old file, not a truncated new one.
    let mut old = String::new();
    (&held).read_to_string(&mut old).unwrap();
    assert_eq!(old, "");
    let names: Vec<_> = std::fs::read_dir(dir.join("DB"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(names.iter().all(|n| !n.starts_with('.')), "{names:?}");
}

#[test]
fn a_password_in_a_url_never_stays_in_a_variable() {
    // Codex's finding 2: libpq's `password=` query key; userinfo in an HTTP base URL.
    let view = of(
        ConnectionType::Sql,
        json!({"url": "postgresql://etl@db/etl?password=query-secret&sslmode=require"}),
    );
    assert!(
        !view.variables["URL"].contains("query-secret"),
        "{}",
        view.variables["URL"]
    );
    assert_eq!(view.variables["PASSWORD"], "query-secret");
    assert_eq!(view.files["pgpass"], "*:*:*:*:query-secret\n");
    let twice = refused(
        Some(ConnectionType::Sql),
        json!({"url": "postgresql://db/etl?password=a", "password": "b"}),
    );
    assert!(twice.contains("once"), "{twice}");
    let view = of(
        ConnectionType::Http,
        json!({"base_url": "https://etl:http%40secret@api.example/v1"}),
    );
    assert_eq!(view.variables["BASE_URL"], "https://api.example/v1");
    assert_eq!(view.variables["USER"], "etl");
    assert_eq!(view.variables["PASSWORD"], "http@secret");
    assert_eq!(view.files["curl.config"], "user = \"etl:http@secret\"\n");
    let twice = refused(
        Some(ConnectionType::Http),
        json!({"base_url": "https://a:b@h", "user": "c"}),
    );
    assert!(twice.contains("once"), "{twice}");
}

#[test]
fn forms_the_masker_doesnt_know_are_recorded_with_their_source() {
    // Codex's finding 3: a password taken out of a URL, escaped for `.pgpass` or my.cnf.
    let view = of(
        ConnectionType::Sql,
        json!({"url": "postgresql://etl:extracted@db/etl", "user": null}),
    );
    assert!(view.derived.contains(&(
        "postgresql://etl:extracted@db/etl".to_owned(),
        "extracted".to_owned()
    )));
    let view = of(
        ConnectionType::Sql,
        json!({"url": "postgresql://db/etl", "password": "hunter:two"}),
    );
    assert!(
        view.derived
            .contains(&("hunter:two".to_owned(), "hunter\\:two".to_owned()))
    );
    let view = of(
        ConnectionType::Sql,
        json!({"url": "mysql://db/etl", "password": "a\"b"}),
    );
    assert!(
        view.derived
            .contains(&("a\"b".to_owned(), "a\\\"b".to_owned()))
    );
}

#[test]
fn a_line_break_curl_or_pgpass_would_split_is_refused() {
    // Codex's findings 4 and 8: a header injected through a token; a password cut short.
    for json in [
        json!({"base_url": "https://h", "token": "ok\r\nX-Injected: yes"}),
        json!({"base_url": "https://h", "headers": {"X-A": "a\nX-B: b"}}),
        json!({"base_url": "https://h", "user": "u\n", "password": "p"}),
    ] {
        let error = refused(Some(ConnectionType::Http), json);
        assert!(error.contains("line break"), "{error}");
    }
    let error = refused(
        Some(ConnectionType::Sql),
        json!({"url": "postgresql://db/x", "password": "first\nsecond"}),
    );
    assert!(error.contains("line break"), "{error}");
}

#[test]
fn mongo_names_its_database_and_where_the_user_logs_in() {
    // Codex's finding 6: the database reaches the tools' URI; the login stays on `admin`.
    let view = of(
        ConnectionType::Mongo,
        json!({"uri": "mongodb://db/?tls=false", "database": "orders", "user": "u",
               "password": "p"}),
    );
    assert_eq!(
        view.variables["URI"],
        "mongodb://db/orders?tls=false&authSource=admin"
    );
    assert_eq!(view.variables["AUTH_SOURCE"], "admin");
    assert!(
        view.files["mongo-tools.yaml"]
            .starts_with("uri: \"mongodb://u@db/orders?tls=false&authSource=admin\"")
    );
    // A path's database is where the user logs in, as the drivers take it.
    let view = of(
        ConnectionType::Mongo,
        json!({"uri": "mongodb://u:p@db/users?tls=false", "database": "orders"}),
    );
    assert_eq!(view.variables["AUTH_SOURCE"], "users");
    assert_eq!(
        view.variables["URI"],
        "mongodb://db/orders?tls=false&authSource=users"
    );
}

#[test]
fn a_nul_or_a_secret_in_a_urls_query_is_refused() {
    // Grok's findings 1 and 4: a NUL ends an option file's value and skips the next line (the
    // TLS mode); `?sslpassword=` and MySQL's `?password=` would be on a command line.
    let error = refused(
        Some(ConnectionType::Sql),
        json!({"url": "mysql://etl@db/shop", "password": "s3cret\u{0}junk"}),
    );
    assert!(error.contains("NUL"), "{error}");
    let error = refused(
        Some(ConnectionType::Sql),
        json!({"url": "postgresql://db/x", "password": "a\u{0}b"}),
    );
    assert!(error.contains("NUL"), "{error}");
    for (kind, json) in [
        (
            ConnectionType::Sql,
            json!({"url": "mysql://db/shop?password=visible"}),
        ),
        (
            ConnectionType::Sql,
            json!({"url": "postgresql://db/x?sslpassword=visible"}),
        ),
        (
            ConnectionType::Mongo,
            json!({"uri": "mongodb://db/?tlsCertificateKeyFilePassword=visible", "database": "x"}),
        ),
    ] {
        let error = refused(Some(kind), json);
        assert!(error.contains("command line"), "{error}");
    }
}
