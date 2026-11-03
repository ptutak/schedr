//! What a command gets of a connection (decisions 184, 190-192): `PREFIX_*` variables, and
//! the files its tools read (curl's `-K` config, an SSH key and config, Postgres's
//! `.pgpass`, a MySQL option file), written in the attempt's folder.
//!
//! A secret goes into its tool's file and into a variable for scripts; a URL variable never
//! holds a password (`psql "$DB_URL"` puts none on a command line). What a command gets
//! verifies TLS as the Python clients do (decision 191): SQL's `sslmode=verify-full`,
//! Mongo's `tls=true`, SSH only its `known_hosts`. Only prefixed names (decision 192).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{DirBuilder, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use schedr_model::Encoding;
use schedr_model::connection::ConnectionType;
use serde_json::{Map, Value};

#[cfg(test)]
mod tests;

/// A connection as a command gets it, before it is written.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct View {
    /// The variables, by name after `PREFIX_` (`URL`, `PGPASSFILE`).
    pub variables: BTreeMap<String, String>,
    /// The files, by name in the connection's folder, with their contents.
    pub files: BTreeMap<String, String>,
    /// Forms of secrets the masker doesn't know, each with what it came from: a password
    /// taken out of a URL, one escaped for a tool's file (`.pgpass`'s `\:`). Masked when what
    /// it came from is.
    pub derived: Vec<(String, String)>,
}

impl View {
    /// Records `form`, written from `source`, for the masker.
    fn derive(&mut self, source: &str, form: &str) {
        if form != source && !form.is_empty() {
            self.derived.push((source.to_owned(), form.to_owned()));
        }
    }

    fn var(&mut self, suffix: &str, value: impl Into<String>) {
        self.variables.insert(suffix.to_owned(), value.into());
    }

    /// Adds the file `name` and the variable `suffix` holding its path.
    fn file(&mut self, dir: &Path, suffix: &str, name: &str, contents: String) -> String {
        let path = dir.join(name).to_string_lossy().into_owned();
        self.files.insert(name.to_owned(), contents);
        self.var(suffix, path.clone());
        path
    }
}

/// The view of a connection of type `kind` (a `@connection` function's: `None`), its values
/// rendered and shown as plain JSON, its files to go in `dir`.
///
/// # Errors
///
/// A value a tool's file can't hold (a quote in an SSH config's value), a password given
/// twice, a parameter that can't be a variable's name.
pub fn view(
    kind: Option<ConnectionType>,
    values: &Map<String, Value>,
    dir: &Path,
) -> Result<View, String> {
    let mut view = View::default();
    match kind {
        Some(ConnectionType::Http) => http(values, dir, &mut view)?,
        Some(ConnectionType::Ssh) => ssh(values, dir, &mut view)?,
        Some(ConnectionType::Mongo) => mongo(values, dir, &mut view)?,
        Some(ConnectionType::Sql) => sql(values, dir, &mut view)?,
        Some(ConnectionType::Generic) | None => own(values, &mut view)?,
    }
    let json = Value::Object(values.clone()).to_string();
    view.file(dir, "JSON", "connection.json", json);
    Ok(view)
}

/// Writes `view`'s files in `dir` (mode 0700; each file 0600), and gives its variables, named
/// `PREFIX_...`.
///
/// # Errors
///
/// A file that can't be written.
pub fn write(view: &View, prefix: &str, dir: &Path) -> Result<BTreeMap<String, String>, String> {
    let failed =
        |e: std::io::Error| format!("can't write a connection file in {}: {e}", dir.display());
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(failed)?;
    for (name, contents) in &view.files {
        // Written aside, then renamed over: a process reading the file (the same connection
        // asked for again) reads the old one or the new one whole, and a file someone made
        // there first is replaced, never reused with its own mode.
        let aside = dir.join(format!(".{name}.{}", unique()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&aside)
            .map_err(failed)?;
        let written = file
            .write_all(contents.as_bytes())
            .and_then(|()| std::fs::rename(&aside, dir.join(name)));
        if let Err(e) = written {
            let _ = std::fs::remove_file(&aside);
            return Err(failed(e));
        }
    }
    Ok(view
        .variables
        .iter()
        .map(|(suffix, value)| (format!("{prefix}_{suffix}"), value.clone()))
        .collect())
}

/// A name no other write in this process uses: the process and a counter.
fn unique() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{}-{n}", std::process::id())
}

/// Where a worker writes its commands' connection files when `[worker] connection_dir`
/// isn't set (decision 201): in memory, under `shm` (`/dev/shm`) when it is a folder, in a
/// folder of the user's own named after the data directory (so workers of one user sharing
/// an ID, a standalone's and a worker's, never sweep each other's); else the data directory.
#[must_use]
pub fn default_dir(data_dir: &Path, shm: &Path, uid: u32) -> PathBuf {
    if shm.is_dir() {
        use sha2::Digest as _;
        let digest = sha2::Sha256::digest(data_dir.as_os_str().as_encoded_bytes());
        let mut name = String::new();
        for byte in &digest[..8] {
            let _ = write!(name, "{byte:02x}");
        }
        shm.join(format!("schedr-{uid}")).join(name)
    } else {
        data_dir.join("connections")
    }
}

/// Makes `dir` (a worker's connection folder) and the folder holding it, mode 0700, before
/// the worker starts (decision 201), and checks the whole path. In a folder others can write
/// in (`/dev/shm`), another user could have made a folder on it first, or a link, and swap
/// what is below: the secrets would go where they can read them. So every folder on the path
/// must be the user's or root's, closed to others' writes unless sticky (`/dev/shm`, `/tmp`),
/// and to group writes unless the group is the user's own; a link on it must be root's
/// (`/var/run`); the last two folders the user's alone, mode 0700.
///
/// # Errors
///
/// A folder that can't be made, or a path someone else could change.
pub fn prepare(dir: &Path) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt as _;
    let uid = rustix::process::geteuid().as_raw();
    let gid = rustix::process::getegid().as_raw();
    let parent = dir.parent().ok_or_else(|| {
        format!(
            "{}: a connection folder has a folder above it",
            dir.display()
        )
    })?;
    if let Some(above) = parent.parent() {
        std::fs::create_dir_all(above)
            .map_err(|e| format!("can't make {}: {e}", above.display()))?;
    }
    for folder in [parent, dir] {
        match DirBuilder::new().mode(0o700).create(folder) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(format!("can't make {}: {e}", folder.display())),
        }
    }
    let refuse = |path: &Path, why: &str| {
        Err(format!(
            "{} {why}: connection files (secrets) can't go below it; remove it, or set \
             [worker] connection_dir",
            path.display()
        ))
    };
    let mut walked = PathBuf::new();
    for component in dir.components() {
        walked.push(component);
        let meta = std::fs::symlink_metadata(&walked)
            .map_err(|e| format!("can't read {}: {e}", walked.display()))?;
        let private = walked == parent || walked == dir;
        let mode = meta.mode();
        if meta.file_type().is_symlink() {
            if private || meta.uid() != 0 {
                return refuse(&walked, "is a link another user could have made");
            }
            continue;
        }
        if !meta.file_type().is_dir() {
            return refuse(&walked, "isn't a folder");
        }
        if private {
            if meta.uid() != uid || mode & 0o077 != 0 {
                return refuse(
                    &walked,
                    &format!(
                        "must be the worker's user's alone, mode 0700 (it is uid {}, mode {:o})",
                        meta.uid(),
                        mode & 0o7777
                    ),
                );
            }
            continue;
        }
        let sticky = mode & 0o1000 != 0;
        let owned = meta.uid() == uid || meta.uid() == 0;
        let others_write = mode & 0o002 != 0 && !sticky;
        let group_write = mode & 0o020 != 0 && meta.gid() != gid && !sticky;
        if !owned || others_write || group_write {
            return refuse(
                &walked,
                &format!(
                    "can be changed by another user (uid {}, gid {}, mode {:o})",
                    meta.uid(),
                    meta.gid(),
                    mode & 0o7777
                ),
            );
        }
    }
    Ok(())
}

/// The folder of the connection given as `prefix` in an attempt's folder.
#[must_use]
pub fn folder(attempt_dir: &Path, prefix: &str) -> PathBuf {
    attempt_dir.join(prefix)
}

/// A value as text: text as it is, a number or a flag as JSON; `None` when absent or null.
fn text(values: &Map<String, Value>, key: &str) -> Option<String> {
    match values.get(key)? {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

fn required(values: &Map<String, Value>, key: &str) -> Result<String, String> {
    text(values, key).ok_or_else(|| format!("`{key}` has no value"))
}

/// A timeout's whole seconds, at least 1, as tools take it.
fn seconds(values: &Map<String, Value>) -> Option<u64> {
    let timeout = values.get("timeout")?.as_f64()?;
    // Seconds as a tool takes them, rounded up; a timeout is never negative or huge.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "rounded, clamped"
    )]
    Some(timeout.ceil().clamp(1.0, 86_400.0) as u64)
}

/// A curl config value's inside: `\` and `"` escaped.
fn curl_escaped(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A curl config value: double-quoted. A line break is refused: curl turns `\n` back into
/// one, and in a header that is another header (`token\r\nX-Admin: yes`).
fn curl_quoted(what: &str, text: &str) -> Result<String, String> {
    if text.contains(['\n', '\r', '\0']) {
        return Err(format!(
            "`{what}` has a line break: curl's config would send it as more of the request"
        ));
    }
    Ok(format!("\"{}\"", curl_escaped(text)))
}

/// A URL with its user and password taken out (`https://u:p@h/x` gives `https://h/x`, `u`,
/// `p`), decoded.
fn without_userinfo(url: &str) -> (String, Option<String>, Option<String>) {
    let Some((scheme, rest)) = url.split_once("://") else {
        return (url.to_owned(), None, None);
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let Some(at) = rest[..end].rfind('@') else {
        return (url.to_owned(), None, None);
    };
    let (user, password) = match rest[..at].split_once(':') {
        Some((user, password)) => (decoded(user), Some(decoded(password))),
        None => (decoded(&rest[..at]), None),
    };
    let user = Some(user).filter(|user| !user.is_empty());
    (format!("{scheme}://{}", &rest[at + 1..]), user, password)
}

fn http(values: &Map<String, Value>, dir: &Path, view: &mut View) -> Result<(), String> {
    let source = required(values, "base_url")?;
    // A user and a password in the URL go to curl's config, never a command line.
    let (base_url, url_user, url_password) = without_userinfo(&source);
    let mut user = text(values, "user");
    let mut password = text(values, "password");
    if url_user.is_some() || url_password.is_some() {
        if user.is_some() || password.is_some() || values.get("token").is_some_and(|t| !t.is_null())
        {
            return Err(
                "a user in `base_url` and `user=`, `password=` or `token=`: give it once"
                    .to_owned(),
            );
        }
        if let Some(from_url) = &url_password {
            view.derive(&source, from_url);
        }
        user = url_user;
        password = url_password;
    }
    view.var("BASE_URL", base_url);
    let mut config = String::new();
    if let Some(token) = text(values, "token") {
        let header = curl_quoted("token", &format!("Authorization: Bearer {token}"))?;
        let _ = writeln!(config, "header = {header}");
        view.derive(&token, &curl_escaped(&token));
        view.var("TOKEN", token);
    }
    if let Some(user) = user {
        let given = password.clone().unwrap_or_default();
        let login = curl_quoted("user", &format!("{user}:{given}"))?;
        let _ = writeln!(config, "user = {login}");
        view.derive(&given, &curl_escaped(&given));
        view.var("USER", user);
        if let Some(password) = password {
            view.var("PASSWORD", password);
        }
    }
    if let Some(Value::Object(headers)) = values.get("headers") {
        for (name, value) in headers {
            let value = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            let header = curl_quoted(&format!("headers[{name:?}]"), &format!("{name}: {value}"))?;
            let _ = writeln!(config, "header = {header}");
            view.derive(&value, &curl_escaped(&value));
        }
    }
    if let Some(ca) = text(values, "ca_cert") {
        let path = view.file(dir, "CA_FILE", "ca.pem", ca);
        let _ = writeln!(config, "cacert = {}", curl_quoted("ca file", &path)?);
    }
    if let Some(cert) = text(values, "client_cert") {
        let path = view.file(dir, "CERT_FILE", "client-cert.pem", cert);
        let _ = writeln!(config, "cert = {}", curl_quoted("cert file", &path)?);
    }
    if let Some(key) = text(values, "client_key") {
        let path = view.file(dir, "KEY_FILE", "client-key.pem", key);
        let _ = writeln!(config, "key = {}", curl_quoted("key file", &path)?);
    }
    if let Some(seconds) = seconds(values) {
        let _ = writeln!(config, "connect-timeout = {seconds}");
    }
    view.file(dir, "CURL_CONFIG", "curl.config", config);
    Ok(())
}

/// An SSH config value: double-quoted (spaces kept), `%` written `%%` where `escape` (the
/// keywords OpenSSH expands tokens in). OpenSSH has no escape for `"` or a line break.
fn ssh_value(what: &str, text: &str, escape: bool) -> Result<String, String> {
    if text.contains(['"', '\n', '\r', '\0']) {
        return Err(format!(
            "`{what}` can't be written in an SSH config (a quote or a line break in it)"
        ));
    }
    let text = if escape {
        text.replace('%', "%%")
    } else {
        text.to_owned()
    };
    Ok(format!("\"{text}\""))
}

fn ssh(values: &Map<String, Value>, dir: &Path, view: &mut View) -> Result<(), String> {
    let host = required(values, "host")?;
    let user = required(values, "user")?;
    let port = text(values, "port").unwrap_or_else(|| "22".to_owned());
    let mut config = String::from("Host target\n");
    let _ = writeln!(config, "  HostName {}", ssh_value("host", &host, true)?);
    let _ = writeln!(config, "  Port {}", ssh_value("port", &port, false)?);
    let _ = writeln!(config, "  User {}", ssh_value("user", &user, false)?);
    view.var("HOST", host);
    view.var("PORT", port);
    view.var("USER", user);
    if let Some(mut key) = text(values, "private_key") {
        // OpenSSH refuses a key without its last line break ("invalid format").
        if !key.ends_with('\n') {
            key.push('\n');
        }
        let path = view.file(dir, "KEY_FILE", "id_key", key);
        let _ = writeln!(
            config,
            "  IdentityFile {}",
            ssh_value("key file", &path, true)?
        );
        config.push_str("  IdentitiesOnly yes\n");
    }
    if let Some(passphrase) = text(values, "passphrase") {
        view.var("KEY_PASSPHRASE", passphrase);
    }
    let password = text(values, "password");
    let insecure = values
        .get("insecure_skip_host_check")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if insecure {
        // As the definition says, loudly: any key is taken.
        config.push_str("  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n");
    } else {
        let known = required(values, "known_hosts")?;
        let path = view.file(dir, "KNOWN_HOSTS", "known_hosts", known);
        let _ = writeln!(
            config,
            "  UserKnownHostsFile {}",
            ssh_value("known_hosts file", &path, true)?
        );
        // Only its own keys: the machine's own aren't the connection's.
        config.push_str("  GlobalKnownHostsFile /dev/null\n  StrictHostKeyChecking yes\n");
    }
    if password.is_none() {
        // Nothing to type: a prompt would hang the task.
        config.push_str("  BatchMode yes\n");
    }
    if let Some(password) = password {
        view.var("PASSWORD", password);
    }
    if let Some(seconds) = seconds(values) {
        let _ = writeln!(config, "  ConnectTimeout {seconds}");
    }
    view.file(dir, "SSH_CONFIG", "ssh_config", config);
    Ok(())
}

/// A URL's parts, as far as a command's view needs them; the authority may list several
/// hosts (`mongodb://h1:27017,h2:27017/`), which URL parsers refuse.
struct UrlParts {
    scheme: String,
    user: Option<String>,
    password: Option<String>,
    hosts: String,
    /// From the `/` after the hosts to the query, `/` included (empty when none).
    path: String,
    /// The query's pairs, decoded, in order.
    query: Vec<(String, String)>,
}

impl UrlParts {
    fn parse(url: &str) -> Result<Self, String> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| "the URL has no `scheme://`".to_owned())?;
        let rest = rest.split('#').next().unwrap_or_default();
        let (before_query, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (authority, path) = match before_query.find('/') {
            Some(at) => before_query.split_at(at),
            None => (before_query, ""),
        };
        let (userinfo, hosts) = match authority.rfind('@') {
            Some(at) => (Some(&authority[..at]), &authority[at + 1..]),
            None => (None, authority),
        };
        let (user, password) = match userinfo {
            None => (None, None),
            Some(info) => match info.split_once(':') {
                Some((user, password)) => (Some(decoded(user)), Some(decoded(password))),
                None => (Some(decoded(info)), None),
            },
        };
        let query = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (decoded(key), decoded(value))
            })
            .collect();
        Ok(Self {
            scheme: scheme.to_owned(),
            user: user.filter(|user| !user.is_empty()),
            password,
            hosts: hosts.to_owned(),
            path: path.to_owned(),
            query,
        })
    }

    fn has(&self, key: &str) -> bool {
        self.query.iter().any(|(k, _)| k.eq_ignore_ascii_case(key))
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    }

    /// The URL again, with the user only (never a password), every value encoded.
    fn without_password(&self) -> String {
        let mut url = format!("{}://", self.scheme);
        if let Some(user) = &self.user {
            url.push_str(&Encoding::Url.apply(user));
            url.push('@');
        }
        url.push_str(&self.hosts);
        url.push_str(&self.path);
        for (i, (key, value)) in self.query.iter().enumerate() {
            if i == 0 && self.path.is_empty() {
                url.push('/');
            }
            url.push(if i == 0 { '?' } else { '&' });
            url.push_str(&Encoding::Url.apply(key));
            url.push('=');
            url.push_str(&Encoding::Url.apply(value));
        }
        url
    }
}

/// Percent-decoded text (`%40` an `@`); a `%` not followed by two hex digits kept.
fn decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| char::from(b).to_digit(16);
        if bytes[i] == b'%'
            && let (Some(high), Some(low)) = (
                bytes.get(i + 1).copied().and_then(hex),
                bytes.get(i + 2).copied().and_then(hex),
            )
        {
            // Two hex digits: one byte.
            #[expect(clippy::cast_possible_truncation, reason = "two hex digits fit a byte")]
            out.push((high * 16 + low) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The user and the password, each from its own value or the URL's, given once.
fn credentials(
    values: &Map<String, Value>,
    source: &str,
    url: &mut UrlParts,
    view: &mut View,
) -> Result<(Option<String>, Option<String>), String> {
    let user = text(values, "user");
    let password = text(values, "password");
    if let Some(from_url) = &url.password {
        // Taken out of a URL a secret holds whole: the masker knows the URL, not it.
        view.derive(source, from_url);
    }
    if url.password.is_some() && password.is_some() {
        return Err("a password in the URL and `password=` both: give it once".to_owned());
    }
    if url.user.is_some() && user.is_some() {
        return Err("a user in the URL and `user=` both: give it once".to_owned());
    }
    if user.is_some() {
        url.user.clone_from(&user);
    }
    Ok((url.user.clone(), password.or_else(|| url.password.take())))
}

fn mongo(values: &Map<String, Value>, dir: &Path, view: &mut View) -> Result<(), String> {
    let source = required(values, "uri")?;
    let mut url = UrlParts::parse(&source)?;
    let (user, password) = credentials(values, &source, &mut url, view)?;
    url.password = None;
    refuse_secret_query(&url)?;
    let database = required(values, "database")?;
    // The database in the path, as the Python client's `client[database]`; a path's database
    // is also the one a user logs in on unless `authSource` says, so it is said: the URI's
    // own, else its path's, else `admin`, as pymongo takes it.
    let written = decoded(url.path.trim_start_matches('/'));
    let auth_source = url.get("authSource").map_or_else(
        || {
            if written.is_empty() {
                "admin".to_owned()
            } else {
                written.clone()
            }
        },
        str::to_owned,
    );
    if user.is_some() && !url.has("authSource") {
        url.query
            .push(("authSource".to_owned(), auth_source.clone()));
    }
    url.path = format!("/{}", Encoding::Url.apply(&database));
    // TLS, verified, unless the URI says otherwise (decision 191, as 189).
    if !url.has("tls") && !url.has("ssl") {
        url.query.push(("tls".to_owned(), "true".to_owned()));
    }
    if let Some(ca) = text(values, "ca_cert") {
        let path = view.file(dir, "CA_FILE", "ca.pem", ca);
        if !url.has("tlsCAFile") {
            url.query.push(("tlsCAFile".to_owned(), path));
        }
    }
    // The database tools (`mongodump --config`, decision 194) read the user from the URI and
    // the password from the file; mongosh would prompt for a user's password, hanging the
    // task, so its URI names none (it logs in with `db.auth` from the variables).
    let mut tools = format!("uri: {}\n", Value::from(url.without_password()));
    if let Some(password) = &password {
        let _ = writeln!(tools, "password: {}", Value::from(password.as_str()));
    }
    url.user = None;
    view.var("URI", url.without_password());
    view.file(dir, "TOOLS_CONFIG", "mongo-tools.yaml", tools);
    view.var("DATABASE", database);
    view.var("AUTH_SOURCE", auth_source);
    if let Some(user) = user {
        view.var("USER", user);
    }
    if let Some(password) = password {
        view.var("PASSWORD", password);
    }
    Ok(())
}

/// A `.pgpass` field: `\` and `:` escaped. A line break is refused: the file is one entry per
/// line, so libpq would read the password up to it.
fn pgpass_field(text: &str) -> Result<String, String> {
    if text.contains(['\n', '\r', '\0']) {
        return Err(
            "the password has a line break or a NUL, which `.pgpass` can't hold: read \
             `_PASSWORD` instead"
                .to_owned(),
        );
    }
    Ok(text.replace('\\', "\\\\").replace(':', "\\:"))
}

/// A MySQL option file value's inside: `\` and `"` escaped, a line break `\n`, `\r`.
fn cnf_escaped(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// A MySQL option file value: double-quoted. A NUL is refused: the clients end the value
/// there and skip the next line, which could be the TLS mode.
fn cnf_quoted(what: &str, text: &str) -> Result<String, String> {
    if text.contains('\0') {
        return Err(format!(
            "`{what}` has a NUL, which MySQL's option file can't hold"
        ));
    }
    Ok(format!("\"{}\"", cnf_escaped(text)))
}

/// Refuses a secret written as a URL's query key (`?sslpassword=`): a URL variable goes on
/// command lines, where anyone on the machine reads it.
fn refuse_secret_query(url: &UrlParts) -> Result<(), String> {
    const SECRET: [&str; 3] = ["password", "sslpassword", "tlsCertificateKeyFilePassword"];
    match url
        .query
        .iter()
        .find(|(key, _)| SECRET.iter().any(|s| key.eq_ignore_ascii_case(s)))
    {
        Some((key, _)) => Err(format!(
            "`{key}=` in the URL would be on a command's command line: give the password as \
             `password=` (or in the URL's user part)"
        )),
        None => Ok(()),
    }
}

fn sql(values: &Map<String, Value>, dir: &Path, view: &mut View) -> Result<(), String> {
    let source = required(values, "url")?;
    let mut url = UrlParts::parse(&source)?;
    let (user, mut password) = credentials(values, &source, &mut url, view)?;
    url.password = None;
    let scheme = url
        .scheme
        .split('+')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    // libpq takes a password as a query key too: out of the URL, as one in its user part.
    if matches!(scheme.as_str(), "postgresql" | "postgres")
        && let Some(at) = url.query.iter().position(|(k, _)| k == "password")
    {
        let (_, from_query) = url.query.remove(at);
        if password.is_some() {
            return Err("a password in the URL and `password=` both: give it once".to_owned());
        }
        view.derive(&source, &from_query);
        password = Some(from_query);
    }
    refuse_secret_query(&url)?;
    let mode = url.get("sslmode").unwrap_or("verify-full").to_owned();
    let ca = text(values, "ca_cert").map(|ca| view.file(dir, "CA_FILE", "ca.pem", ca));
    if let Some(user) = &user {
        view.var("USER", user.clone());
    }
    if let Some(password) = &password {
        view.var("PASSWORD", password.clone());
    }
    match scheme.as_str() {
        "postgresql" | "postgres" => {
            // TLS, verified, unless the URL says otherwise (decision 191, as 187).
            if !url.has("sslmode") {
                url.query.push(("sslmode".to_owned(), mode.clone()));
            }
            if !url.has("sslrootcert") {
                if let Some(ca) = &ca {
                    url.query.push(("sslrootcert".to_owned(), ca.clone()));
                } else if mode == "verify-full" {
                    url.query
                        .push(("sslrootcert".to_owned(), "system".to_owned()));
                }
            }
            if let Some(password) = &password {
                let field = pgpass_field(password)?;
                view.derive(password, &field);
                let path = view.file(dir, "PGPASSFILE", "pgpass", format!("*:*:*:*:{field}\n"));
                // libpq reads it from the URL: `psql "$DB_URL"` logs in, the URL naming the
                // file, never the password.
                if !url.has("passfile") {
                    url.query.push(("passfile".to_owned(), path));
                }
            }
        }
        "mysql" | "mariadb" => {
            let login = Login {
                user: user.as_deref(),
                password: password.as_deref(),
                ca: ca.as_deref(),
            };
            mysql(values, &url, &mode, &login, dir, view)?;
        }
        other => {
            return Err(format!(
                "the URL is postgresql://, mysql:// or mariadb://, not {other}://"
            ));
        }
    }
    view.var("URL", url.without_password());
    Ok(())
}

/// What logs in to a database, from its own values or the URL's.
struct Login<'a> {
    user: Option<&'a str>,
    password: Option<&'a str>,
    /// The path of the CA file written (`ca_cert`).
    ca: Option<&'a str>,
}

/// A MySQL or MariaDB database's option file, for either vendor's client (decision 195):
/// `[client]`, read by every program of both, holds the login and MySQL's TLS as
/// `loose-ssl-mode` (MariaDB's client skips it, with a warning); `[client-mariadb]`, read
/// only by MariaDB's programs, holds MariaDB's TLS.
fn mysql(
    values: &Map<String, Value>,
    url: &UrlParts,
    mode: &str,
    login: &Login<'_>,
    dir: &Path,
    view: &mut View,
) -> Result<(), String> {
    let (host, port) = host_and_port(&url.hosts, "3306");
    let database = decoded(url.path.trim_start_matches('/'));
    let (ssl_mode, mariadb) = match mode {
        "disable" => ("DISABLED", "skip-ssl\n"),
        // MariaDB's client has no "TLS if offered": it requires it.
        "allow" | "prefer" => ("PREFERRED", "ssl\nskip-ssl-verify-server-cert\n"),
        "require" => ("REQUIRED", "ssl\nskip-ssl-verify-server-cert\n"),
        // MariaDB's client checks the name with the certificate, always.
        "verify-ca" => ("VERIFY_CA", "ssl\nssl-verify-server-cert\n"),
        _ => ("VERIFY_IDENTITY", "ssl\nssl-verify-server-cert\n"),
    };
    // TLS first, the login after: a value a client misreads can't take the TLS mode with it.
    let mut cnf = String::from("[client]\n");
    let _ = writeln!(cnf, "loose-ssl-mode = {ssl_mode}");
    if ssl_mode.starts_with("VERIFY") {
        match login.ca {
            Some(ca) => {
                let _ = writeln!(cnf, "ssl-ca = {}", cnf_quoted("ca file", ca)?);
            }
            // The system's CAs, as the Python client's (MySQL's client verifies only
            // against a CA given; MariaDB's reads this too).
            None => cnf.push_str("ssl-capath = \"/etc/ssl/certs\"\n"),
        }
    }
    // Else `localhost` is the local socket, whatever the port: maybe another server.
    cnf.push_str("protocol = TCP\n");
    let _ = writeln!(cnf, "host = {}", cnf_quoted("host", &host)?);
    let _ = writeln!(cnf, "port = {port}");
    if let Some(user) = login.user {
        let _ = writeln!(cnf, "user = {}", cnf_quoted("user", user)?);
    }
    if let Some(password) = login.password {
        let _ = writeln!(cnf, "password = {}", cnf_quoted("password", password)?);
        view.derive(password, &cnf_escaped(password));
    }
    if let Some(seconds) = seconds(values) {
        let _ = writeln!(cnf, "connect-timeout = {seconds}");
    }
    let _ = write!(cnf, "\n[client-mariadb]\n{mariadb}");
    if !database.is_empty() {
        // The `mysql` (`mariadb`) program's own: its default database.
        let quoted = cnf_quoted("database", &database)?;
        let _ = writeln!(cnf, "\n[mysql]\ndatabase = {quoted}");
        view.var("DATABASE", database);
    }
    view.var("HOST", host);
    view.var("PORT", port);
    view.file(dir, "MYSQL_CNF", "my.cnf", cnf);
    Ok(())
}

/// A host and its port (`db:3307`, `[::1]:3307`, `db`), the port `default` when none.
fn host_and_port(hosts: &str, default: &str) -> (String, String) {
    if let Some(rest) = hosts.strip_prefix('[')
        && let Some((host, after)) = rest.split_once(']')
    {
        let port = after
            .strip_prefix(':')
            .filter(|p| !p.is_empty())
            .unwrap_or(default);
        return (host.to_owned(), port.to_owned());
    }
    match hosts.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() => (host.to_owned(), port.to_owned()),
        _ => (hosts.to_owned(), default.to_owned()),
    }
}

/// `Generic`'s keys, or a `@connection`'s parameters: one variable each, text as it is,
/// anything else as JSON (decided 2026-10-25).
fn own(values: &Map<String, Value>, view: &mut View) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for (param, value) in values {
        if !param.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "`{param}` can't be a variable's name (letters, digits and `_` only): read the \
                 values with `schedr task-ctx connection NAME --json`"
            ));
        }
        let suffix = param.to_ascii_uppercase();
        if suffix == "JSON" || !seen.insert(suffix.clone()) {
            return Err(format!(
                "`{param}` would be a variable another value has: read the values with \
                 `schedr task-ctx connection NAME --json`"
            ));
        }
        let text = match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        view.var(&suffix, text);
    }
    Ok(())
}
