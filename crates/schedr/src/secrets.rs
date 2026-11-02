//! `schedr secrets ...`: the `db` secret storage, straight in the database as `schedr db` is
//! (see PROJECT.md, Secret storage). Values are encrypted under `SCHEDR_SECRET_KEY`; only
//! `get` prints one, `list` shows paths and key names, `export` writes files for Vault
//! (decision 170) and prints none.

use std::collections::BTreeMap;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use jiff::Timestamp;
use schedr_config::config::Config;
use schedr_db::secrets::{SecretKey, SecretsError};
use schedr_db::{Database, redact_password};
use schedr_model::SecretName;

use crate::app::{AppError, describe_status};
use crate::cli::SecretsCommand;

#[cfg(test)]
mod tests;

/// Runs a `schedr secrets` command; `stdin` gives a `key=-` value.
///
/// # Errors
///
/// [`AppError::BadArgument`] for a bad path, key or value, or a missing key;
/// [`AppError::Failed`] for a secret that isn't there, or a value that doesn't decrypt;
/// [`AppError::Db`] when the database fails.
pub async fn run(
    command: &SecretsCommand,
    config: &Config,
    env: &BTreeMap<String, String>,
    stdin: &mut impl Read,
    out: &mut impl Write,
) -> Result<(), AppError> {
    if *command == SecretsCommand::NewKey {
        let key = SecretKey::generate().map_err(secrets_error)?;
        writeln!(out, "{}", key.to_base64())?;
        return Ok(());
    }
    let db = connect(config, env).await?;
    match command {
        SecretsCommand::Set { path, values } => {
            check_path(path)?;
            let values = read_values(path, values, stdin)?;
            let key = SecretKey::from_env(env).map_err(secrets_error)?;
            db.set_secret(&key, path, &values, Timestamp::now())
                .await
                .map_err(secrets_error)?;
            let keys: Vec<&str> = values.keys().map(String::as_str).collect();
            writeln!(out, "set `{path}`: {}", keys.join(", "))?;
        }
        SecretsCommand::List { prefix } => {
            for entry in db.secret_entries(prefix.as_deref().unwrap_or("")).await? {
                writeln!(
                    out,
                    "{}\t{}\t{}",
                    entry.path,
                    entry.keys.join(","),
                    entry.updated_at
                )?;
            }
        }
        SecretsCommand::Get { secret } => {
            let (path, wanted) = match secret.split_once('#') {
                Some((path, key)) => (path, Some(key)),
                None => (secret.as_str(), None),
            };
            SecretName::new(path, wanted)
                .check()
                .map_err(AppError::BadArgument)?;
            let key = SecretKey::from_env(env).map_err(secrets_error)?;
            let data = db
                .secret(&key, path)
                .await
                .map_err(secrets_error)?
                .ok_or_else(|| AppError::Failed(format!("no secret `{path}`")))?;
            match wanted {
                Some(wanted) => {
                    let value = data.get(wanted).ok_or_else(|| {
                        AppError::Failed(format!("the secret `{path}` has no key `{wanted}`"))
                    })?;
                    writeln!(out, "{value}")?;
                }
                None => writeln!(
                    out,
                    "{}",
                    serde_json::to_string_pretty(&data).unwrap_or_default()
                )?,
            }
        }
        SecretsCommand::Delete { path } => {
            if !db.delete_secret(path).await? {
                return Err(AppError::Failed(format!("no secret `{path}`")));
            }
            writeln!(out, "deleted `{path}`")?;
        }
        SecretsCommand::Export { to } => {
            let key = SecretKey::from_env(env).map_err(secrets_error)?;
            let count = export(&db, &key, to).await?;
            writeln!(
                out,
                "exported {count} secret(s) to {dir}: load each with `vault kv put \
                 secret/<path> @{dir}/<path>.json`, then remove the directory",
                dir = to.display()
            )?;
        }
        SecretsCommand::NewKey => {}
    }
    Ok(())
}

/// The database the `db` storage is in: `[database] url`, else standalone's own SQLite file
/// in the data directory; its schema must be up to date.
async fn connect(config: &Config, env: &BTreeMap<String, String>) -> Result<Database, AppError> {
    let url = if let Some(url) = &config.database.url {
        url.clone()
    } else {
        let file = crate::standalone::sqlite_file(&crate::roles::data_dir(config, env)?);
        if !file.exists() {
            return Err(AppError::BadArgument(format!(
                "no database: set `SCHEDR_CONFIG__DATABASE__URL` (or [database] url), or \
                 start `schedr standalone --migrate` once to make {}",
                file.display()
            )));
        }
        format!("sqlite://{}", file.display())
    };
    let db = Database::connect(&url).await?;
    let status = db.schema_status().await?;
    if !status.is_usable() {
        return Err(AppError::Failed(format!(
            "{}: {}",
            redact_password(&url),
            describe_status(&status)
        )));
    }
    Ok(db)
}

fn secrets_error(error: SecretsError) -> AppError {
    match error {
        SecretsError::Db(error) => AppError::Db(error),
        SecretsError::Key(message) => AppError::BadArgument(message),
        error @ SecretsError::Decrypt(_) => AppError::Failed(error.to_string()),
    }
}

/// A path as a DAG names it, and one `export` can make a file of: no `.` or `..` part.
fn check_path(path: &str) -> Result<(), AppError> {
    SecretName::new(path, None)
        .check()
        .map_err(AppError::BadArgument)?;
    if path.split('/').any(|part| part == "." || part == "..") {
        return Err(AppError::BadArgument(format!(
            "`{path}`: a path has no `.` or `..` part"
        )));
    }
    no_control(path, "path")
}

/// A path or key name without control characters: `list` writes one per line, and a file
/// name is made of it (Grok's finding in M4.6c's review).
fn no_control(name: &str, what: &str) -> Result<(), AppError> {
    if name.chars().any(char::is_control) {
        return Err(AppError::BadArgument(format!(
            "{name:?}: a {what} has no control characters (a newline, a tab)"
        )));
    }
    Ok(())
}

/// `key=value`, `key=@FILE` and `key=-` read into the values to set.
fn read_values(
    path: &str,
    args: &[String],
    stdin: &mut impl Read,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    let mut stdin_used = false;
    for arg in args {
        // Not echoed: it may be a value given without its key.
        let Some((key, value)) = arg.split_once('=') else {
            return Err(AppError::BadArgument(
                "each value is KEY=VALUE, KEY=@FILE or KEY=-".to_owned(),
            ));
        };
        SecretName::new(path, Some(key))
            .check()
            .map_err(AppError::BadArgument)?;
        no_control(key, "key")?;
        let value = if value == "-" {
            if stdin_used {
                return Err(AppError::BadArgument(
                    "only one value can be read from standard input (`KEY=-`)".to_owned(),
                ));
            }
            stdin_used = true;
            let mut text = String::new();
            stdin.read_to_string(&mut text).map_err(|e| {
                AppError::BadArgument(format!("`{key}`: can't read standard input: {e}"))
            })?;
            let trimmed = text
                .strip_suffix("\r\n")
                .or_else(|| text.strip_suffix('\n'))
                .map(str::to_owned);
            trimmed.unwrap_or(text)
        } else if let Some(file) = value.strip_prefix('@') {
            fs::read_to_string(file).map_err(|e| {
                AppError::BadArgument(format!("`{key}`: can't read `{file}` as text: {e}"))
            })?
        } else {
            value.to_owned()
        };
        if values.insert(key.to_owned(), value).is_some() {
            return Err(AppError::BadArgument(format!("`{key}` is given twice")));
        }
    }
    Ok(values)
}

/// Writes each secret to `dir/<path>.json` (mode 0600, directories 0700); `dir` must be
/// empty or missing, so no file of another's is overwritten or followed. Every secret is read
/// and every path checked first: nothing is written unless all of them can be.
async fn export(db: &Database, key: &SecretKey, dir: &Path) -> Result<usize, AppError> {
    let failed = |what: &Path, e: io::Error| {
        AppError::Failed(format!("can't write `{}`: {e}", what.display()))
    };
    let mut secrets = Vec::new();
    for entry in db.secret_entries("").await? {
        check_path(&entry.path)?;
        let data = db
            .secret(key, &entry.path)
            .await
            .map_err(secrets_error)?
            .unwrap_or_default();
        secrets.push((entry.path, data));
    }
    check_files(secrets.iter().map(|(path, _)| path.as_str()))?;
    // The directory named, not one a link leads to.
    if fs::symlink_metadata(dir).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(AppError::BadArgument(format!(
            "`{}` is a symbolic link: export writes to a directory of its own",
            dir.display()
        )));
    }
    match fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(AppError::BadArgument(format!(
                    "`{}` isn't empty: export writes to an empty or new directory",
                    dir.display()
                )));
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            private_dirs().create(dir).map_err(|e| failed(dir, e))?;
        }
        Err(e) => return Err(failed(dir, e)),
    }
    // A write that fails (a full disk) removes the files written before it: no export is
    // left half done (Grok's finding in M4.6c's review).
    let mut written = Vec::new();
    for (path, data) in &secrets {
        let file: PathBuf = dir.join(format!("{path}.json"));
        if let Err(error) = write_secret(&file, data, &mut written) {
            for file in &written {
                let _ = fs::remove_file(file);
            }
            return Err(failed(&file, error));
        }
    }
    Ok(secrets.len())
}

/// Refuses two paths export can't write side by side: `a` (the file `a.json`) and `a.json/b`
/// (in the directory `a.json`), found by Codex in M4.6c's review.
fn check_files<'a>(paths: impl Iterator<Item = &'a str> + Clone) -> Result<(), AppError> {
    let files: BTreeMap<String, &str> = paths.clone().map(|p| (format!("{p}.json"), p)).collect();
    for path in paths {
        let mut prefix = String::new();
        let parts: Vec<&str> = path.split('/').collect();
        for part in &parts[..parts.len() - 1] {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(part);
            if let Some(other) = files.get(&prefix) {
                return Err(AppError::BadArgument(format!(
                    "`{other}` and `{path}` can't both be exported: `{prefix}` would be its \
                     file and a directory (rename one with `set` and `delete`)"
                )));
            }
        }
    }
    Ok(())
}

/// Writes one secret's file (its directories first), noting it in `written` once it exists.
fn write_secret(
    file: &Path,
    data: &BTreeMap<String, String>,
    written: &mut Vec<PathBuf>,
) -> io::Result<()> {
    if let Some(parent) = file.parent() {
        private_dirs().create(parent)?;
    }
    let mut handle = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(file)?;
    written.push(file.to_path_buf());
    let text = serde_json::to_string_pretty(data).unwrap_or_default();
    handle.write_all(format!("{text}\n").as_bytes())
}

fn private_dirs() -> DirBuilder {
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
}
