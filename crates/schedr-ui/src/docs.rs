//! The documentation site at `/docs`, served from a directory (the image's
//! `/usr/share/schedr/docs`, decision 28A): only files under it, never outside.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};

use crate::Ui;
use crate::page::Page;
use crate::pages::not_found_page;

#[cfg(test)]
mod tests;

/// `GET /docs`: to `/docs/`, so the site's relative links work; not a permanent redirect (its
/// target depends on the proxy's prefix).
pub async fn to_index(headers: HeaderMap) -> Redirect {
    Redirect::temporary(&format!("{}/docs/", crate::page::prefix(&headers)))
}

/// `GET /docs/`: the site's front page.
pub async fn index<T>(State(ui): State<Arc<Ui<T>>>, headers: HeaderMap) -> Response {
    serve(&ui, "index.html", &headers).await
}

/// `GET /docs/<path>`.
pub async fn file<T>(
    State(ui): State<Arc<Ui<T>>>,
    UrlPath(path): UrlPath<String>,
    headers: HeaderMap,
) -> Response {
    serve(&ui, &path, &headers).await
}

async fn serve<T>(ui: &Ui<T>, path: &str, headers: &HeaderMap) -> Response {
    let found = ui.docs_dir.as_deref().and_then(|root| resolve(root, path));
    let Some(file) = found else {
        let page = Page::new(headers, ui.docs_dir.is_some());
        return not_found_page(&page, "That page isn't in the documentation.");
    };
    match tokio::fs::read(&file).await {
        Ok(bytes) => ([(header::CONTENT_TYPE, content_type(&file))], bytes).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// The file `path` names under `root`: a directory's `index.html`; `None` for a path that
/// leaves the root (`..`, an absolute path, a link out of it) or doesn't exist.
#[must_use]
pub fn resolve(root: &Path, path: &str) -> Option<PathBuf> {
    let relative = Path::new(path);
    if !relative
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
    {
        return None;
    }
    let mut file = root.join(relative);
    if file.is_dir() {
        file.push("index.html");
    }
    let root = root.canonicalize().ok()?;
    let file = file.canonicalize().ok()?;
    (file.starts_with(&root) && file.is_file()).then_some(file)
}

/// The content type of a documentation file, by its extension.
#[must_use]
pub fn content_type(file: &Path) -> &'static str {
    match file
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
    {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
