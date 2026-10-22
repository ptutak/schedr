//! The configurations in use, read again when the files change (rotation without a restart).
//!
//! Kubernetes updates a mounted Secret by swapping a symbolic link, so the files are polled
//! (`[tls] reload_interval`, 30 s by default) rather than watched. A change is used once all
//! three files read and go together; until then the previous material stays in use, and the
//! error is logged once per change. New connections use the new material; open ones go on.

use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use rustls::{ClientConfig, ServerConfig};

use crate::material::{Configs, Contents, TlsError, TlsFiles, build};

#[cfg(test)]
mod tests;

/// A role's TLS material, shared by its servers and clients (cheap to clone).
#[derive(Debug, Clone)]
pub struct Tls {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    files: TlsFiles,
    worker_name: String,
    state: RwLock<State>,
}

#[derive(Debug)]
struct State {
    contents: Contents,
    configs: Configs,
    generation: u64,
    /// The last error, so a broken change is reported once.
    failed: Option<String>,
}

/// What [`Tls::reload`] found.
#[derive(Debug)]
pub enum Reload {
    /// The files are as they were (or as they were when they last failed).
    Unchanged,
    /// The files changed and are in use now.
    Reloaded,
    /// The files changed but can't be used; the previous material stays in use.
    Failed(TlsError),
}

impl Tls {
    /// Reads `files`; workers' certificates must name `worker_name`.
    ///
    /// # Errors
    ///
    /// [`TlsError`] if the files can't be read or used.
    pub fn load(files: TlsFiles, worker_name: &str) -> Result<Self, TlsError> {
        let contents = files.read()?;
        let configs = build(&contents, &files, worker_name)?;
        Ok(Self {
            inner: Arc::new(Inner {
                files,
                worker_name: worker_name.to_owned(),
                state: RwLock::new(State {
                    contents,
                    configs,
                    generation: 0,
                    failed: None,
                }),
            }),
        })
    }

    fn configs(&self) -> Configs {
        self.inner
            .state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .configs
            .clone()
    }

    /// The master's gRPC server configuration.
    #[must_use]
    pub fn grpc_server(&self) -> Arc<ServerConfig> {
        self.configs().grpc_server
    }

    /// A worker's log endpoint configuration.
    #[must_use]
    pub fn http_server(&self) -> Arc<ServerConfig> {
        self.configs().http_server
    }

    /// A worker's client configuration towards the master.
    #[must_use]
    pub fn grpc_client(&self) -> Arc<ClientConfig> {
        self.configs().grpc_client
    }

    /// The webserver's client configuration towards workers.
    #[must_use]
    pub fn worker_client(&self) -> Arc<ClientConfig> {
        self.configs().worker_client
    }

    /// How many times the material changed (clients built from it are rebuilt when it does).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner
            .state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .generation
    }

    /// Reads the files again and uses them if they changed and can be used.
    pub fn reload(&self) -> Reload {
        let read = self.inner.files.read();
        let mut state = self
            .inner
            .state
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let built = read.and_then(|contents| {
            if contents == state.contents {
                return Ok(None);
            }
            let configs = build(&contents, &self.inner.files, &self.inner.worker_name)?;
            Ok(Some((contents, configs)))
        });
        match built {
            Ok(None) => {
                state.failed = None;
                Reload::Unchanged
            }
            Ok(Some((contents, configs))) => {
                state.contents = contents;
                state.configs = configs;
                state.generation += 1;
                state.failed = None;
                Reload::Reloaded
            }
            Err(error) => {
                let text = error.to_string();
                if state.failed.as_ref() == Some(&text) {
                    return Reload::Unchanged;
                }
                state.failed = Some(text);
                Reload::Failed(error)
            }
        }
    }

    /// Reloads every `interval` until `stop` completes, logging what changed.
    pub async fn watch(self, interval: Duration, stop: impl Future<Output = ()>) {
        tokio::pin!(stop);
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tokio::select! {
                () = &mut stop => return,
                _ = tick.tick() => match self.reload() {
                    Reload::Unchanged => {}
                    Reload::Reloaded => tracing::info!(
                        cert = %self.inner.files.cert.display(),
                        "TLS certificates changed: new connections use them"
                    ),
                    Reload::Failed(error) => tracing::error!(
                        %error,
                        "TLS certificates changed but can't be used; keeping the previous ones"
                    ),
                },
            }
        }
    }
}
