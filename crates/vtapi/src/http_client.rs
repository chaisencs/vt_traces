use std::{
    fs,
    path::PathBuf,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::Context;
use reqwest::{Certificate, Client, Identity, IntoUrl, RequestBuilder};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct ClusterHttpClient {
    inner: ClusterHttpClientInner,
}

#[derive(Debug, Clone)]
enum ClusterHttpClientInner {
    Static(Client),
    Reloading(std::sync::Arc<ReloadingClusterHttpClient>),
}

#[derive(Debug, Clone)]
pub struct ClusterHttpClientConfig {
    ca_cert_path: Option<PathBuf>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    insecure_skip_verify: bool,
    reload_interval: Duration,
}

#[derive(Debug)]
struct ReloadingClusterHttpClient {
    config: ClusterHttpClientConfig,
    state: Mutex<ReloadingClusterHttpClientState>,
}

#[derive(Debug)]
struct ReloadingClusterHttpClientState {
    client: Client,
    last_reload_check: Instant,
}

impl ClusterHttpClient {
    pub fn from_client(client: Client) -> Self {
        Self {
            inner: ClusterHttpClientInner::Static(client),
        }
    }

    pub fn from_reloading_config(config: ClusterHttpClientConfig) -> anyhow::Result<Self> {
        let client = build_reqwest_client(&config)?;
        Ok(Self {
            inner: ClusterHttpClientInner::Reloading(std::sync::Arc::new(
                ReloadingClusterHttpClient {
                    config,
                    state: Mutex::new(ReloadingClusterHttpClientState {
                        client,
                        last_reload_check: Instant::now(),
                    }),
                },
            )),
        })
    }

    pub fn get(&self, url: impl IntoUrl) -> RequestBuilder {
        self.current().get(url)
    }

    pub fn post(&self, url: impl IntoUrl) -> RequestBuilder {
        self.current().post(url)
    }

    pub fn current(&self) -> Client {
        match &self.inner {
            ClusterHttpClientInner::Static(client) => client.clone(),
            ClusterHttpClientInner::Reloading(reloading) => reloading.current(),
        }
    }
}

impl From<Client> for ClusterHttpClient {
    fn from(value: Client) -> Self {
        Self::from_client(value)
    }
}

impl ClusterHttpClientConfig {
    pub fn new(reload_interval: Duration) -> Self {
        Self {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            insecure_skip_verify: false,
            reload_interval,
        }
    }

    pub fn with_ca_cert_path(mut self, ca_cert_path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(ca_cert_path.into());
        self
    }

    pub fn with_client_identity_paths(
        mut self,
        client_cert_path: impl Into<PathBuf>,
        client_key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert_path = Some(client_cert_path.into());
        self.client_key_path = Some(client_key_path.into());
        self
    }

    pub fn with_insecure_skip_verify(mut self, insecure_skip_verify: bool) -> Self {
        self.insecure_skip_verify = insecure_skip_verify;
        self
    }
}

impl ReloadingClusterHttpClient {
    fn current(&self) -> Client {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.last_reload_check.elapsed() >= self.config.reload_interval {
            state.last_reload_check = Instant::now();
            match build_reqwest_client(&self.config) {
                Ok(client) => state.client = client,
                Err(error) => {
                    warn!(error = %error, "cluster HTTP client reload failed");
                }
            }
        }
        state.client.clone()
    }
}

fn build_reqwest_client(config: &ClusterHttpClientConfig) -> anyhow::Result<Client> {
    let mut builder = Client::builder().use_rustls_tls();

    if let Some(ca_cert_path) = &config.ca_cert_path {
        let certificate = Certificate::from_pem(&fs::read(ca_cert_path).with_context(|| {
            format!(
                "read cluster CA certificate from {}",
                ca_cert_path.display()
            )
        })?)?;
        builder = builder.add_root_certificate(certificate);
    }

    match (&config.client_cert_path, &config.client_key_path) {
        (Some(client_cert_path), Some(client_key_path)) => {
            let mut identity_pem = fs::read(client_cert_path).with_context(|| {
                format!(
                    "read cluster client certificate from {}",
                    client_cert_path.display()
                )
            })?;
            identity_pem.extend(fs::read(client_key_path).with_context(|| {
                format!("read cluster client key from {}", client_key_path.display())
            })?);
            builder = builder.identity(Identity::from_pem(&identity_pem)?);
        }
        (None, None) => {}
        _ => {
            anyhow::bail!("cluster client certificate and key paths must be configured together");
        }
    }

    if config.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    Ok(builder.build()?)
}
