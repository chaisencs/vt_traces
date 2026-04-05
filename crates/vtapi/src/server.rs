use std::{
    io::BufReader,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{anyhow, Context};
use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as AutoBuilder,
    service::TowerToHyperService,
};
use rustls::{server::AllowAnyAuthenticatedClient, Certificate, PrivateKey, RootCertStore};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct ServerTlsConfig {
    source: ServerTlsSource,
    reload_interval: Option<Duration>,
}

#[derive(Debug, Clone)]
enum ServerTlsSource {
    Inline {
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
        client_ca_pem: Option<Vec<u8>>,
    },
    Files {
        cert_path: PathBuf,
        key_path: PathBuf,
        client_ca_path: Option<PathBuf>,
    },
}

impl ServerTlsConfig {
    pub fn from_pem(cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self {
            source: ServerTlsSource::Inline {
                cert_pem,
                key_pem,
                client_ca_pem: None,
            },
            reload_interval: None,
        }
    }

    pub fn from_pem_files(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            source: ServerTlsSource::Files {
                cert_path: cert_path.into(),
                key_path: key_path.into(),
                client_ca_path: None,
            },
            reload_interval: None,
        }
    }

    pub fn with_client_ca_pem(mut self, client_ca_pem: Vec<u8>) -> Self {
        match &mut self.source {
            ServerTlsSource::Inline {
                client_ca_pem: source_client_ca_pem,
                ..
            } => *source_client_ca_pem = Some(client_ca_pem),
            ServerTlsSource::Files { .. } => {
                self.source = ServerTlsSource::Inline {
                    cert_pem: self.load_cert_pem().unwrap_or_default(),
                    key_pem: self.load_key_pem().unwrap_or_default(),
                    client_ca_pem: Some(client_ca_pem),
                };
            }
        }
        self
    }

    pub fn with_client_ca_path(mut self, client_ca_path: impl Into<PathBuf>) -> Self {
        match &mut self.source {
            ServerTlsSource::Inline { .. } => {
                self.source = ServerTlsSource::Files {
                    cert_path: self.cert_path_fallback(),
                    key_path: self.key_path_fallback(),
                    client_ca_path: Some(client_ca_path.into()),
                };
            }
            ServerTlsSource::Files {
                client_ca_path: source_client_ca_path,
                ..
            } => *source_client_ca_path = Some(client_ca_path.into()),
        }
        self
    }

    pub fn with_reload_interval(mut self, reload_interval: Duration) -> Self {
        self.reload_interval = Some(reload_interval);
        self
    }

    pub fn is_mtls_enabled(&self) -> bool {
        match &self.source {
            ServerTlsSource::Inline { client_ca_pem, .. } => client_ca_pem.is_some(),
            ServerTlsSource::Files { client_ca_path, .. } => client_ca_path.is_some(),
        }
    }

    fn reload_interval(&self) -> Option<Duration> {
        self.reload_interval
    }

    fn is_file_backed(&self) -> bool {
        matches!(self.source, ServerTlsSource::Files { .. })
    }

    fn load_cert_pem(&self) -> anyhow::Result<Vec<u8>> {
        match &self.source {
            ServerTlsSource::Inline { cert_pem, .. } => Ok(cert_pem.clone()),
            ServerTlsSource::Files { cert_path, .. } => {
                std::fs::read(cert_path).context("read TLS certificate PEM")
            }
        }
    }

    fn load_key_pem(&self) -> anyhow::Result<Vec<u8>> {
        match &self.source {
            ServerTlsSource::Inline { key_pem, .. } => Ok(key_pem.clone()),
            ServerTlsSource::Files { key_path, .. } => {
                std::fs::read(key_path).context("read TLS private key PEM")
            }
        }
    }

    fn load_client_ca_pem(&self) -> anyhow::Result<Option<Vec<u8>>> {
        match &self.source {
            ServerTlsSource::Inline { client_ca_pem, .. } => Ok(client_ca_pem.clone()),
            ServerTlsSource::Files { client_ca_path, .. } => client_ca_path
                .as_ref()
                .map(|path| std::fs::read(path).context("read TLS client CA PEM"))
                .transpose(),
        }
    }

    fn build_server_config(&self) -> anyhow::Result<rustls::ServerConfig> {
        let cert_pem = self.load_cert_pem()?;
        let key_pem = self.load_key_pem()?;
        let client_ca_pem = self.load_client_ca_pem()?;

        let certificates = load_certificates(&cert_pem).context("load server certificates")?;
        let private_key = load_private_key(&key_pem).context("load server private key")?;
        let builder = rustls::ServerConfig::builder().with_safe_defaults();

        let mut server_config = if let Some(client_ca_pem) = &client_ca_pem {
            let root_store = build_root_store(client_ca_pem).context("load client CA bundle")?;
            builder
                .with_client_cert_verifier(Arc::new(AllowAnyAuthenticatedClient::new(root_store)))
                .with_single_cert(certificates, private_key)
                .context("build mTLS server config")?
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(certificates, private_key)
                .context("build TLS server config")?
        };

        server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(server_config)
    }

    fn cert_path_fallback(&self) -> PathBuf {
        match &self.source {
            ServerTlsSource::Files { cert_path, .. } => cert_path.clone(),
            ServerTlsSource::Inline { .. } => PathBuf::new(),
        }
    }

    fn key_path_fallback(&self) -> PathBuf {
        match &self.source {
            ServerTlsSource::Files { key_path, .. } => key_path.clone(),
            ServerTlsSource::Inline { .. } => PathBuf::new(),
        }
    }
}

pub async fn serve_app(
    listener: TcpListener,
    app: Router,
    tls_config: Option<ServerTlsConfig>,
) -> anyhow::Result<()> {
    match tls_config {
        Some(tls_config) => serve_https(listener, app, tls_config).await,
        None => serve_http(listener, app).await,
    }
}

async fn serve_http(listener: TcpListener, app: Router) -> anyhow::Result<()> {
    loop {
        let (stream, peer_addr) = listener.accept().await.context("accept TCP connection")?;
        let app = app.clone();
        tokio::spawn(async move {
            let builder = AutoBuilder::new(TokioExecutor::new());
            let connection = builder.serve_connection_with_upgrades(
                TokioIo::new(stream),
                TowerToHyperService::new(app),
            );
            if let Err(error) = connection.await {
                warn!(
                    error = %error,
                    peer_addr = %peer_addr,
                    "plain HTTP connection failed"
                );
            }
        });
    }
}

async fn serve_https(
    listener: TcpListener,
    app: Router,
    tls_config: ServerTlsConfig,
) -> anyhow::Result<()> {
    let current_config = Arc::new(RwLock::new(Arc::new(
        tls_config
            .build_server_config()
            .context("build rustls server config")?,
    )));
    if tls_config.is_file_backed() {
        if let Some(interval) = tls_config.reload_interval() {
            let tls_config = tls_config.clone();
            let current_config = current_config.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    match tls_config.build_server_config() {
                        Ok(server_config) => {
                            let mut guard = current_config
                                .write()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            *guard = Arc::new(server_config);
                        }
                        Err(error) => {
                            warn!(error = %error, "TLS reload failed");
                        }
                    }
                }
            });
        }
    }

    loop {
        let (stream, peer_addr) = listener.accept().await.context("accept TCP connection")?;
        let acceptor = {
            let config = current_config
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            TlsAcceptor::from(config)
        };
        let app = app.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(tls_stream) => tls_stream,
                Err(error) => {
                    warn!(
                        error = %error,
                        peer_addr = %peer_addr,
                        "TLS handshake failed"
                    );
                    return;
                }
            };

            let builder = AutoBuilder::new(TokioExecutor::new());
            let connection = builder.serve_connection_with_upgrades(
                TokioIo::new(tls_stream),
                TowerToHyperService::new(app),
            );
            if let Err(error) = connection.await {
                warn!(
                    error = %error,
                    peer_addr = %peer_addr,
                    "HTTPS connection failed"
                );
            }
        });
    }
}

fn load_certificates(pem: &[u8]) -> anyhow::Result<Vec<Certificate>> {
    let mut reader = BufReader::new(pem);
    let certificates = rustls_pemfile::certs(&mut reader).context("parse PEM certificates")?;
    if certificates.is_empty() {
        return Err(anyhow!("no certificates found in PEM payload"));
    }
    Ok(certificates.into_iter().map(Certificate).collect())
}

fn load_private_key(pem: &[u8]) -> anyhow::Result<PrivateKey> {
    let mut reader = BufReader::new(pem);
    if let Some(private_key) = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .context("parse PKCS#8 private keys")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(private_key));
    }

    let mut reader = BufReader::new(pem);
    if let Some(private_key) = rustls_pemfile::rsa_private_keys(&mut reader)
        .context("parse RSA private keys")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(private_key));
    }

    Err(anyhow!("no supported private key found in PEM payload"))
}

fn build_root_store(pem: &[u8]) -> anyhow::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    for certificate in load_certificates(pem)? {
        root_store
            .add(&certificate)
            .map_err(|error| anyhow!("failed to add CA certificate: {error:?}"))?;
    }
    Ok(root_store)
}
