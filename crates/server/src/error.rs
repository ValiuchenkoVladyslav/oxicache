use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("reading {path}: {source}")]
    Pem {
        path: PathBuf,
        source: rustls_pki_types::pem::Error,
    },
    #[error("generating self-signed certificate: {0}")]
    Rcgen(#[from] rcgen::Error),
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
    #[error("tls config not usable for quic: {0}")]
    Quic(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
    #[error("binding {addr}: {source}")]
    Bind {
        addr: std::net::SocketAddr,
        source: std::io::Error,
    },
    #[error("identity has no certificate")]
    NoCertificate,
    #[error("h3 stream: {0}")]
    Stream(#[from] h3::error::StreamError),
}

pub type Result<T> = std::result::Result<T, Error>;
