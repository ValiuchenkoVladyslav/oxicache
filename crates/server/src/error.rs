use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("binding {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("the token must not be empty")]
    EmptyToken,
    #[error("reading {}: {source}", path.display())]
    Pem {
        path: PathBuf,
        source: rustls_pki_types::pem::Error,
    },
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
