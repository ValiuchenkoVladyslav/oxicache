//! TLS material for the QUIC listener: a supplied cert/key pair or an
//! ephemeral self-signed certificate.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::ServerConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject};

pub const ALPN: &[u8] = b"h3";

pub struct Identity {
    pub certs: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

impl Identity {
    pub fn self_signed() -> Result<Self> {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        Ok(Self {
            certs: vec![ck.cert.der().clone()],
            key: PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()).into(),
        })
    }

    pub fn from_pem(cert: &Path, key: &Path) -> Result<Self> {
        let certs = CertificateDer::pem_file_iter(cert)
            .with_context(|| format!("reading {}", cert.display()))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_file(key)
            .with_context(|| format!("reading {}", key.display()))?;
        Ok(Self { certs, key })
    }

    pub fn server_config(self) -> Result<Arc<ServerConfig>> {
        let mut cfg =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])?
                .with_no_client_auth()
                .with_single_cert(self.certs, self.key)?;
        cfg.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Arc::new(cfg))
    }
}
