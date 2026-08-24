//! TLS material for the QUIC listener: a supplied cert/key pair or an
//! ephemeral self-signed certificate.

use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject};

use crate::error::{Error, Result};

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
        fn pem_err(path: &Path) -> impl FnOnce(rustls_pki_types::pem::Error) -> Error {
            let path = path.to_owned();
            move |source| Error::Pem { path, source }
        }
        let certs = CertificateDer::pem_file_iter(cert)
            .map_err(pem_err(cert))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(pem_err(cert))?;
        let key = PrivateKeyDer::from_pem_file(key).map_err(pem_err(key))?;
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
