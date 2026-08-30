//! TLS for both front ends: one PEM file holding the server's certificate
//! chain and its private key, turned into the [`rustls::ServerConfig`] that
//! [`Options::tls`](crate::Options::tls) takes. Clients are not asked for a
//! certificate; the token is what authenticates them.

use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::{Error, Result};

/// Read `pem` — the certificate chain (leaf first) and the private key, in
/// one file — into a server configuration. Only TLS 1.3 with rustls's
/// default cipher suites is offered.
pub fn server_config(pem: &Path) -> Result<Arc<ServerConfig>> {
    let read = |source| Error::Pem {
        path: pem.to_path_buf(),
        source,
    };
    let bytes = std::fs::read(pem).map_err(|e| read(rustls_pki_types::pem::Error::Io(e)))?;
    let chain = CertificateDer::pem_slice_iter(&bytes)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(read)?;
    let key = PrivateKeyDer::from_pem_slice(&bytes).map_err(read)?;
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(chain, key)?;
    Ok(Arc::new(config))
}

/// The fixtures in `testdata/tls`: a CA and a leaf it issued for
/// `localhost`, `127.0.0.1` and `::1`.
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use rustls::ClientConfig;
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, ServerName};
    use tokio_rustls::TlsConnector;

    pub const SERVER_PEM: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/server.pem");
    pub const CA_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/ca.pem");

    pub fn server() -> Arc<rustls::ServerConfig> {
        super::server_config(std::path::Path::new(SERVER_PEM)).unwrap()
    }

    /// A connector trusting the fixture CA, and the name to connect as.
    pub fn client() -> (TlsConnector, ServerName<'static>) {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_file(CA_PEM).unwrap())
            .unwrap();
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        (
            TlsConnector::from(Arc::new(config)),
            ServerName::try_from("localhost").unwrap(),
        )
    }

    #[test]
    fn bad_pem_is_reported_with_its_path() {
        let err = super::server_config(std::path::Path::new(CA_PEM)).unwrap_err();
        assert!(matches!(err, crate::Error::Pem { .. }), "{err}");
        assert!(err.to_string().contains("ca.pem"), "{err}");
        let err = super::server_config(std::path::Path::new("/nonexistent.pem")).unwrap_err();
        assert!(err.to_string().contains("/nonexistent.pem"), "{err}");
    }
}
