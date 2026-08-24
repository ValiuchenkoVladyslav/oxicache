//! HTTP/3 client for oxicache. One [`Client`] owns one QUIC connection and is
//! cheap to clone; every call opens a fresh request stream, so clones may be
//! used concurrently from many tasks.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use http::{Method, Request, StatusCode, Uri};
use oxicache_wire as wire;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connect: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("connection: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("h3: {0}")]
    H3Connection(#[from] h3::error::ConnectionError),
    #[error("h3 stream: {0}")]
    H3Stream(#[from] h3::error::StreamError),
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
    #[error("server returned {status}: {message}")]
    Status { status: StatusCode, message: String },
    #[error("decode: {0}")]
    Decode(#[from] wire::DecodeError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// How the server certificate is validated.
#[derive(Clone, Debug, Default)]
pub enum Tls {
    /// Accept any certificate. Only for development against self-signed servers.
    #[default]
    Insecure,
    /// Trust exactly these DER certificates (e.g. the server's self-signed cert).
    Pinned(Vec<CertificateDer<'static>>),
}

#[derive(Clone, Debug)]
pub struct Config {
    /// SNI / certificate name presented by the server.
    pub server_name: String,
    pub tls: Tls,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_name: "localhost".into(),
            tls: Tls::Insecure,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    send: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    _endpoint: quinn::Endpoint,
}

impl Client {
    pub async fn connect(addr: SocketAddr, config: Config) -> Result<Self> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?;
        let mut tls = match config.tls {
            Tls::Insecure => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipVerify(provider)))
                .with_no_client_auth(),
            Tls::Pinned(certs) => {
                let mut roots = rustls::RootCertStore::empty();
                for c in certs {
                    roots.add(c)?;
                }
                builder.with_root_certificates(roots).with_no_client_auth()
            }
        };
        tls.alpn_protocols = vec![b"h3".to_vec()];

        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let mut client_cfg = quinn::ClientConfig::new(Arc::new(quic));
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_bidi_streams(4096u32.into());
        client_cfg.transport_config(Arc::new(transport));

        let bind: SocketAddr = match addr.ip() {
            IpAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
            IpAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
        };
        let mut endpoint = quinn::Endpoint::client(bind)?;
        endpoint.set_default_client_config(client_cfg);
        let conn = endpoint.connect(addr, &config.server_name)?.await?;

        let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(conn)).await?;
        tokio::spawn(async move {
            let _ = driver.wait_idle().await;
        });
        Ok(Self {
            send,
            _endpoint: endpoint,
        })
    }

    async fn call(&self, path: &'static str, body: Bytes) -> Result<Bytes> {
        let uri = Uri::builder()
            .scheme("https")
            .authority("oxicache")
            .path_and_query(path)
            .build()
            .unwrap();
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .body(())
            .unwrap();
        let mut send = self.send.clone();
        let mut stream = send.send_request(req).await?;
        if !body.is_empty() {
            stream.send_data(body).await?;
        }
        stream.finish().await?;

        let resp = stream.recv_response().await?;
        let mut out = BytesMut::new();
        while let Some(chunk) = stream.recv_data().await? {
            out.put(chunk);
        }
        if resp.status() != StatusCode::OK {
            return Err(Error::Status {
                status: resp.status(),
                message: String::from_utf8_lossy(&out).into_owned(),
            });
        }
        Ok(out.freeze())
    }

    /// Fetch many keys; the result has one slot per key in request order.
    pub async fn get<'a, I>(&self, keys: I) -> Result<Vec<Option<Bytes>>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        Ok(wire::decode_values(
            self.call(wire::path::GET, wire::encode_keys(keys)).await?,
        )?)
    }

    /// Store many key/value pairs.
    pub async fn set<'a, I>(&self, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        self.call(wire::path::SET, wire::encode_entries(entries))
            .await?;
        Ok(())
    }

    /// Delete many keys; returns whether each one existed.
    pub async fn del<'a, I>(&self, keys: I) -> Result<Vec<bool>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        Ok(wire::decode_flags(
            self.call(wire::path::DEL, wire::encode_keys(keys)).await?,
        )?)
    }
}

/// Accepts any server certificate. Only for development against self-signed servers.
#[derive(Debug)]
struct SkipVerify(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
