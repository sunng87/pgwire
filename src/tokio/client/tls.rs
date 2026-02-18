use std::sync::Arc;

use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::client;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::{self, CryptoProvider, WebPkiSupportedAlgorithms};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::ParsedCertificate;
use tokio_rustls::rustls::{CertificateError, Error};
use tokio_rustls::rustls::{DigitallySignedStruct, SignatureScheme};

/// A server certificate verifier that validates the certificate chain but skips hostname verification.
///
/// This is useful for PostgreSQL's `verify-ca` SSL mode, which verifies that the server
/// certificate is issued by a trusted CA, but does not verify that the certificate's
/// hostname matches the server.
///
/// # Example
///
/// ```no_run
/// use tokio_rustls::rustls::{RootCertStore, ClientConfig, crypto};
/// use tokio_rustls::rustls::pki_types::CertificateDer;
/// use pgwire::tokio::client::tls::SkipHostnameVerifier;
/// use std::sync::Arc;
///
/// # fn build_roots() -> Arc<RootCertStore> { Arc::new(RootCertStore::empty()) }
/// let roots = build_roots();
///
/// // Create a crypto provider (requires rustls ring feature)
/// let provider = crypto::CryptoProvider::get_default().unwrap().clone();
///
/// let verifier = SkipHostnameVerifier::new_with_provider(roots, provider);
///
/// let mut config = ClientConfig::builder()
///     .dangerous()
///     .with_custom_certificate_verifier(Arc::new(verifier));
/// ```
#[derive(Debug, Clone)]
pub struct SkipHostnameVerifier {
    roots: Arc<RootCertStore>,
    supported: WebPkiSupportedAlgorithms,
}

impl SkipHostnameVerifier {
    /// Create a new `SkipHostnameVerifier` with the provided root certificates and crypto provider.
    ///
    /// # Arguments
    ///
    /// * `roots` - The set of root CA certificates to trust
    /// * `provider` - The crypto provider to use for signature verification algorithms
    pub fn new_with_provider(
        roots: impl Into<Arc<RootCertStore>>,
        provider: Arc<CryptoProvider>,
    ) -> Self {
        Self {
            roots: roots.into(),
            supported: provider.signature_verification_algorithms,
        }
    }
}

impl ServerCertVerifier for SkipHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let cert = ParsedCertificate::try_from(end_entity)
            .map_err(|_| Error::InvalidCertificate(CertificateError::BadEncoding))?;

        client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.supported.all,
        )?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        crypto::verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        crypto::verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

/// A server certificate verifier that accepts any certificate without validation.
///
/// This is useful for PostgreSQL's `prefer` and `required` SSL modes, which do not
/// perform any certificate validation.
///
/// **Security Warning**: This verifier accepts ANY certificate without validation,
/// making connections vulnerable to man-in-the-middle attacks. Use with extreme caution.
///
/// # Example
///
/// ```no_run
/// use tokio_rustls::rustls::ClientConfig;
/// use pgwire::tokio::client::tls::NoopVerifier;
/// use std::sync::Arc;
///
/// let mut config = ClientConfig::builder()
///     .dangerous()
///     .with_custom_certificate_verifier(Arc::new(NoopVerifier));
/// ```
#[derive(Debug, Clone, Copy)]
pub struct NoopVerifier;

impl ServerCertVerifier for NoopVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![]
    }
}
