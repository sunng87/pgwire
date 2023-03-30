use std::borrow::Cow;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::ops::BitXor;
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::{Sink, SinkExt};
use ring::digest;
use ring::hmac;
use ring::pbkdf2;
use tokio::sync::Mutex;
use x509_certificate::certificate::CapturedX509Certificate;
use x509_certificate::SignatureAlgorithm;

use crate::api::auth::{AuthSource, LoginInfo, Password};
use crate::api::{ClientInfo, MakeHandler, PgWireConnectionState};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::Authentication;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::{ServerParameterProvider, StartupHandler};

#[derive(Debug)]
pub enum ScramState {
    Initial,
    // cached password, channel_binding and partial auth-message
    ServerFirstSent(Password, String, String),
}

#[derive(Debug)]
pub struct SASLScramAuthStartupHandler<A, P> {
    auth_db: Arc<A>,
    parameter_provider: Arc<P>,
    /// state of the client-server communication
    state: Mutex<ScramState>,
    /// base64 encoded certificate signature for tls-server-end-point channel binding
    server_cert_sig: Option<Arc<String>>,
    /// iterations
    iterations: usize,
}

/// Compute salted password from raw password as defined in
/// [RFC5802](https://www.rfc-editor.org/rfc/rfc5802#section-3)
///
/// ```text
/// SaltedPassword  := Hi(Normalize(password), salt, i)
/// ```
///
/// This is a helper function for `AuthSource` implementation if passwords are
/// stored in cleartext.
pub fn gen_salted_password(password: &str, salt: &[u8], iters: usize) -> Vec<u8> {
    // according to postgres doc, if we failed to normalize password, use
    // original password instead of throwing error
    let normalized_pass = stringprep::saslprep(password).unwrap_or(Cow::Borrowed(password));
    let pass_bytes = normalized_pass.as_ref().as_bytes();
    hi(pass_bytes, salt, iters)
}

pub fn random_nonce() -> String {
    let mut buf = [0u8; 18];
    for v in buf.iter_mut() {
        *v = rand::random::<u8>();
    }

    STANDARD.encode(buf)
}

impl<A, P> SASLScramAuthStartupHandler<A, P> {
    fn compute_channel_binding(&self, client_channel_binding: &str) -> String {
        if client_channel_binding.starts_with("p=tls-server-end-point") {
            format!(
                "{}{}",
                STANDARD.encode(client_channel_binding),
                self.server_cert_sig
                    .as_deref()
                    .map(|v| &v[..])
                    .unwrap_or("")
            )
        } else {
            STANDARD.encode(client_channel_binding.as_bytes())
        }
    }
}

#[async_trait]
impl<A: AuthSource, P: ServerParameterProvider> StartupHandler
    for SASLScramAuthStartupHandler<A, P>
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                super::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                let supported_mechanisms = if self.server_cert_sig.is_some() {
                    vec!["SCRAM-SHA-256".to_owned(), "SCRAM-SHA-256-PLUS".to_owned()]
                } else {
                    vec!["SCRAM-SHA-256".to_owned()]
                };
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        supported_mechanisms,
                    )))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(msg) => {
                let salt_and_salted_pass = {
                    let state = self.state.lock().await;
                    match *state {
                        ScramState::Initial => {
                            let login_info = LoginInfo::from_client_info(client);
                            self.auth_db.get_password(&login_info).await?
                        }
                        ScramState::ServerFirstSent(ref pass, _, _) => pass.clone(),
                    }
                };

                let mut success = false;
                let resp = {
                    // this should never block
                    let mut state = self.state.lock().await;
                    match *state {
                        ScramState::Initial => {
                            // initial response, client_first
                            let resp = msg.into_sasl_initial_response()?;
                            // parse into client_first
                            let client_first = resp
                                .data()
                                .as_ref()
                                .ok_or_else(|| {
                                    PgWireError::InvalidScramMessage(
                                        "Empty client-first".to_owned(),
                                    )
                                })
                                .and_then(|data| {
                                    ClientFirst::try_new(String::from_utf8_lossy(data).as_ref())
                                })?;
                            // dbg!(&client_first);

                            // create server_first and send
                            let mut new_nonce = client_first.nonce.clone();
                            new_nonce.push_str(random_nonce().as_str());

                            let server_first = ServerFirst::new(
                                new_nonce,
                                STANDARD.encode(
                                    salt_and_salted_pass
                                        .salt()
                                        .as_ref()
                                        .expect("Salt required for SCRAM auth source"),
                                ),
                                self.iterations,
                            );
                            let server_first_message = server_first.message();

                            *state = ScramState::ServerFirstSent(
                                salt_and_salted_pass,
                                client_first.channel_binding(),
                                format!("{},{}", client_first.bare(), &server_first_message),
                            );
                            Authentication::SASLContinue(Bytes::from(server_first_message))
                        }
                        ScramState::ServerFirstSent(
                            _,
                            ref channel_binding_prefix,
                            ref partial_auth_msg,
                        ) => {
                            // second response, client_final
                            let resp = msg.into_sasl_response()?;
                            let client_final = ClientFinal::try_new(
                                String::from_utf8_lossy(resp.data().as_ref()).as_ref(),
                            )?;
                            // dbg!(&client_final);

                            let channel_binding =
                                self.compute_channel_binding(channel_binding_prefix);
                            client_final.validate_channel_binding(&channel_binding)?;

                            let salted_password = salt_and_salted_pass.password();
                            let client_key = hmac(salted_password.as_ref(), b"Client Key");
                            let stored_key = h(client_key.as_ref());
                            let auth_msg =
                                format!("{},{}", partial_auth_msg, client_final.without_proof());
                            let client_signature = hmac(stored_key.as_ref(), auth_msg.as_bytes());

                            let computed_client_proof = STANDARD.encode(
                                xor(client_key.as_ref(), client_signature.as_ref()).as_slice(),
                            );

                            if computed_client_proof == client_final.proof {
                                let server_key = hmac(salted_password.as_ref(), b"Server Key");
                                let server_signature =
                                    hmac(server_key.as_ref(), auth_msg.as_bytes());
                                let server_final =
                                    ServerFinalSuccess::new(STANDARD.encode(server_signature));
                                success = true;
                                Authentication::SASLFinal(Bytes::from(server_final.message()))
                            } else {
                                let server_final =
                                    ServerFinalError::new("invalid-proof".to_owned());
                                Authentication::SASLFinal(Bytes::from(server_final.message()))
                            }
                        }
                    }
                };

                client
                    .send(PgWireBackendMessage::Authentication(resp))
                    .await?;

                if success {
                    super::finish_authentication(client, self.parameter_provider.as_ref()).await
                }
            }
            _ => {}
        }

        Ok(())
    }
}

#[derive(Debug, new)]
pub struct MakeSASLScramAuthStartupHandler<A, P> {
    auth_db: Arc<A>,
    parameter_provider: Arc<P>,
    #[new(default)]
    server_cert_sig: Option<Arc<String>>,
    #[new(value = "4096")]
    iterations: usize,
}

impl<A, P> MakeSASLScramAuthStartupHandler<A, P> {
    /// enable channel binding (SCRAM-SHA-256-PLUS) by configuring server
    /// certificate.
    ///
    /// Original pem data is required here. We will decode pem and use the first
    /// certificate as server certificate.
    pub fn configure_certificate(&mut self, certs_pem: &[u8]) -> PgWireResult<()> {
        let sig = compute_cert_signature(certs_pem)?;
        self.server_cert_sig = Some(Arc::new(STANDARD.encode(sig)));
        Ok(())
    }

    /// Set password hash iteration count, according to SCRAM RFC, a minimal of
    /// 4096 is required.
    ///
    /// Note that this implementation does not hash password, it just tells
    /// client to hash with this iteration count. You have to implement password
    /// hashing in your `AuthSource` implementation, either after fetching
    /// cleartext password, or before storing hashed password. And this number
    /// should be identical to your `AuthSource` implementation.
    pub fn set_iterations(&mut self, iterations: usize) {
        self.iterations = iterations;
    }
}

impl<A, P> MakeHandler for MakeSASLScramAuthStartupHandler<A, P> {
    type Handler = Arc<SASLScramAuthStartupHandler<A, P>>;

    fn make(&self) -> Self::Handler {
        Arc::new(SASLScramAuthStartupHandler {
            auth_db: self.auth_db.clone(),
            parameter_provider: self.parameter_provider.clone(),
            state: Mutex::new(ScramState::Initial),
            server_cert_sig: self.server_cert_sig.clone(),
            iterations: self.iterations,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct ClientFirst {
    cbind_flag: String,
    auth_zid: String,
    username: String,
    nonce: String,
}

impl ClientFirst {
    fn try_new(s: &str) -> PgWireResult<ClientFirst> {
        let parts: Vec<&str> = s.splitn(4, ',').collect();
        if parts.len() != 4
            || !Self::validate_cbind_flag(parts[0])
            || !parts[2].starts_with("n=")
            || !parts[3].starts_with("r=")
        {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        }
        // now it's safe to unwrap
        let cbind_flag = parts[0].to_owned();
        // add additional check when we don't have channel binding
        // if cbind_flag != 'n' {
        //     return Err(PgWireError::InvalidScramMessage(format!(
        //         "cbing_flag: {}, but channel binding not supported.",
        //         cbind_flag
        //     )));
        // }

        let auth_zid = parts[1].to_owned();
        let username = parts[2].strip_prefix("n=").unwrap().to_owned();
        let nonce = parts[3].strip_prefix("r=").unwrap().to_owned();

        Ok(ClientFirst {
            cbind_flag,
            auth_zid,
            username,
            nonce,
        })
    }

    fn validate_cbind_flag(flag: &str) -> bool {
        flag == "n" || flag == "y" || flag.starts_with("p=")
    }

    fn bare(&self) -> String {
        format!("n={},r={}", self.username, self.nonce)
    }

    fn channel_binding(&self) -> String {
        format!("{},{},", self.cbind_flag, self.auth_zid)
    }
}

#[derive(Debug, new)]
struct ServerFirst {
    nonce: String,
    salt: String,
    iteration: usize,
}

impl ServerFirst {
    fn message(&self) -> String {
        format!("r={},s={},i={}", self.nonce, self.salt, self.iteration)
    }
}

#[derive(Debug)]
struct ClientFinal {
    channel_binding: String,
    nonce: String,
    proof: String,
}

impl ClientFinal {
    fn try_new(s: &str) -> PgWireResult<ClientFinal> {
        let parts: Vec<&str> = s.splitn(3, ',').collect();
        if parts.len() != 3
            || !parts[0].starts_with("c=")
            || !parts[1].starts_with("r=")
            || !parts[2].starts_with("p=")
        {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        }

        // safe to unwrap after check

        let channel_binding = parts[0].strip_prefix("c=").unwrap().to_owned();
        let nonce = parts[1].strip_prefix("r=").unwrap().to_owned();
        let proof = parts[2].strip_prefix("p=").unwrap().to_owned();

        Ok(ClientFinal {
            channel_binding,
            nonce,
            proof,
        })
    }

    fn validate_channel_binding(&self, encoded_channel_binding: &str) -> PgWireResult<()> {
        // compare
        if self.channel_binding == encoded_channel_binding {
            Ok(())
        } else {
            Err(PgWireError::InvalidScramMessage(
                "Channel binding mismatch".to_owned(),
            ))
        }
    }

    fn without_proof(&self) -> String {
        format!("c={},r={}", self.channel_binding, self.nonce)
    }
}

#[derive(Debug, new)]
struct ServerFinalSuccess {
    verifier: String,
}

impl ServerFinalSuccess {
    fn message(&self) -> String {
        format!("v={}", self.verifier)
    }
}

#[derive(Debug, new)]
struct ServerFinalError {
    error: String,
}

impl ServerFinalError {
    fn message(&self) -> String {
        format!("e={}", self.error)
    }
}

fn hi(normalized_password: &[u8], salt: &[u8], iterations: usize) -> Vec<u8> {
    let mut buf = [0u8; 32];

    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations as u32).unwrap(),
        salt,
        normalized_password,
        &mut buf,
    );
    buf.to_vec()
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mac = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&mac, msg).as_ref().to_vec()
}

fn h(msg: &[u8]) -> Vec<u8> {
    digest::digest(&digest::SHA256, msg).as_ref().to_vec()
}

fn xor(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    lhs.iter()
        .zip(rhs.iter())
        .map(|(l, r)| l.bitxor(r))
        .collect()
}

/// Compute signature of server certificate for `tls-server-end-point` channel
/// binding.
///
/// This behaviour is defined in
/// [RFC5929](https://www.rfc-editor.org/rfc/rfc5929)
///
/// 1. use sha-256 if the certificate's algorithm is md5 or sha-1
/// 2. use the certificate's algorithm if it's neither md5 or sha-1
/// 3. if the certificate has 0 or more than 1 signature algorithm, the
/// behaviour is undefined at the time.
fn compute_cert_signature(cert: &[u8]) -> PgWireResult<Vec<u8>> {
    let certs = CapturedX509Certificate::from_pem_multiple(cert)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let x509 = &certs[0];
    let raw = x509.constructed_data();
    match x509.signature_algorithm() {
        Some(SignatureAlgorithm::RsaSha1)
        | Some(SignatureAlgorithm::RsaSha256)
        | Some(SignatureAlgorithm::EcdsaSha256) => {
            Ok(digest::digest(&digest::SHA256, raw).as_ref().to_vec())
        }
        Some(SignatureAlgorithm::RsaSha384) | Some(SignatureAlgorithm::EcdsaSha384) => {
            Ok(digest::digest(&digest::SHA384, raw).as_ref().to_vec())
        }
        Some(SignatureAlgorithm::RsaSha512) => {
            Ok(digest::digest(&digest::SHA512, raw).as_ref().to_vec())
        }
        _ => Err(PgWireError::UnsupportedCertificateSignatureAlgorithm),
    }
}
