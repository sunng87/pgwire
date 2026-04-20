use std::borrow::Cow;
use std::fmt;
use std::fmt::Write;
use std::num::NonZeroU32;
use std::ops::BitXor;
use std::str::{FromStr, Split};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use x509_certificate::SignatureAlgorithm;
use x509_certificate::certificate::CapturedX509Certificate;

use crate::api::ClientInfo;
/// Re-exports client-side SCRAM authentication types.
#[cfg(feature = "client-api")]
pub use crate::api::auth::sasl::scram::client::*;
use crate::api::auth::{AuthSource, LoginInfo, Password};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{Authentication, PasswordMessageFamily};

use super::SASLState;

#[cfg(feature = "_aws-lc-rs")]
use aws_lc_rs::{digest, hmac, pbkdf2};
#[cfg(all(feature = "_ring", not(feature = "_aws-lc-rs")))]
use ring::{digest, hmac, pbkdf2};

/// Default SCRAM iteration count.
pub const SCRAM_ITERATIONS: usize = 4096;

/// SCRAM authentication configuration and state.
#[derive(Debug)]
pub struct ScramAuth {
    auth_db: Arc<dyn AuthSource>,
    authenticator: ScramServerAuth,
}

impl ScramAuth {
    /// Creates a new SCRAM auth instance.
    pub fn new(auth_db: Arc<dyn AuthSource>) -> ScramAuth {
        ScramAuth {
            auth_db,
            authenticator: ScramServerAuth::new(),
        }
    }

    /// enable channel binding (SCRAM-SHA-256-PLUS) by configuring server
    /// certificate.
    ///
    /// Original pem data is required here. We will decode pem and use the first
    /// certificate as server certificate.
    pub fn configure_certificate(&mut self, certs_pem: &[u8]) -> PgWireResult<()> {
        self.authenticator.configure_certificate(certs_pem)
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
        self.authenticator.set_iterations(iterations);
    }

    /// Returns `true` if channel binding is configured.
    pub fn supports_channel_binding(&self) -> bool {
        self.authenticator.server_cert_sig.is_some()
    }
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

/// Generates a random nonce for SCRAM authentication.
pub fn random_nonce() -> String {
    STANDARD.encode(rand::random::<[u8; 18]>())
}

impl ScramAuth {
    /// Process incoming message and return response, new state
    pub async fn process_scram_message<C>(
        &self,
        client: &C,
        msg: PasswordMessageFamily,
        state: &SASLState,
    ) -> PgWireResult<(Authentication, SASLState)>
    where
        C: ClientInfo + Unpin + Send,
    {
        match state {
            SASLState::ScramClientFirstReceived => {
                // initial response, client_first
                let msg = msg.into_sasl_initial_response()?;
                let resp = msg.data.as_ref().ok_or_else(|| {
                    PgWireError::InvalidScramMessage("Empty client-first".to_owned())
                })?;

                let login_info = LoginInfo::from_client_info(client);
                let expected_password = self.auth_db.get_password(&login_info).await?;

                let (server_first, authenticator) = self
                    .authenticator
                    .on_client_first_message(resp, expected_password)?;

                Ok((
                    Authentication::SASLContinue(server_first.into()),
                    SASLState::ScramServerFirstSent(Box::new(authenticator)),
                ))
            }
            SASLState::ScramServerFirstSent(authenticator) => {
                // second response, client_final
                let resp = msg.into_sasl_response()?;
                let server_final = authenticator.on_client_final_message(&resp.data)?;
                Ok((
                    Authentication::SASLFinal(server_final.into()),
                    SASLState::Finished,
                ))
            }
            _ => Err(PgWireError::InvalidSASLState),
        }
    }
}

/// State machine for SCRAM server-side authentication
#[derive(Debug)]
pub struct ScramServerAuth {
    /// base64 encoded certificate signature for tls-server-end-point channel binding
    server_cert_sig: Option<Arc<String>>,
    /// iterations
    iterations: usize,
}

impl Default for ScramServerAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl ScramServerAuth {
    /// Creates a new SCRAM server authenticator.
    pub fn new() -> Self {
        Self {
            server_cert_sig: None,
            iterations: SCRAM_ITERATIONS,
        }
    }

    /// enable channel binding (SCRAM-SHA-256-PLUS) by configuring server
    /// certificate.
    ///
    /// Original pem data is required here. We will decode pem and use the first
    /// certificate as server certificate.
    pub fn configure_certificate(&mut self, certs_pem: &[u8]) -> PgWireResult<()> {
        self.server_cert_sig = Some(Arc::new(
            STANDARD.encode(compute_cert_signature(certs_pem)?),
        ));
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

    /// Client first message with expected username (optional) and expected password
    pub fn on_client_first_message(
        &self,
        client_first_message: &[u8],
        expected_password: Password,
    ) -> PgWireResult<(String, ScramServerAuthWaitingForClientFinal)> {
        let client_first = ClientFirst::from_str(decode_str(client_first_message)?)?;

        // create server_first and send
        let mut new_nonce = client_first.bare.nonce.clone();
        new_nonce.push_str(random_nonce().as_str());

        let server_first = ServerFirst::new(
            new_nonce,
            STANDARD.encode(
                expected_password
                    .salt
                    .as_ref()
                    .expect("Salt required for SCRAM auth source"),
            ),
            self.iterations,
        );

        Ok((
            server_first.to_string(),
            ScramServerAuthWaitingForClientFinal {
                server_cert_sig: self.server_cert_sig.clone(),
                expected_password,
                channel_binding: client_first.gs2header,
                client_first_message_bare: client_first.bare,
                server_first_message: server_first,
            },
        ))
    }
}

/// Follow-up of [`ScramServerAuth`] waiting for the client final message
#[derive(Debug)]
pub struct ScramServerAuthWaitingForClientFinal {
    server_cert_sig: Option<Arc<String>>,
    expected_password: Password,
    channel_binding: Gs2Header,
    client_first_message_bare: ClientFirstBare,
    server_first_message: ServerFirst,
}

impl ScramServerAuthWaitingForClientFinal {
    /// Processes the client final message and verifies the proof.
    pub fn on_client_final_message(&self, client_final_message: &[u8]) -> PgWireResult<String> {
        let client_final = ClientFinal::from_str(decode_str(client_final_message)?)?;

        let channel_binding = compute_channel_binding(
            self.server_cert_sig.as_ref().map(|s| s.as_str()),
            &self.channel_binding,
        );
        if client_final.without_proof.channel_binding != channel_binding {
            return Err(PgWireError::InvalidScramMessage(
                "Channel binding mismatch".to_owned(),
            ));
        }

        let computed_client_proof = compute_client_proof(
            self.expected_password.password(),
            &self.client_first_message_bare,
            &self.server_first_message,
            &client_final.without_proof,
        );
        if computed_client_proof == client_final.proof {
            let verifier = compute_server_signature(
                self.expected_password.password(),
                &self.client_first_message_bare,
                &self.server_first_message,
                &client_final.without_proof,
            );
            Ok(ServerFinal::Success { verifier }.to_string())
        } else {
            Err(PgWireError::InvalidPassword(
                self.client_first_message_bare.username.clone(),
            ))
        }
    }
}

#[cfg(feature = "client-api")]
mod client {
    use super::*;
    use crate::error::{PgWireClientError, PgWireClientResult};

    /// Client-side SCRAM authenticator.
    pub struct ScramClientAuth {
        username: String,
        password: String,
    }

    impl ScramClientAuth {
        /// Creates a new SCRAM client authenticator.
        pub fn new(username: String, password: String) -> Self {
            Self { username, password }
        }

        /// Starts authentication and build the client first message
        pub fn build_client_first(
            &self,
        ) -> PgWireClientResult<(String, ScramClientAuthWaitingForServerFirst)> {
            let username = stringprep::saslprep(&self.username)
                .map_err(|e| PgWireClientError::InvalidConfig(format!("Invalid username: {e}")))?
                .into_owned();
            let nonce = random_nonce();
            let client_first_message_bare = ClientFirstBare { username, nonce };
            let c_bind_flag = CBindFlag::N; // TODO: support channel bindings
            let channel_binding = Gs2Header {
                c_bind_flag: c_bind_flag.clone(),
                authzid: None,
            };
            let client_first_message = ClientFirst {
                gs2header: channel_binding.clone(),
                bare: client_first_message_bare.clone(),
            };
            Ok((
                client_first_message.to_string(),
                ScramClientAuthWaitingForServerFirst {
                    client_first_message_bare,
                    channel_binding,
                    password: self.password.clone(),
                },
            ))
        }
    }

    /// Follow-up of [`ScramClientAuth`] waiting for the server first message
    pub struct ScramClientAuthWaitingForServerFirst {
        client_first_message_bare: ClientFirstBare,
        channel_binding: Gs2Header,
        password: String,
    }

    impl ScramClientAuthWaitingForServerFirst {
        /// Reacts on server first message reply and build the client final message
        pub fn build_client_final(
            &self,
            server_first_message: &[u8],
        ) -> PgWireClientResult<(String, ScramClientAuthWaitingForServerFinal)> {
            let server_first_message = ServerFirst::from_str(decode_str(server_first_message)?)?;

            let channel_binding = compute_channel_binding(None, &self.channel_binding);
            let client_final_without_proof = ClientFinalWithoutProof {
                channel_binding: channel_binding.clone(),
                nonce: server_first_message.nonce.clone(),
            };

            let salted_password = gen_salted_password(
                &self.password,
                &STANDARD.decode(&server_first_message.salt).map_err(|e| {
                    PgWireClientError::ScramError(format!("Invalid salt base64 encoding: {e}"))
                })?,
                server_first_message.iteration_count,
            );
            let client_proof = compute_client_proof(
                &salted_password,
                &self.client_first_message_bare,
                &server_first_message,
                &client_final_without_proof,
            );

            let client_final_message = ClientFinal {
                without_proof: client_final_without_proof.clone(),
                proof: client_proof,
            };
            Ok((
                client_final_message.to_string(),
                ScramClientAuthWaitingForServerFinal {
                    salted_password,
                    client_first_message_bare: self.client_first_message_bare.clone(),
                    server_first_message,
                    client_final_without_proof,
                },
            ))
        }
    }

    /// Follow-up of [`ScramClientAuth`] waiting for the server final message
    pub struct ScramClientAuthWaitingForServerFinal {
        salted_password: Vec<u8>,
        client_first_message_bare: ClientFirstBare,
        server_first_message: ServerFirst,
        client_final_without_proof: ClientFinalWithoutProof,
    }

    impl ScramClientAuthWaitingForServerFinal {
        /// Verifies that the server final message is a success
        pub fn verify_server_final(&self, server_final_message: &[u8]) -> PgWireClientResult<()> {
            match ServerFinal::from_str(decode_str(server_final_message)?)? {
                ServerFinal::Success { verifier } => {
                    let expected_verifier = compute_server_signature(
                        &self.salted_password,
                        &self.client_first_message_bare,
                        &self.server_first_message,
                        &self.client_final_without_proof,
                    );
                    if expected_verifier == verifier {
                        Ok(())
                    } else {
                        Err(PgWireClientError::ScramError(
                            "Invalid verifier returned by the server".into(),
                        ))
                    }
                }
                ServerFinal::Error { value } => Err(PgWireClientError::ScramError(value)),
            }
        }
    }
}

fn compute_channel_binding(
    server_cert_sig: Option<&str>,
    client_channel_binding: &Gs2Header,
) -> String {
    match &client_channel_binding.c_bind_flag {
        CBindFlag::CbName(p) if p == "tls-server-end-point" => {
            format!(
                "{}{}",
                STANDARD.encode(client_channel_binding.to_string()),
                server_cert_sig.unwrap_or("")
            )
        }
        _ => STANDARD.encode(client_channel_binding.to_string()),
    }
}

fn compute_client_proof(
    salted_password: &[u8],
    client_first_message_bare: &ClientFirstBare,
    server_first_message: &ServerFirst,
    client_final_without_proof: &ClientFinalWithoutProof,
) -> String {
    let client_key = hmac(salted_password, b"Client Key");
    let stored_key = h(&client_key);
    let auth_msg = format!(
        "{},{},{}",
        client_first_message_bare, server_first_message, client_final_without_proof
    );
    let client_signature = hmac(&stored_key, auth_msg.as_bytes());

    STANDARD.encode(xor(&client_key, &client_signature))
}

fn compute_server_signature(
    salted_password: &[u8],
    client_first_message_bare: &ClientFirstBare,
    server_first_message: &ServerFirst,
    client_final_without_proof: &ClientFinalWithoutProof,
) -> String {
    let server_key = hmac(salted_password, b"Server Key");
    let auth_msg = format!(
        "{},{},{}",
        client_first_message_bare, server_first_message, client_final_without_proof
    );
    STANDARD.encode(hmac(&server_key, auth_msg.as_bytes()))
}

fn decode_str(data: &[u8]) -> PgWireResult<&str> {
    str::from_utf8(data).map_err(|e| PgWireError::InvalidScramMessage(e.to_string()))
}

#[derive(Debug)]
struct ClientFirst {
    gs2header: Gs2Header,
    bare: ClientFirstBare,
}

impl FromStr for ClientFirst {
    type Err = PgWireError;
    fn from_str(s: &str) -> PgWireResult<Self> {
        // client-first-message = gs2-header client-first-message-bare
        // gs2-header = gs2-cbind-flag "," [ authzid ] ","
        // authzid = "a=" saslname
        // gs2-cbind-flag = ("p=" cb-name) / "n" / "y"
        // client-first-message-bare = [reserved-mext ","] username "," nonce ["," extensions]
        // reserved-mext = "m=" 1*(value-char)
        // username = "n=" saslname
        // nonce = "r=" c-nonce [s-nonce]

        let mut parts = ScamMessageChunker::new(s);

        let c_bind_flag = match parts.next_required()? {
            "y" => CBindFlag::Y,
            "n" => CBindFlag::N,
            c_bind_flag => {
                if let Some(cb_name) = c_bind_flag.strip_prefix("p=") {
                    CBindFlag::CbName(cb_name.into())
                } else {
                    return Err(PgWireError::InvalidScramMessage(s.to_owned()));
                }
            }
        };

        let authzid = parts.next_required()?;
        let authzid = if let Some(saslname) = authzid.strip_prefix("a=") {
            Some(saslname.to_owned())
        } else if authzid.is_empty() {
            None
        } else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        let reserved_mex_or_username = parts.next_required()?;
        let username = if reserved_mex_or_username.starts_with("m=") {
            // It's actually reserved-mex, move to next part
            parts.next_required()?
        } else {
            reserved_mex_or_username
        };
        let Some(username) = username.strip_prefix("n=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        let Some(nonce) = parts.next_required()?.strip_prefix("r=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        Ok(Self {
            gs2header: Gs2Header {
                c_bind_flag,
                authzid,
            },
            bare: ClientFirstBare {
                username: username.to_owned(),
                nonce: nonce.to_owned(),
            },
        })
    }
}

impl fmt::Display for ClientFirst {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.gs2header.fmt(f)?;
        self.bare.fmt(f)
    }
}

#[derive(Debug, Clone)]
struct Gs2Header {
    c_bind_flag: CBindFlag,
    authzid: Option<String>,
}

impl fmt::Display for Gs2Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.c_bind_flag.fmt(f)?;
        f.write_char(',')?;
        if let Some(authzid) = &self.authzid {
            f.write_str("a=")?;
            f.write_str(authzid)?;
        }
        f.write_char(',')
    }
}

#[derive(Debug, Clone)]
enum CBindFlag {
    CbName(String),
    N,
    Y,
}

impl fmt::Display for CBindFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CbName(name) => {
                f.write_str("p=")?;
                f.write_str(name)
            }
            Self::N => f.write_str("n"),
            Self::Y => f.write_str("y"),
        }
    }
}

#[derive(Debug, Clone)]
struct ClientFirstBare {
    username: String,
    nonce: String,
}

impl fmt::Display for ClientFirstBare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("n=")?;
        f.write_str(&self.username)?;
        f.write_str(",r=")?;
        f.write_str(&self.nonce)
    }
}

#[derive(Debug, new)]
struct ServerFirst {
    nonce: String,
    salt: String,
    iteration_count: usize,
}

impl FromStr for ServerFirst {
    type Err = PgWireError;

    fn from_str(s: &str) -> PgWireResult<Self> {
        //  server-first-message = [reserved-mext ","] nonce "," salt "," iteration-count ["," extensions]
        // reserved-mext = "m=" 1*(value-char)
        // nonce = "r=" c-nonce [s-nonce]
        // salt = "s=" base64
        // iteration-count = "i=" posit-number

        let mut parts = ScamMessageChunker::new(s);

        let reserved_mex_or_nonce = parts.next_required()?;
        let nonce = if reserved_mex_or_nonce.starts_with("m=") {
            // It's actually reserved-mex, move to next part
            parts.next_required()?
        } else {
            reserved_mex_or_nonce
        };
        let Some(nonce) = nonce.strip_prefix("r=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        let Some(salt) = parts.next_required()?.strip_prefix("s=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        let Some(iteration_count) = parts.next_required()?.strip_prefix("i=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };
        let Ok(iteration_count) = iteration_count.parse() else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        Ok(Self {
            nonce: nonce.to_owned(),
            salt: salt.to_owned(),
            iteration_count,
        })
    }
}

impl fmt::Display for ServerFirst {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("r=")?;
        f.write_str(&self.nonce)?;
        f.write_str(",s=")?;
        f.write_str(&self.salt)?;
        f.write_str(",i=")?;
        self.iteration_count.fmt(f)
    }
}

#[derive(Debug)]
struct ClientFinal {
    without_proof: ClientFinalWithoutProof,
    proof: String,
}

impl FromStr for ClientFinal {
    type Err = PgWireError;

    fn from_str(s: &str) -> PgWireResult<Self> {
        // client-final-message = client-final-message-without-proof "," proof
        // client-final-message-without-proof = channel-binding "," nonce ["," extensions]
        // channel-binding = "c=" base64
        // nonce = "r=" c-nonce [s-nonce]
        // proof = "p=" base64

        let mut parts = ScamMessageChunker::new(s);

        let Some(channel_binding) = parts.next_required()?.strip_prefix("c=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        let Some(nonce) = parts.next_required()?.strip_prefix("r=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        let Some(proof) = parts.last_required()?.strip_prefix("p=") else {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        };

        Ok(Self {
            without_proof: ClientFinalWithoutProof {
                channel_binding: channel_binding.to_owned(),
                nonce: nonce.to_owned(),
            },
            proof: proof.to_owned(),
        })
    }
}

impl fmt::Display for ClientFinal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.without_proof.fmt(f)?;
        f.write_str(",p=")?;
        f.write_str(&self.proof)
    }
}

#[derive(Debug, Clone)]
struct ClientFinalWithoutProof {
    channel_binding: String,
    nonce: String,
}

impl fmt::Display for ClientFinalWithoutProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("c=")?;
        f.write_str(&self.channel_binding)?;
        f.write_str(",r=")?;
        f.write_str(&self.nonce)
    }
}

#[derive(Debug, new)]
enum ServerFinal {
    Success { verifier: String },
    Error { value: String },
}

impl FromStr for ServerFinal {
    type Err = PgWireError;

    fn from_str(s: &str) -> PgWireResult<Self> {
        // server-final-message = (server-error / verifier) ["," extensions]
        // server-error = "e=" server-error-value
        // verifier = "v=" base64

        let mut parts = ScamMessageChunker::new(s);
        let next = parts.next_required()?;
        if let Some(verifier) = next.strip_prefix("v=") {
            Ok(Self::Success {
                verifier: verifier.to_owned(),
            })
        } else if let Some(value) = next.strip_prefix("e=") {
            Ok(Self::Error {
                value: value.to_owned(),
            })
        } else {
            Err(PgWireError::InvalidScramMessage(s.to_owned()))
        }
    }
}

impl fmt::Display for ServerFinal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success { verifier } => {
                f.write_str("v=")?;
                f.write_str(verifier)
            }
            Self::Error { value } => {
                f.write_str("e=")?;
                f.write_str(value)
            }
        }
    }
}

struct ScamMessageChunker<'a> {
    message: &'a str,
    iter: Split<'a, char>,
}

impl<'a> ScamMessageChunker<'a> {
    fn new(message: &'a str) -> Self {
        Self {
            message,
            iter: message.split(','),
        }
    }

    fn next_required(&mut self) -> PgWireResult<&'a str> {
        self.iter
            .next()
            .ok_or_else(|| PgWireError::InvalidScramMessage(self.message.to_owned()))
    }

    fn last_required(&mut self) -> PgWireResult<&'a str> {
        let mut maybe_last = None;
        for e in &mut self.iter {
            maybe_last = Some(e);
        }
        maybe_last.ok_or_else(|| PgWireError::InvalidScramMessage(self.message.to_owned()))
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
///    behaviour is undefined at the time.
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "client-api")]
    use super::client::*;
    use super::*;
    #[cfg(feature = "client-api")]
    use crate::error::PgWireClientResult;

    #[test]
    fn test_client_first_roundtrip() {
        assert_eq!(
            ClientFirst::from_str("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL")
                .unwrap()
                .to_string(),
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
        );
        assert_eq!(
            ClientFirst::from_str("y,,n=user,r=fyko+d2lbbFgONRv9qkxdawL")
                .unwrap()
                .to_string(),
            "y,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
        );
        assert_eq!(
            ClientFirst::from_str("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL")
                .unwrap()
                .to_string(),
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
        );
        assert_eq!(
            ClientFirst::from_str("p=foo,,n=user,r=fyko+d2lbbFgONRv9qkxdawL")
                .unwrap()
                .to_string(),
            "p=foo,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
        );
        assert_eq!(
            ClientFirst::from_str("n,,m=foo,n=user,r=fyko+d2lbbFgONRv9qkxdawL,foo")
                .unwrap()
                .to_string(),
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
        );
    }

    #[test]
    fn test_server_first_roundtrip() {
        assert_eq!(
            ServerFirst::from_str(
                "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
            )
            .unwrap()
            .to_string(),
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
        );
        assert_eq!(
            ServerFirst::from_str(
                "m=foo,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
            )
            .unwrap()
            .to_string(),
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
        );
    }

    #[test]
    fn test_client_final_roundtrip() {
        assert_eq!(
            ClientFinal::from_str(
                "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
            )
            .unwrap()
            .to_string(),
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
    }

    #[test]
    fn test_server_final_roundtrip() {
        assert_eq!(
            ServerFinal::from_str("v=rmF9pqV8S7suAoZWja4dJRkFsKQ=")
                .unwrap()
                .to_string(),
            "v=rmF9pqV8S7suAoZWja4dJRkFsKQ="
        );

        assert_eq!(
            ServerFinal::from_str("e=invalid-encoding")
                .unwrap()
                .to_string(),
            "e=invalid-encoding"
        );
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn test_auth_roundtrip() -> PgWireClientResult<()> {
        assert_auth_roundtrip("test", "foo", b"bar", 12)
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn test_auth_roundtrip_with_special_characters() -> PgWireClientResult<()> {
        assert_auth_roundtrip("é =", "é\n=", b"bar", 12)
    }

    #[cfg(feature = "client-api")]
    fn assert_auth_roundtrip(
        username: &str,
        password: &str,
        salt: &[u8],
        iterations: usize,
    ) -> PgWireClientResult<()> {
        let client = ScramClientAuth::new(username.into(), password.into());
        let mut server = ScramServerAuth::new();
        server.set_iterations(iterations);
        let (client_first_message, client) = client.build_client_first()?;
        let (server_first_message, server) = server.on_client_first_message(
            client_first_message.as_bytes(),
            Password::new(
                Some(salt.into()),
                gen_salted_password(password, salt, iterations),
            ),
        )?;
        let (client_final_message, client) =
            client.build_client_final(server_first_message.as_bytes())?;
        let server_final_message =
            server.on_client_final_message(client_final_message.as_bytes())?;
        client.verify_server_final(server_final_message.as_bytes())
    }

    #[cfg(feature = "client-api")]
    #[test]
    fn test_auth_with_bad_password() -> PgWireClientResult<()> {
        let client = ScramClientAuth::new("foo".into(), "bar".into());
        let server = ScramServerAuth::new();
        let (client_first_message, client) = client.build_client_first()?;
        let (server_first_message, server) = server.on_client_first_message(
            client_first_message.as_bytes(),
            Password::new(
                Some(b"salt".into()),
                b"baz".into(), // Another password
            ),
        )?;
        let (client_final_message, _) =
            client.build_client_final(server_first_message.as_bytes())?;
        assert!(matches!(
            server
                .on_client_final_message(client_final_message.as_bytes())
                .unwrap_err(),
            PgWireError::InvalidPassword(_)
        ));
        Ok(())
    }
}
