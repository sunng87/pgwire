//! A server with a fully customized startup flow, built from pgwire's
//! composable pieces:
//!
//! - a custom `StartupHandler` owns the protocol: it performs pre-auth
//!   validation (database must be specified), picks the authentication
//!   mechanism **per connection** (cleartext for `legacy` users, SCRAM with
//!   stored verifiers for everyone else), and delegates the mechanism
//!   state machine to an embedded `AuthDriver`,
//! - the user store only keeps postgres-style SCRAM verifiers
//!   (`stored_key`/`server_key`) — no password material on the server,
//! - unknown SCRAM users get a mock verifier, making "user not found"
//!   indistinguishable from a wrong password.
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;
use tokio::net::TcpListener;

use pgwire::api::auth::AuthDriver;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::auth::cleartext::CleartextPasswordAuth;
use pgwire::api::auth::driver::AuthOutcome;
use pgwire::api::auth::mechanism::AuthMechanismFactory;
use pgwire::api::auth::sasl::scram::{ScramAuth, ScramSecret, gen_salted_password};
use pgwire::api::auth::{
    AuthSource, LoginInfo, Password, StartupHandler, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

/// The identity bound to a session after successful authentication.
#[derive(Debug)]
struct AuthenticatedUser {
    #[allow(dead_code)]
    name: String,
}

/// A toy user store: real deployments query a catalog/user provider here.
#[derive(Debug)]
struct UserStore {
    /// cleartext passwords for legacy users
    legacy: HashMap<String, String>,
    /// postgres-style SCRAM verifiers (salt, iterations, stored_key,
    /// server_key) for everyone else
    scram_verifiers: HashMap<String, ScramSecret>,
}

#[async_trait]
impl AuthSource for UserStore {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().unwrap_or_default();
        match self.legacy.get(user) {
            Some(password) => Ok(Password::new(None, password.as_bytes().to_vec())),
            None => Err(PgWireError::InvalidPassword(user.to_owned())),
        }
    }

    async fn get_scram_secret(&self, login: &LoginInfo) -> PgWireResult<ScramSecret> {
        let user = login.user().unwrap_or_default();
        match self.scram_verifiers.get(user) {
            Some(secret) => Ok(mock_safe_clone(secret)),
            // derive a deterministic mock verifier so unknown users are
            // indistinguishable from wrong passwords (timing-safe rejection)
            None => Ok(mock_verifier(user)),
        }
    }
}

fn mock_safe_clone(secret: &ScramSecret) -> ScramSecret {
    match secret {
        ScramSecret::SaltedPassword {
            salt,
            salted_password,
        } => ScramSecret::SaltedPassword {
            salt: salt.clone(),
            salted_password: salted_password.clone(),
        },
        ScramSecret::Verifier {
            salt,
            iterations,
            stored_key,
            server_key,
        } => ScramSecret::Verifier {
            salt: salt.clone(),
            iterations: *iterations,
            stored_key: stored_key.clone(),
            server_key: server_key.clone(),
        },
    }
}

fn mock_verifier(_user: &str) -> ScramSecret {
    // random keys: the proof will never match, but the handshake completes
    // normally instead of failing early on "user not found"
    ScramSecret::Verifier {
        salt: rand::random::<[u8; 16]>().to_vec(),
        iterations: 4096,
        stored_key: rand::random::<[u8; 32]>().to_vec(),
        server_key: rand::random::<[u8; 32]>().to_vec(),
    }
}

/// A custom startup handler: owns the protocol, delegates authentication to
/// an embedded `AuthDriver`. This is the pattern for database servers that
/// need per-connection logic around authentication.
struct DynamicAuthHandler {
    driver: AuthDriver<DefaultServerParameterProvider>,
    user_store: Arc<UserStore>,
    cleartext: Arc<dyn AuthMechanismFactory>,
    scram: Arc<dyn AuthMechanismFactory>,
}

#[async_trait]
impl StartupHandler for DynamicAuthHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);

                // pre-auth validation, with full &mut C access:
                // e.g. force TLS, resolve the database against a catalog,
                // validate the requested timezone...
                if client.metadata().get("database").is_none() {
                    return Err(PgWireError::InvalidStartupMessage);
                }

                // per-connection mechanism selection:
                let mechanism = match client.metadata().get("user") {
                    Some(user) if self.user_store.legacy.contains_key(user) => {
                        self.cleartext.create()
                    }
                    Some(_) => self.scram.create(),
                    None => return Err(PgWireError::UserNameRequired),
                };
                self.driver
                    .begin_authentication(client, mechanism)
                    .await
                    .map(|_| ())
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => {
                match self.driver.advance_authentication(client, message).await {
                    Ok(AuthOutcome::Completed) => {
                        // authentication succeeded and the startup protocol is
                        // complete; bind the identity to the session here
                        let user = client
                            .metadata()
                            .get("user")
                            .cloned()
                            .unwrap_or_else(|| "anonymous".to_owned());
                        client
                            .session_extensions()
                            .insert(AuthenticatedUser { name: user });
                    }
                    Ok(AuthOutcome::InProgress) => {}
                    // auth failures surface as Err; handle them your way
                    // (custom error responses, metrics, ...) or let the
                    // server report them:
                    Err(error) => return Err(error),
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

struct DummyProcessorFactory {
    startup_handler: Arc<DynamicAuthHandler>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup_handler.clone()
    }
}

fn build_user_store() -> UserStore {
    // legacy cleartext user
    let mut legacy = HashMap::new();
    legacy.insert("legacy_user".to_owned(), "legacy_password".to_owned());

    // SCRAM user "scram_user" with password "scram_password": store only the
    // verifier, never the password
    let salt = b"0123456789abcdef".to_vec();
    let iterations = 4096;
    let salted_password = gen_salted_password("scram_password", &salt, iterations);
    let verifier = ScramSecret::verifier_from_salted_password(salt, salted_password, iterations);

    let mut scram_verifiers = HashMap::new();
    scram_verifiers.insert("scram_user".to_owned(), verifier);

    UserStore {
        legacy,
        scram_verifiers,
    }
}

#[tokio::main]
pub async fn main() {
    let user_store = Arc::new(build_user_store());

    let handler = Arc::new(DynamicAuthHandler {
        driver: AuthDriver::new(DefaultServerParameterProvider::default()),
        cleartext: Arc::new(CleartextPasswordAuth::new(user_store.clone())),
        scram: Arc::new(ScramAuth::new(user_store.clone())),
        user_store,
    });

    let factory = Arc::new(DummyProcessorFactory {
        startup_handler: handler,
    });

    let server_addr: SocketAddr = "127.0.0.1:5432".parse().unwrap();
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        tokio::spawn(async move {
            let _ = process_socket(incoming_socket.0, None, factory_ref).await;
        });
    }
}
