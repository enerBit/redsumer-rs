use std::fmt::Debug;

use redis::{Client, ConnectionAddr, ConnectionInfo, ProtocolVersion, RedisConnectionInfo};

#[allow(unused_imports)]
use super::result::{RedsumerError, RedsumerResult};

/// Communication protocol to be used by the client.
pub type CommunicationProtocol = ProtocolVersion;

/// To hold credentials to authenticate to the server.
///
/// This credentials are used to authenticate to the server when required. If server does not require it, you set it to `None`.
#[derive(Clone)]
pub struct ClientCredentials {
    /// User to authenticate to the server.
    user: String,

    /// Password to authenticate to the server.
    password: String,
}

impl ClientCredentials {
    /// Get *user*
    fn get_user(&self) -> &str {
        &self.user
    }

    /// Get *password*
    fn get_password(&self) -> &str {
        &self.password
    }

    /// Build a new instance of [`ClientCredentials`].
    ///
    /// # Arguments:
    /// - **user**: The username to authenticate to the server.
    /// - **password**: The password to authenticate to the server.
    ///
    /// # Returns:
    /// A new instance of [`ClientCredentials`].
    pub fn new(user: &str, password: &str) -> ClientCredentials {
        ClientCredentials {
            user: user.to_owned(),
            password: password.to_owned(),
        }
    }
}

impl Debug for ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("user", &self.user)
            .field("password", &"****")
            .finish()
    }
}

/// Define  the configuration parameters to create a [`Client`] instance.
///
/// Take a look at the following supported connection URL format to infer the client arguments:
///
/// `redis://[<user>][:<password>@]<host>:<port>/<db>`
///
/// *user* and *password* are optional. If you don't need to authenticate to the server, you can ignore them. *port* and *db* are mandatory for the connection. Another connection URL formats are not implemented yet.
#[derive(Debug, Clone)]
pub struct ClientArgs {
    /// Credentials to authenticate to the server.
    credentials: Option<ClientCredentials>,

    /// Host to connect to the server.
    host: String,

    /// Server port.
    port: u16,

    /// Database number.
    db: i64,

    /// Protocol version to communicate with the server.
    protocol: CommunicationProtocol,
}

impl ClientArgs {
    /// Get *credentials*.
    pub fn get_credentials(&self) -> &Option<ClientCredentials> {
        &self.credentials
    }

    /// Get *host*.
    pub fn get_host(&self) -> &str {
        &self.host
    }

    /// Get *port*.
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Get *db*.
    pub fn get_db(&self) -> i64 {
        self.db
    }

    /// Get *protocol*.
    pub fn get_protocol(&self) -> CommunicationProtocol {
        self.protocol
    }

    /// Create a new instance of [`ClientArgs`].
    ///
    /// # Arguments:
    /// - **credentials**: Credentials to authenticate to the server.
    /// - **host**: Host to connect to the server.
    /// - **port**: Server port.
    /// - **db**: Database number.
    /// - **protocol**: Protocol version to communicate with the server.
    ///
    /// # Returns:
    /// A new instance of [`ClientArgs`].
    pub fn new(
        credentials: Option<ClientCredentials>,
        host: &str,
        port: u16,
        db: i64,
        protocol: CommunicationProtocol,
    ) -> ClientArgs {
        ClientArgs {
            credentials,
            host: host.to_owned(),
            port,
            db,
            protocol,
        }
    }
}

/// To build a new instance of [`Client`].
pub trait ClientBuilder {
    /// Build a new instance of [`Client`].
    ///
    /// # Arguments:
    /// - No arguments.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with a new instance of [`Client`]. Otherwise, a [`RedsumerError`] is returned.
    fn build(&self) -> RedsumerResult<Client>;
}

impl ClientBuilder for ClientArgs {
    fn build(&self) -> RedsumerResult<Client> {
        let addr: ConnectionAddr =
            ConnectionAddr::Tcp(String::from(self.get_host()), self.get_port());

        let username: Option<String> = self
            .get_credentials()
            .to_owned()
            .map(|c| c.get_user().to_string());

        let password: Option<String> = self
            .get_credentials()
            .to_owned()
            .map(|c| c.get_password().to_string());

        let redis: RedisConnectionInfo = RedisConnectionInfo {
            db: self.get_db(),
            username,
            password,
            protocol: self.get_protocol(),
        };

        Client::open(ConnectionInfo { addr, redis })
    }
}

#[cfg(test)]
mod test_client_credentials {
    use super::*;

    #[test]
    fn test_client_credentials_builder_ok() {
        // Define the user and password to authenticate in Redis:
        let user: &str = "user";
        let password: &str = "password";

        // Create a new instance of ClientCredentials:
        let credentials: ClientCredentials = ClientCredentials::new(user, password);

        // Verify if the user and password are correct:
        assert_eq!(credentials.get_user(), user);
        assert_eq!(credentials.get_password(), password);
    }

    #[test]
    fn test_client_credentials_debug() {
        // Define the user and password to authenticate in Redis:
        let user: &str = "user";
        let password: &str = "password";

        // Create a new instance of ClientCredentials:
        let credentials: ClientCredentials = ClientCredentials::new(user, password);

        // Verify if the debug is correct:
        assert_eq!(
            format!("{:?}", credentials),
            "ClientCredentials { user: \"user\", password: \"****\" }"
        );
    }

    #[test]
    fn test_client_credentials_clone() {
        // Define the user and password to authenticate in Redis:
        let user: &str = "user";
        let password: &str = "password";

        // Create a new instance of ClientCredentials:
        let credentials: ClientCredentials = ClientCredentials::new(user, password);

        // Clone the credentials:
        let cloned_credentials: ClientCredentials = credentials.clone();

        // Verify if the credentials are correct:
        assert_eq!(credentials.get_user(), cloned_credentials.get_user());
        assert_eq!(
            credentials.get_password(),
            cloned_credentials.get_password()
        );
    }
}

#[cfg(test)]
mod test_client_args {
    use super::*;

    #[test]
    fn test_client_args_builder_ok() {
        // Define the user and password to authenticate in Redis:
        let user: &str = "user";
        let password: &str = "password";

        // Create a new instance of ClientCredentials:
        let credentials: ClientCredentials = ClientCredentials::new(user, password);

        // Define the host to connect to Redis:
        let host: &str = "localhost";

        // Define the port to connect to Redis:
        let port: u16 = 6379;

        // Define the database to connect to Redis:
        let db: i64 = 1;

        // Define the redis protocol version:
        let protocol_version: CommunicationProtocol = CommunicationProtocol::RESP2;

        // Create a new instance of ClientArgs with default port and db:
        let args: ClientArgs = ClientArgs::new(Some(credentials), host, port, db, protocol_version);

        // Verify if the args are correct:
        assert!(args.get_credentials().is_some());
        assert_eq!(args.get_credentials().to_owned().unwrap().get_user(), user);
        assert_eq!(
            args.get_credentials().to_owned().unwrap().get_password(),
            password
        );
        assert_eq!(args.get_host(), host);
        assert_eq!(args.get_port(), port);
        assert_eq!(args.get_db(), db);
    }

    #[test]
    fn test_client_args_debug() {
        // Define the user and password to authenticate in Redis:
        let user: &str = "user";
        let password: &str = "password";

        // Create a new instance of ClientCredentials:
        let credentials: ClientCredentials = ClientCredentials::new(user, password);

        // Define the host to connect to Redis:
        let host: &str = "localhost";

        // Define the port to connect to Redis:
        let port: u16 = 6379;

        // Define the database to connect to Redis:
        let db: i64 = 1;

        // Define the redis protocol version:
        let protocol_version: CommunicationProtocol = CommunicationProtocol::RESP2;

        // Create a new instance of ClientArgs with default port and db:
        let args: ClientArgs = ClientArgs::new(Some(credentials), host, port, db, protocol_version);

        // Verify if the debug is correct:
        assert_eq!(format!("{:?}", args), "ClientArgs { credentials: Some(ClientCredentials { user: \"user\", password: \"****\" }), host: \"localhost\", port: 6379, db: 1, protocol: RESP2 }");
    }

    #[test]
    fn test_client_args_clone() {
        // Define the user and password to authenticate in Redis:
        let user: &str = "user";
        let password: &str = "password";

        // Create a new instance of ClientCredentials:
        let credentials: ClientCredentials = ClientCredentials::new(user, password);

        // Define the host to connect to Redis:
        let host: &str = "localhost";

        // Define the port to connect to Redis:
        let port: u16 = 6379;

        // Define the database to connect to Redis:
        let db: i64 = 1;

        // Define the redis protocol version:
        let protocol_version: CommunicationProtocol = CommunicationProtocol::RESP2;

        // Create a new instance of ClientArgs with default port and db:
        let args: ClientArgs = ClientArgs::new(Some(credentials), host, port, db, protocol_version);

        // Clone the args:
        let cloned_args: ClientArgs = args.clone();

        // Verify if the args are correct:
        assert_eq!(
            args.get_credentials().to_owned().unwrap().get_user(),
            cloned_args.get_credentials().to_owned().unwrap().get_user()
        );
        assert_eq!(
            args.get_credentials().to_owned().unwrap().get_password(),
            cloned_args
                .get_credentials()
                .to_owned()
                .unwrap()
                .get_password()
        );
        assert_eq!(args.get_host(), cloned_args.get_host());
        assert_eq!(args.get_port(), cloned_args.get_port());
        assert_eq!(args.get_db(), cloned_args.get_db());
        assert_eq!(args.get_protocol(), cloned_args.get_protocol());
    }
}

#[cfg(test)]
mod test_redis_client_builder {
    use super::*;

    #[test]
    fn test_redis_client_builder_ok_with_null_credentials() {
        // Create a new instance of ClientArgs with default port and db:
        let args: ClientArgs =
            ClientArgs::new(None, "mylocalhost", 6377, 16, CommunicationProtocol::RESP2);

        // Build a new instance of Client:
        let client_result: RedsumerResult<Client> = args.build();

        // Verify if the client is correct:
        assert!(client_result.is_ok());
    }

    #[test]
    fn test_redis_client_builder_ok_with_credentials() {
        // Create a new instance of ClientArgs with default port and db:
        let args: ClientArgs = ClientArgs::new(
            Some(ClientCredentials::new("user", "password")),
            "mylocalhost",
            6377,
            16,
            CommunicationProtocol::RESP2,
        );

        // Build a new instance of Client:
        let client_result: RedsumerResult<Client> = args.build();

        // Verify if the client is correct:
        assert!(client_result.is_ok());
    }
}
