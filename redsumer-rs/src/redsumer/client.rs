use redis::Client;

use super::types::RedsumerResult;

/// To hold credentials to authenticate in *Redis*.
///
/// This credentials are used to authenticate in *Redis* when server requires it. If server does not require it, you set it to `None`.
///
///  This crate uses a connection URL in format: `redis://[<username>][:<password>@]<hostname>:<port>/<db>`. Other formats are not supported yet.
pub struct ClientCredentials<'k> {
    user: &'k str,
    password: &'k str,
}

impl<'k> ClientCredentials<'k> {
    /// Get *user*
    fn get_user(&self) -> &str {
        self.user
    }

    /// Get *password*
    fn get_password(&self) -> &str {
        self.password
    }

    /// Build a new instance of [`ClientCredentials`].
    ///
    /// # Arguments:
    /// - **user**: Redis user.
    /// - **password**: Redis password.
    ///
    /// # Returns:
    /// A new instance of [`ClientCredentials`].
    ///
    /// ```rust,no_run
    /// use redsumer::ClientCredentials;
    /// let credentials = ClientCredentials::new("user", "password");
    /// ```
    pub fn new(user: &'k str, password: &'k str) -> ClientCredentials<'k> {
        ClientCredentials { user, password }
    }
}

/// Get a new [`Client`] instance to connect to *Redis* using a connection URL in format:
/// `redis://[<username>][:<password>@]<hostname>:<port>/<db>`
///
/// # Arguments:
/// - **credentials**: Option to authenticate in *Redis*.
/// - **host**: Redis host.
/// - **port**: Redis port.
/// - **db**: Redis database.
///
/// # Returns:
/// - A [`RedsumerResult`] with a new instance of [`Client`] to connect to *Redis*. Otherwise, a [`RedsumerError`] is returned.
pub fn get_redis_client(
    credentials: Option<ClientCredentials>,
    host: &str,
    port: &str,
    db: &str,
) -> RedsumerResult<Client> {
    let url: String = match credentials {
        Some(credentials) => {
            format!(
                "redis://{}:{}@{}:{}/{}",
                credentials.get_user(),
                credentials.get_password(),
                host,
                port,
                db,
            )
        }
        None => format!("redis://{}:{}/{}", host, port, db,),
    };

    Ok(Client::open(url)?)
}
