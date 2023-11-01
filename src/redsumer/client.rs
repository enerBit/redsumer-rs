use redis::{Client, Connection, RedisError};

/// Builder for `Redis Client` connection credentials.
pub struct ClientCredentials<'k> {
    user: &'k str,
    password: &'k str,
}

impl<'k> ClientCredentials<'k> {
    fn get_user(&self) -> &'k str {
        self.user
    }

    fn get_password(&self) -> &'k str {
        self.password
    }

    pub fn get(user: &'k str, password: &'k str) -> ClientCredentials<'k> {
        ClientCredentials { user, password }
    }
}

pub struct RedsumerClient<'c> {
    credentials: Option<ClientCredentials<'c>>,
    host: &'c str,
    port: &'c str,
    db: &'c str,
}

impl<'c> RedsumerClient<'c> {
    fn get_credentials(&self) -> &Option<ClientCredentials<'c>> {
        &self.credentials
    }

    fn get_host(&self) -> &'c str {
        self.host
    }

    fn get_port(&self) -> &'c str {
        self.port
    }

    fn get_db(&self) -> &'c str {
        self.db
    }

    fn get_connection_string(&self) -> String {
        let credentials_string: String = match self.get_credentials() {
            Some(credentials) => {
                format!("{}:{}@", credentials.get_user(), credentials.get_password())
            }
            None => String::default(),
        };

        format!(
            "redis://{}{}:{}/{}",
            &credentials_string,
            self.get_host(),
            self.get_port(),
            self.get_db()
        )
    }

    pub async fn get_connection(&self) -> Result<Connection, RedisError> {
        match Client::open(self.get_connection_string()) {
            Ok(client) => match client.get_connection() {
                Ok(connection) => Ok(connection),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    pub fn init(
        credentials: Option<ClientCredentials<'c>>,
        host: &'c str,
        port: &'c str,
        db: &'c str,
    ) -> RedsumerClient<'c> {
        RedsumerClient {
            credentials,
            host,
            port,
            db,
        }
    }
}
