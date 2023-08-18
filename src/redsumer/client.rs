use redis::{Client, Connection};

use crate::RedisError;

pub struct RedsumerClient<'c> {
    url: &'c str,
    db: &'c str,
}

impl<'c> RedsumerClient<'c> {
    pub fn get_host(&self) -> String {
        format!("redis://{}/{}", self.url, self.db)
    }

    pub async fn get_connection(&self) -> Result<Connection, RedisError> {
        match Client::open(self.get_host()) {
            Ok(client) => match client.get_connection() {
                Ok(connection) => Ok(connection),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    pub fn init(url: &'c str, db: &'c str) -> RedsumerClient<'c> {
        RedsumerClient { url, db }
    }
}
