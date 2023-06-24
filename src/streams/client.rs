use redis::{Client, Connection};

pub struct RedisClient {
    url: String,
    db: String,
}

impl RedisClient {
    pub async fn get_conn(&self) -> Connection {
        let host: String = format!("redis://{}/{}", self.url, self.db);

        Client::open(
            host.clone(),
        )
            .expect(
                &format!("Error getting Redis client from host {}", host.clone()),
            )
            .get_connection()
            .expect(
                &format!("Error establishing connection to Redis server redis")
            )
    }

    pub fn init(url: String, db: String) -> RedisClient {
        RedisClient {
            url,
            db,
        }
    }
}