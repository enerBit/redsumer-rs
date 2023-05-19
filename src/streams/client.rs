use redis::{Client, Connection};

pub struct RedisClient {
    url: String,
    db: String,
}

impl RedisClient {
    pub fn get_conn(&self) -> Connection {
        let conn: Connection = Client::open(format!("redis://{}/{}", self.url, self.db))
            .expect(&format!("Error getting Redis client from url={} and db={}", self.url, self.db))
            .get_connection()
            .expect(&format!("Error establishing connection to Redis server redis://{}/{}", self.url, self.db,));

        return conn;
    }

    pub fn init(url: String, db: String) -> RedisClient {
        return RedisClient {
            url,
            db,
        };
    }
}