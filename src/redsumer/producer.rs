use redis::Commands;

use crate::redsumer::client::RedsumerClient;
use crate::{FromRedisValue, RedisError, ToRedisArgs};

/// Manager to produce `stream events` in `Redis Streams`
pub struct RedsumerProducer<'p> {
    client: RedsumerClient<'p>,
    stream_name: &'p str,
}

impl<'p> RedsumerProducer<'p> {
    fn get_client(&self) -> &RedsumerClient<'p> {
        &self.client
    }

    /// Get `stream_name <string>`
    pub fn get_stream_name(&self) -> &'p str {
        self.stream_name
    }

    /// Get Redis `host <redis connection string>`
    pub fn get_host(&self) -> String {
        self.client.get_host()
    }

    /// Build a new [`RedsumerProducer`] from specific `url`, `db` and `stream_name`
    pub fn new(url: &'p str, db: &'p str, stream_name: &'p str) -> RedsumerProducer<'p> {
        RedsumerProducer {
            client: RedsumerClient::init(url, db),
            stream_name,
        }
    }

    /// Produce **stream event** from **vector of items**
    pub async fn produce_from_items<F: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &self,
        items: &Vec<(F, V)>,
    ) -> Result<RV, RedisError> {
        let id: RV =
            self.get_client()
                .get_connection()
                .await?
                .xadd(self.stream_name, "*", items)?;

        Ok(id)
    }

    /// Produce **stream event** from **map**
    pub async fn produce_from_map<M: ToRedisArgs, RV: FromRedisValue>(
        &self,
        map: M,
    ) -> Result<RV, RedisError> {
        let id: RV =
            self.get_client()
                .get_connection()
                .await?
                .xadd_map(self.stream_name, "*", map)?;

        Ok(id)
    }
}
