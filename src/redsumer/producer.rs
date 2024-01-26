use async_trait::async_trait;
use redis::streams::{StreamInfoGroupsReply, StreamInfoStreamReply};
use redis::{Commands, FromRedisValue, RedisError, ToRedisArgs};

use crate::redsumer::client::{ClientCredentials, RedsumerClient};
use crate::redsumer::stream_information::StreamInfo;

/// Manager to produce `stream events` in `Redis Streams`.
pub struct RedsumerProducer<'p> {
    client: RedsumerClient<'p>,
    stream_name: &'p str,
}

impl<'p> RedsumerProducer<'p> {
    fn get_client(&self) -> &RedsumerClient<'p> {
        &self.client
    }

    /// Get `stream_name <String>`.
    pub fn get_stream_name(&self) -> &'p str {
        self.stream_name
    }

    /// Build a new [`RedsumerProducer`] from specific `host`, `port`, `db` and `stream_name`.
    pub fn new(
        credentials: Option<ClientCredentials<'p>>,
        host: &'p str,
        port: &'p str,
        db: &'p str,
        stream_name: &'p str,
    ) -> RedsumerProducer<'p> {
        RedsumerProducer {
            client: RedsumerClient::init(credentials, host, port, db),
            stream_name,
        }
    }

    /// Produce **stream event** from `vector of items`, when each item is a tuple of two elements *(field, value)*.
    /// For more information, take a look about [The Tuple Type](https://doc.rust-lang.org/book/ch03-02-data-types.html#the-tuple-type) and [Vectors](https://doc.rust-lang.org/book/ch08-01-vectors.html).
    pub async fn produce_from_items<F: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &self,
        items: &Vec<(F, V)>,
    ) -> Result<RV, RedisError> {
        let id: RV = self
            .get_client()
            .get_connection()?
            .xadd(self.stream_name, "*", items)?;

        Ok(id)
    }

    /// Produce **stream event** from `map`. Take a look about ***BTreeMap*** and ***HashMap***.
    pub async fn produce_from_map<M: ToRedisArgs, RV: FromRedisValue>(
        &self,
        map: M,
    ) -> Result<RV, RedisError> {
        let id: RV = self
            .get_client()
            .get_connection()?
            .xadd_map(self.stream_name, "*", map)?;

        Ok(id)
    }
}

#[async_trait]
impl<'p> StreamInfo for RedsumerProducer<'p> {
    async fn get_stream_info(&self) -> Result<StreamInfoStreamReply, RedisError> {
        let stream_info: StreamInfoStreamReply = self
            .get_client()
            .get_connection()?
            .xinfo_stream(self.get_stream_name())?;

        Ok(stream_info)
    }

    async fn get_consumer_groups_info(&self) -> Result<StreamInfoGroupsReply, RedisError> {
        let groups_info: StreamInfoGroupsReply = self
            .get_client()
            .get_connection()?
            .xinfo_groups(self.get_stream_name())?;

        Ok(groups_info)
    }
}
