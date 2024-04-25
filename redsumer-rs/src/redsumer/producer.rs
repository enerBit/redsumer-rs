use redis::{Client, Commands, ToRedisArgs};

use super::client::{get_redis_client, ClientCredentials};
use super::types::{Id, RedsumerResult};

/// A producer implementation of *Redis Streams*.
#[derive(Debug, Clone)]
pub struct RedsumerProducer<'p> {
    client: Client,
    stream_name: &'p str,
}

impl<'p> RedsumerProducer<'p> {
    /// Get [`Client`].
    fn get_client(&self) -> &Client {
        &self.client
    }

    /// Get *stream name*.
    pub fn get_stream_name(&self) -> &str {
        self.stream_name
    }

    /// Build a new [`RedsumerProducer`] instance.
    /// # Arguments:
    /// - **credentials**: Optional [`ClientCredentials`] to authenticate in Redis.
    /// - **host**: Redis host.
    /// - **port**: Redis port.
    /// - **db**: Redis database.
    /// - **stream_name**: Stream name to produce messages.
    ///
    /// ```rust,no_run
    /// use redsumer::RedsumerProducer;
    ///
    /// let producer = RedsumerProducer::new(
    ///     None,
    ///     "localhost",
    ///     "6379",
    ///     "0",
    ///     "my_stream",
    /// ).unwrap();
    /// ```
    pub fn new(
        credentials: Option<ClientCredentials<'p>>,
        host: &'p str,
        port: &'p str,
        db: &'p str,
        stream_name: &'p str,
    ) -> RedsumerResult<RedsumerProducer<'p>> {
        let client: Client = get_redis_client(credentials, host, port, db)?;

        Ok(RedsumerProducer {
            client,
            stream_name,
        })
    }

    /// Produce a message in the stream, where message implements [`ToRedisArgs`].
    /// # Arguments:
    /// - **message**: Message to produce in stream.
    pub async fn produce<M>(&self, message: M) -> RedsumerResult<Id>
    where
        M: ToRedisArgs,
    {
        self.get_client().get_connection()?.xadd_map::<_, _, _, Id>(
            self.get_stream_name(),
            "*",
            message,
        )
    }
}
