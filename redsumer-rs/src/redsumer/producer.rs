use redis::{Client, Commands, ConnectionLike, ErrorKind, RedisError, ToRedisArgs};

use super::client::{get_redis_client, ClientCredentials};

#[allow(unused_imports)]
use super::types::{Id, RedsumerError, RedsumerResult};

/// A producer implementation of *Redis Streams*.
///
///  This struct is responsible for producing messages in a stream.
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
    ///
    /// Before creating a new producer, the following validations are performed:
    ///
    /// - If connection string is invalid, a [`RedsumerError`] is returned.
    /// - If connection to Redis server can not be established, a [`RedsumerError`] is returned.
    ///
    /// # Arguments:
    /// - **credentials**: Optional [`ClientCredentials`] to authenticate in Redis.
    /// - **host**: Redis host.
    /// - **port**: Redis port.
    /// - **db**: Redis database.
    /// - **stream_name**: Stream name to produce messages.
    ///
    ///  # Returns:
    ///  - A [`RedsumerResult`] with the new [`RedsumerProducer`] instance. Otherwise, a [`RedsumerError`] is returned.
    ///
    ///  # Example:
    ///	Create a new [`RedsumerProducer`] instance.
    /// ```rust,no_run
    /// use redsumer::RedsumerProducer;
    ///
    /// let producer: RedsumerProducer = RedsumerProducer::new(
    ///     None,
    ///     "localhost",
    ///     "6379",
    ///     "0",
    ///     "my_stream",
    /// ).unwrap_or_else(|err| {
    ///    panic!("Error creating producer: {:?}", err);
    /// });
    /// ```
    pub fn new(
        credentials: Option<ClientCredentials<'p>>,
        host: &'p str,
        port: &'p str,
        db: &'p str,
        stream_name: &'p str,
    ) -> RedsumerResult<RedsumerProducer<'p>> {
        let mut client: Client = get_redis_client(credentials, host, port, db)?;

        if !client.check_connection() {
            return Err(RedisError::from((
                ErrorKind::TryAgain,
                "Error getting connection to Redis server",
            )));
        };

        Ok(RedsumerProducer {
            client,
            stream_name,
        })
    }

    /// Produce a new message in stream.
    ///
    ///  This method produces a new message in the stream setting the *ID* as "*", which means that Redis will generate a new *ID* for the message automatically with the current timestamp.
    ///
    ///  If stream does not exist, it will be created.
    ///
    /// # Arguments:
    /// - **message**: Message to produce in stream. It must implement [`ToRedisArgs`].
    ///
    ///  # Returns:
    /// - A [`RedsumerResult`] with the *ID* of the produced message. Otherwise, a [`RedsumerError`] is returned.
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
