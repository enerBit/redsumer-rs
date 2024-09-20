use redis::{Client, ToRedisArgs};

#[allow(unused_imports)]
use crate::core::{
    client::{ClientArgs, ClientCredentials, RedisClientBuilder},
    connection::VerifyConnection,
    redis_streams::producer::ProducerCommands,
    types::{Id, RedsumerError, RedsumerResult},
};

/// Define the message to be produced in a stream. The message can be a map or a list of items.
/// The map is a key-value pair where the key is the field and the value is the value.
/// The items are a list of tuples where the first element is the field and the second element is the value.
pub enum Message<M, F, V>
where
    M: ToRedisArgs,
    F: ToRedisArgs,
    V: ToRedisArgs,
{
    Map(M),
    Items(Vec<(F, V)>),
}

/// Define the configuration parameters to create a producer instance.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    // Stream name where messages will be produced.
    stream_name: String,
}

impl ProducerConfig {
    /// Get **stream name**.
    pub fn get_stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Create a new [`ProducerConfig`] instance.
    ///
    /// # Arguments:
    /// - **stream_name**: The name of the stream where messages will be produced.
    ///
    /// # Returns:
    /// A new [`ProducerConfig`] instance.
    pub fn new(stream_name: &str) -> Self {
        ProducerConfig {
            stream_name: stream_name.to_owned(),
        }
    }
}

/// A producer implementation of Redis Streams.
///
///  This struct is responsible for producing messages in a stream.
#[derive(Debug, Clone)]
pub struct Producer {
    client: Client,
    config: ProducerConfig,
}

impl Producer {
    /// Get [`Client`].
    fn get_client(&self) -> &Client {
        &self.client
    }

    /// Get *stream name*.
    pub fn get_config(&self) -> &ProducerConfig {
        &self.config
    }

    /// Build a new [`Producer`] instance.
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
    ///  - A [`RedsumerResult`] with the new [`Producer`] instance. Otherwise, a [`RedsumerError`] is returned.
    ///
    ///  # Example:
    ///	Create a new [`Producer`] instance.
    /// ```rust,no_run
    ///  TODO!
    /// ```
    pub fn new(args: &ClientArgs, config: &ProducerConfig) -> RedsumerResult<Producer> {
        let mut client: Client = args.build()?;
        client.ping()?;

        Ok(Producer {
            client,
            config: config.to_owned(),
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
    pub async fn produce<M, F, V>(&self, message: Message<M, F, V>) -> RedsumerResult<Id>
    where
        M: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        match message {
            Message::Map(map) => self
                .get_client()
                .to_owned()
                .produce_from_map(self.get_config().get_stream_name(), map),

            Message::Items(items) => self
                .get_client()
                .to_owned()
                .produce_from_items(self.get_config().get_stream_name(), items.as_slice()),
        }
    }
}
