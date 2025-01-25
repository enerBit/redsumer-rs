use redis::{Client, Commands, ToRedisArgs};
use tracing::{debug, info};

#[allow(unused_imports)]
use crate::core::{
    client::{ClientArgs, ClientCredentials, RedisClientBuilder},
    result::{RedsumerError, RedsumerResult},
    streams::{producer::ProducerCommands, types::Id},
};

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

/// Reply of a produced message in a stream.
#[derive(Debug, Clone)]
pub struct ProduceMessageReply {
    /// *ID* of the produced message.
    id: Id,
}

impl ProduceMessageReply {
    /// Get *ID* of the produced message.
    pub fn get_id(&self) -> &Id {
        &self.id
    }
}

/// Convert a `ID` to a [`ProduceMessageReply`] instance.
impl From<Id> for ProduceMessageReply {
    fn from(id: Id) -> Self {
        ProduceMessageReply { id }
    }
}

/// A producer implementation of Redis Streams. This struct is responsible for producing messages in a stream.
#[derive(Debug, Clone)]
pub struct Producer {
    /// Redis client to interact with Redis server.
    client: Client,

    /// Producer configuration parameters.
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
    pub fn new(args: &ClientArgs, config: &ProducerConfig) -> RedsumerResult<Producer> {
        debug!(
            "Creating a new producer instance by: {:?} and {:?}",
            args, config
        );

        let mut client: Client = args.build()?;
        client.ping::<String>()?;

        info!("Producer instance created successfully and it is ready to be used");

        Ok(Producer {
            client,
            config: config.to_owned(),
        })
    }

    /// Produce a new message in the stream from a map.
    ///
    ///  This method produces a new message in the stream setting the *ID* as "*", which means that Redis will generate a new *ID* for the message automatically with the current timestamp. If stream does not exist, it will be created.
    ///
    /// # Arguments:
    /// - **map**: A map with the message to be produced. It must implement the [`ToRedisArgs`] trait.
    ///
    ///  # Returns:
    /// - A [`RedsumerResult`] with a [`ProduceMessageReply`] instance. Otherwise, a [`RedsumerError`] is returned.
    pub async fn produce_from_map<M>(&self, map: M) -> RedsumerResult<ProduceMessageReply>
    where
        M: ToRedisArgs,
    {
        self.get_client()
            .to_owned()
            .produce_from_map(self.get_config().get_stream_name(), map)
            .map(ProduceMessageReply::from)
    }

    /// Produce a new message in the stream from a list of items.
    ///
    /// This method produces a new message in the stream setting the *ID* as "*", which means that Redis will generate a new *ID* for the message automatically with the current timestamp. If stream does not exist, it will be created.
    ///
    /// # Arguments:
    /// - **items**: A list of items with the message to be produced. Each item is a tuple with the field and the value. Both must implement the [`ToRedisArgs`] trait.
    ///
    /// # Returns:
    /// - A [`RedsumerResult`] with a [`ProduceMessageReply`] instance. Otherwise, a [`RedsumerError`] is returned.
    pub async fn produce_from_items<F, V>(
        &self,
        items: Vec<(F, V)>,
    ) -> RedsumerResult<ProduceMessageReply>
    where
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        self.get_client()
            .to_owned()
            .produce_from_items(self.get_config().get_stream_name(), items.as_slice())
            .map(ProduceMessageReply::from)
    }
}

#[cfg(test)]
mod test_producer_config {
    use super::*;

    #[test]
    fn test_producer_config_new() {
        // Define the stream name.
        let stream_name: &str = "stream_name";

        // Create a new producer configuration.
        let config: ProducerConfig = ProducerConfig::new(stream_name);

        // Verify the result.
        assert_eq!(config.get_stream_name(), stream_name);
    }
}

#[cfg(test)]
mod test_produce_messages_reply {
    use super::*;

    #[test]
    fn test_produce_message_reply_from() {
        // Define the message ID.
        let id: Id = "1234567890".to_string();

        // Create a new produce message reply.
        let reply: ProduceMessageReply = ProduceMessageReply::from(id.to_owned());

        // Verify the result.
        assert_eq!(reply.get_id(), &id);
    }
}
