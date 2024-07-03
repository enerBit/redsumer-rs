use log::{debug, warn};
use std::fmt::Debug;

use redis::{
    streams::{
        StreamClaimOptions, StreamClaimReply, StreamId, StreamPendingCountReply, StreamReadOptions,
        StreamReadReply,
    },
    Client, Commands, ConnectionLike, ErrorKind, RedisError,
};

use super::client::{get_redis_client, ClientCredentials};

#[allow(unused_imports)]
use super::types::{Id, RedsumerError, RedsumerResult};

/// A consumer implementation of Redis Streams.
///
///  The consumer is responsible for consuming messages from a stream. It can read new messages,  pending messages or claim messages from other consumers according to their min idle time.
///
///  The consumer can be created using the [new](`RedsumerConsumer::new`) method. After creating a new consumer, it is possible to consume messages using the [consume](`RedsumerConsumer::consume`) method.
///
///   Also it is possible to verify if a specific message is still in consumer pending list using the [is_still_mine](`RedsumerConsumer::is_still_mine`) method.
///
///  The consumer can also ack a message using the [ack](`RedsumerConsumer::ack`) method. If the message is acked, it is removed from the consumer pending list.
///
///  Take a look at the [new](`RedsumerConsumer::new`) to know more about the consumer creation process and its parameters.
#[derive(Debug, Clone)]
pub struct RedsumerConsumer<'c> {
    client: Client,
    stream_name: &'c str,
    group_name: &'c str,
    consumer_name: &'c str,
    since_id: &'c str,
    min_idle_time_milliseconds: usize,
    new_messages_count: usize,
    pending_messages_count: usize,
    claimed_messages_count: usize,
    block: u8,
}

impl<'c> RedsumerConsumer<'c> {
    /// Get [`Client`].
    fn get_client(&self) -> &Client {
        &self.client
    }

    /// Get *stream_name*.
    pub fn get_stream_name(&self) -> &'c str {
        self.stream_name
    }

    /// Get *group_name*.
    pub fn get_group_name(&self) -> &str {
        self.group_name
    }

    /// Get *since_id*.
    pub fn get_since_id(&self) -> &str {
        self.since_id
    }

    /// Get *consumer_name*.
    pub fn get_consumer_name(&self) -> &str {
        self.consumer_name
    }

    /// Get *min_idle_time_milliseconds*.
    pub fn get_min_idle_time_milliseconds(&self) -> usize {
        self.min_idle_time_milliseconds
    }

    /// Get *new_messages_count*.
    pub fn get_new_messages_count(&self) -> usize {
        self.new_messages_count
    }

    /// Get *pending_messages_count*.
    pub fn get_pending_messages_count(&self) -> usize {
        self.pending_messages_count
    }

    /// Get *claimed_messages_count*.
    pub fn get_claimed_messages_count(&self) -> usize {
        self.claimed_messages_count
    }

    /// Get *block*.
    pub fn get_block(&self) -> u8 {
        self.block
    }

    /// Build a new [`RedsumerConsumer`] instance.
    ///
    ///  Before creating a new consumer, the following validations are performed:
    ///
    ///  - If the *new_messages_count*, *pending_messages_count* and *claimed_messages_count* are all zero, a [`RedsumerError`] is returned.
    /// - If connection string is invalid, a [`RedsumerError`] is returned.
    /// - If connection to Redis server can not be established, a [`RedsumerError`] is returned.
    /// - If the stream does not exist, a [`RedsumerError`] is returned: The stream must exist before creating a new consumer.
    ///  - If the consumers group does not exist, it is created based on the *stream_name*, *group_name* and *since_id*. If the consumers group already exists, a warning is logged. If an error occurs during the creation process, a [`RedsumerError`] is returned.
    ///
    /// # Arguments:
    /// - **credentials**: Optional [`ClientCredentials`] to authenticate in Redis.
    /// - **host**: Redis host.
    /// - **port**: Redis port.
    /// - **db**: Redis database.
    /// - **stream_name**: Stream name to consume messages.
    /// - **group_name**: Consumers group name.
    /// - **consumer_name**: Represents the consumer name within the specified consumers group, which must be ensured to be unique. In a microservices architecture, for example, it is recommended to use the pod name.
    /// - **since_id**: It is used to read and to claim pending messages from stream greater than the specified value. If consumers group does not exist, it is created based on this value.
    /// - **min_idle_time_milliseconds**: It is the minimum idle time to claim pending messages, given in milliseconds. Only pending messages that have been idle for at least this long will be claimed.
    /// - **new_messages_count**: Maximum number of new messages to read.
    /// - **pending_messages_count**: Maximum number of pending messages to read.
    /// - **claimed_messages_count**: Maximum number of claimed messages to read.
    /// - **block**: Max time to wait for new messages, given in milliseconds.
    ///
    ///  # Returns:
    /// - A [`RedsumerResult`] containing a [`RedsumerConsumer`] instance. Otherwise, a [`RedsumerError`] is returned.
    ///
    ///  # Example:
    ///	Create a new [`RedsumerConsumer`] instance.
    /// ```rust,no_run
    ///	use redsumer::{ClientCredentials, RedsumerConsumer};
    ///
    ///	let client_credentials = Some(ClientCredentials::new("user", "password"));
    ///	let host = "localhost";
    ///	let port = "6379";
    ///	let db = "0";
    ///	let stream_name = "my_stream";
    ///	let group_name = "my_consumer_group";
    ///	let consumer_name = "my_consumer";
    ///	let since_id = "0-0";
    /// let min_idle_time_milliseconds = 360000;
    /// let new_messages_count = 10;
    /// let pending_messages_count = 10;
    /// let claimed_messages_count = 10;
    /// let block = 5;
    ///
    /// let consumer: RedsumerConsumer = RedsumerConsumer::new(
    /// 	client_credentials,
    /// 	host,
    /// 	port,
    /// 	db,
    /// 	stream_name,
    /// 	group_name,
    /// 	consumer_name,
    /// 	since_id,
    /// 	min_idle_time_milliseconds,
    /// 	new_messages_count,
    /// 	pending_messages_count,
    /// 	claimed_messages_count,
    /// 	block,
    /// ).unwrap_or_else(|error| {
    /// 	panic!("Error creating new RedsumerConsumer: {}", error);
    /// });
    /// ```
    pub fn new(
        credentials: Option<ClientCredentials<'c>>,
        host: &'c str,
        port: &'c str,
        db: &'c str,
        stream_name: &'c str,
        group_name: &'c str,
        consumer_name: &'c str,
        since_id: &'c str,
        min_idle_time_milliseconds: usize,
        new_messages_count: usize,
        pending_messages_count: usize,
        claimed_messages_count: usize,
        block: u8,
    ) -> RedsumerResult<Self> {
        let total_messages_to_read: usize =
            new_messages_count + pending_messages_count + claimed_messages_count;
        if total_messages_to_read.eq(&0) {
            return Err(RedisError::from((
                ErrorKind::TryAgain,
                "Total messages to read must be grater than zero",
            )));
        }

        let mut client: Client = get_redis_client(credentials, host, port, db)?;

        if !client.check_connection() {
            return Err(RedisError::from((
                ErrorKind::TryAgain,
                "Error getting connection to Redis server",
            )));
        };

        if !client.get_connection()?.exists::<_, bool>(stream_name)? {
            return Err(RedisError::from((
                ErrorKind::TryAgain,
                "Stream does not exist",
            )));
        };

        match client.get_connection()?.xgroup_create::<_, _, _, bool>(
            stream_name,
            group_name,
            since_id,
        ) {
            Ok(_) => {
                debug!("Consumers group {} was created successfully", group_name);
            }
            Err(error) => {
                if error.to_string().contains("BUSYGROUP") {
                    debug!("Consumers group {} already exists", group_name);
                } else {
                    return Err(error);
                }
            }
        };

        Ok(Self {
            client,
            stream_name,
            group_name,
            since_id,
            consumer_name,
            min_idle_time_milliseconds,
            new_messages_count,
            pending_messages_count,
            claimed_messages_count,
            block,
        })
    }

    /// Read new messages from *stream* using [`Commands::xread_options`] ([`XREADGROUP`](https://redis.io/commands/xreadgroup/)).
    fn read_new_messages(&self) -> RedsumerResult<Vec<StreamId>> {
        let xreadgroup_response: StreamReadReply =
            self.get_client().get_connection()?.xread_options(
                &[self.get_stream_name()],
                &[">"],
                &StreamReadOptions::default()
                    .group(self.get_group_name(), self.get_consumer_name())
                    .count(self.get_new_messages_count())
                    .block(self.get_block().into()),
            )?;

        let mut new_messages: Vec<StreamId> = Vec::new();
        for stream in xreadgroup_response.keys.iter() {
            match stream.key.eq(self.get_stream_name()) {
                true => new_messages.extend(stream.ids.to_owned()),
                false => warn!("Unexpected stream name found: {}. ", stream.key),
            };
        }

        Ok(new_messages)
    }

    /// Read pending messages from *stream* until to a maximum of *pending_messages_count* using [`Commands::xread_options`] ([`XREADGROUP`](https://redis.io/commands/xreadgroup/)).
    fn read_pending_messages(&self) -> RedsumerResult<Vec<StreamId>> {
        let xreadgroup_response: StreamReadReply =
            self.get_client().get_connection()?.xread_options(
                &[self.get_stream_name()],
                &[self.get_since_id()],
                &StreamReadOptions::default()
                    .group(self.get_group_name(), self.get_consumer_name())
                    .count(self.get_pending_messages_count()),
            )?;

        let mut pending_messages: Vec<StreamId> = Vec::new();
        for stream in xreadgroup_response.keys.iter() {
            match stream.key.eq(self.get_stream_name()) {
                true => pending_messages.extend(stream.ids.to_owned()),
                false => warn!("Unexpected stream name found: {}. ", stream.key),
            };
        }

        Ok(pending_messages)
    }

    /// Claim pending messages from *stream* from *since_id* to the newest one until to a maximum of *claimed_messages_count* using [`Commands::xpending_count`] ([`XPENDING`](https://redis.io/commands/xpending/)) and [`Commands::xclaim_options`] ([`XCLAIM`](https://redis.io/commands/xclaim/)).
    fn claim_pending_messages(&self) -> RedsumerResult<Vec<StreamId>> {
        let ids_to_claim: Vec<Id> = self
            .get_client()
            .get_connection()?
            .xpending_count::<_, _, _, _, _, StreamPendingCountReply>(
                self.get_stream_name(),
                self.get_group_name(),
                self.get_since_id(),
                "+",
                self.get_claimed_messages_count(),
            )?
            .ids
            .iter()
            .map(|stream_pending_id| stream_pending_id.id.to_owned())
            .collect::<Vec<Id>>();

        if ids_to_claim.is_empty() {
            return Ok(Vec::new());
        }

        Ok(self
            .get_client()
            .get_connection()?
            .xclaim_options::<_, _, _, _, _, StreamClaimReply>(
                self.get_stream_name(),
                self.get_group_name(),
                self.get_consumer_name(),
                self.get_min_idle_time_milliseconds(),
                &ids_to_claim,
                StreamClaimOptions::default(),
            )?
            .ids)
    }

    /// Consume messages from stream according to the following steps:
    ///
    /// 1. Consumer tries to get new messages. If new messages are found, they are returned as a result.
    /// 2. If new messages are not found, consumer tries to get pending messages. If pending messages are found, they are returned as a result.
    /// 3. If pending messages are not found, consumer tries to claim messages from other consumers according to *min_idle_time_milliseconds*. If claimed messages are found, they are returned as a result.
    /// 4. If new, pending or claimed messages are not found, an empty list is returned as a result.
    ///
    ///  # Arguments:
    ///  *No arguments*
    ///
    ///  # Returns:
    ///  - A [`RedsumerResult`] containing a list of [`StreamId`] if new, pending or claimed messages are found, otherwise an empty list is returned. If an error occurs, a [`RedsumerError`] is returned.
    pub async fn consume(&mut self) -> RedsumerResult<Vec<StreamId>> {
        debug!("Consuming messages from stream {}", self.get_stream_name());

        debug!("Processing new messages");
        let new_messages: Vec<StreamId> = match self.get_new_messages_count().gt(&0) {
            true => self.read_new_messages()?,
            false => Vec::new(),
        };
        if new_messages.len().gt(&0) {
            debug!("Total new messages found: {}", new_messages.len());
            return Ok(new_messages);
        }

        debug!("Processing pending messages");
        let pending_messages: Vec<StreamId> = match self.get_pending_messages_count().gt(&0) {
            true => self.read_pending_messages()?,
            false => Vec::new(),
        };
        if pending_messages.len().gt(&0) {
            debug!("Total pending messages found: {}", pending_messages.len());
            return Ok(pending_messages);
        }

        debug!("Processing claimed messages");
        let claimed_messages: Vec<StreamId> = match self.get_claimed_messages_count().gt(&0) {
            true => self.claim_pending_messages()?,
            false => Vec::new(),
        };
        if claimed_messages.len().gt(&0) {
            debug!("Total claimed messages found: {}", claimed_messages.len());
            return Ok(claimed_messages);
        }

        debug!("No messages found");

        Ok(Vec::new())
    }

    /// Verify if a specific message by *id* is still in consumer pending list.
    ///
    ///  If the message is not still in consumer pending list, it is recommended to verify if another consumer has claimed the message before trying to process it again.
    ///
    /// # Arguments:
    /// - **id**: Stream message id.
    ///
    ///  # Returns:
    ///  - A [`RedsumerResult`] containing a boolean value. If the message is still in consumer pending list, `true` is returned. Otherwise, `false` is returned. If an error occurs, a [`RedsumerError`] is returned.
    pub fn is_still_mine(&self, id: &Id) -> RedsumerResult<bool> {
        Ok(self
            .get_client()
            .get_connection()?
            .xpending_consumer_count::<_, _, _, _, _, _, StreamPendingCountReply>(
                self.get_stream_name(),
                self.get_group_name(),
                id,
                id,
                1,
                self.get_consumer_name(),
            )?
            .ids
            .len()
            .gt(&0))
    }

    /// Ack a message by *id*.
    ///
    ///  If the message is acked, it is removed from the consumer pending list. Otherwise, it is recommended to verify if another consumer has claimed the message before trying to process it again.
    ///  
    /// # Arguments:
    /// - **id**: Stream message id.
    ///
    /// # Returns:
    ///  - A [`RedsumerResult`] containing a boolean value. If the message is acked, `true` is returned. Otherwise, `false` is returned. If an error occurs, a [`RedsumerError`] is returned.
    pub async fn ack(&self, id: &Id) -> RedsumerResult<bool> {
        Ok(self.get_client().get_connection()?.xack::<_, _, _, bool>(
            self.stream_name,
            self.group_name,
            &[id],
        )?)
    }
}
