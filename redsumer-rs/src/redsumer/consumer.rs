use log::{debug, warn};
use std::fmt::Debug;

use redis::streams::StreamPendingReply;
use redis::{
    streams::{
        StreamClaimOptions, StreamClaimReply, StreamId, StreamPendingCountReply, StreamReadOptions,
        StreamReadReply,
    },
    Client, Commands, ErrorKind, RedisError,
};

use super::client::{get_redis_client, ClientCredentials};
use super::types::{Id, RedsumerResult};

/// A consumer implementation of *Redis Streams*.
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
    /// When a new instance of [`RedsumerConsumer`] is created, it is checked if the stream exists and if the consumers group exists. If the consumers group does not exist, it is created based on the *stream_name*, *group_name* and *since_id*.
    ///
    /// ```rust,no_run
    /// use redsumer::{RedsumerConsumer, ClientCredentials};
    ///
    /// let consumer = RedsumerConsumer::new(
    ///     Some(ClientCredentials::new("user", "password")),
    ///     "localhost",
    ///     "6379",
    ///     "0",
    ///     "my_stream",
    ///     "my_consumer_group",
    ///     "0-0",
    ///     "my_consumer",
    ///     360000,
    ///     10,
    ///     10,
    ///     10,
    ///     5,
    /// ).unwrap();
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
        let client: Client = get_redis_client(credentials, host, port, db)?;

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

        let total_messages_to_read: usize =
            new_messages_count + pending_messages_count + claimed_messages_count;
        if total_messages_to_read.eq(&0) {
            return Err(RedisError::from((
                ErrorKind::TryAgain,
                "Total messages to read must be grater than zero",
            )));
        }

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

    /// Claim pending messages from *stream* from *since_id* to the newest one until to a maximum of *claimed_messages_count* using [`Commands::xclaim_options`] ([`XCLAIM`](https://redis.io/commands/xclaim/)).
    fn claim_pending_messages(&self) -> RedsumerResult<Vec<StreamId>> {
        let xpending_count_response: StreamPendingCountReply =
            self.get_client().get_connection()?.xpending_count(
                self.get_stream_name(),
                self.get_group_name(),
                self.get_since_id(),
                "+",
                self.get_client()
                    .get_connection()?
                    .xpending::<_, _, StreamPendingReply>(
                        self.get_stream_name(),
                        self.get_group_name(),
                    )?
                    .count(),
            )?;

        let mut non_own_pending_messages: Vec<Id> = Vec::new();
        for pending_message in xpending_count_response.ids.iter() {
            if pending_message.consumer.ne(&self.get_consumer_name())
                && pending_message
                    .last_delivered_ms
                    .ge(&self.get_min_idle_time_milliseconds())
            {
                non_own_pending_messages.push(pending_message.id.to_owned());
            }

            if non_own_pending_messages
                .len()
                .ge(&self.get_claimed_messages_count())
            {
                break;
            }
        }

        let claimed_messages: Vec<StreamId> = match non_own_pending_messages.len().gt(&0) {
            true => {
                self.get_client()
                    .get_connection()?
                    .xclaim_options::<_, _, _, _, _, StreamClaimReply>(
                        self.get_stream_name(),
                        self.get_group_name(),
                        self.get_consumer_name(),
                        self.get_min_idle_time_milliseconds(),
                        &non_own_pending_messages,
                        StreamClaimOptions::default(),
                    )?
                    .ids
            }
            false => Vec::new(),
        };

        Ok(claimed_messages)
    }

    /// Consume messages from *stream* according to the following steps:
    /// 1. Consumer tries to get new messages. If new messages are found, they are returned as a result.
    /// 2. If new messages are not found, consumer tries to get pending messages. If pending messages are found, they are returned as a result.
    /// 3. If pending messages are not found, consumer tries to claim messages from other consumers according to *min_idle_time_milliseconds*. If claimed messages are found, they are returned as a result.
    /// 4. If new, pending or claimed messages are not found, an empty list is returned as a result.
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
    /// # Arguments:
    /// - **id**: Stream message id.
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
    /// # Arguments:
    /// - **id**: Stream message id.
    pub async fn ack(&self, id: &Id) -> RedsumerResult<bool> {
        Ok(self.get_client().get_connection()?.xack::<_, _, _, bool>(
            self.stream_name,
            self.group_name,
            &[id],
        )?)
    }
}
