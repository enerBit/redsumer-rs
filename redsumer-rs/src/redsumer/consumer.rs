use redis::{streams::StreamId, Client};
use tracing::{debug, info};

use crate::core::streams::types::{LatestPendingMessageId, NextIdToClaim};
#[allow(unused_imports)]
use crate::core::{
    client::{ClientArgs, RedisClientBuilder},
    connection::VerifyConnection,
    result::{RedsumerError, RedsumerResult},
    streams::{
        consumer::{ConsumerCommands, BEGINNING_OF_TIME_ID},
        types::{Id, LastDeliveredMilliseconds, TotalTimesDelivered},
    },
};

/// Options used to configure the consume operation when reading new messages from a Redis stream.
#[derive(Debug, Clone)]
pub struct ReadNewMessagesOptions {
    /// The number of new messages to read from the stream.
    count: usize,

    /// The block time [seconds] to wait for new messages to arrive in the stream.
    block: usize,
}

impl ReadNewMessagesOptions {
    /// Get the number of new messages to read from the stream.
    pub fn get_count(&self) -> usize {
        self.count
    }

    /// Get the block time to wait for new messages to arrive in the stream.
    pub fn get_block(&self) -> usize {
        self.block
    }

    /// Create a new instance of [`ReadNewMessagesOptions`].
    ///
    /// # Arguments:
    /// - **count**: The number of new messages to read from the stream.
    /// - **block**: The block time in seconds to wait for new messages to arrive in the stream.
    ///
    /// # Returns:
    /// A new instance of [`ReadNewMessagesOptions`] with the given count and block time.
    pub fn new(count: usize, block: usize) -> Self {
        ReadNewMessagesOptions { count, block }
    }
}

/// Options used to configure the consume operation when reading pending messages from a Redis stream.
#[derive(Debug, Clone)]
pub struct ReadPendingMessagesOptions {
    /// The number of pending messages to read from the stream.
    count: usize,

    /// The latest pending message ID to start reading from.
    latest_pending_message_id: String,
}

impl ReadPendingMessagesOptions {
    /// Get the number of pending messages to read from the stream.
    pub fn get_count(&self) -> usize {
        self.count
    }

    /// Get the latest pending message ID to start reading from.
    fn get_latest_pending_message_id(&self) -> &str {
        &self.latest_pending_message_id
    }

    /// Create a new instance of [`ReadPendingMessagesOptions`].
    ///
    /// # Arguments:
    /// - **count**: The number of pending messages to read from the stream.
    /// - **latest_pending_message_id**: The latest pending message ID to start reading from.
    ///
    /// # Returns:
    /// A new instance of [`ReadPendingMessagesOptions`] with the given count and latest pending message ID.
    pub fn new(count: usize) -> Self {
        ReadPendingMessagesOptions {
            count,
            latest_pending_message_id: BEGINNING_OF_TIME_ID.to_string(),
        }
    }
}

/// Options used to configure the consume operation when claiming messages from a Redis stream.
#[derive(Debug, Clone)]
pub struct ClaimMessagesOptions {
    /// The number of messages to claim from the stream.
    count: usize,

    /// The min idle time [milliseconds] to claim the messages.
    min_idle_time: usize,

    /// The latest ID to start claiming from.
    next_id_to_claim: String,
}

impl ClaimMessagesOptions {
    /// Get the number of messages to claim from the stream.
    pub fn get_count(&self) -> usize {
        self.count
    }

    /// Get the min idle time to claim the messages.
    pub fn get_min_idle_time(&self) -> usize {
        self.min_idle_time
    }

    /// Get the latest ID to start claiming from.
    fn get_next_id_to_claim(&self) -> &str {
        &self.next_id_to_claim
    }

    /// Create a new instance of [`ClaimMessagesOptions`].
    ///
    /// # Arguments:
    /// - **count**: The number of messages to claim from the stream.
    /// - **min_idle_time**: The min idle time in milliseconds to claim the messages.
    ///
    /// # Returns:
    /// A new instance of [`ClaimMessagesOptions`] with the given count, min idle time and latest pending message ID.
    pub fn new(count: usize, min_idle_time: usize) -> Self {
        ClaimMessagesOptions {
            count,
            min_idle_time,
            next_id_to_claim: BEGINNING_OF_TIME_ID.to_string(),
        }
    }
}

/// Define the configuration parameters to create a consumer instance.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Stream name where messages will be consumed.
    stream_name: String,

    /// Group name where the consumer is registered.
    group_name: String,

    /// Consumer name within the specified consumers group.
    consumer_name: String,

    /// Options to configure the read new messages operation.
    read_new_messages_options: ReadNewMessagesOptions,

    /// Options to configure the read pending messages operation.
    read_pending_messages_options: ReadPendingMessagesOptions,

    /// Options to configure the claim messages operation.
    claim_messages_options: ClaimMessagesOptions,
}

impl ConsumerConfig {
    /// Get **stream name**.
    pub fn get_stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Get **group name**.
    pub fn get_group_name(&self) -> &str {
        &self.group_name
    }

    /// Get **consumer name**.
    pub fn get_consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Get **read new messages options**.
    pub fn get_read_new_messages_options(&self) -> &ReadNewMessagesOptions {
        &self.read_new_messages_options
    }

    /// Get **read pending messages options**.
    pub fn get_read_pending_messages_options(&self) -> &ReadPendingMessagesOptions {
        &self.read_pending_messages_options
    }

    /// Get **claim messages options**.
    pub fn get_claim_messages_options(&self) -> &ClaimMessagesOptions {
        &self.claim_messages_options
    }

    /// Create a new [`ConsumerConfig`] instance.
    ///
    /// # Arguments:
    /// - **stream_name**: The name of the stream where messages will be produced.
    /// - **group_name**: Consumers group name.
    /// - **consumer_name**: Represents the consumer name within the specified consumers group, which must be ensured to be unique. In a microservices architecture, for example, it is recommended to use the pod name.
    /// - **since_id**: Latest ID to start reading from.
    /// - **read_new_messages_options**: Options to configure the read new messages operation.
    /// - **read_pending_messages_options**: Options to configure the read pending messages operation.
    /// - **claim_messages_options**: Options to configure the claim messages operation.
    ///
    /// # Returns:
    /// A new [`ConsumerConfig`] instance.
    pub fn new(
        stream_name: &str,
        group_name: &str,
        consumer_name: &str,
        read_new_messages_options: ReadNewMessagesOptions,
        read_pending_messages_options: ReadPendingMessagesOptions,
        claim_messages_options: ClaimMessagesOptions,
    ) -> Self {
        ConsumerConfig {
            stream_name: stream_name.to_owned(),
            group_name: group_name.to_owned(),
            consumer_name: consumer_name.to_owned(),
            read_new_messages_options,
            read_pending_messages_options,
            claim_messages_options,
        }
    }
}

/// Define the kind of messages that were consumed by a specific consumer.
enum MessagesKind {
    /// The messages were obtained from the new messages list and have not been delivered before to any consumer.
    New,

    /// The messages were read from the consumer pending list. They were delivered to a consumer before, but they were not acked yet and they were not claimed by another consumer.
    Pending,

    /// The messages were claimed by another consumer and they were not acked yet.
    Claimed,

    /// Messages were not obtained from stream. It means that there are no new, pending or claimed messages to be processed by a consumer in the specified group.
    NotFound,
}

impl MessagesKind {
    /// Check if the messages are new.
    fn are_new(&self) -> bool {
        matches!(self, MessagesKind::New)
    }

    /// Check if the messages are pending.
    fn are_pending(&self) -> bool {
        matches!(self, MessagesKind::Pending)
    }

    /// Check if the messages were claimed.
    fn were_claimed(&self) -> bool {
        matches!(self, MessagesKind::Claimed)
    }

    /// Check if the messages were not found.
    fn not_found(&self) -> bool {
        matches!(self, MessagesKind::NotFound)
    }
}

/// A reply to consume messages from a Redis stream. It contains a list of stream IDs and the kind of messages.
pub struct ConsumeMessagesReply {
    /// A list of stream IDs.
    messages: Vec<StreamId>,

    /// The kind of messages.
    kind: MessagesKind,
}

impl ConsumeMessagesReply {
    /// Get **messages**.
    pub fn get_messages(&self) -> &Vec<StreamId> {
        &self.messages
    }

    /// Verify if the messages are new.
    pub fn are_new(&self) -> bool {
        self.kind.are_new()
    }

    /// Verify if the messages are pending in the consumer pending list.
    pub fn are_pending(&self) -> bool {
        self.kind.are_pending()
    }

    /// Verify if the messages were claimed by another consumer.
    pub fn were_claimed(&self) -> bool {
        self.kind.were_claimed()
    }

    /// Verify if the messages were not found.
    pub fn not_found(&self) -> bool {
        self.kind.not_found()
    }
}

/// Convert a tuple into a [`ConsumeMessagesReply`] instance.
impl From<(Vec<StreamId>, MessagesKind)> for ConsumeMessagesReply {
    fn from((messages, kind): (Vec<StreamId>, MessagesKind)) -> Self {
        ConsumeMessagesReply { messages, kind }
    }
}

/// A reply to verify if a specific message is still in consumer pending list.
pub struct IsStillMineReply {
    /// A boolean value indicating if the message is still in consumer pending list.
    is_still_mine: bool,

    /// The total time in milliseconds that elapsed since the last message was delivered to the consumer.
    last_delivered_milliseconds: Option<LastDeliveredMilliseconds>,

    /// The total number of times that a message was delivered to any consumer in the group.
    total_times_delivered: Option<TotalTimesDelivered>,
}

impl IsStillMineReply {
    /// Get **is still mine**.
    pub fn get_is_still_mine(&self) -> bool {
        self.is_still_mine
    }

    /// Get **last delivered milliseconds**.
    pub fn get_last_delivered_milliseconds(&self) -> Option<LastDeliveredMilliseconds> {
        self.last_delivered_milliseconds
    }

    /// Get **total times delivered**.
    pub fn get_total_times_delivered(&self) -> Option<TotalTimesDelivered> {
        self.total_times_delivered
    }
}

/// Convert a tuple into a [`IsStillMineReply`] instance.
impl
    From<(
        bool,
        Option<LastDeliveredMilliseconds>,
        Option<TotalTimesDelivered>,
    )> for IsStillMineReply
{
    fn from(
        (is_still_mine, last_delivered_milliseconds, total_times_delivered): (
            bool,
            Option<LastDeliveredMilliseconds>,
            Option<TotalTimesDelivered>,
        ),
    ) -> Self {
        IsStillMineReply {
            is_still_mine,
            last_delivered_milliseconds,
            total_times_delivered,
        }
    }
}

/// A reply to ack a specific message.
pub struct AckMessageReply {
    /// A boolean value indicating if the message is acked.
    was_acked: bool,
}

impl AckMessageReply {
    /// Get **was acked**. If the message was not acked, it is recommended to verify if another consumer has claimed the message before trying to process it again.
    pub fn was_acked(&self) -> bool {
        self.was_acked
    }
}

/// Convert a boolean value into a [`AckMessageReply`] instance.
impl From<bool> for AckMessageReply {
    fn from(was_acked: bool) -> Self {
        AckMessageReply { was_acked }
    }
}

/// A consumer implementation of Redis Streams. The consumer is responsible for consuming messages from a stream. It can read new messages,  pending messages or claim messages from other consumers according to their min idle time.
#[derive(Debug, Clone)]
pub struct Consumer {
    /// Redis client to interact with Redis server.
    client: Client,

    /// Consumer configuration parameters.
    config: ConsumerConfig,
}

impl Consumer {
    /// Get [`Client`].
    fn get_client(&self) -> &Client {
        &self.client
    }

    /// Get *config*.
    pub fn get_config(&self) -> &ConsumerConfig {
        &self.config
    }

    /// Update the latest pending message ID to start reading from.
    fn update_latest_pending_message_id(&mut self, id: &str) {
        self.config
            .read_pending_messages_options
            .latest_pending_message_id = id.to_owned();
    }

    /// Update the next ID to claim.
    fn update_next_id_to_claim(&mut self, id: &str) {
        self.config.claim_messages_options.next_id_to_claim = id.to_owned();
    }

    /// Build a new [`Consumer`] instance.
    ///
    ///  Before creating a new consumer, the following validations are performed:
    ///
    /// - If connection string is invalid, a [`RedsumerError`] is returned.
    /// - If connection to Redis server can not be established, a [`RedsumerError`] is returned.
    /// - If the stream does not exist, a [`RedsumerError`] is returned: The stream must exist before creating a new consumer.
    ///  - If the consumers group does not exist, it is created based on the *stream_name*, *group_name* and the given *initial_stream_id*. If an error occurs during the creation process, a [`RedsumerError`] is returned.
    ///
    /// # Arguments:
    /// - **args**: Client arguments to build a new [`Client`] instance.
    /// - **config**: Consumer configuration parameters.
    /// - **initial_stream_id**: The ID of the message to start consuming.
    ///
    ///  # Returns:
    /// - A [`RedsumerResult`] containing a [`Consumer`] instance. Otherwise, a [`RedsumerError`] is returned.
    pub fn new(
        args: ClientArgs,
        config: ConsumerConfig,
        initial_stream_id: Option<String>,
    ) -> RedsumerResult<Self> {
        debug!(
            "Creating a new consumer instance by: {:?} and {:?}",
            args, config
        );

        let mut client: Client = args.build()?;
        client.ping()?;

        client.verify_if_stream_exists(config.get_stream_name())?;
        client.create_consumer_group(
            config.get_stream_name(),
            config.get_group_name(),
            initial_stream_id.unwrap_or(BEGINNING_OF_TIME_ID.to_string()),
        )?;

        info!("Consumer was created successfully and it is ready to be used");

        Ok(Self { client, config })
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
    ///  - A [`RedsumerResult`] containing a list of [`ConsumeMessagesReply`] if new, pending or claimed messages are found, otherwise an empty list is returned. If an error occurs, a [`RedsumerError`] is returned.
    pub async fn consume(&mut self) -> RedsumerResult<ConsumeMessagesReply> {
        debug!(
            "Consuming messages from stream {}",
            self.get_config().get_stream_name()
        );

        debug!(
            "Processing new messages by: {:?}",
            self.get_config().get_read_new_messages_options()
        );

        let new_messages: Vec<StreamId> = self.get_client().to_owned().read_new_messages(
            &self.get_config().get_stream_name(),
            &self.get_config().get_group_name(),
            &self.get_config().get_consumer_name(),
            self.get_config()
                .get_read_new_messages_options()
                .get_count(),
            self.get_config()
                .get_read_new_messages_options()
                .get_block(),
        )?;
        if new_messages.len().gt(&0) {
            debug!("Total new messages found: {}", new_messages.len());
            return Ok((new_messages, MessagesKind::New).into());
        }

        debug!(
            "Processing pending messages by: {:?}",
            self.get_config().get_read_pending_messages_options()
        );

        let (pending_messages, latest_pending_message_id): (Vec<StreamId>, LatestPendingMessageId) =
            self.get_client().to_owned().read_pending_messages(
                &self.get_config().get_stream_name(),
                &self.get_config().get_group_name(),
                &self.get_config().get_consumer_name(),
                self.get_config()
                    .get_read_pending_messages_options()
                    .get_latest_pending_message_id(),
                self.get_config()
                    .get_read_pending_messages_options()
                    .get_count(),
            )?;

        debug!("Updating latest pending message ID to: {latest_pending_message_id}",);

        self.update_latest_pending_message_id(&latest_pending_message_id);
        if pending_messages.len().gt(&0) {
            debug!("Total pending messages found: {}", pending_messages.len());
            return Ok((pending_messages, MessagesKind::Pending).into());
        }

        debug!(
            "Processing claimed messages by: {:?}",
            self.get_config().get_claim_messages_options()
        );

        let (claimed_messages, next_id_to_claim): (Vec<StreamId>, NextIdToClaim) =
            self.get_client().to_owned().claim_pending_messages(
                &self.get_config().get_stream_name(),
                &self.get_config().get_group_name(),
                &self.get_config().get_consumer_name(),
                self.get_config()
                    .get_claim_messages_options()
                    .get_min_idle_time(),
                self.get_config()
                    .get_claim_messages_options()
                    .get_next_id_to_claim(),
                self.get_config().get_claim_messages_options().get_count(),
            )?;

        debug!("Updating next ID to claim to: {next_id_to_claim}",);

        self.update_next_id_to_claim(&next_id_to_claim);
        if claimed_messages.len().gt(&0) {
            debug!("Total claimed messages found: {}", claimed_messages.len());
            return Ok((claimed_messages, MessagesKind::Claimed).into());
        }

        debug!("No messages found");

        Ok((Vec::new(), MessagesKind::NotFound).into())
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
    pub fn is_still_mine(&self, id: &Id) -> RedsumerResult<IsStillMineReply> {
        self.get_client()
            .to_owned()
            .is_still_mine(
                self.get_config().get_stream_name(),
                self.get_config().get_group_name(),
                self.get_config().get_consumer_name(),
                id,
            )
            .map(IsStillMineReply::from)
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
    pub async fn ack(&self, id: &Id) -> RedsumerResult<AckMessageReply> {
        self.get_client()
            .to_owned()
            .ack(
                self.get_config().get_stream_name(),
                self.get_config().get_group_name(),
                &[id],
            )
            .map(AckMessageReply::from)
    }
}
