use async_trait::async_trait;
use log::{debug, warn};
use tokio::time::{sleep, Duration};

use redis::streams::StreamInfoConsumersReply;
use redis::{
    streams::{
        StreamClaimOptions, StreamClaimReply, StreamId, StreamInfoGroupsReply,
        StreamInfoStreamReply, StreamPendingCountReply, StreamReadOptions, StreamReadReply,
    },
    Commands, ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs,
};

use crate::redsumer::client::{ClientCredentials, RedsumerClient};
use crate::redsumer::stream_information::{StreamConsumersInfo, StreamInfo};

/// Builder options for [`RedsumerConsumer`].
pub struct RedsumerConsumerOptions<'c> {
    latest_id: &'c str,
    max_wait_seconds_for_stream: u8,
    min_idle_time_milliseconds: u32,
    pending_messages_count: u16,
    new_messages_count: u16,
    block: u16,
}

impl<'c> RedsumerConsumerOptions<'c> {
    /// Set `latest_id <message id>`.
    pub fn set_latest_id(mut self, latest_id: &'c str) -> Self {
        self.latest_id = latest_id;
        self
    }

    /// Set `max_wait_seconds_for_stream <seconds>`.
    pub fn set_max_wait_seconds_for_stream(mut self, max_wait_seconds_for_stream: u8) -> Self {
        self.max_wait_seconds_for_stream = max_wait_seconds_for_stream;
        self
    }

    /// Set `min_idle_time_milliseconds <milliseconds>`.
    pub fn set_min_idle_time_milliseconds(mut self, min_idle_time_milliseconds: u32) -> Self {
        self.min_idle_time_milliseconds = min_idle_time_milliseconds;
        self
    }

    /// Set `pending_messages_count <units>`.
    pub fn set_pending_messages_count(mut self, pending_messages_count: u16) -> Self {
        self.pending_messages_count = pending_messages_count;
        self
    }

    /// Set `new_messages_count <units>`.
    pub fn set_new_messages_count(mut self, new_messages_count: u16) -> Self {
        self.new_messages_count = new_messages_count;
        self
    }

    /// Set `block <milliseconds>`.
    pub fn set_block(mut self, block: u16) -> Self {
        self.block = block;
        self
    }

    /// Get `latest_id <message id>`.
    pub fn get_latest_id(&self) -> &'c str {
        self.latest_id
    }

    /// Get `max_wait_seconds_for_stream <seconds>`.
    pub fn get_max_wait_seconds_for_stream(&self) -> u8 {
        self.max_wait_seconds_for_stream
    }

    /// Get `min_idle_time_milliseconds <milliseconds>`.
    pub fn get_min_idle_time_milliseconds(&self) -> u32 {
        self.min_idle_time_milliseconds
    }

    /// Get `pending_messages_count <units>`.
    pub fn get_pending_messages_count(&self) -> u16 {
        self.pending_messages_count
    }

    /// Get `new_messages_count <units>`.
    pub fn get_new_messages_count(&self) -> u16 {
        self.new_messages_count
    }

    /// Get `block <milliseconds>`.
    pub fn get_block(&self) -> u16 {
        self.block
    }

    /// Build a new [`RedsumerConsumerOptions`] with `latest_id`, `max_wait_seconds_for_stream`, `min_idle_time_milliseconds`, `pending_messages_count`, `new_messages_count` and `block`.
    pub fn new(
        latest_id: &'c str,
        max_wait_seconds_for_stream: u8,
        min_idle_time_milliseconds: u32,
        pending_messages_count: u16,
        new_messages_count: u16,
        block: u16,
    ) -> RedisResult<RedsumerConsumerOptions<'c>> {
        if pending_messages_count.eq(&0u16) {
            return Err(RedisError::from((
                ErrorKind::ClientError,
                "Pending messages count must be grater than zero",
            )));
        }

        if new_messages_count.eq(&0u16) {
            return Err(RedisError::from((
                ErrorKind::ClientError,
                "New messages count must be grater than zero",
            )));
        }

        Ok(RedsumerConsumerOptions {
            latest_id,
            max_wait_seconds_for_stream,
            min_idle_time_milliseconds,
            pending_messages_count,
            new_messages_count,
            block,
        })
    }
}

/// Manager to consume `stream events` from `Redis Streams`.
pub struct RedsumerConsumer<'c> {
    client: RedsumerClient<'c>,
    stream_name: &'c str,
    group_name: &'c str,
    consumer_name: &'c str,
    options: RedsumerConsumerOptions<'c>,
}

impl<'c> RedsumerConsumer<'c> {
    fn get_client(&self) -> &RedsumerClient<'c> {
        &self.client
    }

    /// Get `stream_name <String>`.
    pub fn get_stream_name(&self) -> &'c str {
        self.stream_name
    }

    /// Get `group_name <String>`.
    pub fn get_group_name(&self) -> &'c str {
        self.group_name
    }

    /// Get `consumer_name <String>`.
    pub fn get_consumer_name(&self) -> &'c str {
        self.consumer_name
    }

    /// Get `consumer_options` <[`RedsumerConsumerOptions`]>.
    pub fn get_consumer_options(&self) -> &RedsumerConsumerOptions<'c> {
        &self.options
    }

    /// Build a new [`RedsumerConsumer`] from specific `host`, `port`, `db`, `stream_name`, `group_name` and `consumer_name`. Set specific consumer options using [`RedsumerConsumerOptions`].
    pub fn new(
        credentials: Option<ClientCredentials<'c>>,
        host: &'c str,
        port: &'c str,
        db: &'c str,
        stream_name: &'c str,
        group_name: &'c str,
        consumer_name: &'c str,
        options: RedsumerConsumerOptions<'c>,
    ) -> RedsumerConsumer<'c> {
        RedsumerConsumer {
            client: RedsumerClient::init(credentials, host, port, db),
            stream_name,
            group_name,
            consumer_name,
            options,
        }
    }

    /// To check the existence of a specific stream by name. If the stream does not exist, the consumer raises a RedisError.
    async fn validate_stream_existence(&self) -> Result<(), RedisError> {
        debug!("Verifying stream {}", self.get_stream_name());

        let mut step: u32 = 0;
        let mut stream_ready: bool = false;
        while !stream_ready {
            stream_ready = self
                .get_client()
                .get_connection()?
                .exists(self.get_stream_name())?;

            if !stream_ready {
                let wait_time: u64 = 2u64.pow(step);
                if wait_time
                    > self
                        .get_consumer_options()
                        .get_max_wait_seconds_for_stream() as u64
                {
                    return Err(RedisError::from((
                        ErrorKind::TryAgain,
                        "Stream was not found",
                    )));
                }

                warn!("Stream {} not ready yet", self.get_stream_name());
                warn!("Waiting for {} seconds", &wait_time);

                sleep(Duration::from_secs(wait_time)).await;
                step += 1
            }
        }

        debug!("Stream {} is ready", self.get_stream_name());

        Ok(())
    }

    /// To check the existence of the consumers group. If consumers group does not exist, the consumer create it by name.
    fn checking_consumer_group(&self) -> Result<(), RedisError> {
        let consumer_group: RedisResult<bool> = self.get_client().get_connection()?.xgroup_create(
            self.get_stream_name(),
            self.get_group_name(),
            self.get_consumer_options().get_latest_id(),
        );
        match consumer_group {
            Ok(_) => debug!("Consumer group {} created successfully", self.group_name),
            Err(_) => debug!("Consumer group {} already exists", self.group_name),
        };

        Ok(())
    }

    /// To autoclaim stream pending messages:
    /// 1. First step: To get pending messages by `stream name` and `group name`. Consumer reads maximum `count` messages from `latest id` to newest id delivered to any consumer in consumers group.
    /// 2. Second step: To claim ownership of pending messages from first step result by `stream name`, `group name` and `consumer name`. The consumer claims pending messages when `min idle time milliseconds` is exceeded.
    fn autoclaim(&self) -> Result<(), RedisError> {
        let pending_messages_to_claim: StreamPendingCountReply =
            self.get_client().get_connection()?.xpending_count(
                self.get_stream_name(),
                self.get_group_name(),
                self.get_consumer_options().get_latest_id(),
                "+",
                self.get_consumer_options().get_pending_messages_count(),
            )?;

        let ids_to_claim: Vec<String> = pending_messages_to_claim
            .ids
            .iter()
            .map(|stream_pending_id| stream_pending_id.id.to_owned())
            .collect::<Vec<String>>();

        if ids_to_claim.len() > 0 {
            let _claimed_messages: StreamClaimReply =
                self.get_client().get_connection()?.xclaim_options(
                    self.get_stream_name(),
                    self.get_group_name(),
                    self.get_consumer_name(),
                    self.get_consumer_options().get_min_idle_time_milliseconds(),
                    &ids_to_claim,
                    StreamClaimOptions::default(),
                )?;
        }

        Ok(())
    }

    /// To read stream messages by `stream name`, `group name` and `consumer name` as follows:
    /// 1. Pending messages by `consumer name` from `latest id` to newest id under its ownership.
    /// 2. New stream messages never delivered to any `consumer name`.
    ///
    /// Consumer reads maximum `count` messages.
    fn xread_group(&self) -> Result<Vec<StreamId>, RedisError> {
        let mut ids: Vec<StreamId> = Vec::new();

        let pending_messages_reply: StreamReadReply =
            self.get_client().get_connection()?.xread_options(
                &[self.get_stream_name()],
                &[self.get_consumer_options().get_latest_id()],
                &StreamReadOptions::default()
                    .group(self.get_group_name(), self.get_consumer_name())
                    .count(
                        self.get_consumer_options()
                            .get_pending_messages_count()
                            .into(),
                    ),
            )?;

        for key in pending_messages_reply.keys.iter() {
            let key_ids = key.ids.to_owned();
            ids.extend(key_ids);
        }
        let total_pending_messages_read: usize = ids.len();

        let new_messages: StreamReadReply = self.get_client().get_connection()?.xread_options(
            &[self.get_stream_name()],
            &[">"],
            &StreamReadOptions::default()
                .group(self.get_group_name(), self.get_consumer_name())
                .count(self.get_consumer_options().get_new_messages_count().into())
                .block(self.get_consumer_options().get_block().into()),
        )?;

        for key in new_messages.keys.iter() {
            let key_ids = key.ids.to_owned();
            ids.extend(key_ids);
        }

        debug!(
            "Total {} messages read from stream: {} pending messages and {} new messages",
            ids.len(),
            total_pending_messages_read,
            ids.len() - total_pending_messages_read,
        );

        Ok(ids)
    }

    /// **Consumes and returns** stream messages from a **Redis Stream** based on own configuration values.
    ///
    /// The process of **consume** the **stream messages** is carried out according to the following steps:
    ///
    /// 1. **Validate stream**: Checks the existence of `stream_name` in `Redis Stream`. This process is going to wait maximum for `max_wait_seconds_for_stream <seconds>` based on exponential time steps.
    /// 2. **Validate consumer group:** Checks if consumer group exists by `group_name`. If not, consumer group is created based on `stream_name`, `group_name` and `latest_id`.
    /// 3. **Claim pending messages:** Claims pending messages from `stream_name` and `group_name` from `latest_id` to newest one until to a maximum of `count` messages.
    /// 4. **Read messages:** First, it waits a specific amount of time (`block <milliseconds>`) for new stream messages. Then, pending messages and new messages are read from `latest_id` to newest one.
    pub async fn consume(&mut self) -> Result<Vec<StreamId>, RedisError> {
        self.validate_stream_existence().await?;
        self.checking_consumer_group()?;

        self.autoclaim()?;
        let ids: Vec<StreamId> = self.xread_group()?;

        Ok(ids)
    }

    /// To validate ownership of specific **pending stream message** by `id <message id>`.
    pub fn validate_pending_message_ownership(&self, id: &'c str) -> Result<bool, RedisError> {
        let pending_messages: StreamPendingCountReply = self
            .get_client()
            .get_connection()?
            .xpending_count(self.get_stream_name(), self.get_group_name(), id, id, 1)?;

        match pending_messages.ids.len() {
            0 => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Stream message is not pending to consume. Maybe the message was consumed by another consumer",
            ))),
            1 => Ok(pending_messages.ids[0].consumer.eq(self.get_consumer_name())),
            _ => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Fatal error from Redis. More than 1 pending messages were found by specific message id",
            ))),
        }
    }

    /// Ack **pending stream messages** by `id <message id>`.
    pub async fn acknowledge<RV: FromRedisValue, ID: ToRedisArgs>(
        &self,
        id: ID,
    ) -> Result<RV, RedisError> {
        let result: RV =
            self.get_client()
                .get_connection()?
                .xack(self.stream_name, self.group_name, &[id])?;

        Ok(result)
    }
}

#[async_trait]
impl<'p> StreamInfo for RedsumerConsumer<'p> {
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

#[async_trait]
impl<'c> StreamConsumersInfo for RedsumerConsumer<'c> {
    async fn get_consumers_info(&self) -> Result<StreamInfoConsumersReply, RedisError> {
        let consumers_info: StreamInfoConsumersReply = self
            .get_client()
            .get_connection()?
            .xinfo_consumers(self.get_stream_name(), self.get_group_name())?;

        Ok(consumers_info)
    }
}
