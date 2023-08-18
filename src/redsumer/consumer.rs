use log::{debug, warn};
use redis::{
    streams::{StreamClaimOptions, StreamPendingCountReply, StreamReadOptions, StreamReadReply},
    Commands, ErrorKind, RedisResult,
};
use tokio::time::{sleep, Duration};

use crate::redsumer::client::RedsumerClient;
use crate::{FromRedisValue, RedisError, StreamId, ToRedisArgs};

/// Builder options for [`RedsumerConsumer`]
pub struct RedsumerConsumerOptions<'c> {
    latest_id: &'c str,
    max_wait_seconds_for_stream: u8,
    min_idle_time_milliseconds: u16,
    count: u16,
    block: u16,
    read_only: bool,
}

impl<'c> Default for RedsumerConsumerOptions<'c> {
    /// Default builder options for [`RedsumerConsumerOptions`]
    fn default() -> Self {
        Self {
            latest_id: "0",
            max_wait_seconds_for_stream: 10u8,
            min_idle_time_milliseconds: 5000u16,
            count: 5000u16,
            block: 2000u16,
            read_only: false,
        }
    }
}

impl<'c> RedsumerConsumerOptions<'c> {
    /// Set `latest_id <message id>`
    pub fn set_latest_id(mut self, latest_id: &'c str) -> Self {
        self.latest_id = latest_id;
        self
    }

    /// Set `max_wait_seconds_for_stream <seconds>`
    pub fn set_max_wait_seconds_for_stream(mut self, max_wait_seconds_for_stream: u8) -> Self {
        self.max_wait_seconds_for_stream = max_wait_seconds_for_stream;
        self
    }

    /// Set `min_idle_time_milliseconds <milliseconds>`
    pub fn set_min_idle_time_milliseconds(mut self, min_idle_time_milliseconds: u16) -> Self {
        self.min_idle_time_milliseconds = min_idle_time_milliseconds;
        self
    }

    /// Set `count <units>`
    pub fn set_count(mut self, count: u16) -> Self {
        self.count = count;
        self
    }

    /// Set `block <milliseconds>`
    pub fn set_block(mut self, block: u16) -> Self {
        self.block = block;
        self
    }

    /// Set `read_only <bool>`
    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Get `latest_id <message id>`
    pub fn get_latest_id(&self) -> &'c str {
        self.latest_id
    }

    /// Get `max_wait_seconds_for_stream <seconds>`
    pub fn get_max_wait_seconds_for_stream(&self) -> u8 {
        self.max_wait_seconds_for_stream
    }

    /// Get `min_idle_time_milliseconds <milliseconds>`
    pub fn get_min_idle_time_milliseconds(&self) -> u16 {
        self.min_idle_time_milliseconds
    }

    /// Get `count <units>`
    pub fn get_count(&self) -> u16 {
        self.count
    }

    /// Get `block <units>`
    pub fn get_block(&self) -> u16 {
        self.block
    }

    /// Get `read_only <bool>`
    pub fn get_read_only(&self) -> bool {
        self.read_only
    }
}

/// Manager to consume `stream events` from `Redis Streams`
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

    /// Get `stream_name <string>`.
    pub fn get_stream_name(&self) -> &'c str {
        self.stream_name
    }

    /// Get `group_name <string>`.
    pub fn get_group_name(&self) -> &'c str {
        self.group_name
    }

    /// Get `consumer_name <string>`.
    pub fn get_consumer_name(&self) -> &'c str {
        self.consumer_name
    }

    /// Get `consumer_options` <[`RedsumerConsumerOptions`]>.
    pub fn get_consumer_options(&self) -> &RedsumerConsumerOptions<'c> {
        &self.options
    }

    /// Build a new [`RedsumerConsumer`] from specific `url`, `db`, `stream_name`, `group_name` and `consumer_name`. Set specific consumer options using [`RedsumerConsumerOptions`].
    pub fn new(
        url: &'c str,
        db: &'c str,
        stream_name: &'c str,
        group_name: &'c str,
        consumer_name: &'c str,
        options: RedsumerConsumerOptions<'c>,
    ) -> RedsumerConsumer<'c> {
        RedsumerConsumer {
            client: RedsumerClient::init(url, db),
            stream_name,
            group_name,
            consumer_name,
            options,
        }
    }

    async fn validate_stream_existence(&self) -> Result<(), RedisError> {
        debug!("Verifying stream {}", self.get_stream_name());

        let mut step: u32 = 0;
        let mut stream_ready: bool = false;
        while !stream_ready {
            stream_ready = self
                .get_client()
                .get_connection()
                .await?
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

    async fn checking_consumer_group(&self) -> Result<(), RedisError> {
        let consumer_group: RedisResult<bool> =
            self.get_client().get_connection().await?.xgroup_create(
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

    async fn autoclaim(&self) -> Result<(), RedisError> {
        let mut ids_to_claim: Vec<String> = Vec::new();

        let pending_messages: StreamPendingCountReply =
            self.get_client().get_connection().await?.xpending_count(
                self.get_stream_name(),
                self.get_group_name(),
                self.get_consumer_options().get_latest_id(),
                "+",
                self.get_consumer_options().get_count(),
            )?;

        for ids in pending_messages.ids.iter() {
            ids_to_claim.push(ids.id.clone());
        }

        if ids_to_claim.len() > 0 {
            self.get_client().get_connection().await?.xclaim_options(
                self.get_stream_name(),
                self.get_group_name(),
                self.get_consumer_name(),
                self.get_consumer_options().get_min_idle_time_milliseconds(),
                &ids_to_claim,
                StreamClaimOptions::default(),
            )?;

            debug!(
                "Total {} pending messages successfully claimed",
                ids_to_claim.len()
            );
        }

        Ok(())
    }

    async fn xread_group(&self) -> Result<Vec<StreamId>, RedisError> {
        let mut ids: Vec<StreamId> = Vec::new();

        let stream_read_reply: StreamReadReply =
            self.get_client().get_connection().await?.xread_options(
                &[self.get_stream_name(), self.get_stream_name()],
                &[self.get_consumer_options().get_latest_id(), ">"],
                &StreamReadOptions::default()
                    .group(self.get_group_name(), self.get_consumer_name())
                    .block(self.get_consumer_options().get_block().into()),
            )?;
        for key in stream_read_reply.keys.iter() {
            let key_ids = key.ids.clone();
            ids.extend(key_ids);
        }

        debug!("Total {} messages readed from stream", ids.len());

        Ok(ids)
    }

    /// **Consumes and returns** stream messages from a **Redis Stream** based on own configuration values.
    ///
    /// The process of **consume** the **stream messages** is carried out according to the following steps:
    ///
    /// 1. **Validate stream**: Checks the existence of `stream_name` in `Redis Stream`. This process is going to wait maximum for `max_wait_seconds_for_stream <seconds>` based on exponential time steps.
    /// 2. **Validate consumer group:** Checks if consumer group exists by `group_name`. If not, consumer group is created based on `stream_name`, `group_name` and `latest_id`.
    /// 3. **Claim pending messages:** Claims pending messages from `stream_name` and `group_name` from `latest_id` to newest one until to a maximum of `count` messages.
    /// 4. **Read messages:** First, it waits a specific amount of time (`block <milliseconds>`) for new stream messages. Then, pending messages and new messages are readed from `latest_id` to newest one.
    pub async fn consume(&mut self) -> Result<Vec<StreamId>, RedisError> {
        self.validate_stream_existence().await?;
        self.checking_consumer_group().await?;

        self.autoclaim().await?;
        let ids: Vec<StreamId> = self.xread_group().await?;

        Ok(ids)
    }

    /// Ack **pending stream messages** by `id <message id>`
    pub async fn acknowledge<RV: FromRedisValue, ID: ToRedisArgs>(
        &self,
        id: ID,
    ) -> Result<RV, RedisError> {
        if self.get_consumer_options().get_read_only() {
            return Err(RedisError::from((
                ErrorKind::ReadOnly,
                "Read only consumer: true",
            )));
        }

        let result: RV = self.get_client().get_connection().await?.xack(
            self.stream_name.clone(),
            self.group_name.clone(),
            &[id],
        )?;

        Ok(result)
    }
}
