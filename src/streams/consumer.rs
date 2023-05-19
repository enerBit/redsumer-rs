use redis::{Connection, RedisResult, Commands};
use redis::streams::{StreamClaimOptions, StreamClaimReply, StreamPendingCountReply, StreamPendingId, StreamReadOptions, StreamReadReply, StreamKey, StreamId};
use log;
use tokio::time::{Duration, sleep};

use super::client::RedisClient;

pub type StreamMessage = StreamId;
pub type RedisConsumerResult<T, String> = Result<T, String>;

pub struct RedisStreamsConsumer {
    client: RedisClient,
    stream_name: String,
    group_name: String,
    consumer_name: String,
    latest_id: String,
    max_wait_seconds_for_stream: u8,
    min_idle_time_milliseconds: u16,
    count: u16,
    block: u16,
    stream_ready: bool,
}

impl RedisStreamsConsumer {
    /// # Create New Redis Streams Consumer
    ///
    /// Makes a new `RedisStreamsConsumer`.
    /// 
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///    url: String
    ///    db: Option<String> // Default value: "0" 
    ///    stream_name: String
    ///    group_name: String
    ///    consumer_name: String
    ///    latest_id: Option<String> // Default value: "0-0"
    ///    min_idle_time_milliseconds: Option<u16> // Default value: 5000u16
    ///    count: Option<u16> // Default value: 5000u16
    ///    block: Option<u16> // Default value: 2000u16
    ///    max_wait_seconds_for_stream: Option<u8> // Default value: 20u8
    /// ```
    /// ## Returns:
    /// ```text
    ///     struct RedisStreamsConsumer {
    ///         client: RedisClient,
    ///         stream_name: String,
    ///         group_name: String,
    ///         consumer_name: String,
    ///         latest_id: String,
    ///         max_wait_seconds_for_stream: u8,
    ///         min_idle_time_milliseconds: u16,
    ///         count: u16,
    ///         block: u16,
    ///         stream_ready: bool,
    ///     }
    ///     impl RedisStreamsConsumer {
    ///         async fn consume(
    ///         ) -> RedisConsumerResult<Vec<StreamId>, String>
    /// 
    ///         async fn acknowledge(
    ///             &self,
    ///             id: String,
    ///         ) -> RedisConsumerResult<u8, String>
    ///     }
    /// 
    /// ```
    /// Where:
    /// ```text
    ///     RedisConsumerResult<Vec<StreamMessage>, String> = Result<Vec<StreamId>, String>
    ///         Ok(v: Vec<StreamId>) // See StreamId documentation in redis-rs.
    ///         Err(e: String) // error message
    /// 
    ///     RedisConsumerResult<u8, String> = Result<u8, String>
    ///         Ok(v: u8)  // message id (e.g: 1u8 or 0u8)
    ///         Err(e: String) // error message
    /// ```
    /// 
    /// ## Basic Usages:
    /// 1. Create new RedisStreamsConsumer for an specific *url*, *db*, *stream name*, *group name*, *consumer name*, *latest id*, *min idle time milliseconds*, *count*, *block* and *max wait seconds for stream*:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = Some("1".to_string());
    ///     let stream_name: String = "redis-streams-lite".to_string();
    ///     let group_name: String = "test".to_string();
    ///     let consumer_name: String = "test-consumer".to_string();
    ///     let latest_id: Option<String> = Some("1684374081822-0".to_string());
    ///     let min_idle_time_milliseconds: Option<u16> = Some(2000u16);
    ///     let count: Option<u16> = Some(10000u16);
    ///     let block: Option<u16> = Some(1000u16);
    ///     let max_wait_seconds_for_stream: Option<u8> = Some(10u8);
    /// 
    ///     let mut consumer: RedisStreamsConsumer = RedisStreamsConsumer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///         group_name,
    ///         consumer_name,
    ///         latest_id,
    ///         min_idle_time_milliseconds,
    ///         count,
    ///         block,
    ///         max_wait_seconds_for_stream,
    ///     );
    /// ```
    /// 2. Create new RedisStreamsConsumer for an specific *url*, *stream name*, *group name*, *consumer name*:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = None;
    ///     let stream_name: String = "redis-streams-lite".to_string();
    ///     let group_name: String = "test".to_string();
    ///     let consumer_name: String = "test-consumer".to_string();
    ///     let latest_id: Option<String> = None;
    ///     let min_idle_time_milliseconds: Option<u16> = None;
    ///     let count: Option<u16> = None;
    ///     let block: Option<u16> = None;
    ///     let max_wait_seconds_for_stream: Option<u8> = None;
    /// 
    ///     let mut consumer: RedisStreamsConsumer = RedisStreamsConsumer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///         group_name,
    ///         consumer_name,
    ///         latest_id,
    ///         min_idle_time_milliseconds,
    ///         count,
    ///         block,
    ///         max_wait_seconds_for_stream,
    ///     );
    pub fn new(
        url: String,
        db: Option<String>,
        stream_name: String,
        group_name: String,
        consumer_name: String,
        latest_id: Option<String>,
        min_idle_time_milliseconds: Option<u16>,
        count: Option<u16>,
        block: Option<u16>,
        max_wait_seconds_for_stream: Option<u8>,
    ) -> RedisStreamsConsumer {
        let db: String = db.unwrap_or("0".to_string());
        let client: RedisClient = RedisClient::init(url, db);
        let latest_id: String = latest_id.unwrap_or("0-0".to_string());
        let max_wait_seconds_for_stream: u8 = max_wait_seconds_for_stream.unwrap_or(20u8);
        let min_idle_time_milliseconds: u16 = min_idle_time_milliseconds.unwrap_or(5000u16);
        let count: u16 = count.unwrap_or(5000u16);
        let block: u16 = block.unwrap_or(2000u16);

        return RedisStreamsConsumer {
            client,
            stream_name,
            group_name,
            consumer_name,
            latest_id,
            max_wait_seconds_for_stream,
            min_idle_time_milliseconds,
            count,
            block,
            stream_ready: false,
        }
    }

    async fn wait_for_stream(
        &self,
    ) {
        const TIME_DELTA: u8 = 5;
        let mut step: u8 = 0;
        let mut wait_time: u8;

        let mut conn: Connection = self.client.get_conn();
        let mut is_ready: bool = false;

        log::debug!("Checking stream {}", self.stream_name);
        while !is_ready {
            let stream_ready: RedisResult<bool> = conn.exists(self.stream_name.clone());
            match stream_ready {
                Ok(v) => {
                    if v {
                        log::debug!("Stream {} is ready", self.stream_name);
                        is_ready = true;
                    } else {
                        log::warn!("Stream {} is not ready yet", self.stream_name);
                        step += 1;
                        wait_time = TIME_DELTA * step;
                        if wait_time > self.max_wait_seconds_for_stream {
                            panic!("Stream {} was not found", self.stream_name);
                        };

                        log::warn!("Waiting for stream {} for {} seconds", self.stream_name, wait_time);
                        sleep(Duration::from_secs(wait_time as u64)).await;
                    }
                    
                },
                Err(e) => {
                    log::error!("Fatal error: {}", e);
                    panic!("{}", e);
                }
            }
        }
    }

    async fn wait_for_consumer_group(
        &self,
    ) {
        log::debug!("Checking consumer group {}", self.group_name);
        let consumer_group: RedisResult<bool> = self.client.get_conn()
            .xgroup_create(
                self.stream_name.clone(),
                self.group_name.clone(),
                self.latest_id.clone(),
            );
        match consumer_group {
            Ok(_) => {
                log::debug!("Consumer group {} created successfully", self.group_name);
            },
            Err(_) => log::debug!("Consumer group {} already exists", self.group_name),
        };
    }

    async fn autoclaim(
        &self,
    ) -> Result<bool, String> {
        let mut ids_to_claim: Vec<String> = Vec::new();
        let pending_messages: RedisResult<StreamPendingCountReply> = self.client.get_conn()
            .xpending_count(
                self.stream_name.clone(),
                self.group_name.clone(),
                "-",
                "+",
                self.count.clone(),
            );
        if pending_messages.is_ok() {
            let pending_ids: Vec<StreamPendingId> = pending_messages.unwrap().ids;
            for ids in pending_ids.iter() {
                ids_to_claim.push(ids.id.clone());
            };
        } else {
            log::error!("Error reading pending stream messages from stream {} for consumer {} in consumer group {}",
                self.stream_name.clone(),
                self.consumer_name.clone(),
                self.group_name.clone(),
            );
            return Err(pending_messages.unwrap_err().to_string());
        };

        log::debug!("Total {} pending messages to be claimed", {ids_to_claim.len().to_string()});
        if ids_to_claim.len() > 0 {
            let xclaim_opts: StreamClaimOptions = StreamClaimOptions::default();
            let claimed_messages: RedisResult<StreamClaimReply> = self.client.get_conn()
                .xclaim_options(
                    self.stream_name.clone(),
                    self.group_name.clone(),
                    self.consumer_name.clone(),
                    self.min_idle_time_milliseconds.clone(),
                    &ids_to_claim,
                    xclaim_opts,
                );
            if claimed_messages.is_err() {
                log::error!("Error claiming pending stream messages from stream {} for consumer {} in consumer group {}",
                    self.stream_name.clone(),
                    self.consumer_name.clone(),
                    self.group_name.clone(),
                );
                return Err(claimed_messages.unwrap_err().to_string())
        };
        }

        return Ok(true);

    }

    async fn xread_group(
        &self,
    ) -> Result<Vec<StreamId>, String> {
        let mut messages: Vec<StreamId> = Vec::new();

        let xread_opts: &StreamReadOptions = &StreamReadOptions::default()
        .group(self.group_name.clone(), self.consumer_name.clone())
        .block(self.block.clone().into());

        let pending_messages: RedisResult<StreamReadReply> = self.client.get_conn()
            .xread_options(
                &[self.stream_name.clone()],
                &[self.latest_id.clone()],
                xread_opts,
            );
        if pending_messages.is_ok() {
            let messages_keys: Vec<StreamKey> = pending_messages.unwrap().keys;
            for stream_key in messages_keys.iter() {
                if stream_key.key == self.stream_name.clone() {
                    for id in stream_key.ids.iter() {
                        messages.push(id.clone());
                    }
                };
            };
        } else {
            log::error!("Error reading pending messages");
            return Err(pending_messages.unwrap_err().to_string());
        };

        let new_messages: RedisResult<StreamReadReply> = self.client.get_conn()
            .xread_options(
                &[self.stream_name.clone()],
                &[">"],
                xread_opts,
            );
        if new_messages.is_ok() {
            let messages_keys: Vec<StreamKey> = new_messages.unwrap().keys;
            for stream_key in messages_keys.iter() {
                if stream_key.key == self.stream_name.clone() {
                    for id in stream_key.ids.iter() {
                        messages.push(id.clone());
                    }
                };
            };
        } else {
            log::error!("Error reading new stream messages");
            return Err(new_messages.unwrap_err().to_string());
        };

        log::debug!("Total {} messages readed", messages.len());
        return Ok(messages);

    }

    /// # Consume Pending Messages From Redis Stream
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///    *No Arguments*
    /// ```
    /// ## Returns:
    /// ```text
    ///     RedisConsumerResult<Vec<StreamMessage>, String> = Result<Vec<StreamId>, String>
    ///         Ok(v: Vec<StreamId>) // See StreamId documentation in redis-rs.
    ///         Err(e: String) // error message
    /// ```
    /// 
    /// ## Basic Usages:
    /// 1. Consume pending messages from *stream*:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = Some("1".to_string());
    ///     let stream_name: String = "redis-streams-lite".to_string();
    ///     let group_name: String = "test".to_string();
    ///     let consumer_name: String = "test-consumer".to_string();
    ///     let latest_id: Option<String> = Some("1684374081822-0".to_string());
    ///     let min_idle_time_milliseconds: Option<u16> = Some(2000u16);
    ///     let count: Option<u16> = Some(10000u16);
    ///     let block: Option<u16> = Some(1000u16);
    ///     let max_wait_seconds_for_stream: Option<u8> = Some(10u8);
    /// 
    ///     let mut consumer: RedisStreamsConsumer = RedisStreamsConsumer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///         group_name,
    ///         consumer_name,
    ///         latest_id,
    ///         min_idle_time_milliseconds,
    ///         count,
    ///         block,
    ///         max_wait_seconds_for_stream,
    ///     );
    /// 
    ///     let pending_messages: RedisConsumerResult<Vec<StreamId>, String> = consumer.consume().await;
    /// ```
    pub async fn consume(
        &mut self,
    ) -> RedisConsumerResult<Vec<StreamMessage>, String> {
        if !self.stream_ready {
            self.wait_for_stream().await;
            self.wait_for_consumer_group().await;
        };
        self.stream_ready = true;
        log::debug!("Stream {} ready: {}", self.stream_name, self.stream_ready);

        let err: Result<bool, String> = self.autoclaim().await;
        if err.is_err() {
            log::error!("Error autoclaiming pending messages");
            return Err(err.unwrap_err().clone());
        };

       let messages: Result<Vec<StreamId>, String> = self.xread_group().await;
        if messages.is_err() {
            log::error!("Error reading stream messages");
            return Err(messages.unwrap_err().clone());
        }

        return Ok(messages.unwrap());
    }

    /// # Acknowledge Stream Message By Id
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///    id: String
    /// ```
    /// ## Returns:
    /// ```text
    ///     RedisConsumerResult<u8, String> = Result<u8, String>
    ///         Ok(v: u8)  // message id (e.g: 1u8 or 0u8)
    ///         Err(e: String) // error message
    /// ```
    /// 
    /// ## Basic Usages:
    /// 1. Acknowledge message by id:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = Some("1".to_string());
    ///     let stream_name: String = "redis-streams-lite".to_string();
    ///     let group_name: String = "test".to_string();
    ///     let consumer_name: String = "test-consumer".to_string();
    ///     let latest_id: Option<String> = Some("1684374081822-0".to_string());
    ///     let min_idle_time_milliseconds: Option<u16> = Some(2000u16);
    ///     let count: Option<u16> = Some(10000u16);
    ///     let block: Option<u16> = Some(1000u16);
    ///     let max_wait_seconds_for_stream: Option<u8> = Some(10u8);
    /// 
    ///     let mut consumer: RedisStreamsConsumer = RedisStreamsConsumer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///         group_name,
    ///         consumer_name,
    ///         latest_id,
    ///         min_idle_time_milliseconds,
    ///         count,
    ///         block,
    ///         max_wait_seconds_for_stream,
    ///     );
    /// 
    ///     let flag RedisConsumerResult<u8, String> = consumer.acknowledge("1684081214635-0".to_string()).await;
    /// ```
    pub async fn acknowledge (
        &self,
        id: String,
    ) -> RedisConsumerResult<u8, String> {
        let acknowledge_id: RedisResult<u8> = self.client.get_conn()
            .xack(
                self.stream_name.clone(),
                self.group_name.clone(),
                &[id.as_str()],
            );
        if acknowledge_id.is_err() {
            return Err(acknowledge_id.unwrap_err().to_string());
        }

        return Ok(acknowledge_id.unwrap());
    }
}