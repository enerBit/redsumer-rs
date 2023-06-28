use redis::{Connection, RedisResult, Commands, RedisError};
use redis::streams::{
    StreamClaimOptions,
    StreamClaimReply,
    StreamPendingCountReply,
    StreamReadOptions,
    StreamReadReply,
    StreamId,
};
use log::{debug, warn, error};
use tokio::time::{Duration, sleep};

use crate::streams::client::RedisClient;

pub type StreamMessage = StreamId;
pub type RedisConsumerResult<T> = RedisResult<T>;

#[derive(Clone)]
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
        RedisStreamsConsumer {
            client: RedisClient::init(url, db.unwrap_or("0".to_string())),
            stream_name,
            group_name,
            consumer_name,
            latest_id: latest_id.unwrap_or("0-0".to_string()),
            max_wait_seconds_for_stream: max_wait_seconds_for_stream.unwrap_or(20u8),
            min_idle_time_milliseconds: min_idle_time_milliseconds.unwrap_or(5000u16),
            count: count.unwrap_or(5000u16),
            block: block.unwrap_or(2000u16),
            stream_ready: false,
        }
    }

    async fn wait_for_stream(
        &self,
    ) {
        let mut is_ready: bool = false;
        let time_delta: u8 = self.max_wait_seconds_for_stream/5;
        let mut step: u8 = 0;

        let mut conn: Connection = self.client.get_conn().await;

        debug!("Checking stream {}", self.stream_name);
        while !is_ready {
            let stream_ready: RedisResult<bool> = conn.exists(self.stream_name.clone());
            match stream_ready {
                Ok(v) => {
                    if v {
                        debug!("Stream {} is ready", self.stream_name);
                        is_ready = true;

                    } else {
                        warn!("Stream {} is not ready yet", self.stream_name);

                        step += 1;
                        let wait_time:u8 = time_delta * step;

                        if wait_time > self.max_wait_seconds_for_stream {
                            panic!("Stream {} was not found", self.stream_name);
                        };

                        warn!("Waiting for stream {} for {} seconds", self.stream_name, wait_time);
                        sleep(Duration::from_secs(wait_time as u64)).await;
                    }
                    
                },
                Err(e) => {
                    error!("Fatal error: {}", e);
                    panic!("{}", e);
                }
            }
        }
    }

    async fn wait_for_consumer_group(
        &self,
    ) {
        debug!("Checking consumer group {}", self.group_name);

        let consumer_group: RedisResult<bool> = self.client.get_conn().await
            .xgroup_create(
                self.stream_name.clone(),
                self.group_name.clone(),
                self.latest_id.clone(),
            );
        match consumer_group {
            Ok(_) => {
                debug!("Consumer group {} created successfully", self.group_name);
            },
            Err(_) => debug!("Consumer group {} already exists", self.group_name),
        };
    }

    async fn autoclaim(
        &self,
    ) -> Option<RedisError> {
        let mut ids_to_claim: Vec<String> = Vec::new();

        let pending_messages: StreamPendingCountReply = match self.client.get_conn().await
        .xpending_count(
            self.stream_name.clone(),
            self.group_name.clone(),
            "-",
            "+",
            self.count.clone(),
        ) {
            Ok(count_reply) => count_reply,
            Err(error) => return Some(error),
        };
        for ids in pending_messages.ids.iter() {
            ids_to_claim.push(ids.id.clone());
        }

        debug!("Total {} pending messages to be claimed", {ids_to_claim.len().to_string()});
        if ids_to_claim.len() > 0 {
            let _: StreamClaimReply = match self.client.get_conn().await
                .xclaim_options(
                    self.stream_name.clone(),
                    self.group_name.clone(),
                    self.consumer_name.clone(),
                    self.min_idle_time_milliseconds.clone(),
                    &ids_to_claim,
                    StreamClaimOptions::default(),
                ) {
                    Ok(result) => result,
                    Err(error) => {
                        error!("Error claiming pending stream messages from stream {} for consumer {} in consumer group {}",
                            self.stream_name.clone(),
                            self.consumer_name.clone(),
                            self.group_name.clone(),
                        );
                        return Some(error);
                    },
                };
        };

        None
    }

    async fn xread_group(
        &self,
    ) -> Result<Vec<StreamId>, RedisError> {
        let mut ids: Vec<StreamId> = Vec::new();

        let stream_read_reply: StreamReadReply = self.client.get_conn().await
            .xread_options(
                &[self.stream_name.clone(), self.stream_name.clone()],
                &[self.latest_id.clone(), ">".to_string()],
                &StreamReadOptions::default()
            .group(self.group_name.clone(), self.consumer_name.clone())
            .block(self.block.clone().into()),
            )?;
        for key in stream_read_reply.keys.iter() {
            let key_ids = key.ids.clone();
            ids.extend(key_ids);
        };

        debug!("Total {} messages readed", ids.len());
        Ok(ids)
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
    ///     RedisConsumerResult<Vec<StreamMessage>> = RedisResult<Vec<StreamId>>
    ///         Ok(v: Vec<StreamId>) // See StreamId documentation in redis-rs.
    ///         Err(e: RedisError) // Redis error
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
    ///     let pending_messages: RedisConsumerResult<Vec<StreamId>> = consumer.consume().await;
    /// ```
    pub async fn consume(
        &mut self,
    ) -> RedisConsumerResult<Vec<StreamMessage>> {
        if !self.stream_ready {
            self.wait_for_stream().await;
            self.wait_for_consumer_group().await;
        };
        self.stream_ready = true;
        debug!("Stream {} ready: {}", self.stream_name, self.stream_ready);

        match self.autoclaim().await {
            Some(error) => {
                error!("Error autoclaiming pending messages");
                return Err(error);
            },
            None => debug!("messages autoclaimed successfully"),
        };

       let messages: Vec<StreamId> = self.xread_group().await?;

        return Ok(messages);
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
    ///     RedisConsumerResult<RV> = RedisResult<RV>
    ///         Ok(v: T)  // message id (e.g: 1u8 or 0u8)
    ///         Err(e: RedisError) // Redis
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
    ///     let flag RedisConsumerResult<u8> = consumer.acknowledge("1684081214635-0".to_string()).await;
    /// ```
    pub async fn acknowledge<
        RV: redis::FromRedisValue + std::fmt::Debug,
    > (
        &self,
        id: String,
    ) -> RedisConsumerResult<RV> {
        let result: RV = match self.client.get_conn().await
            .xack(
                self.stream_name.clone(),
                self.group_name.clone(),
                &[id.as_str()],
            ) {
                Ok(result) => result,
                Err(error) => return Err(error),
            };
        
        Ok(result)
    }
}