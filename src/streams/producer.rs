
use redis::{RedisResult, Commands};
use log::{debug, error, info};

use super::client::RedisClient;

pub type RedisProducerResult<T> = RedisResult<T>;

pub struct RedisStreamsProducer {
    client: RedisClient,
    stream_name: String,
}

impl RedisStreamsProducer {
    /// # Create New Redis Streams Producer
    ///
    /// Build a new `RedisStreamsProducer`.
    /// 
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///     url: String
    ///     db: Option<String> default: "0"
    ///     stream_name: String
    /// ```
    /// ## Returns:
    /// ```text
    ///     struct RedisStreamsProducer {
    ///         client: RedisClient,
    ///         stream_name: String,
    ///     }
    ///     impl RedisStreamsProducer {
    ///         async fn produce(
    ///             stream_message: BTM,
    ///         ) -> RedisProducerResult<RV>
    ///     }
    /// ```
    /// 
    /// ## Basic Usages:
    /// Create new RedisStreamsProducer for an specific *url*, *db* and *stream name*:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = Some("1".to_string());
    ///     let stream_name: String = "redsumer-rs".to_string();
    ///
    ///     let producer: RedisStreamsProducer = RedisStreamsProducer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///     );
    /// ```
    pub fn new(
        url: String,
        db: Option<String>,
        stream_name: String,
    ) -> RedisStreamsProducer {
        let redis_db: String = db.unwrap_or(String::from("0"));
        info!(
            "Creating redis-streams-lite producer from url={}, db={}, stream_name={}", url, redis_db, stream_name,
        );
        
        RedisStreamsProducer{
            client: RedisClient::init(
                url,
                redis_db,
            ),
            stream_name,
        }
    }

    /// # Produce New Redis Stream Message
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///   stream_message: BTM impl redis::ToRedisArgs
    /// ```
    /// ## Returns:
    /// ```text
    ///   RedisProducerResult<String> = Result<String, RedisError>
    ///       Ok(v: String)  // message id (e.g: "1684370369130-0")
    ///       Err(e: RedisErrorg) // Redis error response
    /// ```
    /// 
    /// ## Basic Usages:
    /// 1. Produce new message in *stream*:
    /// ```text
    /// let mut message: BTreeMap<String, String> = BTreeMap::new();
    ///   message.insert("hello".to_string(), "world".to_String());
    /// 
    ///   let url: String = "localhost:6379".to_string();
    ///   let db: Option<String> = Some("1".to_string());
    ///   let stream_name: String = "redsumer-rs".to_string();
    ///
    ///   let producer: RedisStreamsProducer = RedisStreamsProducer::new(
    ///       url,
    ///       db,
    ///       stream_name,
    ///   );
    /// 
    ///   let producer_result: RedisProducerResult<String> = producer.produce(&message).await;
    /// ```
    pub async fn produce<
        RV: redis::FromRedisValue,
        BTM: redis::ToRedisArgs,
    >(
        &self,
        stream_message: BTM,
    ) -> RedisProducerResult<RV> {
        debug!("Producing new stream message");

        let redis_result: RedisResult<RV> = self.client.get_conn().await
            .xadd_map(self.stream_name.clone(), "*", stream_message);
        match redis_result {
            Ok(result) => {
                debug!("Stream message produced successfully");
                Ok(result)
            },
            Err(error) => {
                error!("Stream message production error: {}", error.to_string());
                Err(error)
            },
        }
    }
}