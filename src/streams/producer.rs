
use std::collections::BTreeMap;
use redis::{Commands, RedisResult};
use log;

use super::client::RedisClient;

pub type RedisProducerResult<T, String> = Result<T, String>;

pub struct RedisStreamsProducer {
    client: RedisClient,
    stream_name: String,
}

impl RedisStreamsProducer {
    /// # Create New Redis Streams Producer
    ///
    /// Makes a new `RedisStreamsProducer`.
    /// 
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///     url: String
    ///     db: Option<String> // Default value: "0"
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
    ///             stream_message: &BTreeMap<String, String>,
    ///         ) -> RedisProducerResult<String, String>
    ///     }
    /// ```
    /// Where:
    /// ```text
    ///     RedisProducerResult<String, String> = Result<String, String>
    ///         Ok(v: String)  // message id (e.g: "1684370369130-0")
    ///         Err(e: String) // error message
    /// ```
    /// 
    /// ## Basic Usages:
    /// 1. Create new RedisStreamsProducer for an specific *url*, *db* and *stream name*:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = Some("1".to_string());
    ///     let stream_name: String = "redis-streams-lite".to_string();
    ///
    ///     let producer: RedisStreamsProducer = RedisStreamsProducer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///     );
    /// ```
    /// 2. Create new RedisStreamsProducer for an specific *url*, default *db* and *stream name*:
    /// ```text
    ///     let url: String = "localhost:6379".to_string();
    ///     let db: Option<String> = None;
    ///     let stream_name: String = "redis-streams-lite".to_string();
    ///
    ///     let producer: RedisStreamsProducer = RedisStreamsProducer::new(
    ///         url,
    ///         db,
    ///         stream_name,
    ///     );
    /// ```
    pub fn new(url: String, db: Option<String>, stream_name: String) -> RedisStreamsProducer {
        let redis_db: String = db.unwrap_or(String::from("0"));
        let client: RedisClient = RedisClient::init(
            url.clone(), redis_db.clone());
        log::info!("Creating redis-streams-lite producer from url={}, db={}, stream_name={}", url, redis_db, stream_name);

        return RedisStreamsProducer{
            client,
            stream_name,
        };
    }

    /// # Produce New Redis Stream Message
    /// ---
    /// 
    /// ## Arguments:
    /// ```text
    ///   stream_message: &BTreeMap<String, String>
    /// ```
    /// ## Returns:
    /// ```text
    ///   RedisProducerResult<String, String> = Result<String, String>
    ///       Ok(v: String)  // message id (e.g: "1684370369130-0")
    ///       Err(e: String) // error message
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
    ///   let stream_name: String = "redis-streams-lite".to_string();
    ///
    ///   let producer: RedisStreamsProducer = RedisStreamsProducer::new(
    ///       url,
    ///       db,
    ///       stream_name,
    ///   );
    /// 
    ///   let producer_result: RedisProducerResult<String, String> = producer.produce(&message).await;
    /// ```
    pub async fn produce(
        &self,
        stream_message: &BTreeMap<String, String>,
    ) -> RedisProducerResult<String, String> {
        log::debug!("Producing new stream message {:?}", stream_message);
        let result: RedisResult<String> = self.client.get_conn()
            .xadd_map(self.stream_name.as_str(), "*", stream_message);
        if result.is_err() {
            log::debug!("Error producing new stream message {:?}", result);
            return Err(result.unwrap_err().to_string())
        }
        log::debug!("New stream message id {:?}", result);
        
        return Ok(result.unwrap().to_string());
    }

}