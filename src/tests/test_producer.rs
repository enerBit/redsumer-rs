#[cfg(test)]
mod tests {
    use tokio::test;
    use std::collections::BTreeMap;
    use chrono;
    use uuid::Uuid;

    use super::super::super::*;
    use streams::producer::{RedisStreamsProducer, RedisProducerResult};

    #[test]
    async fn test_produce_message() {
        let uuid4: String = Uuid::new_v4().to_string();
        let now: String = chrono::Utc::now().to_string();

        let mut message: BTreeMap<String, String> = BTreeMap::new();
        message.insert("id".to_string(), uuid4);
        message.insert("datetime".to_string(), now);

        let url: String = String::from("localhost:6379");
        let db: Option<String> = None;
        let stream_name: String = String::from("test-redsumer-rs");

        let producer: RedisStreamsProducer = RedisStreamsProducer::new(
            url,
            db,
            stream_name,
        );

        let producer_result: RedisProducerResult<String, String> = producer.produce(&message).await;
        assert!(producer_result.is_ok());

    }

}
