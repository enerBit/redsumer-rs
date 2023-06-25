#[cfg(test)]
mod tests {
    use tokio::test;
    use std::{collections::BTreeMap};
    use chrono;
    use uuid::Uuid;

    use crate::streams::producer::{RedisStreamsProducer, RedisProducerResult};

    #[test]
    async fn test_produce_message() {        
        let uuid4: String = Uuid::new_v4().to_string();
        let now: String = chrono::Utc::now().to_string();

        let mut message: BTreeMap<String, String> = BTreeMap::new();
        message.insert("id".to_string(), uuid4);
        message.insert("datetime".to_string(), now);

        let url = String::from("localhost:6379");
        let db = None;
        let stream_name = String::from("test-redsumer-rs");

        let producer = RedisStreamsProducer::new(
            url,
            db,
            stream_name,
        );

        let first_message: RedisProducerResult<String> = producer.produce(&message).await;
        assert!(first_message.is_ok());
    }

}
