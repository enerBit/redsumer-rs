#[cfg(test)]
pub mod test_redsumer_consumer {
    use crate::{
        redsumer::consumer::{RedsumerConsumer, RedsumerConsumerOptions},
        RedisError, StreamId,
    };

    #[tokio::test]
    async fn test_invalid_url() {
        let mut consumer = RedsumerConsumer::new(
            "localhost:5432",
            "0",
            "test-stream-name",
            "test-group-name",
            "test-consumer-name",
            RedsumerConsumerOptions::default(),
        );

        let pending_messages: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(pending_messages.is_err())
    }

    #[tokio::test]
    async fn test_invalid_db() {
        let mut consumer = RedsumerConsumer::new(
            "localhost:6379",
            "lorem-ipsum",
            "test-stream-name",
            "test-group-name",
            "test-consumer-name",
            RedsumerConsumerOptions::default(),
        );

        let pending_messages: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(pending_messages.is_err())
    }
}
