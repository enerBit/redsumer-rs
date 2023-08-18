#[cfg(test)]
pub mod test_streams {
    use crate::{
        redsumer::consumer::{RedsumerConsumer, RedsumerConsumerOptions},
        redsumer::producer::RedsumerProducer,
        RedisError, StreamId,
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn test_akc_messages() {
        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new("localhost:6379", "0", "test-ack");

        for _ in 1..1000 {
            let producer_result: Result<String, RedisError> = producer
                .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
                .await;

            assert!(producer_result.is_ok());
        }

        let mut consumer: RedsumerConsumer = RedsumerConsumer::new(
            "localhost:6379",
            "0",
            "test-ack",
            "test-group",
            "test-consumer",
            RedsumerConsumerOptions::default(),
        );

        let consumer_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;

        assert!(consumer_result.is_ok());

        let pending_messages: Vec<StreamId> = consumer_result.unwrap();
        for message in pending_messages.iter() {
            let id: &String = &message.id;
            let akc_result: Result<bool, RedisError> = consumer.acknowledge(id).await;

            assert!(akc_result.is_ok());
            assert!(akc_result.unwrap());
        }
    }
}
