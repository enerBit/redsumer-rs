#[cfg(test)]
pub mod test_redsumer {
    use redsumer::redis::*;
    use redsumer::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_client_with_server_credentials() {
        let producer_res: RedsumerResult<RedsumerProducer> = RedsumerProducer::new(
            Some(ClientCredentials::new("user", "password")),
            "localhost",
            "6379",
            "0",
            "test",
        );

        assert!(producer_res.is_err());

        let error: RedsumerError = producer_res.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error getting connection to Redis server- TryAgain"
        );
    }

    #[tokio::test]
    async fn test_producer_debug_and_clone() {
        let producer_res: RedsumerResult<RedsumerProducer> = RedsumerProducer::new(
            None,
            "localhost",
            "6379",
            "0",
            "test-producer-debug-and-clone",
        );

        assert!(producer_res.is_ok());
        let producer: RedsumerProducer = producer_res.unwrap();

        assert_eq!(format!("{:?}", producer), "RedsumerProducer { client: Client { connection_info: ConnectionInfo { addr: Tcp(\"localhost\", 6379), redis: RedisConnectionInfo { db: 0, username: None, password: None } } }, stream_name: \"test-producer-debug-and-clone\" }");
        assert_eq!(
            producer.clone().get_stream_name(),
            "test-producer-debug-and-clone"
        );
    }

    #[tokio::test]
    async fn test_consumer_debug_and_clone() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-consumer-debug-and-clone";

        let producer_result: RedsumerResult<RedsumerProducer> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        assert!(producer_result.is_ok());
        let producer: RedsumerProducer = producer_result.unwrap();

        let message: HashMap<String, String> = [("key".to_string(), "value".to_string())]
            .iter()
            .cloned()
            .collect();

        let msg_result: RedsumerResult<Id> = producer.produce(message).await;
        assert!(msg_result.is_ok());

        let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            "group-name",
            "consumer",
            "0-0",
            1000,
            3,
            2,
            1,
            5,
        );

        assert!(consumer_result.is_ok());
        let consumer: RedsumerConsumer = consumer_result.unwrap();

        assert_eq!(format!("{:?}", consumer), "RedsumerConsumer { client: Client { connection_info: ConnectionInfo { addr: Tcp(\"localhost\", 6379), redis: RedisConnectionInfo { db: 0, username: None, password: None } } }, stream_name: \"test-consumer-debug-and-clone\", group_name: \"group-name\", consumer_name: \"consumer\", since_id: \"0-0\", min_idle_time_milliseconds: 1000, new_messages_count: 3, pending_messages_count: 2, claimed_messages_count: 1, block: 5 }");
        assert_eq!(
            consumer.clone().get_stream_name(),
            "test-consumer-debug-and-clone"
        );
    }

    #[tokio::test]
    async fn test_consumer_error_stream_not_found() {
        let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
            None,
            "localhost",
            "6379",
            "0",
            "test-consumer-error-stream-not-found",
            "group-name",
            "test-constructor",
            "0-0",
            1000,
            3,
            2,
            1,
            5,
        );

        assert!(consumer_result.is_err());
        assert_eq!(
            consumer_result.unwrap_err().to_string(),
            "Stream does not exist- TryAgain"
        );
    }

    #[tokio::test]
    async fn test_consumer_error_in_total_messages_to_read() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-consumer-error-in-total-messages-to-read";

        let producer_result: RedsumerResult<RedsumerProducer> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        assert!(producer_result.is_ok());
        let producer: RedsumerProducer = producer_result.unwrap();

        let message: HashMap<String, String> = [("key".to_string(), "value".to_string())]
            .iter()
            .cloned()
            .collect();

        let msg_result: RedsumerResult<Id> = producer.produce(message).await;
        assert!(msg_result.is_ok());

        let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            "group-name",
            "consumer",
            "0-0",
            1000,
            0,
            0,
            0,
            5,
        );

        assert!(consumer_result.is_err());
        assert_eq!(
            consumer_result.unwrap_err().to_string(),
            "Total messages to read must be grater than zero- TryAgain"
        );
    }

    #[tokio::test]
    async fn test_consumer_consume() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-consumer-consume";

        let producer_result: RedsumerResult<RedsumerProducer> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        assert!(producer_result.is_ok());
        let producer: RedsumerProducer = producer_result.unwrap();

        let mut produced_ids: Vec<Id> = Vec::new();
        for i in 0..15 {
            let message: HashMap<String, String> = [("key".to_string(), i.to_string())]
                .iter()
                .cloned()
                .collect();

            let msg_result: RedsumerResult<Id> = producer.produce(message).await;
            assert!(msg_result.is_ok());

            produced_ids.push(msg_result.unwrap());
        }

        // Consume new messages:
        let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            "group-name",
            "consumer",
            "0-0",
            1000,
            15,
            0,
            0,
            5,
        );

        assert!(consumer_result.is_ok());
        let mut consumer: RedsumerConsumer = consumer_result.unwrap();

        let new_messages_result: RedsumerResult<Vec<StreamId>> = consumer.consume().await;
        assert!(new_messages_result.is_ok());

        let new_messages: Vec<StreamId> = new_messages_result.unwrap();
        assert_eq!(new_messages.len(), 15);

        for message in new_messages.iter() {
            assert!(produced_ids.contains(&message.id));
        }

        let new_messages_result: RedsumerResult<Vec<StreamId>> = consumer.consume().await;
        assert!(new_messages_result.is_ok());

        let new_messages: Vec<StreamId> = new_messages_result.unwrap();
        assert_eq!(new_messages.len(), 0);

        // Consume pending messages:
        let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            "group-name",
            "consumer",
            "0-0",
            1000,
            0,
            15,
            0,
            5,
        );

        assert!(consumer_result.is_ok());
        let mut consumer: RedsumerConsumer = consumer_result.unwrap();

        let pending_messages_result: RedsumerResult<Vec<StreamId>> = consumer.consume().await;
        assert!(pending_messages_result.is_ok());

        let pending_messages: Vec<StreamId> = pending_messages_result.unwrap();
        assert_eq!(pending_messages.len(), 15);

        for message in pending_messages.iter() {
            assert!(produced_ids.contains(&message.id));
        }

        sleep(Duration::from_secs(2)).await;

        // Consume claimed messages:
        let ghost_consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            "group-name",
            "ghost-consumer",
            "0-0",
            1000,
            0,
            0,
            15,
            5,
        );

        assert!(ghost_consumer_result.is_ok());
        let mut ghost_consumer: RedsumerConsumer = ghost_consumer_result.unwrap();

        let claimed_messages_result: RedsumerResult<Vec<StreamId>> = ghost_consumer.consume().await;
        assert!(claimed_messages_result.is_ok());

        let claimed_messages: Vec<StreamId> = claimed_messages_result.unwrap();
        assert_eq!(claimed_messages.len(), 15);

        for message in claimed_messages.iter() {
            assert!(produced_ids.contains(&message.id));
        }

        // Verify claimed messages are not available for the original consumer:
        for message in claimed_messages.iter() {
            let is_still_mine_result: RedsumerResult<bool> = consumer.is_still_mine(&message.id);
            assert!(is_still_mine_result.is_ok());

            let is_still_mine: bool = is_still_mine_result.unwrap();
            assert!(!is_still_mine);
        }

        // Verify claimed messages are available for the ghost consumer:
        for message in claimed_messages.iter() {
            let is_still_mine_result: RedsumerResult<bool> =
                ghost_consumer.is_still_mine(&message.id);
            assert!(is_still_mine_result.is_ok());

            let is_still_mine: bool = is_still_mine_result.unwrap();
            assert!(is_still_mine);
        }

        // Ack messages:
        for message in claimed_messages.iter() {
            let ack_result: RedsumerResult<bool> = ghost_consumer.ack(&message.id).await;
            assert!(ack_result.is_ok());
            assert!(ack_result.unwrap());
        }
    }
}
