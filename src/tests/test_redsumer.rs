#[cfg(test)]
pub mod test_streams {
    use redis::streams::StreamInfoConsumersReply;
    use std::collections::{BTreeMap, HashMap};
    use uuid::Uuid;

    use crate::{
        client::ClientCredentials,
        consumer::{RedsumerConsumer, RedsumerConsumerOptions},
        producer::RedsumerProducer,
        streams::{
            info::{StreamConsumersInfo, StreamInfo},
            RedisError, StreamId, StreamInfoGroupsReply, StreamInfoStreamReply,
        },
    };
    #[tokio::test]
    async fn test_produce_from_items() {
        let redis_db_credentials: Option<ClientCredentials<'_>> = None;
        let producer: RedsumerProducer<'_> = RedsumerProducer::new(
            redis_db_credentials,
            "localhost",
            "6379",
            "0",
            "test-produce-from-items",
        );

        let mut message: Vec<(&str, &str)> = Vec::new();
        message.push(("name", "Lorem"));
        message.push(("last_name", "Ipsum"));

        let new_stream_message_result: Result<String, RedisError> =
            producer.produce_from_items(&message).await;
        assert!(new_stream_message_result.is_ok());

        let stream_info_result: Result<StreamInfoStreamReply, RedisError> =
            producer.get_stream_info().await;
        assert!(stream_info_result.is_ok());

        let groups_info_result: Result<StreamInfoGroupsReply, RedisError> =
            producer.get_consumer_groups_info().await;
        assert!(groups_info_result.is_ok())
    }

    #[tokio::test]
    async fn test_produce_from_btreemap() {
        let redis_db_credentials: Option<ClientCredentials<'_>> = None;
        let producer: RedsumerProducer<'_> = RedsumerProducer::new(
            redis_db_credentials,
            "localhost",
            "6379",
            "0",
            "test-produce-from-btreemap",
        );

        let mut message: BTreeMap<&str, &str> = BTreeMap::new();
        message.insert("name", "Lorem");
        message.insert("last_name", "Ipsum");

        let result: Result<String, RedisError> = producer.produce_from_map(&message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_produce_from_hashmap() {
        let redis_db_credentials: Option<ClientCredentials<'_>> = None;
        let producer: RedsumerProducer<'_> = RedsumerProducer::new(
            redis_db_credentials,
            "localhost",
            "6379",
            "0",
            "test-produce-from-hashmap",
        );

        let mut message: HashMap<&str, &str> = HashMap::new();
        message.insert("name", "Lorem");
        message.insert("last_name", "Ipsum");

        let result: Result<String, RedisError> = producer.produce_from_map(&message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_produce_from_object() {
        use std::env;
        use structmap::ToMap;
        use structmap_derive::ToMap;

        let redis_user: String = match env::var("REDIS_USER") {
            Ok(env_var) => env_var,
            Err(error) => panic!("{}", error),
        };

        let redis_password: String = match env::var("REDIS_PASSWORD") {
            Ok(env_var) => env_var,
            Err(error) => panic!("{}", error),
        };

        let redis_host: String = match env::var("REDIS_HOST") {
            Ok(env_var) => env_var,
            Err(error) => panic!("{}", error),
        };

        let redis_port: String = match env::var("REDIS_PORT") {
            Ok(env_var) => env_var,
            Err(error) => panic!("{}", error),
        };

        let redis_db: String = match env::var("REDIS_DB") {
            Ok(env_var) => env_var,
            Err(error) => panic!("{}", error),
        };

        let redis_stream_name: String = match env::var("REDIS_STREAM_NAME") {
            Ok(env_var) => env_var,
            Err(error) => panic!("{}", error),
        };

        let producer: RedsumerProducer<'_> = RedsumerProducer::new(
            Some(ClientCredentials::get(&redis_user, &redis_password)),
            &redis_host,
            &redis_port,
            &redis_db,
            &redis_stream_name,
        );

        #[derive(ToMap, Default)]
        struct MyMessage {
            name: String,
            last_name: String,
        }

        let message: MyMessage = MyMessage {
            name: String::from("Lorem"),
            last_name: String::from("Ipsum"),
        };
        let map = MyMessage::to_stringmap(message);

        let result: Result<String, RedisError> = producer.produce_from_map(&map).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_streams_happy_path() {
        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new(None, "localhost", "6379", "0", "test-happy-path");

        for _ in 0..500 {
            let producer_result: Result<String, RedisError> = producer
                .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
                .await;

            assert!(producer_result.is_ok());
        }

        let consumer_options: RedsumerConsumerOptions =
            RedsumerConsumerOptions::default().set_count(500u16);

        let mut consumer: RedsumerConsumer = RedsumerConsumer::new(
            None,
            "localhost",
            "6379",
            "0",
            "test-happy-path",
            "test-group",
            "test-consumer",
            consumer_options,
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

        let consumer_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(consumer_result.is_ok());

        let pending_messages: Vec<StreamId> = consumer_result.unwrap();
        assert!(pending_messages.len().eq(&0));

        let consumers_info: Result<StreamInfoConsumersReply, RedisError> =
            consumer.get_consumers_info().await;
        assert!(consumers_info.is_ok())
    }

    #[tokio::test]
    async fn test_verify_pending_message_ownership() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-pending-message-ownership";
        let group_name: &str = "test-consumer";

        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        let producer_result: Result<String, RedisError> = producer
            .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
            .await;
        assert!(producer_result.is_ok());

        let mut consumer_alpha: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "alpha",
            RedsumerConsumerOptions::default(),
        );

        let mut consumer_beta: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "beta",
            RedsumerConsumerOptions::default().set_min_idle_time_milliseconds(1u32),
        );

        let consumer_alpha_result: Result<Vec<StreamId>, RedisError> =
            consumer_alpha.consume().await;
        assert!(consumer_alpha_result.is_ok());
        let pending_messages_alpha: Vec<StreamId> = consumer_alpha_result.unwrap();

        let consumer_beta_result: Result<Vec<StreamId>, RedisError> = consumer_beta.consume().await;
        assert!(consumer_beta_result.is_ok());
        let pending_messages_beta: Vec<StreamId> = consumer_beta_result.unwrap();

        let beta_ownership_result = consumer_beta
            .validate_pending_message_ownership(&pending_messages_beta[0].id)
            .await;
        assert!(beta_ownership_result.is_ok());
        assert!(beta_ownership_result.unwrap());

        let alpha_ownership_result = consumer_alpha
            .validate_pending_message_ownership(&pending_messages_alpha[0].id)
            .await;
        assert!(&alpha_ownership_result.is_ok());
        assert!(!&alpha_ownership_result.unwrap());

        let akc_result: Result<bool, RedisError> = consumer_beta
            .acknowledge(&pending_messages_beta[0].id)
            .await;
        assert!(akc_result.is_ok());
        assert!(akc_result.unwrap());

        let beta_ownership_result = consumer_beta
            .validate_pending_message_ownership(&pending_messages_beta[0].id)
            .await;
        assert!(beta_ownership_result.is_err());
    }

    #[tokio::test]
    async fn test_consumer_count() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-consumer-count";
        let group_name: &str = "test-consumer";

        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        for _ in 0..100 {
            let producer_result: Result<String, RedisError> = producer
                .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
                .await;

            assert!(producer_result.is_ok());
        }

        let mut consumer: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "alpha",
            RedsumerConsumerOptions::default()
                .set_min_idle_time_milliseconds(1u32)
                .set_count(200u16),
        );

        let consumer_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(consumer_result.is_ok());

        for _ in 0..1000 {
            let producer_result: Result<String, RedisError> = producer
                .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
                .await;

            assert!(producer_result.is_ok());
        }

        let consumer_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(consumer_result.is_ok());
        let pending_messages: Vec<StreamId> = consumer_result.unwrap();
        assert!(pending_messages.len().eq(&200))
    }

    #[tokio::test]
    async fn test_consumer_min_idle_time_ms() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-consumer-min-idle-time-ms";
        let group_name: &str = "test-consumer";

        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        for _ in 0..10 {
            let producer_result: Result<String, RedisError> = producer
                .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
                .await;

            assert!(producer_result.is_ok());
        }

        let mut consumer_alpha: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "alpha",
            RedsumerConsumerOptions::default(),
        );

        let consumer_alpha_result: Result<Vec<StreamId>, RedisError> =
            consumer_alpha.consume().await;
        assert!(consumer_alpha_result.is_ok());

        let mut consumer_beta: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "beta",
            RedsumerConsumerOptions::default().set_min_idle_time_milliseconds(60000u32),
        );

        let consumer_beta_result: Result<Vec<StreamId>, RedisError> = consumer_beta.consume().await;
        assert!(consumer_beta_result.is_ok());

        let mut consumer_beta: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "beta",
            RedsumerConsumerOptions::default().set_min_idle_time_milliseconds(1u32),
        );

        let consumer_beta_result: Result<Vec<StreamId>, RedisError> = consumer_beta.consume().await;
        assert!(consumer_beta_result.is_ok());

        let pending_messages: Vec<StreamId> = consumer_beta_result.unwrap();
        assert!(pending_messages.len().eq(&10));
    }

    #[tokio::test]
    async fn test_consumer_block_for_new_messages() {
        let host: &str = "localhost";
        let port: &str = "6379";
        let db: &str = "0";
        let stream_name: &str = "test-consumer-block-for-new-messages";
        let group_name: &str = "test-consumer";

        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new(None, host, port, db, stream_name);

        let producer_result: Result<String, RedisError> = producer
            .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
            .await;
        assert!(producer_result.is_ok());

        let mut consumer: RedsumerConsumer = RedsumerConsumer::new(
            None,
            host,
            port,
            db,
            stream_name,
            group_name,
            "alpha",
            RedsumerConsumerOptions::default().set_block(10000u16),
        );

        let consumer_alpha_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(consumer_alpha_result.is_ok());
        let pending_messages: Vec<StreamId> = consumer_alpha_result.unwrap();
        assert!(pending_messages.len().eq(&1));

        let consumer_alpha_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(consumer_alpha_result.is_ok());
        let pending_messages: Vec<StreamId> = consumer_alpha_result.unwrap();
        assert!(pending_messages.len().eq(&1));

        for _ in 0..5 {
            let producer_result: Result<String, RedisError> = producer
                .produce_from_items(&vec![(String::from("id"), Uuid::new_v4().to_string())])
                .await;
            assert!(producer_result.is_ok());
        }

        let consumer_alpha_result: Result<Vec<StreamId>, RedisError> = consumer.consume().await;
        assert!(consumer_alpha_result.is_ok());
        let pending_messages: Vec<StreamId> = consumer_alpha_result.unwrap();
        assert!(pending_messages.len().eq(&6));
    }
}
