#[cfg(test)]
mod tests {
    use tokio::test;

    use super::super::super::*;
    use streams::consumer::{RedisStreamsConsumer, RedisConsumerResult, StreamMessage};

    use env_logger;

    #[test]
    async fn test_consume_stream() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .init();

        let url: String = String::from("localhost:6379");
        let stream_name: String = String::from("test-redsumer-rs");
        let group_name: String = String::from("test-group-name");
        let consumer_name: String = String::from("redsumer-rs-consumer");

        let mut consumer: RedisStreamsConsumer = RedisStreamsConsumer::new(
            url,
            None,
            stream_name,
            group_name,
            consumer_name,
            None,
            Some(2000u16),
            None,
            None,
            None,
        );

        let pending_messages: RedisConsumerResult<Vec<StreamMessage>, String>= consumer.consume().await;
        assert!(pending_messages.is_ok());
        
        let akcn_id: RedisConsumerResult<u8, String> = consumer.acknowledge("1684081214635-0".to_string()).await;
        assert!(akcn_id.is_ok());
    }

}
