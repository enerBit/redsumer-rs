#[cfg(test)]
pub mod test_producer {
    use crate::{redsumer::producer::RedsumerProducer, RedisError};
    use std::collections::{BTreeMap, HashMap};

    #[tokio::test]
    async fn test_produce_from_items() {
        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new("localhost:6379", "0", "test-produce-from-items");

        let mut message: Vec<(&str, &str)> = Vec::new();
        message.push(("name", "Lorem"));
        message.push(("last_name", "Ipsum"));
        message.push(("description", "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit..."));

        let result: Result<String, RedisError> = producer.produce_from_items(&message).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_produce_from_btreemap() {
        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new("localhost:6379", "0", "test-produce-from-btreemap");

        let mut message: BTreeMap<&str, &str> = BTreeMap::new();
        message.insert("name", "Lorem");
        message.insert("last_name", "Ipsum");
        message.insert("description", "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...");

        let result: Result<String, RedisError> = producer.produce_from_map(&message).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_produce_from_hashmap() {
        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new("localhost:6379", "0", "test-produce-from-hashmap");

        let mut message: HashMap<&str, &str> = HashMap::new();
        message.insert("name", "Lorem");
        message.insert("last_name", "Ipsum");
        message.insert("description", "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...");

        let result: Result<String, RedisError> = producer.produce_from_map(&message).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_produce_from_object() {
        use structmap::ToMap;
        use structmap_derive::ToMap;

        let producer: RedsumerProducer<'_> =
            RedsumerProducer::new("localhost:6379", "0", "test-produce-from-object");

        #[derive(ToMap, Default)]
        struct MyMessage {
            name: String,
            last_name: String,
            description: String,
        }

        let message: MyMessage = MyMessage{
            name: String::from("Lorem"),
            last_name: String::from("Ipsum"),
            description: String::from("Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit..."),
        };
        let map = MyMessage::to_stringmap(message);

        let result: Result<String, RedisError> = producer.produce_from_map(&map).await;

        assert!(result.is_ok());
    }
}
