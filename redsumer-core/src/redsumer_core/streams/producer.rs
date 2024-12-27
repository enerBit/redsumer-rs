use redis::{Commands, FromRedisValue, RedisResult, ToRedisArgs};
use tracing::{debug, error};

#[allow(unused_imports)]
use crate::redsumer_core::result::{RedsumerError, RedsumerResult};

/// Produce a message to a stream from a map.
///
/// To set the ID of the message, this method use the value "*" to indicate that server should generate a new ID with the current timestamp.
fn produce_from_map<C, K, M, ID>(c: &mut C, key: K, map: M) -> RedisResult<ID>
where
    C: Commands,
    K: ToRedisArgs,
    M: ToRedisArgs,
    ID: FromRedisValue,
{
    match c.xadd_map(key, "*", map) {
        Ok(id) => {
            debug!("Message produced successfully");
            Ok(id)
        }
        Err(e) => {
            error!("Error producing message: {:?}", e);
            Err(e)
        }
    }
}

/// Produce a message to a stream from a list of items.
///
/// To set the ID of the message, this method use the value "*" to indicate that server should generate a new ID with the current timestamp.
fn produce_from_items<C, K, F, V, ID>(c: &mut C, key: K, items: &[(F, V)]) -> RedisResult<ID>
where
    C: Commands,
    K: ToRedisArgs,
    F: ToRedisArgs,
    V: ToRedisArgs,
    ID: FromRedisValue,
{
    match c.xadd(key, "*", items) {
        Ok(id) => {
            debug!("Message produced successfully");
            Ok(id)
        }
        Err(e) => {
            error!("Error producing message: {:?}", e);
            Err(e)
        }
    }
}

/// A trait that bundles methods for producing messages to a stream
pub trait ProducerCommands {
    /// Produce a message to a stream from a map.
    ///
    /// # Arguments:
    /// - **key**: The stream key.
    /// - **map**: A map with the fields and values of the message.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with the message ID if the message was produced successfully. Otherwise, a [`RedsumerError`] is returned.
    fn produce_from_map<K, M>(&mut self, key: K, map: M) -> RedsumerResult<String>
    where
        K: ToRedisArgs,
        M: ToRedisArgs;

    /// Produce a message to a stream from a list of items.
    ///
    /// # Arguments:
    ///  - **key**: The stream key.
    /// - **items**: A list of tuples with the fields and values of the message.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with the message ID if the message was produced successfully. Otherwise, a [`RedsumerError`] is returned.
    fn produce_from_items<K, F, V>(&mut self, key: K, items: &[(F, V)]) -> RedsumerResult<String>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs;
}

impl<C> ProducerCommands for C
where
    C: Commands,
{
    fn produce_from_map<K, M>(&mut self, key: K, map: M) -> RedsumerResult<String>
    where
        K: ToRedisArgs,
        M: ToRedisArgs,
    {
        produce_from_map(self, key, map)
    }

    fn produce_from_items<K, F, V>(&mut self, key: K, items: &[(F, V)]) -> RedsumerResult<String>
    where
        K: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
    {
        produce_from_items(self, key, items)
    }
}

#[cfg(test)]
mod test_produce_from_map {
    use std::collections::BTreeMap;

    use redis::{cmd, ErrorKind, Value};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_produce_from_map_ok() {
        // Define the key:
        let key: &str = "my-key";

        // Define the map:
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("field", "value");

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XADD").arg(key).arg("*").arg(map.to_owned()),
                Ok(Value::SimpleString("1-0".to_string())),
            )]);

        // Produce the message:
        let result: RedsumerResult<String> = conn.produce_from_map(key, map);

        // Verify the result:
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_from_map_error() {
        // Define the key:
        let key: &str = "my-key";

        // Define the map:
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("field", "value");

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XADD").arg(key).arg("*").arg(map.to_owned()),
                Err(RedsumerError::from((
                    ErrorKind::ResponseError,
                    "XADD Error",
                    "XADD command failed".to_string(),
                ))),
            )]);

        // Produce the message:
        let result: RedsumerResult<String> = conn.produce_from_map(key, map);

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_produce_from_items {
    use redis::{cmd, ErrorKind, Value};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_produce_from_items_ok() {
        // Define the key:
        let key: &str = "my-key";

        // Define the items:
        let items: Vec<(&str, u8)> = vec![("number", 3), ("double", 6)];

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XADD").arg(key).arg("*").arg(&items),
                Ok(Value::SimpleString("1-0".to_string())),
            )]);

        // Produce the message:
        let result: RedsumerResult<String> = conn.produce_from_items(key, &items);

        // Verify the result:
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_from_items_error() {
        // Define the key:
        let key: &str = "my-key";

        // Define the items:
        let items: Vec<(&str, &str)> = vec![("field", "value")];

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XADD").arg(key).arg("*").arg(&items),
                Err(RedsumerError::from((
                    ErrorKind::ResponseError,
                    "XADD Error",
                    "XADD command failed".to_string(),
                ))),
            )]);

        // Produce the message:
        let result: RedsumerResult<String> = conn.produce_from_items(key, &items);

        // Verify the result:
        assert!(result.is_err());
    }
}
