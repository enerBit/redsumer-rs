use redis::{
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamPendingCountReply,
        StreamReadOptions, StreamReadReply,
    },
    Commands, ErrorKind, RedisError, RedisResult, ToRedisArgs,
};
use tracing::{debug, error, warn};

#[allow(unused_imports)]
use crate::core::{
    result::{RedsumerError, RedsumerResult},
    streams::types::{LatestPendingMessageId, NextIdToClaim},
};

pub const BEGINNING_OF_TIME_ID: &str = "0-0";

/// Get StreamIds from a StreamReadReply by key.
trait UnwrapStreamReadReply<K> {
    /// Unwrap StreamReadReply by key into a Vec<StreamId>.
    ///
    /// # Arguments:
    /// - **key**: A key to filter the StreamReadReply.
    ///
    /// # Returns:
    /// A Vec<StreamId> with the StreamIds found.
    fn unwrap_by_key(&self, key: &K) -> Vec<StreamId>
    where
        K: ToString;
}

impl<K> UnwrapStreamReadReply<K> for StreamReadReply
where
    K: ToString,
{
    fn unwrap_by_key(&self, key: &K) -> Vec<StreamId> {
        let mut ids: Vec<StreamId> = Vec::new();

        for stream in self.keys.iter() {
            match stream.key.eq(&key.to_string()) {
                true => ids.extend(stream.ids.to_owned()),
                false => warn!(
                    "An unexpected stream name found while extracting the key {}: {}. ",
                    &key.to_string(),
                    stream.key,
                ),
            };
        }

        ids
    }
}

/// Verify if a stream exists in Redis Stream service.
fn verify_if_stream_exists<C, K>(conn: &mut C, key: K) -> RedsumerResult<()>
where
    C: Commands,
    K: ToRedisArgs,
{
    match conn.exists::<_, bool>(key) {
        Ok(true) => {
            debug!("The stream already exists");
            Ok(())
        }
        Ok(false) => {
            error!("The stream does not exist");
            Err(RedisError::from((
                ErrorKind::ClientError,
                "Stream does not exist",
            )))
        }
        Err(e) => {
            error!("Error verifying if stream exists: {:?}", e);
            Err(RedisError::from((
                ErrorKind::ClientError,
                "Error verifying if stream exists",
            )))
        }
    }
}

/// Create a consumer group in a stream.
fn create_consumer_group<C, K, G, ID>(
    conn: &mut C,
    key: K,
    group: G,
    since_id: ID,
) -> RedisResult<bool>
where
    C: Commands,
    K: ToRedisArgs,
    G: ToRedisArgs,
    ID: ToRedisArgs,
{
    match conn.xgroup_create::<_, _, _, String>(key, group, since_id) {
        Ok(_) => {
            debug!("The consumers group was successfully created");
            Ok(true)
        }
        Err(e) => {
            if e.to_string().contains("BUSYGROUP") {
                debug!("The consumer group already exists");
                Ok(false)
            } else {
                error!("Error creating consumer group: {:?}", e);
                Err(e)
            }
        }
    }
}

/// Read new messages from a stream.
fn read_new_messages<C, K, G, N>(
    conn: &mut C,
    key: &K,
    group: &G,
    consumer: &N,
    count: usize,
    block: usize,
) -> RedisResult<Vec<StreamId>>
where
    C: Commands,
    K: ToRedisArgs + ToString,
    G: ToRedisArgs,
    N: ToRedisArgs,
{
    Ok(match count.gt(&0) {
        true => conn
            .xread_options::<_, _, StreamReadReply>(
                &[key],
                &[">"],
                &StreamReadOptions::default()
                    .group(group, consumer)
                    .count(count)
                    .block(block),
            )?
            .unwrap_by_key(key),
        false => Vec::new(),
    })
}

/// Read pending messages from a stream.
fn read_pending_messages<C, K, G, N, ID>(
    conn: &mut C,
    key: &K,
    group: &G,
    consumer: &N,
    latest_pending_message_id: ID,
    count: usize,
) -> RedisResult<(Vec<StreamId>, LatestPendingMessageId)>
where
    C: Commands,
    K: ToRedisArgs + ToString,
    G: ToRedisArgs,
    N: ToRedisArgs,
    ID: ToRedisArgs,
{
    match count.gt(&0) {
        true => {
            let pending_messages: Vec<StreamId> = conn
                .xread_options::<_, _, StreamReadReply>(
                    &[key],
                    &[latest_pending_message_id],
                    &StreamReadOptions::default()
                        .group(group, consumer)
                        .count(count),
                )?
                .unwrap_by_key(key);

            let latest_pending_message_id: String = match pending_messages.last() {
                Some(s) => s.id.to_owned(),
                None => BEGINNING_OF_TIME_ID.to_owned(),
            };

            Ok((pending_messages, latest_pending_message_id))
        }
        false => {
            return Ok((Vec::new(), BEGINNING_OF_TIME_ID.to_owned()));
        }
    }
}

/// Claim pending messages from a stream.
fn claim_pending_messages<C, K, G, N, ID>(
    conn: &mut C,
    key: &K,
    group: &G,
    consumer: &N,
    min_idle_time: usize,
    next_id_to_claim: ID,
    count: usize,
) -> RedisResult<(Vec<StreamId>, NextIdToClaim)>
where
    C: Commands,
    K: ToRedisArgs,
    G: ToRedisArgs,
    N: ToRedisArgs,
    ID: ToRedisArgs,
{
    match count.gt(&0) {
        true => {
            let reply: StreamAutoClaimReply = conn
                .xautoclaim_options::<_, _, _, _, _, StreamAutoClaimReply>(
                    key,
                    group,
                    consumer,
                    min_idle_time,
                    next_id_to_claim,
                    StreamAutoClaimOptions::default().count(count),
                )?;

            Ok((reply.claimed.to_owned(), reply.next_stream_id.to_owned()))
        }
        false => {
            return Ok((Vec::new(), BEGINNING_OF_TIME_ID.to_owned()));
        }
    }
}

/// Verify if a message is still in the consumer pending list.
fn is_still_mine<C, K, G, CN, ID>(
    conn: &mut C,
    key: K,
    group: G,
    consumer: CN,
    id: ID,
) -> RedsumerResult<bool>
where
    C: Commands,
    K: ToRedisArgs,
    G: ToRedisArgs,
    CN: ToRedisArgs,
    ID: ToRedisArgs,
{
    match conn.xpending_consumer_count::<_, _, _, _, _, _, StreamPendingCountReply>(
        key, group, &id, &id, 1, consumer,
    ) {
        Ok(r) => match r.ids.len().gt(&0) {
            true => {
                debug!("The message is still in the consumer pending list");
                Ok(true)
            }
            false => {
                debug!("The message is not in the consumer pending list");
                Ok(false)
            }
        },
        Err(e) => {
            error!(
                "Error verifying if message is still in consumer pending list: {:?}",
                e
            );
            Err(e)
        }
    }
}

/// Ack a message in a consumer group.
fn ack<C, K, G, ID>(conn: &mut C, key: K, group: G, id: ID) -> RedsumerResult<bool>
where
    C: Commands,
    K: ToRedisArgs,
    G: ToRedisArgs,
    ID: ToRedisArgs,
{
    match conn.xack::<_, _, _, bool>(key, group, &[id]) {
        Ok(true) => {
            debug!("The message was successfully acknowledged");
            Ok(true)
        }
        Ok(false) => {
            debug!("The message was not acknowledged");
            Ok(false)
        }
        Err(e) => {
            error!("Error acknowledging message: {:?}", e);
            Err(e)
        }
    }
}

/// A trait that bundles methods for consuming messages from a Redis stream
pub trait ConsumerCommands<K>
where
    K: ToRedisArgs,
{
    /// Verify if a stream exists in Redis Stream service.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with the result of the operation.
    /// If the stream exists, the function will return a success result.
    /// If the stream does not exist, the function will return an error result.
    /// If an error occurs, the function will return an error result.
    fn verify_if_stream_exists(&mut self, key: K) -> RedsumerResult<()>;

    /// Create a consumer group in a Redis stream.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    /// - **group**: A consumers group, which must implement the `ToRedisArgs` trait.
    /// - **since_id**: The ID of the message to start consuming, which must implement the `ToRedisArgs` trait.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with the result of the operation.
    /// If the consumer group already exists, the function will return a success result with a `false` value.
    /// If the consumer group does not exist, the function will create it and return a success result with a `true` value.
    /// If an error occurs, the function will return an error result.
    fn create_consumer_group<G, ID>(
        &mut self,
        key: K,
        group: G,
        since_id: ID,
    ) -> RedsumerResult<bool>
    where
        G: ToRedisArgs,
        ID: ToRedisArgs;

    /// Read new messages from a stream.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    /// - **group**: A consumers group, which must implement the `ToRedisArgs` trait.
    /// - **consumer**: A consumer name, which must implement the `ToRedisArgs` trait.
    /// - **count**: The number of messages to read.
    /// - **block**: The time to block waiting for new messages.
    ///
    /// # Returns:
    /// A [`RedisResult`] with a vector of [`StreamId`]s.
    /// If the operation is successful, the function will return a vector of [`StreamId`]s.
    /// If an error occurs, the function will return an error result.
    fn read_new_messages<G, N>(
        &mut self,
        key: &K,
        group: &G,
        consumer: &N,
        count: usize,
        block: usize,
    ) -> RedisResult<Vec<StreamId>>
    where
        G: ToRedisArgs,
        N: ToRedisArgs;

    /// Read pending messages from a stream.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    /// - **group**: A consumers group, which must implement the `ToRedisArgs` trait.
    /// - **consumer**: A consumer name, which must implement the `ToRedisArgs` trait.
    /// - **latest_pending_message_id**: The ID of the latest pending message, which must implement the `ToRedisArgs` trait.
    /// - **count**: The number of messages to read.
    ///
    /// # Returns:
    /// A [`RedisResult`] with a tuple of a vector of [`StreamId`]s and the latest pending message ID.
    /// If the operation is successful, the function will return a tuple with a vector of [`StreamId`]s and the latest pending message ID.
    /// If an error occurs, the function will return an error result.
    fn read_pending_messages<G, N, ID>(
        &mut self,
        key: &K,
        group: &G,
        consumer: &N,
        latest_pending_message_id: ID,
        count: usize,
    ) -> RedisResult<(Vec<StreamId>, LatestPendingMessageId)>
    where
        G: ToRedisArgs,
        N: ToRedisArgs,
        ID: ToRedisArgs;

    /// Claim pending messages from a stream.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    /// - **group**: A consumers group, which must implement the `ToRedisArgs` trait.
    /// - **consumer**: A consumer name, which must implement the `ToRedisArgs` trait.
    /// - **min_idle_time**: The minimum idle time in milliseconds.
    /// - **next_id_to_claim**: The next ID to claim, which must implement the `ToRedisArgs` trait.
    /// - **count**: The number of messages to claim.
    ///
    /// # Returns:
    /// A [`RedisResult`] with a tuple of a vector of [`StreamId`]s and the next ID to claim.
    /// If the operation is successful, the function will return a tuple with a vector of [`StreamId`]s and the next ID to claim.
    /// If an error occurs, the function will return an error result.
    fn claim_pending_messages<G, N, ID>(
        &mut self,
        key: &K,
        group: &G,
        consumer: &N,
        min_idle_time: usize,
        next_id_to_claim: ID,
        count: usize,
    ) -> RedisResult<(Vec<StreamId>, NextIdToClaim)>
    where
        G: ToRedisArgs,
        N: ToRedisArgs,
        ID: ToRedisArgs;

    /// Verify if a message is still in the consumer pending list.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    /// - **group**: A consumers group, which must implement the `ToRedisArgs` trait.
    /// - **consumer**: A consumer name, which must implement the `ToRedisArgs` trait.
    /// - **id**: The ID of the message to verify, which must implement the `ToRedisArgs` trait.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with a boolean value. If the message is still in the consumer pending list, the function will return `true`. If the message is not in the consumer pending list, the function will return `false`. If an error occurs, the function will return an error result.
    fn is_still_mine<G, CN, ID>(
        &mut self,
        key: K,
        group: G,
        consumer: CN,
        id: ID,
    ) -> RedsumerResult<bool>
    where
        G: ToRedisArgs,
        CN: ToRedisArgs,
        ID: ToRedisArgs;

    /// Acknowledge a message in a consumer group.
    ///
    /// # Arguments:
    /// - **key**: A stream key, which must implement the `ToRedisArgs` trait.
    /// - **group**: A consumers group, which must implement the `ToRedisArgs` trait.
    /// - **id**: The ID of the message to acknowledge, which must implement the `ToRedisArgs` trait.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with a boolean value. If the message was successfully acknowledged, the function will return `true`. If the message was not acknowledged, the function will return `false`. If an error occurs, the function will return an error result.
    fn ack<G, ID>(&mut self, key: K, group: G, id: ID) -> RedsumerResult<bool>
    where
        G: ToRedisArgs,
        ID: ToRedisArgs;
}

impl<C, K> ConsumerCommands<K> for C
where
    C: Commands,
    K: ToRedisArgs + ToString,
{
    fn verify_if_stream_exists(&mut self, key: K) -> RedsumerResult<()>
    where
        K: ToRedisArgs,
    {
        verify_if_stream_exists(self, key)
    }

    fn create_consumer_group<G, ID>(
        &mut self,
        key: K,
        group: G,
        since_id: ID,
    ) -> RedsumerResult<bool>
    where
        G: ToRedisArgs,
        ID: ToRedisArgs,
    {
        create_consumer_group(self, key, group, since_id)
    }

    fn read_new_messages<G, N>(
        &mut self,
        key: &K,
        group: &G,
        consumer: &N,
        count: usize,
        block: usize,
    ) -> RedisResult<Vec<StreamId>>
    where
        G: ToRedisArgs,
        N: ToRedisArgs,
    {
        read_new_messages(self, key, group, consumer, count, block)
    }

    fn read_pending_messages<G, N, ID>(
        &mut self,
        key: &K,
        group: &G,
        consumer: &N,
        latest_pending_message_id: ID,
        count: usize,
    ) -> RedisResult<(Vec<StreamId>, LatestPendingMessageId)>
    where
        G: ToRedisArgs,
        N: ToRedisArgs,
        ID: ToRedisArgs,
    {
        read_pending_messages(self, key, group, consumer, latest_pending_message_id, count)
    }

    fn claim_pending_messages<G, N, ID>(
        &mut self,
        key: &K,
        group: &G,
        consumer: &N,
        min_idle_time: usize,
        next_id_to_claim: ID,
        count: usize,
    ) -> RedisResult<(Vec<StreamId>, NextIdToClaim)>
    where
        G: ToRedisArgs,
        N: ToRedisArgs,
        ID: ToRedisArgs,
    {
        claim_pending_messages(
            self,
            key,
            group,
            consumer,
            min_idle_time,
            next_id_to_claim,
            count,
        )
    }

    fn is_still_mine<G, CN, ID>(
        &mut self,
        key: K,
        group: G,
        consumer: CN,
        id: ID,
    ) -> RedsumerResult<bool>
    where
        G: ToRedisArgs,
        CN: ToRedisArgs,
        ID: ToRedisArgs,
    {
        is_still_mine(self, key, group, consumer, id)
    }

    fn ack<G, ID>(&mut self, key: K, group: G, id: ID) -> RedsumerResult<bool>
    where
        G: ToRedisArgs,
        ID: ToRedisArgs,
    {
        ack(self, key, group, id)
    }
}

#[cfg(test)]
mod test_create_consumer_group {
    use redis::{cmd, ErrorKind, RedisError};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_create_non_existent_consumer_group() {
        // Define the key, group, and since_id:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let since_id: &str = "0";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, &str>(
                cmd("XGROUP")
                    .arg("CREATE")
                    .arg(key)
                    .arg(group)
                    .arg(since_id),
                Ok("Ok"),
            )]);

        // Create the consumer group:
        let result: RedsumerResult<bool> = conn.create_consumer_group(key, group, since_id);

        // Verify the result:
        assert!(result.is_ok());
        assert!(result.unwrap())
    }

    #[test]
    fn test_create_existent_consumer_group() {
        // Define the key, group, and since_id:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let since_id: &str = "0";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, &str>(
                cmd("XGROUP")
                    .arg("CREATE")
                    .arg(key)
                    .arg(group)
                    .arg(since_id),
                Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "BUSYGROUP Consumer Group name already exists",
                ))),
            )]);

        // Create the consumer group:
        let result: RedsumerResult<bool> = conn.create_consumer_group(key, group, since_id);

        // Verify the result:
        assert!(result.is_ok());
        assert!(!result.unwrap())
    }

    #[test]
    fn test_create_consumer_group_error() {
        // Define the key, group, and since_id:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let since_id: &str = "0";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, &str>(
                cmd("XGROUP")
                    .arg("CREATE")
                    .arg(key)
                    .arg(group)
                    .arg(since_id),
                Err(RedisError::from((ErrorKind::ResponseError, "XGROUP Error"))),
            )]);

        // Create the consumer group:
        let result: RedsumerResult<bool> = conn.create_consumer_group(key, group, since_id);

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_verify_if_stream_exists {
    use redis::{cmd, ErrorKind, RedisError};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_verify_if_stream_exists() {
        // Define the key:
        let key: &str = "my-key";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, i64>(cmd("EXISTS").arg(key), Ok(1))]);

        // Verify if the stream exists:
        let result: RedsumerResult<()> = conn.verify_if_stream_exists(key);

        // Verify the result:
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_if_stream_does_not_exist() {
        // Define the key:
        let key: &str = "my-key";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, i64>(cmd("EXISTS").arg(key), Ok(0))]);

        // Verify if the stream exists:
        let result: RedsumerResult<()> = conn.verify_if_stream_exists(key);

        // Verify the result:
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_if_stream_exists_error() {
        // Define the key:
        let key: &str = "my-key";

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![MockCmd::new::<_, i64>(
            cmd("EXISTS").arg(key),
            Err(RedisError::from((ErrorKind::ResponseError, "EXISTS Error"))),
        )]);

        // Verify if the stream exists:
        let result: RedsumerResult<()> = conn.verify_if_stream_exists(key);

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_read_new_messages {
    use redis::{cmd, Value};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_read_new_messages_with_zero_count() {
        // Define the key, group, consumer, count, and block:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let consumer: &str = "my-consumer";
        let count: usize = 0;
        let block: usize = 1;

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![]);

        // Read new messages:
        let result: RedisResult<Vec<StreamId>> =
            conn.read_new_messages(&key, &group, &consumer, count, block);

        // Verify the result:
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_read_new_messages_ok() {
        // Define the key, group, and consumer:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let consumer: &str = "my-consumer";
        let count: usize = 2;
        let block: usize = 1;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XREADGROUP")
                    .arg(
                        &StreamReadOptions::default()
                            .group(group, consumer)
                            .count(count)
                            .block(block),
                    )
                    .arg("STREAMS")
                    .arg(&[key])
                    .arg(&[">"]),
                Ok(Value::Array(vec![Value::Map(vec![
                    (
                        Value::SimpleString("my-key".to_string()),
                        Value::Array(vec![Value::Map(vec![(
                            Value::SimpleString("1-0".to_string()),
                            Value::Array(vec![
                                Value::SimpleString("code".to_string()),
                                Value::Int(1),
                            ]),
                        )])]),
                    ),
                    (
                        Value::SimpleString("fake-key".to_string()),
                        Value::Array(vec![Value::Map(vec![(
                            Value::SimpleString("666-0".to_string()),
                            Value::Array(vec![
                                Value::SimpleString("code".to_string()),
                                Value::Int(666),
                            ]),
                        )])]),
                    ),
                ])])),
            )]);

        // Consume messages:
        let result: RedsumerResult<Vec<StreamId>> =
            conn.read_new_messages(&key, &group, &consumer, count, block);

        // Verify the result:
        assert!(result.is_ok());

        // Verify the messages:
        let messages: Vec<StreamId> = result.unwrap();
        assert!(messages.len().eq(&1));

        assert!(messages[0].id.eq("1-0"));
        assert!(messages[0].map.get("code").unwrap().eq(&Value::Int(1)));
    }

    #[test]
    fn test_read_new_messages_error() {
        // Define the key, group, and consumer:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let consumer: &str = "my-consumer";
        let count: usize = 2;
        let block: usize = 1;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XREADGROUP")
                    .arg(
                        &StreamReadOptions::default()
                            .group(group, consumer)
                            .count(count)
                            .block(block),
                    )
                    .arg("STREAMS")
                    .arg(&[key])
                    .arg(&[">"]),
                Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "XREADGROUP Error",
                ))),
            )]);

        // Consume messages:
        let result: RedsumerResult<Vec<StreamId>> =
            conn.read_new_messages(&key, &group, &consumer, count, block);

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_read_pending_messages {
    use redis::{cmd, Value};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_read_pending_messages_with_zero_count() {
        // Define the key, group, consumer, latest_pending_message_id, and count:
        let key: &str = "my-key";
        let group: &str = "my-group";
        let consumer: &str = "my-consumer";
        let latest_pending_message_id: &str = "0-0";
        let count: usize = 0;

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![]);

        // Read pending messages:
        let result: RedsumerResult<(Vec<StreamId>, LatestPendingMessageId)> =
            conn.read_pending_messages(&key, &group, &consumer, latest_pending_message_id, count);

        // Verify the result:
        assert!(result.is_ok());

        let (messages, next_id_to_claim): (Vec<StreamId>, LatestPendingMessageId) = result.unwrap();
        assert!(messages.is_empty());
        assert!(next_id_to_claim.eq(BEGINNING_OF_TIME_ID));
    }

    #[test]
    fn test_read_pending_messages_empty() {
        // Define the key, group, consumer, latest_pending_message_id, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let latest_pending_message_id = "0-0";
        let count = 2;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XREADGROUP")
                    .arg(
                        &StreamReadOptions::default()
                            .group(group, consumer)
                            .count(count),
                    )
                    .arg("STREAMS")
                    .arg(&[key])
                    .arg(&[latest_pending_message_id]),
                Ok(Value::Array(vec![Value::Map(vec![])])),
            )]);

        // Read pending messages:
        let result: RedsumerResult<(Vec<StreamId>, LatestPendingMessageId)> =
            conn.read_pending_messages(&key, &group, &consumer, latest_pending_message_id, count);

        // Verify the result:
        assert!(result.is_ok());

        let (messages, next_id_to_claim): (Vec<StreamId>, LatestPendingMessageId) = result.unwrap();
        assert!(messages.len().eq(&0));
        assert!(next_id_to_claim.eq(BEGINNING_OF_TIME_ID));
    }

    #[test]
    fn test_read_pending_messages_ok() {
        // Define the key, group, consumer, latest_pending_message_id, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let latest_pending_message_id = "0-0";
        let count = 2;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XREADGROUP")
                    .arg(
                        &StreamReadOptions::default()
                            .group(group, consumer)
                            .count(count),
                    )
                    .arg("STREAMS")
                    .arg(&[key])
                    .arg(&[latest_pending_message_id]),
                Ok(Value::Array(vec![Value::Map(vec![
                    (
                        Value::SimpleString("my-key".to_string()),
                        Value::Array(vec![Value::Map(vec![(
                            Value::SimpleString("1-0".to_string()),
                            Value::Array(vec![
                                Value::SimpleString("code".to_string()),
                                Value::Int(1),
                            ]),
                        )])]),
                    ),
                    (
                        Value::SimpleString("fake-key".to_string()),
                        Value::Array(vec![Value::Map(vec![(
                            Value::SimpleString("666-0".to_string()),
                            Value::Array(vec![
                                Value::SimpleString("code".to_string()),
                                Value::Int(666),
                            ]),
                        )])]),
                    ),
                ])])),
            )]);

        // Read pending messages:
        let result: RedsumerResult<(Vec<StreamId>, LatestPendingMessageId)> =
            conn.read_pending_messages(&key, &group, &consumer, latest_pending_message_id, count);

        // Verify the result:
        assert!(result.is_ok());

        let (messages, next_id_to_claim): (Vec<StreamId>, LatestPendingMessageId) = result.unwrap();
        assert!(messages.len().eq(&1));

        assert!(messages[0].id.eq("1-0"));
        assert!(messages[0].map.get("code").unwrap().eq(&Value::Int(1)));

        assert!(next_id_to_claim.eq("1-0"));
    }

    #[test]
    fn test_read_pending_messages_error() {
        // Define the key, group, consumer, latest_pending_message_id, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let latest_pending_message_id = "0-0";
        let count = 2;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XREADGROUP")
                    .arg(
                        &StreamReadOptions::default()
                            .group(group, consumer)
                            .count(count),
                    )
                    .arg("STREAMS")
                    .arg(&[key])
                    .arg(&[latest_pending_message_id]),
                Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "XREADGROUP Error",
                ))),
            )]);

        // Read pending messages:
        let result: RedsumerResult<(Vec<StreamId>, LatestPendingMessageId)> =
            conn.read_pending_messages(&key, &group, &consumer, latest_pending_message_id, count);

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_claim_pending_messages {
    use redis::{cmd, Value};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_claim_pending_messages_with_zero_count() {
        // Define the key, group, consumer, min_idle_time, next_id_to_claim, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let min_idle_time = 1000;
        let next_id_to_claim = "0-0";
        let count = 0;

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![]);

        // Claim pending messages:
        let result: RedisResult<(Vec<StreamId>, NextIdToClaim)> = conn.claim_pending_messages(
            &key,
            &group,
            &consumer,
            min_idle_time,
            next_id_to_claim,
            count,
        );

        // Verify the result:
        assert!(result.is_ok());

        let (messages, next_id_to_claim): (Vec<StreamId>, NextIdToClaim) = result.unwrap();
        assert!(messages.is_empty());
        assert!(next_id_to_claim.eq(BEGINNING_OF_TIME_ID));
    }

    #[test]
    fn test_claim_pending_messages_empty() {
        // Define the key, group, consumer, min_idle_time, next_id_to_claim, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let min_idle_time = 1000;
        let next_id_to_claim = "0-0";
        let count = 2;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XAUTOCLAIM")
                    .arg(key)
                    .arg(group)
                    .arg(consumer)
                    .arg(min_idle_time)
                    .arg(next_id_to_claim)
                    .arg(&StreamAutoClaimOptions::default().count(count)),
                Ok(Value::Array(vec![
                    Value::SimpleString("0-0".to_string()),
                    Value::Array(vec![]),
                    Value::Array(vec![]),
                ])),
            )]);

        // Claim pending messages:
        let result: RedisResult<(Vec<StreamId>, NextIdToClaim)> = conn.claim_pending_messages(
            &key,
            &group,
            &consumer,
            min_idle_time,
            next_id_to_claim,
            count,
        );

        // Verify the result:
        assert!(result.is_ok());

        let (messages, next_id_to_claim): (Vec<StreamId>, NextIdToClaim) = result.unwrap();
        assert!(messages.len().eq(&0));
        assert!(next_id_to_claim.eq(BEGINNING_OF_TIME_ID));
    }

    #[test]
    fn test_claim_pending_messages_ok() {
        // Define the key, group, consumer, min_idle_time, next_id_to_claim, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let min_idle_time = 1000;
        let next_id_to_claim = "0-0";
        let count = 2;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XAUTOCLAIM")
                    .arg(key)
                    .arg(group)
                    .arg(consumer)
                    .arg(min_idle_time)
                    .arg(next_id_to_claim)
                    .arg(&StreamAutoClaimOptions::default().count(count)),
                Ok(Value::Array(vec![
                    Value::SimpleString("1-0".to_string()),
                    Value::Array(vec![Value::Array(vec![
                        Value::SimpleString("1-0".to_string()),
                        Value::Array(vec![Value::SimpleString("code".to_string()), Value::Int(1)]),
                    ])]),
                    Value::Array(vec![]),
                ])),
            )]);

        // Claim pending messages:
        let result: RedisResult<(Vec<StreamId>, NextIdToClaim)> = conn.claim_pending_messages(
            &key,
            &group,
            &consumer,
            min_idle_time,
            next_id_to_claim,
            count,
        );

        // Verify the result:
        assert!(result.is_ok());

        let (messages, next_id_to_claim): (Vec<StreamId>, NextIdToClaim) = result.unwrap();
        assert!(messages.len().eq(&1));

        assert!(messages[0].id.eq("1-0"));
        assert!(messages[0].map.get("code").unwrap().eq(&Value::Int(1)));

        assert!(next_id_to_claim.eq("1-0"));
    }

    #[test]
    fn test_claim_pending_messages_error() {
        // Define the key, group, consumer, min_idle_time, next_id_to_claim, and count:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let min_idle_time = 1000;
        let next_id_to_claim = "0-0";
        let count = 2;

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XAUTOCLAIM")
                    .arg(key)
                    .arg(group)
                    .arg(consumer)
                    .arg(min_idle_time)
                    .arg(next_id_to_claim)
                    .arg(&StreamAutoClaimOptions::default().count(count)),
                Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "XAUTOCLAIM Error",
                ))),
            )]);

        // Claim pending messages:
        let result: RedisResult<(Vec<StreamId>, NextIdToClaim)> = conn.claim_pending_messages(
            &key,
            &group,
            &consumer,
            min_idle_time,
            next_id_to_claim,
            count,
        );

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_if_is_still_mine {
    use redis::{cmd, Value};
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_is_still_mine_true() {
        // Define the key, group, consumer, and id:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let id = "1-0";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XPENDING")
                    .arg(key)
                    .arg(group)
                    .arg(&[id])
                    .arg(&[id])
                    .arg(1)
                    .arg(consumer),
                Ok(Value::Array(vec![Value::Array(vec![
                    Value::BulkString(b"1526984818136-0".to_vec()),
                    Value::BulkString(b"consumer-123".to_vec()),
                    Value::Int(196415),
                    Value::Int(1),
                ])])),
            )]);

        // Verify if the message is still in the consumer pending list:
        let result: RedsumerResult<bool> = conn.is_still_mine(key, group, consumer, id);

        // Verify the result:
        assert!(result.is_ok());
        // TODO! assert!(result.unwrap());
    }

    #[test]
    fn test_is_still_mine_false() {
        // Define the key, group, consumer, and id:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";
        let id = "1-0";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XPENDING")
                    .arg(key)
                    .arg(group)
                    .arg(&[id])
                    .arg(&[id])
                    .arg(1)
                    .arg(consumer),
                Ok(Value::Array(vec![])),
            )]);

        // Verify if the message is still in the consumer pending list:
        let result: RedsumerResult<bool> = conn.is_still_mine(key, group, consumer, id);

        // Verify the result:
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_is_still_mine_error() {
        // Define the key, group, consumer, and id:
        let key = "my-key";
        let group = "my-group";
        let consumer = "my-consumer";

        // Create a mock connection:
        let mut conn: MockRedisConnection =
            MockRedisConnection::new(vec![MockCmd::new::<_, Value>(
                cmd("XPENDING")
                    .arg(key)
                    .arg(group)
                    .arg(&["1-0"])
                    .arg(&["1-0"])
                    .arg(1)
                    .arg(consumer),
                Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "XPENDING Error",
                ))),
            )]);

        // Verify if the message is still in the consumer pending list:
        let result: RedsumerResult<bool> = conn.is_still_mine(key, group, consumer, "1-0");

        // Verify the result:
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod test_ack {
    use redis::cmd;
    use redis_test::{MockCmd, MockRedisConnection};

    use super::*;

    #[test]
    fn test_ack_ok_true() {
        // Define the key, group, and id:
        let key = "my-key";
        let group = "my-group";
        let id: &str = "1-0";

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![MockCmd::new::<_, i64>(
            cmd("XACK").arg(key).arg(group).arg(&[id]),
            Ok(1),
        )]);

        // Acknowledge the message:
        let result: RedsumerResult<bool> = conn.ack(key, group, id);

        // Verify the result:
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_ack_ok_false() {
        // Define the key, group, and id:
        let key = "my-key";
        let group = "my-group";
        let id = "1-0";

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![MockCmd::new::<_, i64>(
            cmd("XACK").arg(key).arg(group).arg(&[id]),
            Ok(0),
        )]);

        // Acknowledge the message:
        let result: RedsumerResult<bool> = conn.ack(key, group, id);

        // Verify the result:
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_ack_error() {
        // Define the key, group, and id:
        let key = "my-key";
        let group = "my-group";
        let id = "1-0";

        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![MockCmd::new::<_, i64>(
            cmd("XACK").arg(key).arg(group).arg(&[id]),
            Err(RedisError::from((ErrorKind::ResponseError, "XACK Error"))),
        )]);

        // Acknowledge the message:
        let result: RedsumerResult<bool> = conn.ack(key, group, id);

        // Verify the result:
        assert!(result.is_err());
    }
}
