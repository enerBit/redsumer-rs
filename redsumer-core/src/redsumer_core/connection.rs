//! Server connection utilities.
use redis::{Commands, ErrorKind, RedisError, RedisResult};
use tracing::{debug, error};

#[allow(unused_imports)]
use crate::redsumer_core::result::{RedsumerError, RedsumerResult};

/// Verify the connection to the server.
///
/// This method sends a `PING` command to the server to verify the connection and returns `PONG` if the connection was verified successfully.
fn ping<C>(c: &mut C) -> RedisResult<String>
where
    C: Commands,
{
    match c.check_connection() {
        true => {
            debug!("The connection to the server was verified");
            Ok("PONG".into())
        }
        false => {
            let e: &str = "The connection to the server could not be verified. Please verify the client configuration or server availability";
            error!(e);
            Err(RedisError::from((ErrorKind::ClientError, e)))
        }
    }
}

/// A trait to verify the connection to the server.
pub trait VerifyConnection {
    /// Verify the connection to the server.
    ///
    /// # Arguments:
    /// - No arguments.
    ///
    /// # Returns:
    /// A [`RedsumerResult`] with a [`String`] equal to `PONG` if the connection was verified successfully. Otherwise, a [`RedsumerError`] is returned.
    fn ping(&mut self) -> RedsumerResult<String>;
}

impl<C> VerifyConnection for C
where
    C: Commands,
{
    fn ping(&mut self) -> RedsumerResult<String> {
        ping(self)
    }
}

#[cfg(test)]
mod test_connection {
    use redis::Client;
    use redis_test::MockRedisConnection;

    use super::*;

    #[test]
    fn test_ping_ok() {
        // Create a mock connection:
        let mut conn: MockRedisConnection = MockRedisConnection::new(vec![]);

        // Ping the server:
        let ping_result: RedsumerResult<String> = conn.ping();

        // Verify the connection to the server:
        assert!(ping_result.is_ok());
        assert_eq!(ping_result.unwrap(), "PONG".to_string());
    }

    #[test]
    fn test_ping_error() {
        // Create a client from a fake host:
        let mut client: Client = Client::open("redis://fakehost/0").unwrap();

        // Ping the server:
        let ping_result: RedsumerResult<String> = client.ping();

        // Verify the connection to the server:
        assert!(ping_result.is_err());
        assert_eq!(ping_result.unwrap_err().to_string(), "The connection to the server could not be verified. Please verify the client configuration or server availability- ClientError");
    }
}
