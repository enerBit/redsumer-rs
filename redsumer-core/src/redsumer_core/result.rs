use redis::RedisError;

/// Error type for *redsumer* operations.
pub type RedsumerError = RedisError;

/// Result type for *redsumer* operations.
pub type RedsumerResult<T> = Result<T, RedsumerError>;
