use redis::RedisError;

/// Error type for *redsumer* operations, it is an alias for [`RedisError`].
pub type RedsumerError = RedisError;

/// Result type for *redsumer* operations.
pub type RedsumerResult<T> = Result<T, RedsumerError>;
