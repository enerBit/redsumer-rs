//! A **lightweight implementation** of **Redis Streams** for Rust, that allows you managing streaming messages in an easy way.
//!
//! With redsumer-rs you can:
//! - **Produce** stream messages in specific *database* and *stream name*.
//! - **Consume** stream messages from specific *stream name* and *consumer group*, setting config parameters like *min idle time*, *first id*, *max number of messages to consume* and more. Also you can set message as consumed for specific *consumer group*.
//!
//! To use redsumer-rs from github repository, set the dependency in Cargo.toml file as follows:
//!
//! ```ini
//! [dependencies]
//! redsumer-rs = {git = "https://github.com/enerBit/redsumer-rs"}
//! ```
//!
//! If you need to use an specific version, set the dependency in Cargo.toml file as follows:
//!
//!  ```ini
//! [dependencies]
//! redsumer-rs = {git = "https://github.com/enerBit/redsumer-rs", version = "*"}
//! ```
//!
//! For more dependency options from git, check section [3.0 Cargo Reference](https://doc.rust-lang.org/cargo/reference/index.html) from `The Cargo Book`.
//!

mod redsumer;
mod tests;

/// Structs and traits from crate [redis](https://docs.rs/redis/0.23.3/redis) and its module [redis::streams](https://docs.rs/redis/0.23.3/redis/streams). Traits to get streams information.
pub mod streams {
    pub use redis::streams::{
        StreamId, StreamInfoConsumer, StreamInfoConsumersReply, StreamInfoGroup,
        StreamInfoGroupsReply, StreamInfoStreamReply,
    };
    pub use redis::{
        from_redis_value, ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value,
    };

    pub mod info {
        // Utilities to get stream, group and consumers information.
        pub use crate::redsumer::stream_information::{StreamConsumersInfo, StreamInfo};
    }
}

/// Redis client configuration parameters
pub mod client {
    pub use crate::redsumer::client::ClientCredentials;
}

/// Redis streams producer handler
pub mod producer {
    pub use crate::redsumer::producer::*;
}

/// Redis streams consumer handler
pub mod consumer {
    pub use crate::redsumer::consumer::*;
}
