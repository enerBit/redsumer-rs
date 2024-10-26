//! A lightweight implementation of Redis Streams for Rust, allowing you to manage streaming messages in a simplified way. With redsumer you can:
//!
//! - **Produce** new messages in a specific *stream*.
//! - **Consume** messages from specific *stream*, setting config parameters that allow you a flexible implementation. It also provides an option to minimize the possibility of two or more consumers from the same consumer group consuming the same message simultaneously.
//!
//! To use ***redsumer*** from GitHub repository with specific version, set the dependency in Cargo.toml file as follows:
//!
//! ```ini
//! [dependencies]
//! redsumer = { git = "https://github.com/enerBit/redsumer-rs.git", package = "redsumer", version = "0.5.0-beta.1" }
//! ```
//!
//! You can depend on it via cargo by adding the following dependency to your `Cargo.toml` file:
//!
//! ```ini
//! [dependencies]
//! redsumer = { version = "0.5.0-beta.1" }
//! ```
//!
//! ## Basic Usage
//!
//! #### Produce a new stream message:
//!
//! Create a new producer instance and produce a new stream message from a [BTreeMap](`std::collections::BTreeMap`):
//!
//! ```rust,no_run
//! use std::collections::BTreeMap;
//!
//! use redsumer::*;
//! use time::OffsetDateTime;
//! use uuid::Uuid;
//!
//! #[tokio::main]
//! async fn main() {
//!     let credentials: Option<ClientCredentials> = None;
//!     let host: &str = "localhost";
//!     let port: u16 = 6379;
//!     let db: i64 = 0;
//!     let stream_name: &str = "my-stream";
//!
//!     let args: ClientArgs = ClientArgs::new(
//!         credentials,
//!         host,
//!         port,
//!         db,
//!         CommunicationProtocol::RESP2,
//!     );
//!
//!     let config: ProducerConfig = ProducerConfig::new(stream_name);
//!
//!     let producer_result: RedsumerResult<Producer> =
//!         Producer::new(
//!             &args,
//!             &config,
//!         );
//!
//!     let producer: Producer = producer_result.unwrap_or_else(|error| {
//!         panic!("Error creating a new RedsumerProducer instance: {:?}", error);
//!     });
//!
//!     let mut message_1: BTreeMap<&str, String> = BTreeMap::new();
//!     message_1.insert("id", Uuid::new_v4().to_string());
//!     message_1.insert("started_at", OffsetDateTime::now_utc().to_string());
//!
//!     let mut message_2: Vec<(String, String)> = Vec::new();
//!     message_2.push(("id".to_string(), Uuid::new_v4().to_string()));
//!     message_2.push(("started_at".to_string(), OffsetDateTime::now_utc().to_string()));
//!
//!     let id_1: Id = producer.produce_from_map(message_1).await.unwrap_or_else(|error| {
//!         panic!("Error producing stream message from BTreeMap: {:?}", error.to_string());
//!     });
//!
//!     let id_2: Id = producer.produce_from_items(message_2).await.unwrap_or_else(|error| {
//!         panic!("Error producing stream message from Vec: {:?}", error.to_string());
//!     });
//!
//!     println!("Message 1 produced with id: {:?}", id_1);
//!     println!("Message 2 produced with id: {:?}", id_2);
//! }
//! ```
//!
//! Similar to the previous example, you can produce a message from a [HashMap](std::collections::HashMap) or a [HashSet](std::collections::HashSet). Go to [examples](https://github.com/enerBit/redsumer-rs/tree/main/examples) directory to see more  use cases like producing a stream message from an instance of a struct.
//!
//! The [produce_from_map](Producer::produce_from_map) and [produce_from_items](Producer::produce_from_items) methods accepts generic types that implements the [ToRedisArgs](redis::ToRedisArgs) trait. Take a look at the documentation for more information.
//!
//! #### Consume messages from a stream:
//!
//! Create a new consumer instance and consume messages from stream:
//!
//! ```rust,no_run
//! use redsumer::*;
//! use redsumer::redis::StreamId;
//!
//! #[tokio::main]
//! async fn main() {
//!     let credentials: Option<ClientCredentials> = None;
//!     let host: &str = "localhost";
//!     let port: u16 = 6379;
//!     let db: i64 = 0;
//!     let stream_name: &str = "my-stream";
//!     let group_name: &str = "group-name";
//!     let consumer_name: &str = "consumer";
//!     let initial_stream_id: &str = "0-0";
//!     let min_idle_time_milliseconds: usize = 1000;
//!     let new_messages_count: usize = 3;
//!     let pending_messages_count: usize = 2;
//!     let claimed_messages_count: usize = 1;
//!     let block: usize = 5;
//!
//!     let args: ClientArgs = ClientArgs::new(
//!         credentials,
//!         host,
//!         port,
//!         db,
//!         CommunicationProtocol::RESP2,
//!     );
//!
//!     let config: ConsumerConfig = ConsumerConfig::new(
//!         stream_name,
//!         group_name,
//!         consumer_name,
//!         ReadNewMessagesOptions::new(
//!             new_messages_count,
//!             block
//!         ),
//!         ReadPendingMessagesOptions::new(
//!             pending_messages_count
//!         ),
//!         ClaimMessagesOptions::new(
//!             claimed_messages_count,
//!             min_idle_time_milliseconds
//!         ),
//!     );
//!
//!     let consumer_result: RedsumerResult<Consumer> = Consumer::new(
//!         args,
//!         config,
//!         Some(initial_stream_id.to_string()),
//!     );
//!
//!     let mut consumer: Consumer = consumer_result.unwrap_or_else(|error| {
//!         panic!("Error creating a new RedsumerConsumer instance: {:?}", error);
//!     });
//!
//!     loop {
//!         let messages: Vec<StreamId> = consumer.consume().await.unwrap_or_else(|error| {
//!             panic!("Error consuming messages from stream: {:?}", error);
//!         });
//!
//!         for message in messages {
//!             if consumer.is_still_mine(&message.id).unwrap_or_else(|error| {
//!                 panic!(
//!                     "Error checking if message is still in consumer pending list: {:?}", error
//!                 );
//!             }) {
//!                 // Process message ...
//!                 println!("Processing message: {:?}", message);
//!                 // ...
//!
//!                 let ack: bool = consumer.ack(&message.id).await.unwrap_or_else(|error| {
//!                     panic!("Error acknowledging message: {:?}", error);
//!                 });
//!
//!                 if ack {
//!                      println!("Message acknowledged: {:?}", message);
//!                 }
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! In this example, the [consume](Consumer::consume) method is called in a loop to consume messages from the stream.
//! The [consume](Consumer::consume) method returns a vector of [StreamId](redis::StreamId) instances. Each [StreamId](redis::StreamId) instance represents a message in the stream.
//! The [is_still_mine](Consumer::is_still_mine) method is used to check if the message is still in the consumer pending list.
//! If it is, the message is processed and then acknowledged using the [ack](Consumer::ack) method.
//! The [ack](Consumer::ack) method returns a boolean indicating if the message was successfully acknowledged.
//!
//! The main objective of this message consumption strategy is to minimize the possibility that two or more consumers from the same consumer group operating simultaneously consume the same message at the same time.
//! Knowing that it is a complex problem with no definitive solution, including business logic in the message processing instance will always improve results.
//!
//! Take a look at the [examples](https://github.com/enerBit/redsumer-rs/tree/main/examples) directory to see more use cases.
//!
//! #### Utilities from [redis] crate:
//!
//! The [redis] module provides utilities from the [redis](https://docs.rs/redis) crate. You can use these utilities to interact with Redis values and errors.
//!
//! #### Unwrap [Value](redis::Value) to a specific type:
//!
//! The [Value](redis::Value) enum represents a Redis value. It can be converted to a specific type using the [from_redis_value](redis::from_redis_value) function. This function can be imported from the [redis] module.
//!
//! ## Contributing
//!
//! We welcome contributions to `redsumer-rs`. Here are some ways you can contribute:
//!
//! - **Bug Reports**: If you find a bug, please create an issue detailing the problem, the steps to reproduce it, and the expected behavior.
//! - **Feature Requests**: If you have an idea for a new feature or an enhancement to an existing one, please create an issue describing your idea.
//! - **Pull Requests**: If you've fixed a bug or implemented a new feature, we'd love to see your work! Please submit a pull request. Make sure your code follows the existing style and all tests pass.
//!
//! Thank you for your interest in improving `redsumer-rs`!
mod core;
mod redsumer;

pub use core::{
    client::{ClientArgs, ClientCredentials, CommunicationProtocol},
    result::{RedsumerError, RedsumerResult},
    streams::types::Id,
};
pub use redsumer::consumer::{
    ClaimMessagesOptions, Consumer, ConsumerConfig, ReadNewMessagesOptions,
    ReadPendingMessagesOptions,
};
pub use redsumer::producer::{Producer, ProducerConfig};

pub mod redis {
    //! Utilities from [redis] crate.
    pub use redis::streams::StreamId;
    pub use redis::{from_redis_value, ErrorKind, FromRedisValue, RedisError, ToRedisArgs, Value};
}
