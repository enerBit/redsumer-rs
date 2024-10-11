# Changelog 📘💜

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## ✨ v0.5.0-beta.1 [2024-09-30]

### Added:

- ⚡ Implement `Debug` for `ClientCredentials`.
- ⚡ Implement `CommunicationProtocol` type to define Redis Protocol version.
- ⚡ Implement `ClientArgs` and `RedisClientBuilder` to build Redis Client.
- ⚡ Implement `VerifyConnection` trait and `ping()` function to verify connection to Redis Server.
- ⚡ Implement `produce_from_map()`, `produce_from_items()` and `ProducerCommands` in producer core module. 
- ⚡ Implement `ProducerConfig` to manage the configuration parameters for `Producer`. Implement `ClientArgs` in  `Producer`. **[BreakingChange]**
- ⚡ Implement `ConsumerConfig` to manage the configuration parameters for `Consumer`. Implement `ClientArgs` in  `Consumer`. Implement `ReadNewMessagesOptions` , `ReadPendingMessagesOptions`  and `ClaimMessagesOptions`  in  `ConsumerConfig` **[BreakingChange]**

### Changed:

- 🚀 Rename `RedsumerProducer` to `Producer`. **[BreakingChange]**

### Removed:

- ❌ Remove `FromRedisValueHandler` from crate. **[BreakingChange]**
- ❌ Remove internal function `get_redis_client()` from client module.

## ✨ v0.4.1 [2024-06-13]

### Fixed:

- 🛠 Fixing BUG reported in [issue #15](https://github.com/enerBit/redsumer-rs/issues/15) with arguments in function xclaim.

## ✨ v0.4.0 [2024-04-23]

### Added:

- ⚡ Implementation of new types: `RedsumerResult`, `RedsumerError` and `Id`. **[BreakingChange]**
- ⚡ `Debug` and `Clone` implementation in `RedsumerProducer` and `RedsumerConsumer`.
- ⚡ The consumer configuration parameters were implemented directly in `RedsumerConsumer`. **[BreakingChange]**

### Fixed:

- 🛠 General refactoring of the package in order to improve performance.

### Changed:

- 🚀 New project structure as workspace.
- 🚀 Update dependencies and documentation.
- 🚀 Library modules reorganization. **[BreakingChange]**
- 🚀 `FromRedisValueImplHandler` was changed to `FromRedisValueHandler`. **[BreakingChange]**
- 🚀 The `produce_from_map()` method was replaced by the `produce()` method in `RedsumerProducer`. **[BreakingChange]**
- 🚀 The `validate_pending_message_ownership()` method was replaced by `is_still_mine()` in `RedsumerConsumer`. **[BreakingChange]**
- 🚀 The acknowledge method was replaced by ack in `RedsumerConsumer`. **[BreakingChange]**
- 🚀 The consume method was refactored in `RedsumerConsumer` in order to implement a new consumption methodology that allows scalability in distributed systems. To understand this new implementation in detail, take a look at the project https://github.com/elpablete/refactored-computing-machine.

### Removed:

- ❌ The *stream_information.rs* module was removed from the project: `StreamInfo` and `StreamConsumersInfo` implementations were removed. **[BreakingChange]**
- ❌ `RedsumerConsumerOptions` was removed. **[BreakingChange]**
- ❌ The `produce_from_items()` method was removed from `RedsumerProducer`. **[BreakingChange]**