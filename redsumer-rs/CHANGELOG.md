# Changelog 📘💜

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added:

- ⚡ Implement `belongs_to_me()` in `IsStillMineReply` to verify if the message is still in consumer pending list. Function `is_still_mine()` was deprecated. By [@JMTamayo](https://github.com/JMTamayo).

### Changed:

- 🚀 Unify *Unit Tests* and *Coverage* steps in CI pipeline. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 Making the *Checkout* step the first step in the pipeline to ensure that the code is available prior to the execution of the CI flow. By [@JMTamayo](https://github.com/JMTamayo).

## ✨ v0.5.0 [2024-10-26]

### Added:

- ⚡ Implement `Debug` for `ClientCredentials`. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `CommunicationProtocol` type to define Redis Protocol version. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `ClientArgs` and `RedisClientBuilder` to build Redis Client. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `VerifyConnection` trait and `ping()` function to verify connection to Redis Server. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `produce_from_map()`, `produce_from_items()` and `ProducerCommands` in producer core module.  By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `ProducerConfig` to manage the configuration parameters for `Producer`. Implement `ClientArgs` in  `Producer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `ProduceMessageReply` to handle the response from `produce_from_map()` and `produce_from_items()` functions in `Producer`. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `ConsumerConfig` to manage the configuration parameters for `Consumer`. Implement `ClientArgs` in  `Consumer`. Implement `ReadNewMessagesOptions` , `ReadPendingMessagesOptions`  and `ClaimMessagesOptions`  in  `ConsumerConfig` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement types `LastDeliveredMilliseconds` and`TotalTimesDelivered` to handle the response from the `is_still_mine()` core function. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `ConsumeMessagesReply` to handle the response from `consume()` function in `Consumer`. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `AckMessageReply` to handle the response from `ack()` function in `Consumer`. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Implement `IsStillMineReply` to handle the response from `is_still_mine()` function in `Consumer`. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ Refactor in the import of library modules: The prelude, consumer, producer and client modules were implemented **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).

### Changed:

- 🚀 Rename `RedsumerProducer` to `Producer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 Include minimum line coverage target as a env variable in CI pipeline. By [@JMTamayo](https://github.com/JMTamayo).

### Removed:

- ❌ Remove `FromRedisValueHandler` from crate **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- ❌ Remove internal function `get_redis_client()` from client module. By [@JMTamayo](https://github.com/JMTamayo).
- ❌ Remove step `Upload coverage to Codecov` from CI pipeline. By [@JMTamayo](https://github.com/JMTamayo).

## ✨ v0.4.1 [2024-06-13]

### Fixed:

- 🛠 Fixing BUG reported in [issue #15](https://github.com/enerBit/redsumer-rs/issues/15) with arguments in function xclaim. By [@JMTamayo](https://github.com/JMTamayo).

## ✨ v0.4.0 [2024-04-23]

### Added:

- ⚡ Implementation of new types: `RedsumerResult`, `RedsumerError` and `Id` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ `Debug` and `Clone` implementation in `RedsumerProducer` and `RedsumerConsumer`. By [@JMTamayo](https://github.com/JMTamayo).
- ⚡ The consumer configuration parameters were implemented directly in `RedsumerConsumer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).

### Fixed:

- 🛠 General refactoring of the package in order to improve performance. By [@JMTamayo](https://github.com/JMTamayo).

### Changed:

- 🚀 New project structure as workspace. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 Update dependencies and documentation. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 Library modules reorganization **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 `FromRedisValueImplHandler` was changed to `FromRedisValueHandler` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 The `produce_from_map()` method was replaced by the `produce()` method in `RedsumerProducer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 The `validate_pending_message_ownership()` method was replaced by `is_still_mine()` in `RedsumerConsumer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 The acknowledge method was replaced by ack in `RedsumerConsumer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- 🚀 The consume method was refactored in `RedsumerConsumer` in order to implement a new consumption methodology that allows scalability in distributed systems. To understand this new implementation in detail, take a look at the project https://github.com/elpablete/refactored-computing-machine. By [@JMTamayo](https://github.com/JMTamayo).

### Removed:

- ❌ The *stream_information.rs* module was removed from the project: `StreamInfo` and `StreamConsumersInfo` implementations were removed **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- ❌ `RedsumerConsumerOptions` was removed **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).
- ❌ The `produce_from_items()` method was removed from `RedsumerProducer` **[BreakingChange]**. By [@JMTamayo](https://github.com/JMTamayo).