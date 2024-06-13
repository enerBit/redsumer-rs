# Changelog 📘💜

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## ✨ v0.4.1 [2024-06-13]

### Fixed:

- 🛠 Fixing BUG reported in [issue #15](https://github.com/enerBit/redsumer-rs/issues/15) with arguments in function xclaim.

## ✨ v0.4.0 [2024-04-23]

### Added:

- ⚡ Implementation of new types: RedsumerResult, RedsumerError and Id (Breaking change).
- ⚡ Debug and Clone implementation in RedsumerProducer and RedsumerConsumer.
- ⚡ The consumer configuration parameters were implemented directly in RedsumerConsumer (Breaking change).

### Fixed:

- 🛠 General refactoring of the package in order to improve performance.

### Changed:

- 🚀 New project structure as workspace.
- 🚀 Update dependencies and documentation.
- 🚀 Library modules reorganization (Breaking change).
- 🚀 FromRedisValueImplHandler was changed to FromRedisValueHandler (Breaking change).
- 🚀 The produce_from_map method was replaced by the produce method in RedsumerProducer (Breaking change).
- 🚀 The validate_pending_message_ownership method was replaced by is_still_mine in RedsumerConsumer (Breaking change).
- 🚀 The acknowledge method was replaced by ack in RedsumerConsumer (Breaking change).
- 🚀 The consume method was refactored in RedsumerConsumer in order to implement a new consumption methodology that allows scalability in distributed systems. To understand this new implementation in detail, take a look at the project https://github.com/elpablete/refactored-computing-machine.

### Removed:

- ❌ The stream_information.rs module was removed from the project: StreamInfo and StreamConsumersInfo implementations were removed (Breaking change).
- ❌ RedsumerConsumerOptions was removed (Breaking change).
- ❌ The produce_from_items method was removed from RedsumerProducer (Breaking change).