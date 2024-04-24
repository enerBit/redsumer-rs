# redsumer-rs

A **lightweight implementation** of **Redis Streams** for Rust, that allows you managing streaming messages in an easy way.
With `redsumer-rs` you can:
- **Produce** new messages in a specific *stream*.
- **Consume** messages from specific *stream*, setting config parameters that allow you a flexible implementation.

For more dependency options from git, check section [3.0 Cargo Reference](https://doc.rust-lang.org/cargo/reference/index.html) from `The Cargo Book`.

## Basic Usage

### Producer:

Create a message producer and send an event:

```rust
    use std::collections::HashMap;
    use redsumer_rs::{RedsumerProducer, RedsumerResult, Id};

    let producer_result: RedsumerResult<RedsumerProducer> =
        RedsumerProducer::new(
            None,
            "localhost",
            "6379",
            "0",
            "my-stream"
        );

    let producer: RedsumerProducer = producer_result.unwrap();

    let message: HashMap<String, String> = [("key".to_string(), "value".to_string())]
        .iter()
        .cloned()
        .collect();

    let msg_result: RedsumerResult<Id> = producer.produce(message).await;
    let id: Id = msg_result.unwrap();
```

### Consumer:

Create a stream consumer and consume messages:

```rust
    use redsumer::{RedsumerConsumer, RedsumerResult};
    use redsumer::redis::StreamId;

    let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
        None,
        "localhost",
        "6379",
        "0",
        "my-stream",
        "group-name",
        "consumer",
        "0-0",
        1000,
        10,
        10,
        30,
        5,
    );

    let mut consumer: RedsumerConsumer = consumer_result.unwrap();

    let messages_result: RedsumerResult<Vec<StreamId>> = consumer.consume().await;
    let messages: Vec<StreamId> = messages_result.unwrap();
```

## Contributing

We welcome contributions to `redsumer-rs`. Here are some ways you can contribute:

- **Bug Reports**: If you find a bug, please create an issue detailing the problem, the steps to reproduce it, and the expected behavior.
- **Feature Requests**: If you have an idea for a new feature or an enhancement to an existing one, please create an issue describing your idea.
- **Pull Requests**: If you've fixed a bug or implemented a new feature, we'd love to see your work! Please submit a pull request. Make sure your code follows the existing style and all tests pass.

Thank you for your interest in improving `redsumer-rs`!