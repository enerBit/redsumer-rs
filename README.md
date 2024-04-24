# redsumer-rs

A **lightweight implementation** of **Redis Streams** for Rust, that allows you managing streaming messages in an easy way.
With `redsumer-rs` you can:
- **Produce** new messages in a specific *stream*.
- **Consume** messages from specific *stream*, setting config parameters that allow you a flexible implementation.

For more dependency options from git, check section [3.0 Cargo Reference](https://doc.rust-lang.org/cargo/reference/index.html) from `The Cargo Book`.

## Basic Usage

### Producer:

Create a new producer instance:

```rust,no_run
    use redsumer::*;
    
    let credentials: Option<ClientCredentials> = None;
    let host: &str = "localhost";
    let port: &str = "6379";
    let db: &str = "0";
    let stream_name: &str = "my-stream";

    let producer_result: RedsumerResult<RedsumerProducer> =
        RedsumerProducer::new(
            credentials,
            host,
            port,
            db,
            stream_name,
        );

    let producer: RedsumerProducer = producer_result.unwrap();
```

### Consumer:

Create a new consumer instance:

```rust,no_run
    use redsumer::*;

    let credentials: Option<ClientCredentials> = None;
    let host: &str = "localhost";
    let port: &str = "6379";
    let db: &str = "0";
    let stream_name: &str = "my-stream";
    let group_name: &str = "group-name";
    let consumer_name: &str = "consumer";
    let since_id: &str = "0-0";
    let min_idle_time_milliseconds: usize = 1000;
    let new_messages_count: usize = 3;
    let pending_messages_count: usize = 2;
    let claimed_messages_count: usize = 1;
    let block: u8 = 5;

    let consumer_result: RedsumerResult<RedsumerConsumer> = RedsumerConsumer::new(
        credentials,
        host,
        port,
        db,
        stream_name,
        group_name,
        consumer_name,
        since_id,
        min_idle_time_milliseconds,
        new_messages_count,
        pending_messages_count,
        claimed_messages_count,
        block,
    );

    let mut consumer: RedsumerConsumer = consumer_result.unwrap();
```

## Contributing

We welcome contributions to `redsumer-rs`. Here are some ways you can contribute:

- **Bug Reports**: If you find a bug, please create an issue detailing the problem, the steps to reproduce it, and the expected behavior.
- **Feature Requests**: If you have an idea for a new feature or an enhancement to an existing one, please create an issue describing your idea.
- **Pull Requests**: If you've fixed a bug or implemented a new feature, we'd love to see your work! Please submit a pull request. Make sure your code follows the existing style and all tests pass.

Thank you for your interest in improving `redsumer-rs`!