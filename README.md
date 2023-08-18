# redsumer-rs

A **lightweight implementation** of **Redis Streams** for Rust, that allows you managing streaming messages in an easy way.
With redsumer-rs you can:
- **Produce** stream messages in specific *database* and *stream name*.
- **Consume** stream messages from specific *stream name* and *consumer group*, setting config parameters like *min idle time*, *fisrt id*, *max number of messages to consume* and more. Also you can set message as consumed for specific *consumer group*.
To use redsumer-rs from github repository, set the dependency in Cargo.toml file as follows:

```ini
[dependencies]
redsumer-rs = {git = "https://github.com/enerBit/redsumer-rs"}
```

If you need to use an specific version, set the dependency in Cargo.toml file as follows:
 ```ini
[dependencies]
redsumer-rs = {git = "https://github.com/enerBit/redsumer-rs", version = "0.2.2"}
```

For more dependency options from git, check section [3.0 Cargo Reference](https://doc.rust-lang.org/cargo/reference/index.html) from `The Cargo Book`.
