use async_trait::async_trait;
use redis::{
    streams::{StreamInfoConsumersReply, StreamInfoGroupsReply, StreamInfoStreamReply},
    RedisError,
};

// Get `stream` and `consumer groups` information.
#[async_trait]
pub trait StreamInfo {
    async fn get_stream_info(&self) -> Result<StreamInfoStreamReply, RedisError>;
    async fn get_consumer_groups_info(&self) -> Result<StreamInfoGroupsReply, RedisError>;
}

// Get `consumers` information.
#[async_trait]
pub trait StreamConsumersInfo {
    async fn get_consumers_info(&self) -> Result<StreamInfoConsumersReply, RedisError>;
}
