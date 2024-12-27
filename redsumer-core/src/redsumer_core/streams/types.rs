//! Types used in the streams module for responses and requests.
/// Stream message identifier. It is used to identify any message in a stream.
pub type Id = String;

/// Represents the latest message ID that is pending to be processed. It is used to the read pending messages operation.
pub type LatestPendingMessageId = Id;

/// Represents the next message ID to claim. It is used to the claim messages operation.
pub type NextIdToClaim = Id;

/// Represents the total time in milliseconds that elapsed since the last message was delivered to the consumer.
pub type LastDeliveredMilliseconds = usize;

/// Represents the total number of times that a message was delivered to any consumer in the group.
pub type TotalTimesDelivered = usize;
