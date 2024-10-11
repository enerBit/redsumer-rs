/// Stream message identifier. It is used to identify any message in a stream.
pub type Id = String;

/// Represents the latest message ID that is pending to be processed. It is used to the read pending messages operation.
pub type LatestPendingMessageId = Id;

/// Represents the next message ID to claim. It is used to the claim messages operation.
pub type NextIdToClaim = Id;
