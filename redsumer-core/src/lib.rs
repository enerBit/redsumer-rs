//! This package contains the core functionality of *redsumer*, whose process focuses on consuming messages from a stream, reducing the probability that two or more consumers from the same group process the same message successfully, incurring major errors in the implementation.
mod redsumer_core;
pub use redsumer_core::*;
