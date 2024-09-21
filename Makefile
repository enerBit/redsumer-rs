fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy-check:
	cargo clippy --all-features

install-llvm-cov:
	cargo install cargo-llvm-cov

test-llvm-cov:
	cargo llvm-cov --html --workspace --all-features

test:
	cargo test --all-features