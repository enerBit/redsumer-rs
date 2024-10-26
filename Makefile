fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy-check:
	cargo clippy --all-features

install-llvm-cov:
	cargo install cargo-llvm-cov

test-llvm-cov:
	cargo llvm-cov --workspace --all-features --summary-only --fail-under-lines 80

test:
	cargo test --all-features

doc:
	cargo doc --all-features

doc-open:
	cargo doc --all-features --open