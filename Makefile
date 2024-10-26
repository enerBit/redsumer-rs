fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

check:
	cargo check --all-features

clippy-check:
	cargo clippy --all-features

install-llvm-cov:
	cargo install cargo-llvm-cov

test-llvm-cov-report:
	cargo llvm-cov --html --workspace --all-features

test-llvm-cov-target:
	cargo llvm-cov --workspace --all-features --summary-only --fail-under-lines 70

test:
	cargo test --all-features

doc:
	cargo doc --all-features

doc-open:
	cargo doc --all-features --open