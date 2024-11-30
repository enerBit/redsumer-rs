fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

check:
	cargo check --workspace --all-features

clippy-check:
	cargo clippy --workspace --all-features

install-llvm-cov:
	cargo install cargo-llvm-cov

test-llvm-cov-report:
	cargo llvm-cov --workspace --all-features --show-missing-lines --open

test-llvm-cov-target:
	cargo llvm-cov --workspace --all-features --show-missing-lines --summary-only --fail-under-lines 80

install-cargo-audit:
	cargo install cargo-audit

audit:
	cargo audit --json

install-cargo-deny:
	cargo install cargo-deny

deny-check:
	cargo deny --log-level debug check

test-doc:
	cargo test --workspace --all-features --doc

doc:
	cargo doc --workspace --all-features --document-private-items --verbose --open

install-yamlfmt:
	go install github.com/google/yamlfmt/cmd/yamlfmt@latest

yamlfmt:
	yamlfmt .

yamlfmt-check:
	yamlfmt --lint .

verify-project:
	cargo verify-project --verbose