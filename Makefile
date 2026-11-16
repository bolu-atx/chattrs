# Local gates. `make ci` runs what CI runs.
.PHONY: check fmt test ci

# Lint rules: formatting and clippy. Changes no files.
check:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

# Rewrite the source with rustfmt.
fmt:
	cargo fmt

# stdin is closed: one unit test reads stdin and would wait on a terminal.
test:
	cargo test < /dev/null

ci: check test
