# Contributing to dox-http-native

Thank you for helping Dox. The full guide is here:
https://dartondox.dev/latest/community/contribute/

This repo is a Dart pub workspace. The packages are in `packages/`:
`dox_http_native` (the server) in `packages/dox-http-native` and
`dox_http_client` (the client) in `packages/dox-http-client`. Run
`dart pub get` once at the root.

## Bugs and feature requests

Use GitHub issues: https://github.com/dartondox/dox-http-native/issues

For security problems, see [SECURITY.md](SECURITY.md). Do not open a public issue.

## Pull requests

Send changes as pull requests against `main`. Before you ask for review:

- CI is green.
- In the `native/` folder of each package that you changed: `cargo fmt`, `cargo clippy --release --all-targets -- -D warnings` and `cargo test` pass.
- `dart format` is clean (run it at the root).
- `dart analyze --fatal-infos` has no warnings (run it at the root).
- Add a changelog entry under `## Unreleased` in the `CHANGELOG.md` of the package.
- Do not change versions in the PR.
- After a change of `packages/dox-http-client/native/Cargo.toml` or `Cargo.lock`, run `dart run tool/third_party_notices.dart` in `packages/dox-http-client` (CI checks `THIRD_PARTY_NOTICES`).

## Tests

Every new feature and every bug fix must add tests.

In each package, run the Rust tests with `cargo test` in `native/`. Run the Dart tests with `dart test --concurrency=1` in the package folder. Run `cargo build --release` in `native/` first, so the build hook uses your local build. CI runs all tests on every push and pull request.

The Dart tests of `dox_http_client` need `openssl` on the PATH.

## Releases

Each package has its own version and its own tags: `<package>-v<version>`,
for example `dox_http_native-v3.2.4` or `dox_http_client-v3.2.0`. A tag on
`main` runs `.github/workflows/release.yaml`. It runs the tests first, and
publishes the package to pub.dev only when they pass.

Releases of `dox_http_native` before the workspace have the tag `v<version>`.
