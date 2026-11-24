# dox-http-native

Native HTTP packages for Dart. Each package is in `packages/` and has its own
version and its own releases.

| Package | What it is |
| --- | --- |
| [dox_http_native](packages/dox-http-native) ([pub.dev](https://pub.dev/packages/dox_http_native)) | Native HTTP server for Dart. It speaks HTTP/1.1 and HTTP/2, and implements the `dart:io` `HttpServer` API. It is the default HTTP server of [Dox](https://dartondox.dev). |
| [dox_http_client](packages/dox-http-client) ([pub.dev](https://pub.dev/packages/dox_http_client)) | Native HTTP/1.1 client for Dart, with HTTPS and keep-alive. It does not depend on Dox. |

A Rust library does the socket work of each package through dart:ffi. Each
package has its own Rust crate in `native/` and its own ready-built
libraries.

Run `dart pub get` once at the root. It links all packages (a Dart pub
workspace).

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

MIT. See [LICENSE](LICENSE).
