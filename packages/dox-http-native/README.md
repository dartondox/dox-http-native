# dox_http_native

Native HTTP server for Dart. It speaks HTTP/1.1 and HTTP/2. It is the default HTTP server of Dox.

[![pub package](https://img.shields.io/pub/v/dox_http_native.svg)](https://pub.dev/packages/dox_http_native)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-http-native/blob/main/LICENSE)
[![CI](https://github.com/dartondox/dox-http-native/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/dartondox/dox-http-native/actions/workflows/ci.yaml?query=branch%3Amain)
[![Fuzz](https://github.com/dartondox/dox-http-native/actions/workflows/fuzz.yml/badge.svg?branch=main)](https://github.com/dartondox/dox-http-native/actions/workflows/fuzz.yml?query=branch%3Amain)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart.
Source code: [github.com/dartondox/dox-http-native](https://github.com/dartondox/dox-http-native).

## Install

Most apps do not add this package. `dox_core` depends on it and uses it by default.
Add it only to use the server without Dox:

```bash
dart pub add dox_http_native
```

To see what is ready and what can change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## How it works

- Rust owns the sockets and parses HTTP/1.1 with [httparse](https://github.com/seanmonstar/httparse). HTTP/2 has its own parser in this package.
- The socket work runs on the thread of your isolate. There is no extra network thread that gives requests to Dart.
- Your handlers stay in Dart.
- The server implements the `HttpServer`, `HttpRequest` and `HttpResponse` API of `dart:io`. Code for `dart:io` works with no change.

## The native library

A build hook (`hook/build.dart`) gets the Rust library. It tries these sources in this order:

1. A path that you set in the `pubspec.yaml` of your app: `hooks: user_defines: dox_http_native: lib_path: ...`.
2. A local build in `native/target/release`, for the same OS and CPU.
3. A ready-built library from the GitHub release. The hook checks its sha256.
   There are builds for macOS (arm64, x64), Linux (x64, arm64) and Windows (x64).
4. A build from source with `cargo`, when Rust is installed.

Outside the pub cache, step 4 runs before step 3. This applies to this repo, a path dependency and a git dependency. The source there can be newer than the release.

Thus Rust is not necessary on the systems of step 3.
Use `dart run`, `dart test` or `dart build cli`. `dart compile exe` does not run build hooks.

## Use it

```dart
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';

void main() async {
  NativeHttpServer server = await NativeHttpServer.bind(3000);
  server.listen((HttpRequest req) {
    req.response.write('Hello World');
    req.response.close();
  });
}
```

Dox always runs on this server. If the native library does not load, Dox stops at start.
Then Dox prints the cause (for example an ABI mismatch) and the correction.

## HTTP/2

HTTP/2 lets one connection carry many requests at the same time. gRPC needs it. It is off by default. When it is off, nothing changes.

```dart
NativeHttpServer server = await NativeHttpServer.bind(3000, http2: true);
```

How it works:

- The same port speaks HTTP/1.1 and HTTP/2 over plain TCP. This is h2c with prior knowledge. `curl --http2-prior-knowledge` is an example of a client.
- A connection that starts with the HTTP/2 preface is HTTP/2. All other connections are HTTP/1.1.
- There is no TLS, and browsers use HTTP/2 only with TLS. Thus put a proxy in front, for example Caddy or Envoy. The proxy ends TLS and speaks h2c to the server.
- Your handlers stay the same. `request.protocolVersion` is `2.0`. Answers go out in any order.

Requests and answers:

- One connection has a maximum of `maxConcurrentStreams` open requests. The default is 250. More requests get `REFUSED_STREAM`.
- The server reads a request body fully before the handler gets it, as on HTTP/1.1. The limits are the same: `maxBodySize`, `maxMultipartSize` and `bodyTimeout`.
- A body over the limit gets `413`, and a slow body gets `408`. Only that stream stops. The other streams and the connection continue.
- A request header block over 16 KiB gets `431` on its stream. A block over 64 KiB closes the connection.
- Streamed answers (`addStream`, `flush`) go out as DATA frames. The windows of the client give the same backpressure as on HTTP/1.1.
- `NativeResponse.hasRoom` is true when a streamed answer started and Rust has room for more. Then `flush()` completes immediately.
- A graceful `close()` sends GOAWAY, and the open streams get their answers.
- A connection with no open stream for `idleTimeout` gets GOAWAY, then it closes. A PING does not keep it open.

Trailers are headers that go out after the body, for example `grpc-status`. They go out on HTTP/2 only. On HTTP/1.1 the server drops them.

```dart
NativeResponse res = req.response as NativeResponse;
res.trailers.set('grpc-status', '0');
```

gRPC:

- A request with `content-type: application/grpc` reaches the handler when its headers come. `request.isStreamed` is true. This also applies to types such as `application/grpc+proto`.
- The body of such a request comes in parts while the handler runs. It has no size limit and no body timeout.
- The client can send more only after the handler took the parts. A handler that does not read holds the client at the 1 MiB stream window.
- The unread body bytes of one connection have a limit of 16 MiB. Over the limit, the server resets the stream with the most unread bytes.

Flood limits:

- Stream resets have a rate limit (Rapid Reset, CVE-2023-44487). The limit is `http2ResetRate` resets a second (default 100), after a burst of `http2ResetBurst`.
- The default of `http2ResetBurst` is 2 x `maxConcurrentStreams`, with a minimum of 1000.
- Over the limit, the connection drains: it sends GOAWAY `NO_ERROR`, and the open requests get their answers. If the resets continue, the connection closes with GOAWAY `ENHANCE_YOUR_CALM`.
- Too many SETTINGS frames, PING frames or frames with no work also close the connection.
- The answer bytes that wait for a client that does not read have a limit of 32 MiB for each connection. Over the limit, new streams get `REFUSED_STREAM`.

The h2spec conformance suite runs in CI in strict mode.

## WebSocket

```dart
server.listen((HttpRequest req) async {
  if (!NativeWebSocket.isUpgradeRequest(req)) return; // a normal request
  WebSocket ws = await NativeWebSocket.upgrade(
    req,
    protocol: 'chat', // optional subprotocol
    maxMessageSize: 1024 * 1024,
  );
  ws.listen((dynamic message) => ws.add('echo: $message'));
});
```

How it works:

- Rust marks a request that can upgrade. Your Dart code runs first, as for all requests: the router, the middleware and the `Origin` check.
- To refuse the upgrade, send a normal response, for example `403`.
- `NativeWebSocket.upgrade` accepts the request. Rust sends the `101` and speaks the WebSocket protocol with [tungstenite](https://github.com/snapview/tungstenite-rs).
- Rust does the ping and the pong, the close handshake and the UTF-8 check of text.
- A message or a frame over `maxMessageSize` bytes closes the socket with `1009`. Dart does not get it.
- The server does not agree on compression (permessage-deflate).
- Dart gets full messages: a `String` for text, a `Uint8List` for binary. They come in the same batches as requests.
- `NativeWebSocket` implements the `WebSocket` API of `dart:io`.
- `detachSocket` is not supported.
- On a graceful `close()`, each open WebSocket gets the close code `1001` (going away).

## Features

Isolates and threads:

- Each isolate can bind the same port. Linux uses `SO_REUSEPORT`. macOS uses one shared socket.
- There is one thread for each isolate. The socket work (accept, read, parse, write) runs on the thread of the isolate.
- A small helper thread only monitors the sockets while the isolate is idle. It tells the isolate when a request comes.
- If the isolate stops without `close()` (for example on an uncaught error), the server closes in approximately 100 ms. New clients get "connection refused".

Connections:

- Keep-alive and pipelined requests are supported.
- A maximum of 64 requests of one connection wait for an answer. A maximum of 1 MB of answers waits for the write. At these limits, the server stops the read of that connection until answers go out.
- A request head must arrive in `idleTimeout` (default 30 seconds). A connection with no request for `idleTimeout` after its last answer closes.
- Behind a load balancer, set `idleTimeout` longer than the idle timeout of the balancer. For AWS ALB, an example is `bind(port, idleTimeout: Duration(seconds: 65))`.
- A client that closed its send side (a half-close) waits a maximum of `idleTimeout` for its answer. With no write in that time, the connection closes, because most such clients are gone.
- Set a longer `idleTimeout` when such clients call slow handlers.
- A client that reads nothing of its answer for 30 seconds is closed.
- `bind(port, maxConnections: 1000)` holds a maximum of 1000 connections open. At the limit, new clients wait in the listen queue of the OS. The default is no limit.
- `bind(port, address: '127.0.0.1')` listens on one host. With no address, the server listens on all interfaces.
- `close()` is graceful: the server accepts no new connections, open requests get their answers, then `close()` completes.
- `close(force: true)` closes all connections immediately.

Requests:

- Chunked request bodies and `Expect: 100-continue` are supported.
- A bad request gets `400`. Headers over 16 KB get `431`.
- A body over `maxBodySize` gets `413`. For a multipart form, the limit is `maxMultipartSize`.
- If the `Content-Length` is too large and the request has `Expect: 100-continue`, the client gets `413` before it sends the body.
- All other bodies that are too large are read and dropped first. Thus the client sees the `413`.
- The client gets `408` when it sends no body bytes for `bodyTimeout`. This is idle time, and the default is 120 seconds. A slow upload that does not stop is permitted.

Answers:

- `autoCompress`: Rust gzips the body on a helper thread, not on the isolate.
- A small body goes out in one piece on `close`.
- When `addStream` passes 64 KB, the answer starts, and the remaining body goes out in parts. A large file does not stay in memory.
- `flush()` also starts the answer: the head and the bytes so far go out. Later writes go out immediately, for example server-sent events from a timer.
- After the head is out, the server ignores a header change or a status change. It logs a warning one time.
- `response.done` completes with no error when the client is gone. This also applies when the handler does not close the response.
- A handler that writes from its own timer can stop the timer there: `response.done.then((_) => timer.cancel())`. The server drops writes after that.

## Differences from dart:io

The server implements the `dart:io` API. These parts are different on purpose. The tests in `test/dart_io/` come from the Dart SDK. Each skipped test there names one of these differences.

- There is no TLS. A proxy ends TLS. `bindSecure` and `listenOn` do not exist.
- There is no `detachSocket` and no `HttpSession`. `connectionsInfo()` and `sessionTimeout` do nothing. `idleTimeout` is an argument of `bind`.
- Rust reads the whole request body before your handler runs. A request that the client cuts short never reaches the handler. The connection closes at once.
- A custom `reasonPhrase` is not sent. The answer has the standard phrase of the status code.
- A text body is always UTF-8, also when the content type is missing or has no charset.
- `HttpResponse.deadline` has no effect.
- `bufferOutput` has no effect. A buffered body goes out on `close`. Use `flush()` to start the answer at once. `addStream` starts the answer after 64 KB or on `flush()`.
- The server sets `Content-Length` and the framing from the real body. A `contentLength` that is too small for a buffered body does not throw. The answer has the real length. `Transfer-Encoding` that your handler sets is ignored.
- The server sends its own `Date` header. A `Date` header that your handler sets is ignored.
- A `HEAD` answer with no body keeps the `contentLength` that your handler set.
- An HTTP/1.0 request with `Connection: keep-alive` can stay open, when the answer has a length.
- `request.contentLength` is the length of the body. It is `0`, not `-1`, when there is no `Content-Length` header.
- A request header value is not split on commas. You get one list entry for each header line.
- `response.persistentConnection` is `true` until you set it. The server still closes the connection after a request with `Connection: close`.
- `requestedUri` does not use `x-forwarded-proto` or `x-forwarded-host`. A client can send them.
- A header name is always lower case. `preserveHeaderCase` and `noFolding` have no effect.
- A folded request header (obs-fold) gets `400`.
- `flush()` and `addStream()` complete with `null`, not with the response. After a failed `addStream`, the response stays open.
- `response.add` and `webSocket.add` throw an `ArgumentError` for a value that is not a byte. `dart:io` fails `done`.
- `NativeWebSocket.upgrade` takes a `protocol`, not a `protocolSelector`.

## AES-GCM

`AesGcm` is AES-256-GCM in Rust. It uses the AES instructions and the carry-less multiply instructions of the CPU. `dox_core` uses it for encrypted cookies. It works without a server.

```dart
Uint8List sealed = AesGcm.seal(key, plain, aad);   // nonce + ciphertext + tag
Uint8List? plain = AesGcm.open(key, sealed, aad);  // null when changed
```

- `key` is 32 bytes. `aad` binds the value to a context, for example a cookie name.
- Each `seal` makes a new random nonce of 12 bytes.
- One cookie of 200 bytes takes 1 µs to 2 µs. The same work in pure Dart took approximately 100 µs.

## Security notes

- On the open internet, run the server behind a reverse proxy, for example nginx or a load balancer. The proxy must end TLS, limit the request rate and drop slow clients.
- Set limits that are correct for your app: `maxBodySize`, `maxMultipartSize`, `bodyTimeout`, `maxConnections` and `idleTimeout`.
- A different process can bind the same port and take some of the connections. On Linux, that is a process of the same OS user (`SO_REUSEPORT`). On macOS, it is a process that binds a more specific address.
- An example on macOS is `127.0.0.1` while the server listens on all interfaces. Run the server as its own user, on a host that you control.
- `req.uri` is the target that the client sent. The server does not remove `..` and `%2e%2e`. Do a check of each path before you use it for files.
- The server accepts a request head with bare LF line ends (no CR), as most servers do. A proxy in front must normalize it.
- `Content-Length` must be ASCII digits. A request with two different lengths gets `400`. A request with `Content-Length` and `Transfer-Encoding: chunked` also gets `400`.
- `Transfer-Encoding` must be exactly `chunked`, in one header. A different coding gets `501`. Two headers, or `Transfer-Encoding` in an HTTP/1.0 request, get `400`.
- A WebSocket upgrade needs one `Sec-WebSocket-Key` that is base64 of 16 bytes (24 characters, RFC 6455). With a different key, or with two keys, there is no upgrade. The request goes to the app as a normal request.
- Only `multipart/form-data` gets the multipart body limit. All other content types get the normal body limit.
- A streamed answer must have exactly the bytes of its `contentLength`. More bytes throw, and fewer bytes make `close()` fail. In the two cases, the server cuts the connection. Thus the client can not read the remaining bytes as the next answer.
- For the HTTP/2 flood limits, see [HTTP/2](#http2).

## Tests

The CI and Fuzz badges at the top show the last result on `main`. CI runs the tests below on each push and on each pull request. The fuzz tests run only when `native/` changes.

| Test set | Count (2026-11-25) | What it checks |
| --- | --- | --- |
| Dart tests of the `dart:io` server, from the Dart SDK | 48 files, 750 tests pass, 69 skipped | The server gives the same results as the `dart:io` server. |
| Request parser inputs from the Dart SDK (part of the 750 tests) | 519 tests | Bad, split and too large requests. Each request goes in as 1 piece, in 10-byte pieces and in 1-byte pieces. |
| Other Dart tests of this package | 129 tests | The server, the headers, HTTP/2, WebSocket and AES-GCM. |
| Rust unit tests | 179 tests | The parser, the body reader, the limits and the HTTP/2 frames. |
| [h2spec](https://github.com/summerwind/h2spec) (strict mode) | all specs but 1 | The HTTP/2 conformance suite. |
| Fuzz tests | 6 targets | Random input on the parsers and on a full connection. Refer to [Fuzz tests](#fuzz-tests). |

The SDK tests are in `test/dart_io/`. Each file has the same name as its source file in [`tests/standalone/io`](https://github.com/dart-lang/sdk/tree/main/tests/standalone/io) of the Dart SDK.

- The tests use the `dart:io` client on the server of this package.
- A skipped test has a reason that starts with `dox_http_native:`. Most reasons are an API that this server does not have, for example `detachSocket`.
- Refer to [Differences from dart:io](#differences-from-dartio) for the full list.

To run the Dart tests:

```sh
cd native
cargo build --release
cd ..
dart test -j 4
```

## Fuzz tests

The fuzz targets are in `native/fuzz/`. CI runs each target for 60 seconds when `native/` changes.

- `parse_head`: random bytes as one request. It compares the record for Dart with the data that `httparse` reads from the same bytes: method, target, header names, header values, lengths and flags.
  It also compares the body framing (no body, a length, chunked, or a refusal) with a small reference of RFC 9112 section 6. The reference is written apart from the server. This target finds request smuggling bugs.
- `chunked_body`: a chunked body, in one piece and in random pieces. The two must give the same result.
- `conn`: many requests on one connection (pipelined, bodies, keep-alive, close, upgrade). The input goes in as one piece and in random pieces, with a large Dart buffer and a small Dart buffer. Dart must get the same records each time. The server must parse nothing after a close or a bad request.
- `aead_open`: the cookie decryption on random data. It must not panic. A sealed value must open again. One changed byte must make the open fail.
- `hpack`: random HTTP/2 header blocks (HPACK) on one decoder. It must not panic. The decoded list must stay below the size limit, and the table size must stay correct. Each decoded block is encoded again and must decode to the same fields.
- `h2_conn`: random bytes after the HTTP/2 preface, on one connection, with cuts as in `conn`. When Dart does not answer, one piece and random pieces must give the same records and the same bytes back.
  With answers (full or streamed), each frame that the server writes must be complete and valid. Nothing can come after a GOAWAY with an error. No stream can end two times. `STREAM_SENT` must not count more bytes than the answers have.

To run one target on your computer:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz --locked
cd native
cp Cargo.lock fuzz/Cargo.lock
mkdir -p /tmp/corpus/conn fuzz/corpus/conn
cargo +nightly fuzz run conn /tmp/corpus/conn fuzz/corpus/conn -- -max_total_time=300 -max_len=32768
```

- `mkdir` also makes the seed folder, for a target that has no seeds. `aead_open` is such a target: all random bytes are a good input.
- The first folder gets the new inputs that the fuzzer finds. Use a temporary folder, so `fuzz/corpus/` keeps only the small seed files. Commit a new seed there only when it is useful.
- `-max_len=32768` lets inputs go above the head limit of 16 KB.
- A crash stops the run. The input that caused it is in `fuzz/artifacts/<target>/`. To run it again, use `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.
- The fuzz crate builds the same source as `native/`. Keep the dependencies in `native/fuzz/Cargo.toml` the same as in `native/Cargo.toml`. CI does a check of this.

## Documentation

- [Native server](https://dartondox.dev/latest/digging-deeper/native-server/)
- [API reference](https://pub.dev/documentation/dox_http_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-http-native/blob/main/LICENSE).
