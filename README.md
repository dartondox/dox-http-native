# dox_http_native

Native HTTP/1.1 server for Dart. It is the default HTTP server of Dox.

[![pub package](https://img.shields.io/pub/v/dox_http_native.svg)](https://pub.dev/packages/dox_http_native)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-http-native/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart, like Laravel.
Source code: [github.com/dartondox/dox-http-native](https://github.com/dartondox/dox-http-native).

## Install

You usually do not add this package. `dox_core` depends on it and uses it by default.
Add it only to use the server without Dox:

```bash
dart pub add dox_http_native
```

To see what is ready and what may still change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## How it works

- Rust owns the sockets and parses HTTP/1.1 ([httparse](https://github.com/seanmonstar/httparse)).
- The socket work runs on the thread of your isolate. There is no extra network thread to hand requests to.
- Your handlers stay in Dart.
- It implements the `dart:io` `HttpServer`, `HttpRequest` and `HttpResponse` API. Code written for `dart:io` works unchanged.

## The native library

A build hook (`hook/build.dart`) gets the Rust library. It tries these in order:

1. A path you set in your app `pubspec.yaml` (`hooks: user_defines: dox_http_native: lib_path: ...`).
2. A local build in `native/target/release`, for the same OS and CPU.
3. A ready-built library from the GitHub release, checked with sha256.
   There are builds for macOS (arm64, x64), Linux (x64, arm64) and Windows (x64).
4. A build from source with `cargo`, when Rust is installed.

Outside the pub cache (this repo, or a path or git dependency), step 4 runs before step 3. The source there may be newer than the release.

So on these systems you do not need Rust.
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

Dox always runs on this server. When the native library does not load, Dox
stops at start and prints why (eg. an ABI mismatch) and how to fix it.

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

- Rust marks a request that can upgrade. Your Dart code runs first, like
  for any request: routing, middleware, the `Origin` check. Answer with a
  normal response (eg. `403`) to refuse it.
- `NativeWebSocket.upgrade` accepts it. Rust
  ([tungstenite](https://github.com/snapview/tungstenite-rs))
  sends the `101` and speaks the WebSocket protocol: ping and pong, the
  close handshake and the UTF-8 check of text.
- A message or frame over `maxMessageSize` bytes closes the socket with
  `1009`. Dart never gets it.
- Compression (permessage-deflate) is never agreed.
- Dart gets whole messages: a `String` for text, a `Uint8List` for binary.
  They come in the same batches as requests.
- `NativeWebSocket` implements the `dart:io` `WebSocket` API.

## Features

- Each isolate can bind the same port (`SO_REUSEPORT` on Linux, one shared socket on macOS).
- One thread per isolate: the socket work (accept, read, parse, write) runs on the isolate's own thread. A small helper thread only watches the sockets while the isolate is idle, and wakes it when a request comes.
- Keep-alive and pipelining. At most 64 requests of one connection wait for an answer, and at most 1 MB of answers waits to be written. Then the server stops reading that connection until answers go out.
- A request head must arrive within 30 seconds. A connection with no request for 30 seconds after its last answer is closed.
- A client that reads nothing of its answer for 30 seconds is closed.
- `bind(port, maxConnections: 1000)` holds at most that many connections open. At the limit new clients wait in the listen queue of the OS. Default: no limit.
- When the isolate dies without `close()` (eg. an uncaught error), the server closes within about 100 ms. New clients get "connection refused".
- Chunked request bodies and `Expect: 100-continue`.
- `400` for bad requests, `431` for headers over 16 KB, `413` for bodies over `maxBodySize` (`maxMultipartSize` for a multipart form). A too big `Content-Length` with `Expect: 100-continue` gets `413` before the body is sent. Any other too big body is read and dropped first, so the client sees the `413`.
- `408` when the client sends no body bytes for `bodyTimeout` (idle time, default 120 seconds). A slow but steady upload is fine.
- `bind(port, address: '127.0.0.1')` listens on one host. With no address it listens on all interfaces.
- `close()` is graceful: no new connections, open requests get their answer, then it completes. `close(force: true)` closes every connection at once.
- WebSocket upgrade with `NativeWebSocket.upgrade`, see above. `detachSocket` is not supported.
- On a graceful `close()`, every open WebSocket gets close code `1001` (going away).
- `autoCompress`: Rust gzips the body on a helper thread, not on the isolate.
- Streaming: a small body is sent in one piece on `close`. When `addStream` passes 64 KB, the answer starts and the rest of the body goes out in parts. A big file never sits in memory.

## Security notes

- Run it behind a reverse proxy (eg. nginx or a load balancer) on the open internet. The proxy should end TLS, limit the request rate and drop slow clients.
- Set limits that fit your app: `maxBodySize`, `maxMultipartSize`, `bodyTimeout` and `maxConnections`.
- Another process can bind the same port and take some of the connections. On Linux that is a process of the same OS user (`SO_REUSEPORT`). On macOS it is a process that binds a more specific address (eg. `127.0.0.1` while the server listens on all interfaces). Run the server as its own user, on a host you control.
- `req.uri` is the target as the client sent it. `..` and `%2e%2e` are not removed. Check paths before you use them for files.
- A request head with bare LF line ends (no CR) is accepted, like most servers do. A proxy in front should normalize it.
- `Content-Length` must be ASCII digits. A request with two different lengths, or with both `Content-Length` and `Transfer-Encoding: chunked`, gets `400`.
- A streamed answer must keep to its `contentLength`. More bytes throw, and fewer fail `close()`. In both cases the connection is cut, so the client can not mix the rest up with the next answer.

## Documentation

- [Native server](https://dartondox.dev/latest/digging-deeper/native-server/)
- [API reference](https://pub.dev/documentation/dox_http_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-http-native/blob/main/LICENSE).
