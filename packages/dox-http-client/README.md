# dox_http_client

Fast native HTTP/1.1 client for Dart. It sends requests to other servers, for example when a handler calls an API.

[![pub package](https://img.shields.io/pub/v/dox_http_client.svg)](https://pub.dev/packages/dox_http_client)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-http-native/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart.
Source code: [github.com/dartondox/dox-http-native](https://github.com/dartondox/dox-http-native).

The package does not depend on Dox. You can use it in any Dart app.

## Install

```bash
dart pub add dox_http_client
```

## How it works

- Rust owns the sockets and parses the answers with [httparse](https://github.com/seanmonstar/httparse).
- The socket work runs on the thread of your isolate. This makes the client faster than `HttpClient` of `dart:io`.
- HTTPS uses [rustls](https://github.com/rustls/rustls).
- The client keeps its connections open (keep-alive) and uses them again.

## The native library

A build hook (`hook/build.dart`) gets the Rust library. It tries these sources in this order:

1. A path that you set in the `pubspec.yaml` of your app: `hooks: user_defines: dox_http_client: lib_path: ...`.
2. A local build in `native/target/release`, for the same OS and CPU.
3. A ready-built library from the GitHub release. The hook checks its sha256.
   There are builds for macOS (arm64, x64), Linux (x64, arm64) and Windows (x64).
4. A build from source with `cargo`, when Rust and a C compiler are installed. The TLS library compiles C code and assembly code.

Outside the pub cache, step 4 runs before step 3. This applies to this repo, a path dependency and a git dependency. The source there can be newer than the release.

Thus Rust is not necessary on the systems of step 3.
Use `dart run`, `dart test` or `dart build cli`. `dart compile exe` does not run build hooks.

When no library loads, the first `DoxHttpClient()` throws an `UnsupportedError`. The error tells why the library did not load and how to fix it.

The library of this package and the library of `dox_http_native` can be in the same app. All functions of this library start with `dox_client_`, so their names do not clash.

## Use it

Make one client and use it for all requests. The client keeps its connections open and uses them again:

```dart
import 'package:dox_http_client/dox_http_client.dart';

final DoxHttpClient client = DoxHttpClient();

Future<String> rates() async {
  DoxHttpClientResponse res = await client.get(
    Uri.parse('https://api.example.com/rates'),
  );
  return res.body;
}
```

Each isolate must have its own client.

## Send a request

`get`, `head`, `post`, `put`, `patch` and `delete` send a request. Give the body as text with `body`, or as bytes with `bodyBytes`:

```dart
DoxHttpClientResponse res = await client.post(
  Uri.parse('http://127.0.0.1:8080/users'),
  headers: <String, String>{'content-type': 'application/json'},
  body: '{"name":"Dox"}',
);
```

For other methods, use `send`:

```dart
await client.send('OPTIONS', Uri.parse('http://127.0.0.1:8080/users'));
```

The client sets the `host`, `content-length` and `accept-encoding` headers. A text body with no `content-type` header goes out as `text/plain; charset=utf-8`.

## Read the answer

```dart
res.statusCode; // 200
res.body; // the body as text (UTF-8)
res.bodyBytes; // the body as bytes
res.headers; // all headers, names in lower case
res.header('Content-Type'); // one header, or null
res.url; // the URL that gave this answer
```

An answer with an error status, for example 404 or 500, is an answer and not an exception.

## Failures

When no answer comes, the future fails with a `DoxHttpClientException`. Its `failure` gives the cause:

| `DoxHttpFailure` | Cause |
| --- | --- |
| `lookupFailed` | The host name has no address. |
| `connectionFailed` | The server did not accept the connection. Nothing was sent. |
| `timeout` | The full answer did not come in the timeout. The timeout includes the lookup of the host name. |
| `connectionClosed` | The connection closed before the answer was complete. |
| `badResponse` | The answer is not correct HTTP. |
| `responseTooBig` | The body is larger than `maxResponseSize`. |
| `tlsFailed` | TLS failed. For example, the certificate is not trusted or is for a different host name. |
| `clientClosed` | `close()` ran before the answer came. |
| `tooManyRedirects` | The request got more redirects than `maxRedirects`. |
| `badRequestBody` | The body stream of `sendStreamed` gave more or fewer bytes than `contentLength`. |

## Redirects

A redirect is an answer with status 301, 302, 303, 307 or 308 and a `location` header. The client sends the request to the new address and gives you the last answer. `res.url` is the URL that gave this answer.

- A 307 or 308 redirect keeps the method and the body.
- A 303 redirect changes the method to `GET` and removes the body. `GET` and `HEAD` do not change.
- A 301 or 302 redirect changes `POST` to `GET` and removes the body.
- If the new address is a different server, the client does not send the `authorization`, `cookie`, `proxy-authorization` and `host` headers of the request. A different scheme, host name or port is a different server.
- All other headers go to the new address, and a 307 or 308 redirect also sends the body there. If a request has a secret in a different header, set `followRedirects` to `false`.

One request follows a maximum of 5 redirects. Then it fails with `tooManyRedirects`. Change the maximum with `maxRedirects`.

To get the redirect answer, set `followRedirects` to `false` for the client or for one `send` call:

```dart
DoxHttpClient client = DoxHttpClient(followRedirects: false);
await client.send('GET', url, followRedirects: false);
```

## Compressed bodies

The client sends `accept-encoding: gzip, br` with each request. When the answer body is gzip or brotli, the client decodes it in Rust. You get the decoded body in `res.body` and `res.bodyBytes`.

- A decoded answer has no `content-encoding` header and no `content-length` header. These headers are for the compressed body.
- `maxResponseSize` is the limit for the decoded body and for the compressed body. It is off by default. A small compressed body can decode to a very large body. Thus set `maxResponseSize` when you send requests to servers that you do not trust.
- Bytes after the end of a gzip body that are not gzip are ignored.
- A compressed body that is not correct fails the request with `badResponse`.
- A body with a different encoding, for example `zstd`, stays as the server sent it.

A request with a `range` header asks for a part of a body. The client does not send `accept-encoding` with it and does not decode its answer, because a part of a compressed body can not be decoded.

To get the body as the server sent it, set your own `accept-encoding` header:

```dart
await client.get(url, headers: <String, String>{'accept-encoding': 'identity'});
```

## Streamed bodies

`send` keeps the request body and the answer body in memory. For big bodies, use `sendStreamed`. It never holds a whole body in memory. The future completes when the head of the answer is there. The body comes later, in `res.body`.

Download a big file to disk:

```dart
DoxHttpClientStreamedResponse res = await client.sendStreamed(
  'GET',
  Uri.parse('https://example.com/big.iso'),
);
IOSink sink = File('big.iso').openWrite();
await sink.addStream(res.body);
await sink.close();
```

Upload a file. With `contentLength`, the client sends a `content-length` header. Without it, the client sends the body with `transfer-encoding: chunked`:

```dart
File file = File('big.iso');
DoxHttpClientStreamedResponse res = await client.sendStreamed(
  'PUT',
  Uri.parse('https://example.com/upload'),
  body: file.openRead(),
  contentLength: await file.length(),
);
print(res.statusCode);
await res.body.drain<void>();
```

The rules:

- `res.body` is a stream with one subscription. You must listen to it or cancel it, also when you do not need the body. A body that nobody uses keeps its connection open until `close()`.
- The client reads a maximum of about 256 KiB ahead. When you pause the subscription, the client stops to read the connection. Thus the memory stays small for a 5 GB download.
- The client listens to the request `body` when the connection to the server works. It pauses that subscription while more than 256 KiB wait to go out.
- When you cancel `res.body` before its end, the client closes the connection. The connection is not used again.
- A failure after the head is an error event on `res.body`, then the stream closes. The causes are a closed connection, a chunk that is not correct, gzip data that is not correct and a body over `maxResponseSize`.
- The client decodes a gzip or brotli body in parts. `maxResponseSize` is the limit for all decoded bytes.
- `timeout` is for the time until the head of the answer is there. After the head, there is no timeout.
- With a `body`, the timeout starts again each time bytes of the body go out. Thus a big body can need more time than the timeout. The request fails when no bytes go out for the timeout.
- A request with no `body` follows redirects like `send`. A request with a `body` never follows redirects, because the stream can not be read again. You get the redirect answer.
- The client sends a request with a `body` only once. It does not send it again when a kept connection was closed.
- If the `body` stream gives more or fewer bytes than `contentLength`, the request fails with `badRequestBody`. If the `body` stream fails with an error, the request fails with that error. In both cases the client closes the connection.
- The server can answer before the whole `body` went out, for example with 401 or 413. The client cancels the `body` subscription and gives you that answer. The connection is not used again.
- The headers `content-length` and `transfer-encoding` are not allowed. Use `contentLength`.
- `close()` fails open streams with `clientClosed`.

## Timeout

A request fails when its full answer does not come in 30 seconds. The timeout includes the redirects. Change this for the client or for one request. `Duration.zero` is no timeout:

```dart
DoxHttpClient client = DoxHttpClient(
  timeout: const Duration(seconds: 5),
);
await client.get(url, timeout: const Duration(minutes: 2));
```

## HTTPS

An `https://` URL uses TLS 1.2 or TLS 1.3. The client trusts the root certificates of Mozilla, which are in the library. Thus HTTPS also works in a `FROM scratch` Docker image.

To trust your own certificate authority or a self-signed certificate, give the certificates as PEM text:

```dart
DoxHttpClient client = DoxHttpClient(
  trustedCertificates: File('ca.pem').readAsStringSync(),
);
```

## Idle connections

A connection with no request for 15 seconds closes. Change this with `idleTimeout`. `Duration.zero` keeps idle connections open:

```dart
DoxHttpClient client = DoxHttpClient(
  idleTimeout: const Duration(minutes: 1),
);
```

## Limits

Each limit is off by default:

- `maxConnectionsPerHost: 20` holds a maximum of 20 connections open to one address. More requests wait for a free connection.
- `maxResponseSize: 10 * 1024 * 1024` makes a request fail when its answer body is larger than 10 MB.

## Close the client

`close()` closes all connections. Open requests fail with `clientClosed`.

Call `close()` when the client is not necessary any more. If you do not, the connections close when Dart collects the client object.

## What the client does not do

- It speaks HTTP/1.1 only.
- `send` keeps each request body and each answer body in memory. `sendStreamed` does not.
- It sends a request again only when a kept connection closed before an answer byte came, and only for `GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS` and `TRACE`.

## Fuzz tests

The fuzz target is in `native/fuzz/`. CI runs it for 60 seconds when `native/` changes.

- `client_response`: random bytes as the answer of a server. The parser and the gzip and brotli decoders must not panic. A chunked body must give the same result in one piece and byte by byte.

To run it on your computer:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz --locked
cd native
cp Cargo.lock fuzz/Cargo.lock
mkdir -p /tmp/corpus/client_response
cargo +nightly fuzz run client_response /tmp/corpus/client_response fuzz/corpus/client_response -- -max_total_time=300 -max_len=32768
```

- The first folder gets the new inputs that the fuzzer finds. Use a temporary folder, so `fuzz/corpus/` keeps only the small seed files.
- A crash stops the run. The input that caused it is in `fuzz/artifacts/client_response/`.
- The fuzz crate builds the same source as `native/`. Keep the dependencies in `native/fuzz/Cargo.toml` the same as in `native/Cargo.toml`. CI does a check of this.

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-http-native/blob/main/LICENSE).

The native library contains code and data of other projects, for example rustls, ring and the root certificates of Mozilla (webpki-roots). Their licenses are in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
