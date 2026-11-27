## Unreleased

- Faster: an HTTP/2 answer encodes its header lines one time for each connection, and then reuses the block while the lines and the status do not change. `content-length` is written with no `String`. A body that fits one frame and the flow windows goes out at once in one DATA frame, with no copy into the stream. A body that is bigger, or that waits behind another stream, uses the queue as before. On the HttpArena json-h2c test (2 cores), with a fixed-length list in the app, this gives 18% more requests a second.
- Faster: `autoCompress` gzips with libdeflate, not with flate2. libdeflate compresses a whole body in one call. The native library builds it from C source, so no system library is necessary.
- New: `NativeHttpServer.compressLevel`, the gzip level of `autoCompress`, from 1 (fast) to 12 (small). The default is 1. A level that is not 1 to 12 throws a `RangeError`. For a 6.7 kB JSON body, the gzip time goes from 35.7 µs (flate2, level 6) to 13.5 µs (level 1). The body is 1,421 bytes, not 1,271 bytes. libdeflate at level 6 takes 32.6 µs for 1,270 bytes. Levels 10 to 12 are slow and use 9 MB each, and they make the other gzipped answers of the server wait. The client reads every level the same way. Set `compressLevel = 6` to get about the same size as before.

## 3.2.6

- New: HTTPS without a proxy. It is off by default. When it is off, a plain connection uses the same code as before.
  - `NativeHttpServer.bind(port, tls: TlsOptions(certificatePath: ..., keyPath: ...))` makes the port a TLS port. Rust does the TLS work with rustls and the `ring` crypto provider. There is no OpenSSL.
  - A bad certificate file or key file throws a `TlsException` at `bind`.
  - TLS 1.2 and TLS 1.3. With `http2: true`, ALPN selects HTTP/2 or HTTP/1.1. HTTP/1.1, HTTP/2, WebSocket and streamed answers work on the TLS port.
  - A plain HTTP client on the TLS port gets no answer. The handshake must end in `idleTimeout`.
  - `requestedUri` has the `https` scheme. A clean close sends the TLS `close_notify` alert.
  - `server.reloadCertificate()` reads the two files again. New connections get the new certificate, and open connections keep the old certificate. A bad file throws a `TlsException`, and the server keeps the old certificate. All servers in the process that use the same files get the new certificate.
  - `TlsOptions.check()` reads the two files as `bind` reads them, and throws a `TlsException` for a bad file. It opens no port. Use it to find a bad file before an app starts its isolates.
  - Not included: ACME, a redirect from port 80, mTLS and SNI.
- ABI version 11 (was 10). The new functions are `dox_tls` and `dox_tls_check`. A library of an older version does not load. From pub.dev, the correct library downloads automatically. In a git checkout, run `cargo build --release` in `native/`.

## 3.2.5

- Tests: the HTTP server tests of the Dart SDK (`tests/standalone/io`) now run against this server, in `test/dart_io/`.
- Fix: a client that sent more than 64 pipelined requests and then closed its send side got answers for only the first 64. The server closed the connection and dropped the requests that still waited in its read buffer. Now it reads on and answers all of them.
- Fix: a client that closed its connection in the middle of a request body kept the connection open until `bodyTimeout` (120 seconds by default), and a graceful `close()` waited for it. Now the connection closes at once, as no handler saw the request.
- Fix: `requestedUri` was wrong for a request line with an absolute URI (`GET http://example.com/ HTTP/1.1`). It gave `http://example.comhttp//example.com/`. Now it gives the URI of the request line.
- Fix: a chunked request body whose chunk size line was empty (`\r\n`) or started with `;` was read as the last chunk. Now the request gets `400`. A proxy that reads such a line as an error or skips it saw another end of the body, which is a request smuggling risk.
- Fix (security): a chunked request body with a control byte in a chunk size line or in a trailer was accepted, for example a lone `\n` in a chunk extension or in a trailer line. A proxy that ends the line at the lone `\n` sees another end of the body. This is a request smuggling risk (the same type as CVE-2025-55315). Now the request gets `400`. A tab is permitted.
- Fix: a handler that set a `Date` header made an answer with two `Date` headers (RFC 9110 allows one). The server now always sends its own `Date`, as it already did for `Content-Length` and `Transfer-Encoding`.
- Fix: `response.headers.clear()` did not reset `chunkedTransferEncoding`.
- Fix: `response.add` and `webSocket.add` with a list that holds a value outside 0 to 255 (for example `[256]` or a `Uint16List`) sent other bytes (256 became 0). Now they throw an `ArgumentError`, as dart:io does. A `Uint8List` is not checked.
- Fix: the answer to a `HEAD` request with a `contentLength` that the handler set, and no body, had `content-length: 0`. Now it has the length that the handler set, as for dart:io. This also applies to HTTP/2.
- Fix: `NativeWebSocket.upgrade(protocol:)` sent the protocol also when the client did not offer it (RFC 6455 section 4.2.2). Now, as in dart:io, the request gets `500` and `upgrade` throws a `WebSocketException`. When the client offers no protocol, the answer has no protocol.
- Fix: when the stream of `addStream` failed before the head went out, the request got no answer. A later `close()` sent the part of the body that came before the error, as a `200`. Now the request gets `500` with no body.

## 3.2.4

- New releases have the tag `dox_http_native-v<version>`, because the repository now holds more than one package. The build hook downloads the library from this tag first. When it is not there, the hook uses the old tag form `v<version>`.
- The ready-built Linux libraries do not need `libgcc_s.so.1`. They need only glibc 2.34 or newer, as before. A `FROM scratch` Docker image with the Dart `/runtime/` files does not need a copy of `libgcc_s.so.1` for this package. The release build now links the Linux libraries with `cargo zigbuild`, and CI does a check of each library.

## 3.2.3

- Faster: lower tail latency, and more requests a second when the server has idle CPU cores. After the server sends a whole answer and no request is open, it now waits up to 1 ms for the next requests in the same call. Before, it gave the sockets to the idle thread at once, and each return to Dart cost 2 thread wake-ups. Measured on 4 server cores with 64 connections: p99 goes from 2.4 ms to 0.3 ms, and requests a second go up 1% to 6%. With 256 connections, p99 goes from 2.8 ms to approximately 1 ms. On 2 server cores, the numbers do not change. There is no wait while a request or a streamed answer is open. A WebSocket message and a part of a streamed answer do not start a wait. The waits between 2 turns of the event loop are 1 ms in total, plus at most one more wait. Thus other work of the isolate (for example a timer) can start up to 2 ms later.
- Fix: a gzip answer did not go out until other traffic came, when the gzip thread finished while the server waited for socket events. The wait used the wake-up of the gzip thread, and then the server did not look for the finished answer. Now the server sends the answer in the same call.

## 3.2.2

- ABI version 10 (was 9). `dox_bind` has 4 new arguments: `http2`, `maxConcurrentStreams`, `http2ResetBurst` and `http2ResetRate`. A library of an older version does not load. From pub.dev, the correct library downloads automatically. In a git checkout, run `cargo build --release` in `native/`.
- New: HTTP/2 over plain TCP (h2c with prior knowledge). It is off by default. When it is off, nothing changes.
  - `NativeHttpServer.bind(port, http2: true)` makes the same port speak HTTP/1.1 and HTTP/2. A connection that starts with the HTTP/2 preface is HTTP/2. Handlers stay the same. `request.protocolVersion` is `2.0`, and answers go out in any order.
  - `maxConcurrentStreams` (default 250) caps the open requests of one connection. More requests get `REFUSED_STREAM`. A graceful `close()` sends GOAWAY and lets open streams finish. An HTTP/2 connection with no open stream for `idleTimeout` gets GOAWAY before it closes. A PING does not keep it open.
  - Request bodies: a body is read whole before the handler gets it, as on HTTP/1.1, with the same `maxBodySize`, `maxMultipartSize` and `bodyTimeout`. A body over the limit gets `413`, and a slow body gets `408`. Only that stream ends. The other streams and the connection continue. The server gives the flow-control window back as the body comes, so a big upload does not stall. A `content-length` that does not match the body is an error. Request trailers are read and dropped.
  - Streamed answers (`addStream`, `flush`) go out as DATA frames under the client's windows, with the same backpressure as on HTTP/1.1. A streamed answer that fails resets only its stream.
  - gRPC support: a request with `content-type: application/grpc` (or `application/grpc+proto` and similar) reaches the handler when its headers come, with `request.isStreamed` true. Its body then comes in parts while the handler runs. The client can send more only after the handler took the parts. A slow or paused handler, or one that does not read, holds the client back at the 1 MiB stream window. Such a body has no size limit and no body timeout. When the client resets the stream or the connection stops, the body stream ends with an `HttpException`. When the handler answers before the request body ended, the client gets `RST_STREAM NO_ERROR`.
  - New `NativeResponse.trailers`: headers sent after the body, for example `grpc-status`. They go out on HTTP/2 only. On HTTP/1.1 they are dropped.
  - Unread gRPC bodies do not hold the connection flow-control window. Only the stream window waits for the handler. The bytes that wait for handlers are capped at 16 MiB per connection. Over the cap, the stream with the most unread bytes is reset (`RST_STREAM ENHANCE_YOUR_CALM`, gRPC `RESOURCE_EXHAUSTED`). A handler that reads its body does not come near the cap.
  - Answer bytes that wait for a client that does not read are capped at 32 MiB per connection. Over the cap, new streams get `REFUSED_STREAM` until the client reads.
  - A request header block over 16 KiB (encoded) gets `431` on its stream, and the connection continues. Only a block over 64 KiB closes the connection. A `:path` that does not start with `/` is a malformed request (`RST_STREAM PROTOCOL_ERROR`). `*` is permitted for `OPTIONS`.
- New: HTTP/2 flood limits.
  - Resets (Rapid Reset, CVE-2023-44487 and CVE-2025-8671): over `http2ResetRate` resets a second (default 100) after a burst of `http2ResetBurst` (default 2 x `maxConcurrentStreams`, minimum 1000), the connection drains. It sends GOAWAY `NO_ERROR`, the open requests still get their answers, and new streams get `REFUSED_STREAM`. A proxy (for example Caddy or Envoy over h2c) then sends them again on a new connection. If the resets continue for one more burst, the connection closes with GOAWAY `ENHANCE_YOUR_CALM`. Streams that the server resets for a bad frame also count.
  - Each answer with a body gives back half a reset, so a busy gRPC client whose calls go over their deadline is not cut. Answers with no body (for example a gRPC error) give nothing back. A `RST_STREAM` for a stream that is already closed costs nothing, up to one burst a second.
  - Over 100 SETTINGS or 100 PINGs a second, over 10 SETTINGS or 100 PINGs with no read answer, or over 100 frames in a row with no work (for example empty DATA or CONTINUATION) close the connection at once with GOAWAY `ENHANCE_YOUR_CALM`. PINGs between DATA frames do not count. Protocol errors also close the connection at once.
  - A Dart handler can not be stopped. A request that the client reset counts toward `maxConcurrentStreams` until its handler answers, so fast resets can not pile up work in Dart. A handler that does not end stops counting after 30 seconds. A streamed answer that started counts until its last part. A refusal that only lost handlers cause costs a quarter of a reset.
  - A frame on a stream that the client reset is a `STREAM_CLOSED` error, as RFC 9113 says.
- New (internal): an HPACK decoder and encoder (RFC 7541) for HTTP/2 header compression. Tests use all RFC 7541 Appendix C examples. New fuzz targets `hpack` and `h2_conn`. The h2spec conformance suite (strict) runs in CI, and the HTTP/1.1 tests also run with HTTP/2 on.
- Faster streamed answers: parts that a handler makes together now go out in one write, not one write per part. A part is sent in the next event-loop turn. A whole answer, a stream end or a WebSocket message that comes while a part waits is also sent one event-loop turn later.
- New: `NativeResponse.hasRoom`. It is true when a streamed answer started and Rust has room for more, so `flush()` would complete at once.

## 3.2.1

- Fix (leak): a client that closed its connection while its request still waited for an answer kept the connection, its socket and its state for ever when the handler never answered (eg. a database call with no timeout). The same happened to a streamed answer (eg. server-sent events) that wrote nothing after the client left. Now, when the client closed its side and nothing was written for the idle time (`idleTimeout`, 30 seconds by default), the connection closes. A client that only closed its sending side and still waits gets its answer when it comes within that time. A late answer is dropped. This also holds for a connection that reads nothing while it waits (eg. a WebSocket upgrade request, or 64 open requests): the server sees the close in the poll. Note: a client that closed only its sending side (a half-close) now waits at most `idleTimeout` for its answer. Raise `idleTimeout` when such clients call slow handlers.
- Fix (leak): when the client of a streamed answer leaves, `addStream` now cancels its source at once. Before, it waited for the next part of the source, so a quiet source (eg. server-sent events with no news) stayed subscribed.
- Fix: `NativeWebSocket.pingInterval` did nothing. Now it works like in `dart:io`: when the client sent nothing for that long, the server sends a ping. When the client still sends nothing for one more interval, the socket closes with 1001 (going away). This finds clients that vanished with no close (eg. a phone that lost its network). Default: off (null), as before.
- New: accepted sockets get TCP keep-alive: after 60 seconds with no traffic the OS checks the client every 10 seconds. After 6 checks with no answer (about 2 minutes) the connection is dead and closes. A live client always answers these checks, so nothing changes for it. It is set once on the listening socket where accepted sockets inherit it (Linux, and all but the idle time on macOS), so an accept costs few extra system calls.
- Fix (leak): `response.done` never completed when the client left before the response was closed. So a handler that writes server-sent events with `response.add()` from its own timer kept the timer for ever. Now `done` completes (with no error) when the client is gone. Writes after that are dropped, as before. This also holds when `done` is first read after the client left. Only responses whose `done` is used before `close()` are tracked, so normal requests cost nothing more.
- Fix: `response.flush()` did nothing, so `add()` plus `flush()` (eg. server-sent events from a timer) sent nothing until `close()`. Now it works like in `dart:io`: the first flush sends the head and the bytes so far, and the body goes out in parts (chunked, or up to the `contentLength` when set). Later writes go out at once. It completes when the server has room for more, like `addStream`. An error (eg. more bytes than the content length) fails its Future. Once the head is out (a flush, or `addStream` past 64 KB), a change of a header, `statusCode` or `reasonPhrase` is ignored, and a warning is logged once to stderr. A flush that waits for room goes on when the response is closed. After `close()`, after the client left, or while `addStream` runs, flush does nothing. A response that never calls flush is sent as before.
- New: `NativeResponse.headSent`. It is true once the status and the headers went out (a flush, or `addStream` past 64 KB). Then an error handler can not send an error page any more.
- Fix: a streamed answer (`addStream` past 64 KB, or a flush) with status 1xx, 204 or 304 wrote a chunk end after the head. That broke the next answer on a keep-alive connection. Now only the head goes out, like for HEAD.

- New: `NativeResponse.end()`. It sends the response like `close()`, but makes no Future. `close()` makes a Future and a microtask per response. When the caller does not use that Future, it only keeps the request and its batch of bytes alive a bit longer, so under load the heap grows. Use `end()` when you do not await the response. `close()` works the same as before.
- Memory: each connection kept a 16 KB read buffer for its whole life. With 512 keep-alive connections that is 8 MB. Now all connections of one core read into one shared buffer. A connection keeps its own buffer only for a part of a request that did not come whole yet.
- Memory: after a big answer (up to 1 MB), a keep-alive connection kept a write buffer that big. Now a write buffer over 64 KB is freed once it is written.
- Memory: a request that is still open after its batch (an async handler, a stream, a WebSocket) kept the whole batch copy alive (up to 256 KB or more). Now such a request copies only its own headers and body bytes. Requests answered right away copy nothing.
- Speed: request headers now scan the raw bytes for the first 4 lookups (was 1) before they parse all headers into a map. A scan that finds nothing makes no object.

## 3.2.0

- Fix: each download try of the native library now has one time limit of 60 s, from connect to the last byte. Before, there was no limit while waiting for the answer, and the 60 s limit for the body only counted the gap between two chunks. So a slow server could hang the build for a long time. Now 3 tries take about 3 minutes at most.
- Fix: a chunked body with a chunk size line over 1024 bytes (with its extensions) now always gets `400`. Before, it got `400` only when the line came in small reads. When the whole line came in one read, it passed. A trailer over 16 KB now always gets `431` for the same reason. Found by the new fuzz tests.
- New: fuzz tests (cargo-fuzz) for the request parser and the chunked body reader, in `native/fuzz/`. CI runs them when `native/` changes.
- Fix: a method like `GOT`, `PUX` or `POOL` came to the app as `GET`, `PUT` or `POST`. Dart checked only the length and the first byte of the method. Now it checks every byte.
- Fix: `Transfer-Encoding` in an HTTP/1.0 request, or in two headers, now gets `400` (RFC 9112). Before, the body was read as chunked. A proxy in front could read the body another way (request smuggling).
- Fix: `Connection: close` in one header and `keep-alive` in another now closes the connection. Before, the last header won.
- Fix: a WebSocket key or version sent twice no longer upgrades (RFC 6455). A key that is not base64 of 16 bytes (24 characters, eg. an empty key) no longer upgrades either. These requests go to the app as normal requests. `websocket` in any `Upgrade` header and `100-continue` in any `Expect` header now count, not only in the last one.
- Fix: only `multipart/form-data` gets the multipart body limit. Before, any content type with the text `form-data` in it got it (eg. `multipart/x-form-data`).
- New: the fuzz tests now check the request record for Dart and the body framing against a reference, and fuzz a whole connection (`conn`) and the cookie decryption (`aead_open`).

## 3.0.4

- Fix: the build hook now tries the download of the native library up to 3 times (waits 1 s, then 3 s). Before, one bad answer from GitHub (eg. 503 or 504) meant no native library, and the app stopped at start. It tries again on a 5xx or 429 status, a connection error or timeout, and a wrong sha256. A 404 is not tried again. If all 3 tries fail, nothing changes: the hook falls back to cargo or adds no library.

## 3.0.3

- New: `NativeHttpServer.bind(idleTimeout: ...)`. A keep-alive connection with no new request for this time closes. A request head must also come whole in this time. Default: 30 seconds, as before. Behind a load balancer, set it longer than the idle timeout of the balancer (eg. 65 seconds for AWS ALB, which keeps connections for 60 seconds). Else the server may close a connection the balancer still uses, and clients get 502.
- The `idleTimeout` field now shows the value given to `bind`. Setting it later still has no effect, and it stays deprecated.
- Fix: with `autoCompress`, answers that could be gzipped now send `Vary: Accept-Encoding`, so a shared cache does not give a gzip body to a client that can not read it.
- Fix: `Accept-Encoding: gzip;q=0` means "no gzip", and is now read that way.
- Fix: bodies that are compressed already (images, audio, video, zip, gz, woff and so on) are not gzipped again. `image/svg+xml` is still gzipped.
- Native ABI 9 (`dox_bind` takes the idle timeout). The build hook gets the new native library.

## 3.0.2

- New: `AesGcm.seal` and `AesGcm.open`, AES-256-GCM in Rust. It uses the AES and carry-less multiply instructions of the CPU. dox_core uses it for encrypted cookies and `AESEncryptor`. One cookie took about 100 µs in pure Dart (pointycastle), and now takes under a few µs. The layout (nonce + ciphertext + tag) is the same, so values made before still open.
- Native ABI 8 (two new FFI calls). The build hook gets the new native library.

## 3.0.1

- Fix (Linux): a client that sent its request and then closed its sending side never had its connection closed. The close came in the same event as the request, and the server waited for a new event that never came. Now the server keeps the close from the event and reads on until it sees it. Test: a client that stops sending still gets its answer, and the connection closes.
- Fix: an answer that comes later (eg. after a database query) could wait up to seconds. The idle helper thread held the sockets, and Dart had to wait until the helper gave them back. Now the helper only watches the poll file (epoll or kqueue) and never holds the sockets, so Dart never waits for it. Tests: answers after a reply from another isolate, after timers of different lengths, a long async task next to fast requests, and a long synchronous task.
- Perf: one thread per isolate. The socket work (accept, read, parse, write) now runs on the isolate's own thread, in a loop of FFI calls (`dox_run`). Before, each isolate had its own Rust network thread, and every request and answer went from one thread to the other. A small helper thread now only watches the sockets while the isolate is idle, and wakes it when a request comes.
- All features stay the same: WebSocket (now the sync `tungstenite` crate), streamed answers, big bodies without a copy, gzip (on a helper thread), `Expect: 100-continue`, `400`/`413`/`431`/`408`, lingering close, graceful `close()` and `close(force: true)`.
- New: at most 64 requests of one connection wait for an answer. Then the server stops reading that connection until answers go out, so one client can not fill the memory with pipelined requests.
- New: a request head must arrive within 30 seconds, and a connection with no request for 30 seconds is closed (like hyper before).
- New: a request with both `Content-Length` and `Transfer-Encoding: chunked` gets `400`. A `Transfer-Encoding` other than `chunked` gets `501`.
- Removed: the hyper and tokio server and the `DOX_HTTP_THREAD=shared` setting.
- New: `NativeHttpServer.bind(maxConnections: ...)`. At the limit the server stops accepting until a connection closes. Default: no limit.
- Breaking: native ABI 7 (new FFI calls, the big bodies come as native memory with a finalizer, `dox_bind` takes the connection limit). The build hook gets the new native library.

### Security

- A WebSocket client that sent ping frames without end held the isolate, so other requests waited for seconds. Now one call reads at most 256 KB of one WebSocket, and each connection gets one turn per call.
- A WebSocket client that sent pings and never read the pongs made memory grow without limit. Now the server stops reading its frames while more than 256 KB waits to be written.
- A client that pipelined requests and never read the answers made memory grow without limit. Now the server stops reading its requests while 1 MB of answers waits.
- A client that never read its HTTP answer kept the connection open forever. Now a write with no progress for 30 seconds closes it.
- A streamed answer with a wrong `contentLength` could look like two answers to the client (response desync). Now Rust counts the bytes and closes the connection on a mismatch. In Dart, like `dart:io`, `add()` throws when the body passes the length, and `close()` fails when it is shorter.
- When the isolate died while it owned the sockets, the port stayed open and clients waited forever. Now the helper thread checks every 100 ms that the isolate lives, and closes the server when it is gone.
- `headers.contentType` with a line break in a part could add a header. Now it throws a `FormatException`.
- `Content-Length` took `+5` and other white space. Now only ASCII digits pass, else `400`.
- An idle connection no longer holds a 16 KB read buffer.
- The idle time of a keep-alive connection now counts from its last answer, not its last request.
- A huge `bodyTimeout` no longer overflows the clock (it is cut to one year). A port out of range throws an `ArgumentError`.
- A raw UTF-8 byte in the request target now reaches `req.uri` as `%XX` of that byte, not as Latin-1 text.

## 3.0.0

- README: streamed bodies (over 64 KB with `addStream`) are supported. The build hook has 4 steps, not 3.

## 3.0.0-alpha.8

- Perf: pipelined requests are faster. Before, hyper served the requests of one connection one at a time. So 16 pipelined requests needed 16 trips to Dart, one after the other, and 16 writes. Now Rust reads the pipelined GET and HEAD requests ahead and sends them to Dart in the same batch. The answers still go out in order, and the ones that are ready go out in one write. A request with a body, a chunked body, an upgrade or `Connection: close` stops the read ahead, and hyper serves the rest alone like before.
- Fix: `headers.set()` and `headers.add()` with a bad value (eg. a line break) threw, but `set()` had already removed the old value. Now all values are checked first, so a bad value leaves the headers unchanged.

## 3.0.0-alpha.7

- The package moved to its own repository: https://github.com/dartondox/dox-http-native. The ready-built libraries are on its releases (tag `v<version>`).

- New: a streamed response (`addStream`, eg. a file or a download) over 64 KB goes out in parts. Before, the whole body was kept in memory until `close()`, and the client got nothing until then. Now the client gets the first bytes early, and at most 1 MB waits in Rust. Dart reads more only when the client has read what was sent. When the client leaves, `addStream` stops reading the source. A small response still goes out in one piece. A streamed response with `contentLength` set sends `Content-Length`, else it is chunked.
- Perf: a request body of 64 KB or more goes to Dart without a copy (external typed data). Before, it was copied 3 times.
- Perf: `compress: true` gzips on a Rust thread, not on the isolate. Same output (gzip, default level). A streamed response is not compressed.
- Perf: a response with the default headers and one content type (eg. JSON) copies no headers, and its header bytes are cached.
- Perf: request headers are parsed into a map on the second lookup, not scanned again and again.
- Perf: a request path with no query skips the full `Uri.parse`. Rust takes the lock of the answers once per batch.
- Breaking: native ABI 6 (streamed body records, gzip flag, big bodies as their own part of the message). The build hook gets the new native library.

- New: WebSocket upgrade on the native server. `NativeWebSocket.upgrade(request, protocol: ..., maxMessageSize: ...)` accepts it and gives a `dart:io` `WebSocket`. Rust (tokio-tungstenite) speaks the protocol: ping and pong, the close handshake, the UTF-8 check and the size limit (a bigger message closes with `1009`). Compression is never agreed. `NativeWebSocket.isUpgradeRequest(request)` tells if a request asks for one.
- On a graceful `close()`, open WebSockets get close code `1001`.
- WebSocket limits, so a slow client or a busy isolate can not make memory grow forever:
  - Messages to a client wait in a queue. Over 4 times `maxMessageSize` (at least 1 MiB) the socket closes with `1008` (policy violation).
  - A message that can not be sent in 30 seconds drops the socket (`1006`). The close handshake (our close frame and the answer) has 5 seconds in all, then the socket is dropped (`1006`).
  - When Dart has more than 8 MiB of client messages not yet handled, Rust stops reading from WebSocket clients until it catches up. No message is lost. HTTP is not paused.
- New: `NativeHttpServer.ensureAvailable()` throws an error that says why the library did not load and how to fix it. `bind` throws the same error.
- Breaking: native ABI 5 (new WebSocket records, and an ack record from Dart). The build hook gets the new native library.
- Breaking: Dox has no `dart:io` server any more. When this library does not load, Dox stops at start with the reason.
- Security: a multipart form may be up to `maxMultipartSize` bytes (new `bind` option, default `maxBodySize`). Any other body, eg. JSON, raw or text, is limited to `maxBodySize`. Dox passes its own two limits, so a JSON body over 10 MB now gets `413` from Rust.
- Security: a body with a too big `Content-Length` and `Expect: 100-continue` gets `413` at once. Without `Expect`, Rust reads and drops the body (up to 64 MB more, no memory kept), then sends `413`, so the client sees the `413` and not a connection reset.
- Security: Rust no longer reserves memory from the client `Content-Length` (at most 64 KB up front).
- Security: new `bind` option `bodyTimeout` (default 120 seconds). It is the idle time with no body bytes: a client that sends nothing for that long gets `408`. A slow but steady upload is fine.
- Native ABI 4 (`dox_start` takes the multipart limit and the body timeout). Needs a new native library build.
- Security: response headers now refuse bad input with a `FormatException`, like `dart:io`. A header name must be a token. A header value must not hold a line break or other control byte. Before, a `\n` in a value (or a char like `\u010A`, written as its low byte) added one more header, eg. `Set-Cookie`, when an app put request input in a header. The same check now runs on `serverHeader` and on each cookie when the response closes, because `Cookie` does not check its domain.

## 3.0.0-alpha.6

- Version bump only, to match the other packages.

## 3.0.0-alpha.5

- New: `NativeHttpServer.bind(port, address: ...)` listens on one host, like `HttpServer.bind`. An `InternetAddress` or a host name. With no address it listens on all interfaces, as before. `server.address` is the real address.
- Fix: `close()` never finished when nobody called `listen()` on the server.
- Native ABI 3 (`dox_start` takes the host). Needs a new native library build. Until the next release, the build hook builds it with cargo.
- New: `close()` is graceful, like `dart:io`. The server stops taking connections at once, each open connection closes after its current answer, and the future completes when all are closed. `close(force: true)` closes every connection at once.

## 3.0.0-alpha.4

- Version bump only, to match the other packages.

## 3.0.0-alpha.3

- `DOX_HTTP_THREAD=shared` runs one `dox-http` thread for all isolates.
  Default is still one thread per isolate, which measured faster.
- Less work per request: request headers are read one name at a time,
  answers are not copied again in Rust, one wake per answer batch.
- Native ABI 2 (new header layout in the request record). Needs a new
  native library build.

## 3.0.0-alpha.2

- First release.
- Native HTTP/1.1 server: Rust (hyper, tokio) owns the sockets and parses
  HTTP. Handlers stay in Dart.
- Implements the `dart:io` `HttpServer`, `HttpRequest` and `HttpResponse`
  API, so code written for `dart:io` works unchanged.
- One server per isolate on the same port (`SO_REUSEPORT` on Linux and macOS).
- 400 for bad requests, 431 for big headers, 413 for big bodies.
- The build hook downloads a ready-built library. No Rust needed.
