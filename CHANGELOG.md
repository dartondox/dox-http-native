## 3.0.1

- Perf: one thread per isolate. The socket work (accept, read, parse, write) now runs on the isolate's own thread, in a loop of FFI calls (`dox_run`). Before, each isolate had its own Rust network thread, and every request and answer went from one thread to the other. A small helper thread now only watches the sockets while the isolate is idle, and wakes it when a request comes.
- All features stay the same: WebSocket (now the sync `tungstenite` crate), streamed answers, big bodies without a copy, gzip (on a helper thread), `Expect: 100-continue`, `400`/`413`/`431`/`408`, lingering close, graceful `close()` and `close(force: true)`.
- New: at most 64 requests of one connection wait for an answer. Then the server stops reading that connection until answers go out, so one client can not fill the memory with pipelined requests.
- New: a request head must arrive within 30 seconds, and a connection with no request for 30 seconds is closed (like hyper before).
- New: a request with both `Content-Length` and `Transfer-Encoding: chunked` gets `400`. A `Transfer-Encoding` other than `chunked` gets `501`.
- Removed: the hyper and tokio server and the `DOX_HTTP_THREAD=shared` setting.
- New: `NativeHttpServer.bind(maxConnections: ...)`. At the limit the server stops accepting until a connection closes. Default: no limit.
- Native ABI 9 (new FFI calls, the big bodies come as native memory with a finalizer, `dox_bind` takes the connection limit). Needs a new native library build. The build hook builds it with cargo until the next release.

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
- Breaking: native ABI 6 (streamed body records, gzip flag, big bodies as their own part of the message). Needs a new native library build. The build hook builds it with cargo until the next release.

- New: WebSocket upgrade on the native server. `NativeWebSocket.upgrade(request, protocol: ..., maxMessageSize: ...)` accepts it and gives a `dart:io` `WebSocket`. Rust (tokio-tungstenite) speaks the protocol: ping and pong, the close handshake, the UTF-8 check and the size limit (a bigger message closes with `1009`). Compression is never agreed. `NativeWebSocket.isUpgradeRequest(request)` tells if a request asks for one.
- On a graceful `close()`, open WebSockets get close code `1001`.
- WebSocket limits, so a slow client or a busy isolate can not make memory grow forever:
  - Messages to a client wait in a queue. Over 4 times `maxMessageSize` (at least 1 MiB) the socket closes with `1008` (policy violation).
  - A message that can not be sent in 30 seconds drops the socket (`1006`). The close handshake (our close frame and the answer) has 5 seconds in all, then the socket is dropped (`1006`).
  - When Dart has more than 8 MiB of client messages not yet handled, Rust stops reading from WebSocket clients until it catches up. No message is lost. HTTP is not paused.
- New: `NativeHttpServer.ensureAvailable()` throws an error that says why the library did not load and how to fix it. `bind` throws the same error.
- Breaking: native ABI 5 (new WebSocket records, and an ack record from Dart). Needs a new native library build. The build hook builds it with cargo until the next release.
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
