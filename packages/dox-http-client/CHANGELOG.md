## 0.0.2

- HTTP/2. On an `https://` URL, the client asks for HTTP/2 during the TLS handshake (ALPN) and uses it when the server has it. If not, it uses HTTP/1.1.
  - The requests to one HTTP/2 server share one connection. A request that fails or gets a timeout stops only its stream. Requests that start at the same time to a new server can open more than one connection.
  - `send`, `sendStreamed` and the other methods do not change. Streamed bodies use the flow control of HTTP/2: when the listener pauses, the server stops to send.
  - A request that the server did not work on (a refused stream, or a GOAWAY frame for an earlier stream) is sent again one time, for each method. When the connection closes before the head of an answer, only `GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS` and `TRACE` are sent again. A request with a body stream is never sent again.
  - `DoxHttpClient(http2: false)` uses HTTP/1.1 only.
  - The native library does the chunked framing of a request body stream now. The ABI version of the native library is 4.
  - The fuzz target also gives random bytes to the HTTP/2 connection and to the HPACK decoder.
- `DoxHttpClient.sendStreamed` sends a request with a body stream and gives the answer body as a stream. The memory stays small for bodies of many GB.
  - The answer body is `DoxHttpClientStreamedResponse.body`. The new base class `DoxHttpClientResponseHead` has `url`, `statusCode`, `headers` and `header(name)`.
  - The client reads a maximum of about 512 KiB ahead. When the listener pauses, the client stops to read the connection. When you cancel the body, the client closes the connection.
  - The request `body` is a `Stream<List<int>>`. `contentLength` sets the `content-length` header. Without it, the client sends the body chunked. The client pauses the stream while more than 256 KiB wait to go out.
  - `timeout` is for the time until the head of the answer is there. With a body stream, the timeout starts again each time bytes of the body go out.
  - A gzip or brotli body is decoded in parts. `maxResponseSize` is the limit for the decoded bytes.
  - A failure after the head is an error event on the body stream.
  - A request with no body follows redirects. A request with a body stream does not follow them and is sent only once.
  - If the server answers before the whole body went out, the client cancels the upload and does not use the connection again.
  - New `DoxHttpFailure.badRequestBody`: the body stream gave more or fewer bytes than `contentLength`.
  - The records between Dart and the native library have a kind, so the answers of requests that are not streamed are as cheap as before.
- The client follows redirects (status 301, 302, 303, 307 and 308). One request follows a maximum of 5 redirects, then it fails with the new `DoxHttpFailure.tooManyRedirects`.
  - `followRedirects` (for the client or for one `send` call) and `maxRedirects` change this.
  - A 303 redirect changes the method to `GET`. A 301 or 302 redirect changes `POST` to `GET`. A 307 or 308 redirect keeps the method and the body.
  - The `authorization`, `cookie`, `proxy-authorization` and `host` headers do not go to a different server.
  - The timeout of a request includes its redirects.
- `DoxHttpClientResponse.url` is the URL that gave the answer.
- The client sends `accept-encoding: gzip, br` and decodes a gzip or brotli answer body in Rust.
  - A decoded answer has no `content-encoding` header and no `content-length` header.
  - `maxResponseSize` is the limit for the decoded body.
  - A request with its own `accept-encoding` header or with a `range` header gets the body as the server sent it.
  - The native library has two more crates: [flate2](https://github.com/rust-lang/flate2-rs) and [brotli-decompressor](https://github.com/dropbox/rust-brotli-decompressor).

## 0.0.1

- First version. `DoxHttpClient` is an HTTP client for calls to other servers. Rust owns the sockets and reads the answers on the thread of the isolate.
  - `get`, `head`, `post`, `put`, `patch`, `delete` and `send` return a `DoxHttpClientResponse` with `statusCode`, `body`, `bodyBytes`, `headers` and `header(name)`.
  - HTTP/1.1 with keep-alive: the client keeps its connections open and uses them again. Chunked answers, answers that end with the connection, IPv4, IPv6 and host names are supported. When the first address of a host name does not accept the connection, the client tries the next address.
  - HTTPS with TLS 1.2 and TLS 1.3 ([rustls](https://github.com/rustls/rustls)). The client trusts the root certificates of Mozilla, which are in the library. `trustedCertificates` adds your own certificates as PEM text.
  - A request fails with a `DoxHttpClientException` when no answer comes. `failure` gives the cause as a `DoxHttpFailure`.
  - The default timeout of a request is 30 seconds. Set `timeout` for the client or for one request. `Duration.zero` is no timeout.
  - A connection with no request for `idleTimeout` (default 15 seconds) closes. `Duration.zero` keeps idle connections open.
  - `maxConnectionsPerHost` and `maxResponseSize` are off by default.
  - Before a kept connection gets the next request, the client reads it. If the server closed it or sent bytes that nobody asked for, the client closes it and uses a different connection.
  - The client does not do these things: HTTP/2, redirects, compressed bodies and streamed bodies.
  - The package has its own native library. Its functions start with `dox_client_`, so an app can load it together with the library of `dox_http_native`.
  - A build from source needs a C compiler, because the TLS library compiles C code and assembly code. The ready-built libraries from the GitHub release do not need one.
  - Fuzz target `client_response` for the answer parser.
  - `THIRD_PARTY_NOTICES` lists the licenses of the code and data in the native library. Each release also has it next to the libraries.
