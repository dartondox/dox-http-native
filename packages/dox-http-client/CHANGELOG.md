## Unreleased

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
