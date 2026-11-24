/// A fast HTTP client (HTTP/1.1 and HTTP/2) for Dart. Rust owns the sockets and parses the
/// answers through dart:ffi, on the thread of the isolate.
library;

export 'src/client.dart'
    show
        DoxHttpClient,
        DoxHttpClientException,
        DoxHttpClientResponse,
        DoxHttpClientResponseHead,
        DoxHttpClientStreamedResponse,
        DoxHttpFailure;
