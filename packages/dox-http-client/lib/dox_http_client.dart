/// A fast HTTP/1.1 client for Dart. Rust owns the sockets and parses the
/// answers through dart:ffi, on the thread of the isolate.
library;

export 'src/client.dart'
    show
        DoxHttpClient,
        DoxHttpClientException,
        DoxHttpClientResponse,
        DoxHttpFailure;
