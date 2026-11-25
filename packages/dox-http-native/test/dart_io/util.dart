// Shared helpers for the tests ported from dart-lang/sdk tests/standalone/io.
// Every server binds to an ephemeral port and closes in a tearDown.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

/// Binds a native server on [host] port 0, listens with [handler], and closes
/// it (force) when the test ends.
Future<NativeHttpServer> serve(
  void Function(HttpRequest request) handler, {
  String host = '127.0.0.1',
  Duration? idleTimeout,
  int? maxBodySize,
  bool autoCompress = false,
}) async {
  NativeHttpServer server = await NativeHttpServer.bind(
    0,
    address: host,
    idleTimeout: idleTimeout ?? const Duration(seconds: 30),
    maxBodySize: maxBodySize ?? (1 << 30),
  );
  server.autoCompress = autoCompress;
  addTearDown(() => server.close(force: true));
  server.listen(handler);
  return server;
}

/// A client that is closed (force) when the test ends.
HttpClient newClient() {
  HttpClient client = HttpClient();
  addTearDown(() => client.close(force: true));
  return client;
}

/// Sends [request] as latin1 on a raw socket. Returns everything that the
/// server sends until it closes the connection or [wait] passes.
Future<String> raw(
  int port,
  Object request, {
  Duration wait = const Duration(seconds: 3),
  Iterable<Object>? chunks,
}) async {
  Socket socket = await Socket.connect('127.0.0.1', port);
  StringBuffer out = StringBuffer();
  Completer<void> done = Completer<void>();
  socket.listen(
    (List<int> data) => out.write(latin1.decode(data)),
    onError: (Object _) {},
    onDone: () {
      if (!done.isCompleted) done.complete();
    },
  );
  void send(Object o) =>
      o is List<int> ? socket.add(o) : socket.add(latin1.encode(o as String));
  try {
    send(request);
    await socket.flush();
    if (chunks != null) {
      for (Object c in chunks) {
        await Future<void>.delayed(const Duration(milliseconds: 20));
        send(c);
        await socket.flush();
      }
    }
  } on SocketException {
    // the server closed already (for example a bad request)
  }
  await done.future.timeout(wait, onTimeout: () {});
  socket.destroy();
  return out.toString();
}

/// The status code of the first response in [response], or 0 when empty.
int statusOf(String response) {
  RegExpMatch? m = RegExp(r'^HTTP/1\.[01] (\d{3})').firstMatch(response);
  return m == null ? 0 : int.parse(m.group(1)!);
}

/// Reads the whole response and returns it as a string.
Future<String> body(Stream<List<int>> s) => utf8.decodeStream(s);

/// A server for the tests that do `server.first`: binds, and gives the port
/// and the future of the first request.
Future<(int, Future<HttpRequest>)> serveFirst({
  String host = '127.0.0.1',
}) async {
  Completer<HttpRequest> first = Completer<HttpRequest>();
  NativeHttpServer server = await serve((HttpRequest r) {
    if (!first.isCompleted) first.complete(r);
  }, host: host);
  return (server.port, first.future);
}
