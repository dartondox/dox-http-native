import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

/// test app: `/` hello, `/echo` the body back, `/who` the isolate name,
/// `/headers` some request data as json, anything else 404.
void handle(HttpRequest req) async {
  HttpResponse res = req.response;
  switch (req.uri.path) {
    case '/':
      res.write('Hello World');
    case '/echo':
      List<int> body = <int>[for (List<int> c in await req.toList()) ...c];
      res.headers.contentType = ContentType.binary;
      res.add(body);
    case '/slow':
      await Future<void>.delayed(const Duration(milliseconds: 500));
      res.write('slow done');
    case '/timer':
      // no other request is open: the answer must not wait for Rust
      await Future<void>.delayed(const Duration(milliseconds: 50));
      res.write('timer done');
    case '/who':
      res.write(Isolate.current.debugName);
    case '/json':
      res.headers.contentType = ContentType.json;
      res.write('{"a":1}');
    case '/long':
      res.write('a' * 1000);
    case '/stream':
      // 5 MiB in 64 KiB parts. ?length=1 sets the content length.
      res.headers.contentType = ContentType.binary;
      if (req.uri.queryParameters['length'] == '1') {
        res.contentLength = 80 * 65536;
      }
      await res.addStream(parts(80));
    case '/liar':
      // a content length that is too small for the streamed body
      res.contentLength = 100000;
      try {
        await res.addStream(parts(2));
      } on HttpException catch (e) {
        lengthErrors.add(e.message);
      }
    case '/short':
      // a content length that is too big for the streamed body
      res.contentLength = 200000;
      await res.addStream(parts(2));
      try {
        await res.close();
      } on HttpException catch (e) {
        lengthErrors.add(e.message);
      }
      return;
    case '/forever':
      // never ends on its own: only a client that leaves stops it
      await res.addStream(parts(null));
      foreverDone.complete(sentParts);
    case '/headers':
      res.headers.contentType = ContentType.json;
      res.cookies.add(Cookie('a', 'b'));
      res.write(
        jsonEncode(<String, dynamic>{
          'method': req.method,
          'query': req.uri.queryParameters,
          'x': req.headers.value('x-test'),
          'ip': req.connectionInfo?.remoteAddress.address,
          'cookie': req.cookies
              .map((Cookie c) => '${c.name}=${c.value}')
              .join(),
        }),
      );
    case String path when path.startsWith('/status/'):
      // a body for 204 or 304 must not go out
      res.statusCode = int.parse(path.substring(8));
      res.write('x');
    case String path when path.startsWith('/n/'):
      res.write(path.substring(3));
    case '/bad-cookie':
      res.cookies.add(Cookie('a', 'b')..domain = 'x\r\nset-cookie: c=d');
      try {
        await res.close();
      } on FormatException {
        res.cookies.clear();
        res.write('refused');
      }
    default:
      res.statusCode = HttpStatus.notFound;
      res.write('not found');
  }
  await res.close();
}

/// [count] parts of 64 KiB (null: no end). Part i holds the byte i % 256.
Stream<List<int>> parts(int? count) async* {
  sentParts = 0;
  for (int i = 0; count == null || i < count; i++) {
    sentParts++;
    yield Uint8List(65536)..fillRange(0, 65536, i % 256);
  }
}

int sentParts = 0;

/// errors of streamed bodies that did not keep to their content length
List<String> lengthErrors = <String>[];
Completer<int> foreverDone = Completer<int>();

Future<NativeHttpServer> start(int port, {int maxBodySize = 1024}) async {
  NativeHttpServer server = await NativeHttpServer.bind(
    port,
    maxBodySize: maxBodySize,
  );
  server.listen(handle);
  return server;
}

/// send raw bytes, read until the server closes or [wait] passes.
Future<String> raw(int port, String request, {Duration? wait}) async {
  Socket socket = await Socket.connect('127.0.0.1', port);
  socket.write(request);
  await socket.flush();
  StringBuffer out = StringBuffer();
  Completer<void> done = Completer<void>();
  socket.listen(
    (List<int> data) => out.write(latin1.decode(data)),
    onDone: () {
      if (!done.isCompleted) done.complete();
    },
    onError: (Object _) {
      if (!done.isCompleted) done.complete();
    },
  );
  await done.future.timeout(
    wait ?? const Duration(seconds: 5),
    onTimeout: () {},
  );
  socket.destroy();
  return out.toString();
}

/// Load from another isolate: 32 clients send requests to [args] port
/// one after the other, until [args] milliseconds passed.
void load(List<Object> args) async {
  int port = args[0] as int;
  Stopwatch clock = Stopwatch()..start();
  HttpClient client = HttpClient()..maxConnectionsPerHost = 32;
  Future<void> one() async {
    while (clock.elapsedMilliseconds < (args[1] as int)) {
      HttpClientResponse res = await (await client.get(
        '127.0.0.1',
        port,
        '/',
      )).close();
      await res.drain<void>();
    }
  }

  await Future.wait(<Future<void>>[for (int i = 0; i < 32; i++) one()]);
  client.close(force: true);
  (args[2] as SendPort).send(true);
}

void spawned(List<Object> args) async {
  SendPort ready = args[1] as SendPort;
  await start(args[0] as int);
  ready.send(true);
}

/// A server whose isolate dies on its first request (an uncaught error),
/// while Dart owns the sockets. Sends its port to [args].
void dies(SendPort ready) async {
  NativeHttpServer server = await NativeHttpServer.bind(
    0,
    address: '127.0.0.1',
  );
  server.listen((HttpRequest req) {
    scheduleMicrotask(() => throw StateError('the isolate dies'));
  });
  ready.send(server.port);
}

void main() {
  late NativeHttpServer server;
  late int port;
  HttpClient client = HttpClient();

  Future<(int, String)> get(String path, {Map<String, String>? headers}) async {
    HttpClientRequest req = await client.get('127.0.0.1', port, path);
    headers?.forEach(req.headers.set);
    HttpClientResponse res = await req.close();
    return (res.statusCode, await utf8.decodeStream(res));
  }

  setUpAll(() async {
    expect(NativeHttpServer.loadError, isNull);
    server = await start(0);
    port = server.port;
  });

  tearDownAll(() async {
    client.close(force: true);
    await server.close();
  });

  test('GET', () async {
    HttpClientRequest req = await client.get('127.0.0.1', port, '/');
    HttpClientResponse res = await req.close();
    expect(res.statusCode, 200);
    expect(res.headers.contentType?.mimeType, 'text/plain');
    expect(res.headers.value('x-content-type-options'), 'nosniff');
    expect(await utf8.decodeStream(res), 'Hello World');
  });

  test('request data: method, query, headers, ip, cookies', () async {
    (int, String) res = await get(
      '/headers?a=1&b=two',
      headers: <String, String>{'x-test': 'yes', 'cookie': 'k=v'},
    );
    expect(res.$1, 200);
    expect(jsonDecode(res.$2), <String, dynamic>{
      'method': 'GET',
      'query': <String, dynamic>{'a': '1', 'b': 'two'},
      'x': 'yes',
      'ip': anyOf('127.0.0.1', '::ffff:127.0.0.1'),
      'cookie': 'k=v',
    });
  });

  test('POST body comes back the same', () async {
    List<int> body = List<int>.generate(1000, (int i) => i % 256);
    HttpClientRequest req = await client.post('127.0.0.1', port, '/echo');
    req.add(body);
    HttpClientResponse res = await req.close();
    expect(res.statusCode, 200);
    expect(<int>[for (List<int> c in await res.toList()) ...c], body);
  });

  test('a chunked request body comes back the same', () async {
    HttpClientRequest req = await client.post('127.0.0.1', port, '/echo');
    req.contentLength = -1; // chunked
    req.add(List<int>.filled(300, 7));
    req.add(List<int>.filled(200, 8));
    HttpClientResponse res = await req.close();
    expect(res.statusCode, 200);
    expect(
      <int>[for (List<int> c in await res.toList()) ...c],
      <int>[...List<int>.filled(300, 7), ...List<int>.filled(200, 8)],
    );
  });

  test('a handler that awaits a timer answers on time', () async {
    // warm up the connection, then time a request with no other one open
    await get('/');
    for (int i = 0; i < 5; i++) {
      Stopwatch clock = Stopwatch()..start();
      expect(await get('/timer'), (200, 'timer done'));
      expect(clock.elapsedMilliseconds, lessThan(100));
    }
  });

  test('a timer on the server isolate runs on time under load', () async {
    ReceivePort done = ReceivePort();
    await Isolate.spawn(load, <Object>[port, 1500, done.sendPort]);
    await Future<void>.delayed(const Duration(milliseconds: 200));
    // like a database reply: a short wait on the isolate that serves
    int worst = 0;
    for (int i = 0; i < 20; i++) {
      Stopwatch clock = Stopwatch()..start();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      worst = max(worst, clock.elapsedMilliseconds - 10);
    }
    expect(worst, lessThan(50), reason: 'late by $worst ms');
    await done.first;
  });

  test('a line break in a cookie domain or server header is refused', () async {
    expect(await get('/bad-cookie'), (200, 'refused'));
    expect(() => server.serverHeader = 'a\nb', throwsFormatException);
  });

  test('404', () async {
    expect(await get('/nope'), (404, 'not found'));
  });

  test('keep-alive and pipelining: 3 answers on one connection', () async {
    String out = await raw(
      port,
      'GET / HTTP/1.1\r\nHost: a\r\n\r\n'
      'GET /nope HTTP/1.1\r\nHost: a\r\n\r\n'
      'GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n',
    );
    expect(
      RegExp(r'HTTP/1.1 (\d+)').allMatches(out).map((Match m) => m[1]),
      <String>['200', '404', '200'],
    );
  });

  test('16 pipelined requests in one write: 16 answers in order', () async {
    // 3 is slow (Dart answers it last), 9 has a body: the order must hold
    String request(int i) => switch (i) {
      3 => 'GET /slow HTTP/1.1\r\nHost: a\r\n\r\n',
      9 => 'POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 4\r\n\r\nbody',
      15 => 'GET /n/15 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n',
      _ => 'GET /n/$i HTTP/1.1\r\nHost: a\r\n\r\n',
    };
    String out = await raw(
      port,
      <String>[for (int i = 0; i < 16; i++) request(i)].join(),
    );
    List<String> bodies = <String>[];
    int at = 0;
    while (true) {
      int head = out.indexOf('\r\n\r\n', at);
      if (head < 0) break;
      String length = RegExp(
        r'content-length: (\d+)',
        caseSensitive: false,
      ).firstMatch(out.substring(at, head))![1]!;
      at = head + 4 + int.parse(length);
      bodies.add(out.substring(head + 4, at));
    }
    expect(bodies, <String>[
      for (int i = 0; i < 16; i++)
        i == 3 ? 'slow done' : (i == 9 ? 'body' : '$i'),
    ]);
  });

  test('400 for garbage', () async {
    String out = await raw(port, 'HELLO\r\n\r\n');
    expect(out, startsWith('HTTP/1.1 400'));
  });

  test('431 for headers that are too large', () async {
    String big = 'a' * 20000;
    String out = await raw(
      port,
      'GET / HTTP/1.1\r\nHost: a\r\nX-Big: $big\r\n\r\n',
    );
    expect(out, startsWith('HTTP/1.1 431'));
  });

  group('HTTP edge cases', () {
    test('204 and 304 have no body, the connection goes on', () async {
      String out = await raw(
        port,
        'GET /status/204 HTTP/1.1\r\n\r\n'
        'GET /status/304 HTTP/1.1\r\n\r\n'
        'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(out, startsWith('HTTP/1.1 204'));
      expect(out, contains('HTTP/1.1 304'));
      expect(out, endsWith('\r\n\r\nHello World'));
      // each head is followed by the next answer, with no body between
      expect(out, contains('\r\n\r\nHTTP/1.1 304'));
      expect(out, contains('\r\n\r\nHTTP/1.1 200'));
    });

    test('a request sent one byte at a time', () async {
      Socket socket = await Socket.connect('127.0.0.1', port);
      StringBuffer out = StringBuffer();
      Future<void> done = socket
          .listen((List<int> data) => out.write(latin1.decode(data)))
          .asFuture<void>();
      for (int b in latin1.encode(
        'POST /echo HTTP/1.1\r\nContent-Length: 5\r\n'
        'Connection: close\r\n\r\nhello',
      )) {
        socket.add(<int>[b]);
        await socket.flush();
      }
      await done.timeout(const Duration(seconds: 5));
      socket.destroy();
      expect(out.toString(), startsWith('HTTP/1.1 200'));
      expect(out.toString(), endsWith('hello'));
    });

    test('a client that stops sending still gets its answer', () async {
      Socket socket = await Socket.connect('127.0.0.1', port);
      StringBuffer out = StringBuffer();
      Future<void> done = socket
          .listen((List<int> data) => out.write(latin1.decode(data)))
          .asFuture<void>();
      socket.write('GET /slow HTTP/1.1\r\n\r\n');
      await socket.close(); // closes the sending side only
      await done.timeout(const Duration(seconds: 5));
      expect(out.toString(), startsWith('HTTP/1.1 200'));
      expect(out.toString(), endsWith('slow done'));
    });

    test(
      'a GET with a body: the body is read, the next request works',
      () async {
        String out = await raw(
          port,
          'GET /echo HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc'
          'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
        );
        expect(out, contains('\r\n\r\nabcHTTP/1.1 200'));
        expect(out, endsWith('Hello World'));
      },
    );

    test('two different Content-Length values get 400', () async {
      String out = await raw(
        port,
        'POST /echo HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 4\r\n'
        '\r\nabcd',
      );
      expect(out, startsWith('HTTP/1.1 400'));
    });

    test('a request with no Host', () async {
      String out = await raw(
        port,
        'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      // accepted, like dart:io and hyper. RFC 9112 asks for 400.
      expect(out, startsWith('HTTP/1.1 200'));
    });

    test('a very long URL', () async {
      String out = await raw(
        port,
        'GET /${'a' * 20000} HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      // the URL counts toward the head limit
      expect(out, startsWith('HTTP/1.1 431'));
    });
  });

  test('413 for a body over the limit, with and without length', () async {
    HttpClientRequest req = await client.post('127.0.0.1', port, '/echo');
    req.add(List<int>.filled(5000, 1));
    expect((await req.close()).statusCode, 413);

    HttpClientRequest chunked = await client.post('127.0.0.1', port, '/echo');
    chunked.contentLength = -1;
    chunked.add(List<int>.filled(5000, 1));
    expect((await chunked.close()).statusCode, 413);
  });

  test('multipart limit, 413 and idle body timeout', () async {
    NativeHttpServer small = await NativeHttpServer.bind(
      0,
      maxBodySize: 10,
      maxMultipartSize: 100,
      bodyTimeout: const Duration(milliseconds: 200),
    );
    small.listen(handle);
    String body = 'a' * 50;
    String post(String type, int length) =>
        'POST /echo HTTP/1.1\r\nHost: a\r\nConnection: close\r\n'
        'Content-Type: $type\r\nContent-Length: $length\r\n\r\n';
    String form = await raw(
      small.port,
      '${post('multipart/form-data', 50)}$body',
    );
    expect(form, startsWith('HTTP/1.1 200'));
    String json = await raw(small.port, '${post('application/json', 50)}$body');
    expect(json, startsWith('HTTP/1.1 413'));
    // "Expect: 100-continue": the 413 comes before the body is sent
    String early = await raw(
      small.port,
      post(
        'application/json',
        1 << 30,
      ).replaceFirst('Host: a\r\n', 'Host: a\r\nExpect: 100-continue\r\n'),
    );
    expect(early, startsWith('HTTP/1.1 413'));
    // a full body over the limit with HttpClient: 413, not a reset
    HttpClientRequest req = await client.post('127.0.0.1', small.port, '/');
    req.add(List<int>.filled(1 << 20, 1));
    expect((await req.close()).statusCode, 413);
    // slow but steady: 500 ms in all, never 200 ms without bytes
    Socket steady = await Socket.connect('127.0.0.1', small.port);
    steady.write(post('text/plain', 5));
    for (int i = 0; i < 5; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 100));
      steady.write('a');
      await steady.flush();
    }
    String out = latin1.decode(<int>[
      for (List<int> c in await steady.toList()) ...c,
    ]);
    steady.destroy();
    expect(out, startsWith('HTTP/1.1 200'));
    // stalled: no more bytes, 408 after 200 ms
    String slow = await raw(small.port, '${post('text/plain', 5)}ab');
    expect(slow, startsWith('HTTP/1.1 408'));
    await small.close();
  });

  test('a large body and response (5 MB)', () async {
    NativeHttpServer big = await start(0, maxBodySize: 10 << 20);
    List<int> body = List<int>.generate(5 << 20, (int i) => i % 251);
    HttpClientRequest req = await client.post('127.0.0.1', big.port, '/echo');
    req.add(body);
    HttpClientResponse res = await req.close();
    List<int> back = <int>[for (List<int> c in await res.toList()) ...c];
    expect(back.length, body.length);
    expect(back, body);
    await big.close();
  });

  test('JSON answer keeps the default headers, one content type', () async {
    HttpClientRequest req = await client.get('127.0.0.1', port, '/json');
    HttpClientResponse res = await req.close();
    expect(res.headers[HttpHeaders.contentTypeHeader], <String>[
      'application/json; charset=utf-8',
    ]);
    expect(res.headers.value('x-frame-options'), 'SAMEORIGIN');
    expect(await utf8.decodeStream(res), '{"a":1}');
  });

  test('a streamed body arrives whole, with and without a length', () async {
    for (String path in <String>['/stream?length=1', '/stream']) {
      HttpClientRequest req = await client.get('127.0.0.1', port, path);
      HttpClientResponse res = await req.close();
      expect(res.contentLength, path.endsWith('1') ? 80 * 65536 : -1);
      int at = 0;
      await for (List<int> chunk in res) {
        for (int byte in chunk) {
          expect(byte, (at ~/ 65536) % 256, reason: 'byte $at of $path');
          at++;
        }
      }
      expect(at, 80 * 65536, reason: path);
    }
  });

  test('a streamed body must keep to its content length', () async {
    // the next request on the connection must not get an answer: the
    // client could take it for the rest of the body
    String next = 'GET / HTTP/1.1\r\n\r\n';
    String liar = await raw(port, 'GET /liar HTTP/1.1\r\n\r\n$next');
    expect(liar, contains('content-length: 100000'));
    expect(liar, isNot(contains('Hello World')));
    String short = await raw(port, 'GET /short HTTP/1.1\r\n\r\n$next');
    expect(short, contains('content-length: 200000'));
    expect(short, isNot(contains('Hello World')));
    expect(lengthErrors, <Matcher>[contains('exceeds'), contains('below')]);
  });

  test('a streamed body stops when the client leaves', () async {
    foreverDone = Completer<int>();
    Socket socket = await Socket.connect('127.0.0.1', port);
    socket.write('GET /forever HTTP/1.1\r\nHost: a\r\n\r\n');
    int got = 0;
    Completer<void> some = Completer<void>();
    socket.listen((List<int> data) {
      got += data.length;
      if (got > 1 << 20 && !some.isCompleted) some.complete();
    });
    await some.future;
    socket.destroy();
    // the source is not read further: addStream ends
    int sent = await foreverDone.future.timeout(const Duration(seconds: 5));
    expect(sent, lessThan(10000));
  });

  test('compress: Rust gzips the body', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    HttpClientRequest req = await client.get('127.0.0.1', zip.port, '/long');
    HttpClientResponse res = await req.close();
    expect(res.headers.value(HttpHeaders.contentEncodingHeader), 'gzip');
    expect(
      res.compressionState,
      HttpClientResponseCompressionState.decompressed,
    );
    expect(await utf8.decodeStream(res), 'a' * 1000);
    await zip.close();
  });

  test('HEAD gets the headers with no body', () async {
    String out = await raw(
      port,
      'HEAD / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n',
    );
    expect(out, startsWith('HTTP/1.1 200'));
    expect(out.toLowerCase(), contains('content-length: 11'));
    expect(out, endsWith('\r\n\r\n'));
  });

  test('a raw UTF-8 path is read as UTF-8', () async {
    String res = await raw(
      port,
      'GET /n/é HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    // uri.path keeps the %XX: é is C3 A9 in UTF-8 (not C3 83 C2 A9)
    expect(res, endsWith('\r\n\r\n%C3%A9'));
  });

  test('a port out of range or a bad limit is refused', () async {
    for (int bad in <int>[-1, 65536, 70000]) {
      await expectLater(NativeHttpServer.bind(bad), throwsArgumentError);
    }
    await expectLater(
      NativeHttpServer.bind(0, maxConnections: 0),
      throwsArgumentError,
    );
  });

  test('maxConnections: at the limit new clients wait', () async {
    NativeHttpServer one = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      maxConnections: 1,
    );
    one.listen(handle);
    Socket first = await Socket.connect('127.0.0.1', one.port);
    first.write('GET / HTTP/1.1\r\n\r\n');
    await first.first; // its answer: the connection is open
    // the second waits in the listen queue while the first is open
    Future<String> second = raw(
      one.port,
      'GET /n/two HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    String early = await raw(
      one.port,
      'GET /n/three HTTP/1.1\r\nConnection: close\r\n\r\n',
      wait: const Duration(milliseconds: 300),
    );
    expect(early, isEmpty);
    first.destroy();
    expect(await second, endsWith('two'));
    await one.close(force: true);
  });

  test('bind to one host only', () async {
    NativeHttpServer local = await NativeHttpServer.bind(
      0,
      address: InternetAddress.loopbackIPv4,
    );
    local.listen(handle);
    expect(local.address, InternetAddress.loopbackIPv4);
    HttpClientResponse res = await (await client.get(
      '127.0.0.1',
      local.port,
      '/',
    )).close();
    expect(res.statusCode, 200);
    await res.drain<void>();
    // not on the IPv6 loopback
    await expectLater(
      Socket.connect(InternetAddress.loopbackIPv6, local.port),
      throwsA(isA<SocketException>()),
    );
    await local.close();

    NativeHttpServer named = await NativeHttpServer.bind(
      0,
      address: 'localhost',
    );
    expect(named.address.isLoopback, isTrue);
    await named.close();
  });

  test('close() lets open requests finish, then frees the port', () async {
    NativeHttpServer a = await start(0);
    int p = a.port;
    HttpClient c = HttpClient();
    Future<String> slow = c
        .get('127.0.0.1', p, '/slow')
        .then((HttpClientRequest r) => r.close())
        .then((HttpClientResponse r) => utf8.decodeStream(r));
    await Future<void>.delayed(const Duration(milliseconds: 100));
    Stopwatch clock = Stopwatch()..start();
    Future<void> closed = a.close();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    // no new connections while it drains
    await expectLater(
      Socket.connect('127.0.0.1', p),
      throwsA(isA<SocketException>()),
    );
    expect(await slow, 'slow done');
    await closed;
    expect(clock.elapsedMilliseconds, greaterThan(200));
    c.close(force: true);
    NativeHttpServer b = await start(p);
    expect(b.port, p);
    await b.close();
  });

  test('close(force: true) cuts open requests', () async {
    NativeHttpServer a = await start(0);
    HttpClient c = HttpClient();
    Future<Object> slow = c
        .get('127.0.0.1', a.port, '/slow')
        .then((HttpClientRequest r) => r.close())
        .then<Object>((HttpClientResponse r) => utf8.decodeStream(r))
        .catchError((Object error) => error);
    await Future<void>.delayed(const Duration(milliseconds: 100));
    Future<void> graceful = a.close();
    await a.close(force: true);
    await graceful; // a waiting graceful close ends too
    expect(await slow, isA<Exception>());
    c.close(force: true);
  });

  test('close frees the port', () async {
    NativeHttpServer a = await start(0);
    int p = a.port;
    await a.close();
    NativeHttpServer b = await start(p);
    expect(b.port, p);
    await b.close();
  });

  test('the port of an isolate that died is closed', () async {
    ReceivePort ready = ReceivePort();
    ReceivePort exit = ReceivePort();
    ReceivePort errors = ReceivePort(); // keeps the error out of the output
    await Isolate.spawn(
      dies,
      ready.sendPort,
      onExit: exit.sendPort,
      onError: errors.sendPort,
    );
    int p = await ready.first as int;
    Socket socket = await Socket.connect('127.0.0.1', p);
    socket.write('GET / HTTP/1.1\r\n\r\n');
    await exit.first;
    socket.destroy();
    errors.close();
    // new clients get "connection refused", not a wait
    Object? refused;
    for (int i = 0; i < 40 && refused == null; i++) {
      try {
        (await Socket.connect('127.0.0.1', p)).destroy();
        await Future<void>.delayed(const Duration(milliseconds: 50));
      } on SocketException catch (e) {
        refused = e;
      }
    }
    expect(refused, isA<SocketException>());
  });

  test('many isolates on one port share the connections', () async {
    NativeHttpServer main = await start(0);
    int shared = main.port;
    List<Isolate> isolates = <Isolate>[];
    for (int i = 0; i < 3; i++) {
      ReceivePort ready = ReceivePort();
      isolates.add(
        await Isolate.spawn(spawned, <Object>[
          shared,
          ready.sendPort,
        ], debugName: 'worker$i'),
      );
      await ready.first;
    }
    Set<String> seen = <String>{};
    for (int round = 0; round < 10 && seen.length < 2; round++) {
      List<Future<String>> calls = <Future<String>>[
        for (int i = 0; i < 50; i++)
          raw(
            shared,
            'GET /who HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n',
          ),
      ];
      for (String out in await Future.wait(calls)) {
        expect(out, startsWith('HTTP/1.1 200'));
        seen.add(out.split('\r\n\r\n').last);
      }
    }
    expect(seen.length, greaterThan(1));

    // a killed isolate stops its server (the idle probe finds it within
    // 100 ms), the others keep the port. no request may be lost after that.
    for (Isolate isolate in isolates) {
      isolate.kill(priority: Isolate.immediate);
    }
    await Future<void>.delayed(const Duration(milliseconds: 400));
    int ok = 0;
    for (int i = 0; i < 20; i++) {
      String out = await raw(
        shared,
        'GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n',
        wait: const Duration(seconds: 2),
      );
      if (out.startsWith('HTTP/1.1 200')) ok++;
    }
    expect(ok, 20);
    await main.close();
  });

  group('answers that come later', () {
    test(
      'a reply from another isolate (like a database) is fast under load',
      () async {
        // the bug: the idle helper held the sockets and Dart waited for it,
        // so each late answer waited up to seconds
        SendPort db = await replier();
        NativeHttpServer server = await NativeHttpServer.bind(0);
        server.listen((HttpRequest req) async {
          ReceivePort reply = ReceivePort();
          db.send(<Object>[reply.sendPort, req.uri.queryParameters['id']!, 0]);
          req.response.write(await reply.first);
          await req.response.close();
        });
        List<int> r = await clientLoad(server.port, '/db', 32, 100);
        expect(r[1], 0, reason: 'wrong answers');
        expect(r[0], lessThan(5000), reason: '3200 requests took ${r[0]} ms');
        await server.close();
      },
    );

    test('answers after timers of different lengths', () async {
      NativeHttpServer server = await NativeHttpServer.bind(0);
      server.listen((HttpRequest req) async {
        String id = req.uri.queryParameters['id']!;
        await Future<void>.delayed(Duration(milliseconds: int.parse(id) % 7));
        req.response.write(id);
        await req.response.close();
      });
      List<int> r = await clientLoad(server.port, '/t', 32, 50);
      expect(r[1], 0, reason: 'wrong answers');
      expect(r[0], lessThan(5000), reason: '1600 requests took ${r[0]} ms');
      await server.close();
    });

    test('a long task does not hold the other requests', () async {
      NativeHttpServer server = await NativeHttpServer.bind(0);
      server.listen((HttpRequest req) async {
        if (req.uri.path == '/long') {
          await Future<void>.delayed(const Duration(milliseconds: 1500));
          req.response.write('long');
        } else {
          req.response.write(req.uri.queryParameters['id']);
        }
        await req.response.close();
      });
      Stopwatch clock = Stopwatch()..start();
      HttpClient longClient = HttpClient();
      Future<String> long() async {
        HttpClientResponse res = await (await longClient.get(
          '127.0.0.1',
          server.port,
          '/long',
        )).close();
        return utf8.decodeStream(res);
      }

      List<Future<String>> longs = <Future<String>>[
        for (int i = 0; i < 4; i++) long(),
      ];
      List<int> fast = await clientLoad(server.port, '/fast', 16, 25);
      expect(fast[1], 0, reason: 'wrong answers');
      expect(
        fast[0],
        lessThan(1000),
        reason: '400 fast requests took ${fast[0]} ms',
      );
      expect(await Future.wait(longs), everyElement('long'));
      expect(clock.elapsedMilliseconds, inInclusiveRange(1500, 3000));
      longClient.close(force: true);
      await server.close();
    });

    test('a long synchronous task answers, and the server goes on', () async {
      NativeHttpServer server = await NativeHttpServer.bind(0);
      server.listen((HttpRequest req) async {
        if (req.uri.path == '/busy') {
          Stopwatch work = Stopwatch()..start();
          while (work.elapsedMilliseconds < 300) {}
          req.response.write('busy');
        } else {
          req.response.write(req.uri.queryParameters['id']);
        }
        await req.response.close();
      });
      HttpClient one = HttpClient();
      HttpClientResponse res = await (await one.get(
        '127.0.0.1',
        server.port,
        '/busy',
      )).close();
      expect(await utf8.decodeStream(res), 'busy');
      List<int> r = await clientLoad(server.port, '/after', 8, 25);
      expect(r[1], 0, reason: 'wrong answers');
      expect(r[0], lessThan(2000), reason: '200 requests took ${r[0]} ms');
      one.close(force: true);
      await server.close();
    });
  });
}

/// A stand-in for a database driver: another isolate that answers each
/// message `[reply port, id, delay ms]` with the id.
Future<SendPort> replier() async {
  ReceivePort ready = ReceivePort();
  await Isolate.spawn(_replier, ready.sendPort);
  return await ready.first as SendPort;
}

void _replier(SendPort out) {
  ReceivePort inbox = ReceivePort();
  out.send(inbox.sendPort);
  inbox.listen((Object? msg) {
    List<Object?> m = msg as List<Object?>;
    (m[0] as SendPort).send(m[1]);
  });
}

/// Load from another isolate: [clients] clients each send [count] requests
/// to `path?id=<n>`, one after the other, and check that the body is `<n>`.
/// Returns [milliseconds, wrong answers].
Future<List<int>> clientLoad(
  int port,
  String path,
  int clients,
  int count,
) async {
  ReceivePort done = ReceivePort();
  await Isolate.spawn(_clientLoad, <Object>[
    done.sendPort,
    port,
    path,
    clients,
    count,
  ]);
  return (await done.first as List<Object?>).cast<int>();
}

void _clientLoad(List<Object> args) async {
  SendPort out = args[0] as SendPort;
  int port = args[1] as int;
  String path = args[2] as String;
  int clients = args[3] as int;
  int count = args[4] as int;
  HttpClient client = HttpClient()..maxConnectionsPerHost = clients;
  int wrong = 0;
  Stopwatch clock = Stopwatch()..start();
  Future<void> one(int c) async {
    for (int i = 0; i < count; i++) {
      String id = '${c * count + i}';
      HttpClientResponse res = await (await client.get(
        '127.0.0.1',
        port,
        '$path?id=$id',
      )).close();
      if (await utf8.decodeStream(res) != id) wrong++;
    }
  }

  await Future.wait(<Future<void>>[for (int c = 0; c < clients; c++) one(c)]);
  client.close(force: true);
  out.send(<int>[clock.elapsedMilliseconds, wrong]);
}
