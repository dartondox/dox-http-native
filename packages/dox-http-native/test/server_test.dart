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
    case '/json-long':
      // only the content type changes: the headers stay the defaults
      res.headers.contentType = ContentType.json;
      res.write('{"a":"${'a' * 1000}"}');
    case '/jpeg':
      res.headers.contentType = ContentType('image', 'jpeg');
      res.add(List<int>.filled(1000, 7));
    case '/stream':
      // 5 MiB in 64 KiB parts. ?length=1 sets the content length.
      res.headers.contentType = ContentType.binary;
      if (req.uri.queryParameters['length'] == '1') {
        res.contentLength = 80 * 65536;
      }
      await res.addStream(parts(80));
    case '/drip':
      // a real stream (the flush starts it), then 200 small parts, each
      // made in its own event-loop turn
      res.headers.contentType = ContentType.binary;
      res.add(const <int>[7]);
      await res.flush();
      await res.addStream(drip(200));
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
    case '/hang':
      // answers only when the test says so
      await hang.future;
      res.write('late');
    case '/quiet':
      // one part, then nothing (like server-sent events with no news)
      StreamController<List<int>> source = StreamController<List<int>>(
        onCancel: () => quietCancelled.complete(),
      );
      source.add(Uint8List(70000));
      await res.addStream(source.stream);
    case '/sse-timer':
      // server-sent events written from a timer, not with addStream
      res.headers.contentType = ContentType('text', 'event-stream');
      Timer timer = Timer.periodic(const Duration(milliseconds: 20), (_) {
        res.write('data: tick\n\n');
      });
      unawaited(
        res.done.then((_) {
          timer.cancel();
          sseStopped.complete();
        }),
      );
      return;
    case '/cancel-error':
      // the source fails when it is cancelled (the client left)
      StreamController<List<int>> source = StreamController<List<int>>(
        onCancel: () => throw StateError('cancel failed'),
      );
      source.add(Uint8List(70000));
      try {
        await res.addStream(source.stream);
      } catch (e) {
        cancelError.complete(e);
      }
      return;
    case '/gone-race':
      // the client leaves while a chunk is on its way: no error
      StreamController<List<int>> source = StreamController<List<int>>();
      Future<void> adding = res.addStream(source.stream);
      source.add(<int>[1]);
      scheduleMicrotask((res as NativeResponse).markGone);
      source.add(<int>[2]);
      await adding;
      res.add(<int>[3]); // dropped
      raceDone.complete();
    case '/sse-flush':
      // server-sent events: add() and flush() from a timer
      res.headers.contentType = ContentType('text', 'event-stream');
      int n = 0;
      Timer timer = Timer.periodic(const Duration(milliseconds: 20), (_) {
        res.write('data: ${n++}\n\n');
        unawaited(res.flush());
      });
      unawaited(
        res.done.then((_) {
          timer.cancel();
          sseStopped.complete();
        }),
      );
      return;
    case '/flush-length':
      // flush keeps to the content length
      res.contentLength = 10;
      res.write('hello');
      await res.flush();
      res.write('world');
    case '/room':
      // hasRoom: before the stream, after a small flush, with a full
      // window, and after Rust sent the data
      NativeResponse nr = res as NativeResponse;
      List<bool> room = <bool>[nr.hasRoom];
      res.write('a');
      await res.flush();
      room.add(nr.hasRoom);
      res.add(Uint8List(NativeResponse.window + 10));
      room.add(nr.hasRoom);
      await res.flush();
      room.add(nr.hasRoom);
      res.write(room.join(','));
    case '/many-parts':
      // flush first: the stream starts, so every part goes through sendPart
      res.write('start,');
      await res.flush();
      await res.addStream(manyParts());
    case '/flush-then-stream':
      res.write('a');
      await res.flush();
      await res.addStream(Stream<List<int>>.value(utf8.encode('bc')));
    case '/stream-then-flush':
      await res.addStream(Stream<List<int>>.value(utf8.encode('ab')));
      res.write('c');
      await res.flush();
      res.write('d');
    case '/flush-after-close':
      res.write('closed');
      await res.close();
      await res.flush(); // does nothing, no error
      flushChecks.add('after close ok');
      return;
    case '/flush-head':
      // a HEAD request: the head goes out, the body is dropped
      res.write('x');
      await res.flush();
      res.write('y');
      flushChecks.add('head done ${(await res.close()) == res}');
      return;
    case '/flush-locked':
      res.write('first ');
      bool before = (res as NativeResponse).headSent;
      await res.flush();
      flushChecks.add('headSent $before ${res.headSent}');
      // the head is out: these are ignored, with no error
      res.headers.set('x-late', '1');
      res.headers.contentType = ContentType.json;
      res.statusCode = 404;
      flushChecks.add(
        'ignored ${res.headers.value('x-late') == null} ${res.statusCode}',
      );
      res.write('second');
    case '/stream-locked':
      // addStream over 64 KB sends the head: later changes are ignored
      await res.addStream(Stream<List<int>>.value(Uint8List(70000)));
      res.headers.set('x-late', '1');
      res.statusCode = 500;
      flushChecks.add(
        'ignored ${res.headers.value('x-late') == null} ${res.statusCode}',
      );
    case '/no-body-flush':
      // 204 has no body: flush sends the head only
      res.statusCode = HttpStatus.noContent;
      res.write('x');
      await res.flush();
      res.write('y');
    case '/no-body-length':
      // 204 with a content length: no length line goes out
      res.statusCode = HttpStatus.noContent;
      res.contentLength = 1;
      res.write('x');
      await res.flush();
    case '/no-body-stream':
      // 304 has no body, also when addStream passes 64 KB
      res.statusCode = HttpStatus.notModified;
      await res.addStream(Stream<List<int>>.value(Uint8List(70000)));
    case '/flush-error':
      // too many bytes for the content length: the Future fails
      res.contentLength = 2;
      res.write('abc');
      Future<void> flushed = res.flush(); // no throw here
      try {
        await flushed;
      } on HttpException {
        flushChecks.add('flush failed');
      }
      res.headers.contentLength = -1;
    case '/flush-stranded':
      // the client does not read: the flush waits for room. A close
      // from another task ends the wait.
      res.add(Uint8List(16 << 20));
      Future<void> flushed = res.flush();
      await Future<void>.delayed(const Duration(milliseconds: 100));
      unawaited(res.close());
      await flushed.timeout(const Duration(seconds: 3));
      flushChecks.add('flush went on');
      return;
    case '/late-upgrade':
      // the client leaves before the WebSocket accept
      await lateDoneGate.future;
      await NativeWebSocket.upgrade(req);
      flushChecks.add('upgraded');
      return;
    case '/late-done':
      // the client leaves before the handler reads done
      await lateDoneGate.future;
      await res.done.timeout(const Duration(seconds: 3));
      flushChecks.add('late done');
      return;
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
    case '/end':
      // end() sends with no Future. close() after it still gives one.
      res.write('ended');
      (res as NativeResponse).end();
      closedWith = await res.close();
      return;
    case '/later':
      // runs after its batch: the request must keep its own bytes
      await Future<void>.delayed(const Duration(milliseconds: 20));
      List<int> body = <int>[for (List<int> c in await req.toList()) ...c];
      res.write('${req.headers.value('x-test')} ${utf8.decode(body)}');
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

/// what `close()` after `end()` gave in `/end`
HttpResponse? closedWith;

/// errors of streamed bodies that did not keep to their content length
List<String> lengthErrors = <String>[];
Completer<int> foreverDone = Completer<int>();
Completer<void> hang = Completer<void>();
Completer<void> quietCancelled = Completer<void>();
Completer<void> sseStopped = Completer<void>();
Completer<Object> cancelError = Completer<Object>();
Completer<void> raceDone = Completer<void>();
List<String> flushChecks = <String>[];
Completer<void> lateDoneGate = Completer<void>();

Future<NativeHttpServer> start(int port, {int maxBodySize = 1024}) async {
  NativeHttpServer server = await NativeHttpServer.bind(
    port,
    maxBodySize: maxBodySize,
    // CI runs these HTTP/1.1 tests once more with HTTP/2 on
    http2: Platform.environment['DOX_TEST_HTTP2'] == '1',
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
Stream<List<int>> drip(int count) async* {
  for (int i = 0; i < count; i++) {
    await Future<void>.delayed(Duration.zero);
    yield const <int>[7];
  }
}

/// One connection that sends `GET /` about every 0.7 ms for `args[1]` ms.
void steady(List<Object> args) async {
  Socket socket = await Socket.connect('127.0.0.1', args[0] as int);
  socket.setOption(SocketOption.tcpNoDelay, true);
  StreamIterator<Uint8List> answers = StreamIterator<Uint8List>(socket);
  Stopwatch clock = Stopwatch()..start();
  while (clock.elapsedMilliseconds < (args[1] as int)) {
    socket.add(ascii.encode('GET / HTTP/1.1\r\nHost: a\r\n\r\n'));
    if (!await answers.moveNext()) break;
    // a timer is too coarse for 0.7 ms
    int until = clock.elapsedMicroseconds + 700;
    while (clock.elapsedMicroseconds < until) {}
  }
  socket.destroy();
  (args[2] as SendPort).send(true);
}

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

/// 300 small numbered parts, each made after a microtask
Stream<List<int>> manyParts() async* {
  for (int i = 0; i < 300; i++) {
    yield utf8.encode('$i,');
  }
}

/// what [manyParts] adds up to
String manyPartsText() =>
    'start,${<int>[for (int i = 0; i < 300; i++) i].join(',')},';

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

  test('an unknown method keeps its name', () async {
    // only exact GET, POST, PUT and DELETE take the short path
    for (String m in <String>['GOT', 'POOL', 'PUX', 'DELETX']) {
      String r = await raw(
        port,
        '$m /headers HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(r, contains('"method":"$m"'));
    }
  });

  test('end() sends the answer, close() after it still completes', () async {
    closedWith = null;
    expect(await get('/end'), (200, 'ended'));
    expect(closedWith, isA<NativeResponse>());
  });

  test('close() gives a Future that completes with the response', () async {
    NativeHttpServer s = await NativeHttpServer.bind(0, address: '127.0.0.1');
    Completer<Object?> result = Completer<Object?>();
    s.listen((HttpRequest req) {
      req.response.write('x');
      Future<dynamic> f = req.response.close();
      f.then((dynamic r) => result.complete(identical(r, req.response)));
    });
    HttpClientResponse res = await (await client.get(
      '127.0.0.1',
      s.port,
      '/',
    )).close();
    expect(await utf8.decodeStream(res), 'x');
    expect(await result.future, isTrue);
    await s.close();
  });

  test(
    'a request that outlives its batch keeps its headers and body',
    () async {
      List<Future<String>> all = <Future<String>>[
        for (int i = 0; i < 20; i++)
          () async {
            HttpClientRequest req = await client.post(
              '127.0.0.1',
              port,
              '/later',
            );
            req.headers.set('x-test', 'h$i');
            req.write('body$i');
            return utf8.decodeStream(await req.close());
          }(),
      ];
      expect(await Future.wait(all), <String>[
        for (int i = 0; i < 20; i++) 'h$i body$i',
      ]);
    },
  );

  test('detach copies the views into the batch, not other bytes', () async {
    NativeHttpServer s = await NativeHttpServer.bind(0, address: '127.0.0.1');
    Uint8List batch = Uint8List.fromList(<int>[
      ...<int>[1, 0, 1, 0, 120, 121], // header x: y
      ...<int>[104, 105], // body hi
      ...List<int>.filled(16, 0)..[15] = 1, // ip
    ]);
    Uint8List own = Uint8List.fromList(<int>[111]);
    for (bool early in <bool>[false, true]) {
      NativeRequest req = NativeRequest(
        s,
        1,
        'POST',
        '/',
        Uint8List.sublistView(batch, 0, 6),
        early ? own : Uint8List.sublistView(batch, 6, 8),
        Uint8List.sublistView(batch, 8, 24),
        0,
        http10: false,
      );
      if (early) req.headers; // made before the detach
      req.detach(batch);
      Uint8List copy = Uint8List.fromList(batch);
      batch.fillRange(0, batch.length, 0);
      expect(req.headers.value('x'), 'y');
      expect(utf8.decode(req.body), early ? 'o' : 'hi');
      expect(identical(req.body, own), early);
      batch.setAll(0, copy);
    }
    await s.close();
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

  test(
    'the event loop gets its turns while requests come one by one',
    () async {
      // each answer is followed by a short wait for the next request. The
      // waits of one chain of calls must not add up: a timer, a database
      // reply or another port needs its turn.
      ReceivePort done = ReceivePort();
      await Isolate.spawn(steady, <Object>[port, 400, done.sendPort]);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      int ticks = 0;
      Timer timer = Timer.periodic(
        const Duration(milliseconds: 1),
        (Timer _) => ticks++,
      );
      await Future<void>.delayed(const Duration(milliseconds: 300));
      timer.cancel();
      expect(ticks, greaterThan(75), reason: '$ticks turns in 300 ms');
      await done.first.timeout(const Duration(seconds: 10));
    },
  );

  test(
    'a streamed answer does not wait while other requests get answers',
    () async {
      // The stream is open, and whole answers go out on another connection.
      // A wait for requests after each of them would hold the next part.
      await get('/');
      ReceivePort done = ReceivePort();
      await Isolate.spawn(steady, <Object>[port, 1500, done.sendPort]);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      // the best of 5: one stall of the machine does not fail the test
      int best = 1 << 30;
      for (int run = 0; run < 5; run++) {
        Stopwatch clock = Stopwatch()..start();
        HttpClientRequest req = await client.get('127.0.0.1', port, '/drip');
        HttpClientResponse res = await req.close();
        int length = 0;
        await for (List<int> part in res) {
          length += part.length;
        }
        expect(length, 201);
        best = min(best, clock.elapsedMilliseconds);
      }
      expect(best, lessThan(100), reason: 'the best run took $best ms');
      await done.first.timeout(const Duration(seconds: 10));
    },
  );

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

  group('streamed answers', () {
    test('hasRoom follows the stream and the window', () async {
      HttpClientRequest req = await client.get('127.0.0.1', port, '/room');
      HttpClientResponse res = await req.close();
      List<int> body = <int>[for (List<int> c in await res.toList()) ...c];
      String tail = utf8.decode(body.sublist(body.length - 21));
      expect(tail, 'false,true,false,true');
    });

    test('many parts made together arrive whole and in order', () async {
      expect(await get('/many-parts'), (200, manyPartsText()));
    });
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

  test('compress: every level from 1 to 12 gives the body', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    expect(zip.compressLevel, 1);
    for (int level = 1; level <= 12; level++) {
      zip.compressLevel = level;
      HttpClientRequest req = await client.get('127.0.0.1', zip.port, '/long');
      HttpClientResponse res = await req.close();
      expect(res.headers.value(HttpHeaders.contentEncodingHeader), 'gzip');
      expect(await utf8.decodeStream(res), 'a' * 1000, reason: 'level $level');
    }
    // The level reaches Rust: byte 8 of a gzip body (XFL) is 4 at level
    // 1, 0 at levels 2 to 7 and 2 at levels 8 to 12. This also runs with
    // `connection: close` (kind bit 9 next to the level).
    for ((int, int) want in <(int, int)>[(1, 4), (5, 0), (12, 2)]) {
      zip.compressLevel = want.$1;
      String out = await raw(
        zip.port,
        'GET /long HTTP/1.1\r\nAccept-Encoding: gzip\r\n'
        'Connection: close\r\n\r\n',
      );
      List<int> body = latin1.encode(
        out.substring(out.indexOf('\r\n\r\n') + 4),
      );
      expect(body[8], want.$2, reason: 'level ${want.$1}');
      expect(utf8.decode(gzip.decode(body)), 'a' * 1000);
    }
    await zip.close();
  });

  test('compress: a level that is not 1 to 12 is refused', () async {
    NativeHttpServer zip = await start(0);
    expect(() => zip.compressLevel = 0, throwsRangeError);
    expect(() => zip.compressLevel = 13, throwsRangeError);
    expect(zip.compressLevel, 1);
    await zip.close();
  });

  test('compress: Vary: Accept-Encoding is sent', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    String gz = await raw(
      zip.port,
      'GET /long HTTP/1.1\r\nAccept-Encoding: gzip\r\n'
      'Connection: close\r\n\r\n',
    );
    expect(gz.toLowerCase(), contains('vary: accept-encoding'));
    // also when this client does not take gzip: a cache must still know
    String plain = await raw(
      zip.port,
      'GET /long HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(plain.toLowerCase(), contains('vary: accept-encoding'));
    expect(plain, endsWith('a' * 1000));
    await zip.close();
  });

  /// the count of header lines `name: ...` in a raw answer
  int lines(String answer, String name) {
    String head = answer.substring(0, answer.indexOf('\r\n\r\n'));
    return head
        .toLowerCase()
        .split('\r\n')
        .where((String l) => l.startsWith('$name:'))
        .length;
  }

  /// [body] of a raw answer, gunzipped when it says gzip
  String bodyOf(String answer) {
    int at = answer.indexOf('\r\n\r\n') + 4;
    List<int> body = latin1.encode(answer.substring(at));
    return answer.toLowerCase().contains('content-encoding: gzip')
        ? utf8.decode(gzip.decode(body))
        : utf8.decode(body);
  }

  const String gzipGet = 'Accept-Encoding: gzip\r\nConnection: close\r\n\r\n';

  test('compress: a JSON answer with the default headers', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    for (int i = 0; i < 3; i++) {
      String out = await raw(zip.port, 'GET /json-long HTTP/1.1\r\n$gzipGet');
      expect(lines(out, 'vary'), 1);
      expect(lines(out, 'content-encoding'), 1);
      expect(lines(out, 'content-type'), 1);
      expect(out.toLowerCase(), contains('content-type: application/json'));
      expect(bodyOf(out), '{"a":"${'a' * 1000}"}');
      String plain = await raw(
        zip.port,
        'GET /json-long HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(lines(plain, 'vary'), 1);
      expect(lines(plain, 'content-encoding'), 0);
      expect(bodyOf(plain), '{"a":"${'a' * 1000}"}');
    }
    await zip.close();
  });

  test('compress: default headers with Vary', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    // it names Accept-Encoding already: no second line
    zip.defaultResponseHeaders.set(HttpHeaders.varyHeader, 'Accept-Encoding');
    for (String path in <String>['/long', '/json-long']) {
      String out = await raw(zip.port, 'GET $path HTTP/1.1\r\n$gzipGet');
      expect(lines(out, 'vary'), 1, reason: path);
      expect(lines(out, 'content-encoding'), 1, reason: path);
    }
    // another Vary: Accept-Encoding is added. The change of the defaults
    // after the first answers is seen.
    zip.defaultResponseHeaders.set(HttpHeaders.varyHeader, 'Origin');
    for (String path in <String>['/long', '/json-long']) {
      String out = await raw(zip.port, 'GET $path HTTP/1.1\r\n$gzipGet');
      expect(out.toLowerCase(), contains('vary: origin'), reason: path);
      expect(out.toLowerCase(), contains('vary: accept-encoding'));
      expect(lines(out, 'vary'), 2, reason: path);
      expect(bodyOf(out), contains('a' * 1000));
    }
    await zip.close();
  });

  test('compress: default headers that say not to gzip', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    zip.defaultResponseHeaders.set(HttpHeaders.contentEncodingHeader, 'br');
    String out = await raw(zip.port, 'GET /json-long HTTP/1.1\r\n$gzipGet');
    expect(lines(out, 'content-encoding'), 1);
    expect(out.toLowerCase(), contains('content-encoding: br'));
    expect(out, endsWith('"}'));
    zip.defaultResponseHeaders.removeAll(HttpHeaders.contentEncodingHeader);
    // a default content type that is compressed already
    zip.defaultResponseHeaders.contentType = ContentType('image', 'png');
    out = await raw(zip.port, 'GET /long HTTP/1.1\r\n$gzipGet');
    expect(lines(out, 'content-encoding'), 0);
    expect(out, endsWith('a' * 1000));
    // the answer sets its own type: that one counts
    out = await raw(zip.port, 'GET /json-long HTTP/1.1\r\n$gzipGet');
    expect(lines(out, 'content-encoding'), 1);
    expect(bodyOf(out), '{"a":"${'a' * 1000}"}');
    await zip.close();
  });

  test('compress: the default headers are not changed', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    await raw(zip.port, 'GET /json-long HTTP/1.1\r\n$gzipGet');
    await raw(zip.port, 'GET /long HTTP/1.1\r\n$gzipGet');
    expect(zip.defaultResponseHeaders.value(HttpHeaders.varyHeader), isNull);
    expect(
      zip.defaultResponseHeaders.value(HttpHeaders.contentEncodingHeader),
      isNull,
    );
    await zip.close();
  });

  test('compress: gzip;q=0 is not gzipped', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    String out = await raw(
      zip.port,
      'GET /long HTTP/1.1\r\nAccept-Encoding: gzip;q=0, identity\r\n'
      'Connection: close\r\n\r\n',
    );
    expect(out.toLowerCase(), isNot(contains('content-encoding')));
    expect(out, endsWith('a' * 1000));
    await zip.close();
  });

  test('compress: image/jpeg is not gzipped', () async {
    NativeHttpServer zip = await start(0)
      ..autoCompress = true;
    String out = await raw(
      zip.port,
      'GET /jpeg HTTP/1.1\r\nAccept-Encoding: gzip\r\n'
      'Connection: close\r\n\r\n',
    );
    expect(out.toLowerCase(), isNot(contains('content-encoding')));
    expect(out.toLowerCase(), contains('content-length: 1000'));
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
    await expectLater(
      NativeHttpServer.bind(0, idleTimeout: Duration.zero),
      throwsArgumentError,
    );
  });

  test('idleTimeout: an idle keep-alive connection closes after it', () async {
    NativeHttpServer quick = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      idleTimeout: const Duration(milliseconds: 300),
    );
    quick.listen(handle);
    // ignore: deprecated_member_use_from_same_package
    expect(quick.idleTimeout, const Duration(milliseconds: 300));
    Socket s = await Socket.connect('127.0.0.1', quick.port);
    Stopwatch clock = Stopwatch()..start();
    s.write('GET / HTTP/1.1\r\n\r\n');
    // the answer, then the end when the server closes the idle connection
    await s.drain<void>().timeout(const Duration(seconds: 5));
    expect(clock.elapsed, greaterThan(const Duration(milliseconds: 250)));
    s.destroy();
    await quick.close(force: true);
  });

  test('a client that left a request with no answer frees its connection, '
      'a late answer is dropped', () async {
    hang = Completer<void>();
    NativeHttpServer one = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      maxConnections: 1,
      idleTimeout: const Duration(milliseconds: 300),
    );
    one.listen(handle);
    Socket s = await Socket.connect('127.0.0.1', one.port);
    s.write('GET /hang HTTP/1.1\r\n\r\n');
    await s.flush();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    s.destroy(); // a FIN: the client is gone
    // the only connection slot is free again after the idle time
    String out = await raw(
      one.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    ).timeout(const Duration(seconds: 5));
    expect(out, endsWith('Hello World'));
    hang.complete(); // the late answer goes nowhere
    await Future<void>.delayed(const Duration(milliseconds: 50));
    out = await raw(one.port, 'GET / HTTP/1.1\r\nConnection: close\r\n\r\n');
    expect(out, endsWith('Hello World'));
    await one.close(force: true);
  });

  test('a quiet stream stops when the client leaves', () async {
    quietCancelled = Completer<void>();
    NativeHttpServer quick = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      idleTimeout: const Duration(milliseconds: 300),
    );
    quick.listen(handle);
    Socket s = await Socket.connect('127.0.0.1', quick.port);
    s.write('GET /quiet HTTP/1.1\r\n\r\n');
    int got = 0;
    Completer<void> all = Completer<void>();
    s.listen((List<int> data) {
      got += data.length;
      if (got >= 70000 && !all.isCompleted) all.complete();
    });
    await all.future.timeout(const Duration(seconds: 5));
    expect(quick.streams, hasLength(1));
    s.destroy();
    // the source is cancelled, with no new part
    await quietCancelled.future.timeout(const Duration(seconds: 5));
    expect(quick.streams, isEmpty);
    await quick.close(force: true);
  });

  test('done completes when the client of an open response leaves, '
      'so a timer can stop', () async {
    sseStopped = Completer<void>();
    NativeHttpServer quick = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      idleTimeout: const Duration(milliseconds: 300),
    );
    quick.listen(handle);
    Socket s = await Socket.connect('127.0.0.1', quick.port);
    s.write('GET /sse-timer HTTP/1.1\r\n\r\n');
    await s.flush();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(quick.watched, hasLength(1));
    s.destroy();
    await sseStopped.future.timeout(const Duration(seconds: 5));
    expect(quick.watched, isEmpty);
    await quick.close(force: true);
  });

  test('an error of the source on cancel goes to addStream', () async {
    cancelError = Completer<Object>();
    NativeHttpServer quick = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      idleTimeout: const Duration(milliseconds: 300),
    );
    quick.listen(handle);
    Socket s = await Socket.connect('127.0.0.1', quick.port);
    s.write('GET /cancel-error HTTP/1.1\r\n\r\n');
    int got = 0;
    Completer<void> all = Completer<void>();
    s.listen((List<int> data) {
      got += data.length;
      if (got >= 70000 && !all.isCompleted) all.complete();
    });
    await all.future.timeout(const Duration(seconds: 5));
    s.destroy();
    Object error = await cancelError.future.timeout(const Duration(seconds: 5));
    expect(error, isA<StateError>());
    await quick.close(force: true);
  });

  test('a client that leaves while a chunk comes gives no error', () async {
    raceDone = Completer<void>();
    String out = await raw(
      server.port,
      'GET /gone-race HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    await raceDone.future.timeout(const Duration(seconds: 5));
    expect(out, startsWith('HTTP/1.1 200'));
  });

  test('flush: server-sent events from a timer reach the client part by '
      'part, and stop when it leaves', () async {
    sseStopped = Completer<void>();
    HttpClient sse = HttpClient();
    HttpClientResponse res = await (await sse.get(
      '127.0.0.1',
      port,
      '/sse-flush',
    )).close();
    expect(res.headers.contentType?.mimeType, 'text/event-stream');
    expect(res.headers.chunkedTransferEncoding, isTrue);
    StringBuffer got = StringBuffer();
    Completer<void> three = Completer<void>();
    StreamSubscription<String> sub = res.transform(utf8.decoder).listen((
      String part,
    ) {
      got.write(part);
      if (got.toString().contains('data: 2\n\n') && !three.isCompleted) {
        three.complete();
      }
    });
    // the response is never closed: the events come while it is open
    await three.future.timeout(const Duration(seconds: 5));
    expect(got.toString(), startsWith('data: 0\n\ndata: 1\n\n'));
    unawaited(sub.cancel());
    sse.close(force: true);
    await sseStopped.future.timeout(const Duration(seconds: 5));
  });

  test('flush keeps to the content length', () async {
    HttpClientResponse res = await (await client.get(
      '127.0.0.1',
      port,
      '/flush-length',
    )).close();
    expect(res.headers.contentLength, 10);
    expect(await utf8.decodeStream(res), 'helloworld');
  });

  test('flush with addStream, both ways', () async {
    expect(await get('/flush-then-stream'), (200, 'abc'));
    expect(await get('/stream-then-flush'), (200, 'abcd'));
  });

  test('flush after close does nothing; HEAD gets no body', () async {
    flushChecks.clear();
    expect(await get('/flush-after-close'), (200, 'closed'));
    HttpClientResponse head = await (await client.head(
      '127.0.0.1',
      port,
      '/flush-head',
    )).close();
    expect(head.statusCode, 200);
    expect(await head.toList(), isEmpty);
    // the connection still works
    expect(await get('/'), (200, 'Hello World'));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(flushChecks, <String>['after close ok', 'head done true']);
  });

  test(
    'after the head went out header and status changes are ignored',
    () async {
      flushChecks.clear();
      HttpClientResponse res = await (await client.get(
        '127.0.0.1',
        port,
        '/flush-locked',
      )).close();
      expect(res.statusCode, 200);
      expect(res.headers.value('x-late'), isNull);
      expect(await utf8.decodeStream(res), 'first second');
      res = await (await client.get(
        '127.0.0.1',
        port,
        '/stream-locked',
      )).close();
      expect(res.statusCode, 200);
      expect(res.headers.value('x-late'), isNull);
      expect(
        (await res.toList()).fold(0, (int n, List<int> c) => n + c.length),
        70000,
      );
      expect(flushChecks, <String>[
        'headSent false true',
        'ignored true 200',
        'ignored true 200',
      ]);
    },
  );

  test('a streamed 204 or 304 sends no body, the connection goes on', () async {
    for (String path in <String>[
      '/no-body-flush',
      '/no-body-length',
      '/no-body-stream',
    ]) {
      String out = await raw(
        port,
        'GET $path HTTP/1.1\r\n\r\n'
        'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      List<String> answers = out.split('HTTP/1.1 ');
      expect(answers, hasLength(3), reason: out);
      expect(answers[1], startsWith(path == '/no-body-stream' ? '304' : '204'));
      expect(answers[1], endsWith('\r\n\r\n'));
      expect(answers[1], isNot(contains('chunked')));
      expect(answers[1], isNot(contains('content-length')));
      expect(answers[2], endsWith('Hello World'));
    }
  });

  test('request headers: chunkedTransferEncoding is a no-op, other '
      'setters throw as before', () async {
    NativeHeaders h = NativeHeaders.request(Uint8List(0), http10: false);
    h.chunkedTransferEncoding = true;
    expect(h.chunkedTransferEncoding, isFalse);
    expect(() => h.contentLength = 1, throwsA(isA<HttpException>()));
    expect(() => h.set('x', '1'), throwsA(isA<HttpException>()));
    expect(
      () => h.contentType = ContentType.json,
      throwsA(isA<HttpException>()),
    );
    expect(h.clear, throwsA(isA<HttpException>()));
  });

  test('a request answered at once keeps no gone id', () async {
    for (int i = 0; i < 20; i++) {
      Socket s = await Socket.connect('127.0.0.1', port);
      s.write('GET / HTTP/1.1\r\n\r\n');
      await s.flush();
      s.destroy();
    }
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(server.goneIds, isEmpty);
  });

  test('flush fails its Future, it does not throw', () async {
    flushChecks.clear();
    expect(await get('/flush-error'), (200, 'abc'));
    expect(flushChecks, <String>['flush failed']);
  });

  test('a flush that waits for room goes on after close', () async {
    flushChecks.clear();
    Socket s = await Socket.connect('127.0.0.1', port);
    s.write('GET /flush-stranded HTTP/1.1\r\n\r\n');
    await s.flush();
    StreamSubscription<List<int>> sub = s.listen(null)..pause();
    for (int i = 0; i < 100 && flushChecks.isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    expect(flushChecks, <String>['flush went on']);
    await sub.cancel();
    s.destroy();
  });

  test('a WebSocket accept after the client left keeps no gone id', () async {
    flushChecks.clear();
    lateDoneGate = Completer<void>();
    NativeHttpServer quick = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      idleTimeout: const Duration(milliseconds: 300),
    );
    quick.listen(handle);
    Socket s = await Socket.connect('127.0.0.1', quick.port);
    s.write(
      'GET /late-upgrade HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\n'
      'Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n'
      'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n',
    );
    await s.flush();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    s.destroy();
    for (int i = 0; i < 100 && quick.goneIds.isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    expect(quick.goneIds, hasLength(1));
    lateDoneGate.complete();
    for (int i = 0; i < 100 && flushChecks.isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    expect(flushChecks, <String>['upgraded']);
    expect(quick.goneIds, isEmpty);
    await quick.close(force: true);
  });

  test('done read after the client left completes', () async {
    flushChecks.clear();
    lateDoneGate = Completer<void>();
    NativeHttpServer quick = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      idleTimeout: const Duration(milliseconds: 300),
    );
    quick.listen(handle);
    Socket s = await Socket.connect('127.0.0.1', quick.port);
    s.write('GET /late-done HTTP/1.1\r\n\r\n');
    await s.flush();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    s.destroy();
    for (int i = 0; i < 100 && quick.goneIds.isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    expect(quick.goneIds, hasLength(1));
    lateDoneGate.complete();
    for (int i = 0; i < 100 && flushChecks.isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    expect(flushChecks, <String>['late done']);
    expect(quick.goneIds, isEmpty);
    await quick.close(force: true);
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
