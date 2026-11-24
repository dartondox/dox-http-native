import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_client/dox_http_client.dart';
import 'package:test/test.dart';

/// The connections the test server got (their remote ports).
final Set<int> connections = <int>{};

/// Requests in work at the same time, and the most that were.
int busy = 0;
int mostBusy = 0;

/// What `/redirect` got in its `authorization` header, per request.
final List<String?> redirectAuth = <String?>[];

/// Bytes the `/stream/N` route gave to its socket so far.
int streamSent = 0;

/// `/hold` reads its request body after this completes.
Completer<void> holdGate = Completer<void>();

/// The byte of the test data at [at]: a pattern that repeats.
int patternByte(int at) => at % 251;

Uint8List pattern(int start, int length) {
  Uint8List bytes = Uint8List(length);
  for (int i = 0; i < length; i++) {
    bytes[i] = patternByte(start + i);
  }
  return bytes;
}

/// test app: `/redirect/N?to=URL` answers status N with that location,
/// `/` hello, `/echo` the request back as json, `/chunked` a
/// body with no length, `/slow` waits, `/big` many bytes, `/status/N`,
/// `/stream/N` N bytes as fast as the client takes them, `/hold` counts a
/// request body after [holdGate], `/early` answers 401 and reads nothing,
/// `/die` closes the connection in the body, `/trickle` a slow body.
Future<void> handle(HttpRequest req) async {
  connections.add(req.connectionInfo!.remotePort);
  busy++;
  if (busy > mostBusy) mostBusy = busy;
  HttpResponse res = req.response;
  bool detached = false;
  try {
    List<String> path = req.uri.pathSegments;
    switch (path.isEmpty ? '' : path.first) {
      case '':
        res.headers.add('x-test', 'one');
        res.headers.add('x-test', 'two');
        res.write('Hello World');
      case 'echo':
        Uint8List body = await collect(req);
        res.headers.contentType = ContentType.json;
        res.write(
          jsonEncode(<String, Object?>{
            'method': req.method,
            'uri': req.uri.toString(),
            'host': req.headers.value('host'),
            'type': req.headers.value('content-type'),
            'length': req.headers.value('content-length'),
            'x-name': req.headers.value('x-name'),
            'accept-encoding': req.headers.value('accept-encoding'),
            'body': utf8.decode(body),
          }),
        );
      case 'count':
        res.write('${(await collect(req)).length}');
      case 'chunked':
        res.write('part one, ');
        await res.flush();
        res.write('part two');
      case 'slow':
        await Future<void>.delayed(const Duration(milliseconds: 300));
        res.write('slow done');
      case 'pause':
        await Future<void>.delayed(const Duration(milliseconds: 20));
        res.write('done');
      case 'big':
        res.contentLength = 3 << 20;
        res.add(Uint8List(3 << 20)..fillRange(0, 3 << 20, 120));
      case 'stream':
        int size = int.parse(path[1]);
        res.contentLength = size;
        streamSent = 0;
        while (streamSent < size) {
          int n = size - streamSent < 65536 ? size - streamSent : 65536;
          res.add(pattern(streamSent, n));
          await res.flush();
          streamSent += n;
        }
      case 'hold':
        await holdGate.future;
        int length = 0;
        bool exact = true;
        try {
          await for (List<int> part in req) {
            for (int i = 0; i < part.length; i++) {
              if (part[i] != patternByte(length + i)) exact = false;
            }
            length += part.length;
          }
        } on HttpException {
          // the client stopped the request
          return;
        }
        res.write(jsonEncode(<String, Object?>{'length': length, 'ok': exact}));
      case 'early':
        res.statusCode = 401;
      case 'die':
        res.contentLength = 1000000;
        detached = true;
        Socket socket = await res.detachSocket();
        socket.add(Uint8List(1000));
        await socket.flush();
        socket.destroy();
      case 'trickle':
        res.contentLength = 8;
        res.write('trick');
        await res.flush();
        await Future<void>.delayed(const Duration(milliseconds: 400));
        res.write('les');
      case 'status':
        res.statusCode = int.parse(path[1]);
      case 'gzip':
        res.headers.set('content-encoding', 'gzip');
        res.add(gzip.encode(utf8.encode('zipped text ' * 1000)));
      case 'redirect':
        redirectAuth.add(req.headers.value('authorization'));
        res.statusCode = int.parse(path[1]);
        String? to = req.uri.queryParameters['to'];
        if (to != null) res.headers.set('location', to);
      default:
        res.statusCode = 404;
    }
  } finally {
    busy--;
    if (!detached) await res.close();
  }
}

Future<Uint8List> collect(Stream<List<int>> stream) async {
  BytesBuilder bytes = BytesBuilder(copy: false);
  await stream.forEach(bytes.add);
  return bytes.takeBytes();
}

Matcher failsWith(DoxHttpFailure failure) {
  return throwsA(
    isA<DoxHttpClientException>().having(
      (DoxHttpClientException e) => e.failure,
      'failure',
      failure,
    ),
  );
}

void main() {
  late HttpServer server;
  late DoxHttpClient client;
  late Uri base;

  setUp(() async {
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen(handle);
    base = Uri.parse('http://127.0.0.1:${server.port}');
    client = DoxHttpClient();
    connections.clear();
    mostBusy = 0;
    holdGate = Completer<void>();
  });

  tearDown(() async {
    client.close();
    await server.close(force: true);
  });

  test('get gives the status, the body and the headers', () async {
    DoxHttpClientResponse res = await client.get(base);
    expect(res.statusCode, 200);
    expect(res.body, 'Hello World');
    expect(res.bodyBytes, utf8.encode('Hello World'));
    expect(res.headers['x-test'], 'one, two');
    expect(res.header('X-Test'), 'one, two');
    expect(res.header('content-type'), startsWith('text/plain'));
    expect(res.header('nope'), isNull);
    expect(res.reasonPhrase, 'OK');
    expect(
      (await client.get(base.resolve('/status/404'))).reasonPhrase,
      'Not Found',
    );
  });

  test('lookup gives the addresses of a host name', () async {
    List<String> asked = <String>[];
    DoxHttpClient mine = DoxHttpClient(
      lookup: (String host) async {
        asked.add(host);
        if (host == 'refused.invalid') throw StateError('refused');
        if (host == 'none.invalid') return <InternetAddress>[];
        return <InternetAddress>[InternetAddress.loopbackIPv4];
      },
    );
    addTearDown(mine.close);
    Uri url = Uri.parse('http://app.invalid:${server.port}/echo');
    DoxHttpClientResponse res = await mine.get(url);
    Map<String, Object?> echo = jsonDecode(res.body) as Map<String, Object?>;
    expect(echo['host'], 'app.invalid:${server.port}');
    await expectLater(
      mine.get(Uri.parse('http://refused.invalid:${server.port}/')),
      failsWith(DoxHttpFailure.lookupFailed),
    );
    await expectLater(
      mine.get(Uri.parse('http://none.invalid:${server.port}/')),
      failsWith(DoxHttpFailure.lookupFailed),
    );
    // an IP address needs no lookup
    expect((await mine.get(base)).statusCode, 200);
    expect(asked, <String>['app.invalid', 'refused.invalid', 'none.invalid']);
  });

  test('an error status is an answer, not an exception', () async {
    expect((await client.get(base.resolve('/status/404'))).statusCode, 404);
    expect((await client.get(base.resolve('/status/500'))).statusCode, 500);
    DoxHttpClientResponse none = await client.get(base.resolve('/status/204'));
    expect(none.statusCode, 204);
    expect(none.bodyBytes, isEmpty);
  });

  test('the path, the query and the headers go out', () async {
    DoxHttpClientResponse res = await client.get(
      base.resolve('/echo/a%20b?x=1&y=%C3%A9'),
      headers: <String, String>{'X-Name': 'dox'},
    );
    Map<String, dynamic> echo = jsonDecode(res.body) as Map<String, dynamic>;
    expect(echo['method'], 'GET');
    expect(echo['uri'], '/echo/a%20b?x=1&y=%C3%A9');
    expect(echo['host'], '127.0.0.1:${server.port}');
    expect(echo['x-name'], 'dox');
    expect(echo['length'], isNull);
  });

  test('post, put, patch and delete send a body', () async {
    Uri url = base.resolve('/echo');
    Map<String, dynamic> echo(DoxHttpClientResponse res) {
      return jsonDecode(res.body) as Map<String, dynamic>;
    }

    Map<String, dynamic> post = echo(await client.post(url, body: 'héllo'));
    expect(post['method'], 'POST');
    expect(post['body'], 'héllo');
    expect(post['length'], '6');
    expect(post['type'], 'text/plain; charset=utf-8');

    Map<String, dynamic> put = echo(
      await client.put(
        url,
        bodyBytes: utf8.encode('{"a":1}'),
        headers: <String, String>{'content-type': 'application/json'},
      ),
    );
    expect(put['method'], 'PUT');
    expect(put['body'], '{"a":1}');
    expect(put['type'], 'application/json');

    expect(echo(await client.patch(url, body: 'p'))['method'], 'PATCH');
    expect(echo(await client.delete(url))['method'], 'DELETE');
    // a POST with no body says so
    expect(echo(await client.post(url))['length'], '0');
    expect(echo(await client.send('OPTIONS', url))['method'], 'OPTIONS');
  });

  test('head has no body', () async {
    DoxHttpClientResponse res = await client.head(base);
    expect(res.statusCode, 200);
    expect(res.bodyBytes, isEmpty);
    // the connection still works
    expect((await client.get(base)).body, 'Hello World');
    expect(connections.length, 1);
  });

  test('a body with no length (chunked)', () async {
    DoxHttpClientResponse res = await client.get(base.resolve('/chunked'));
    expect(res.header('transfer-encoding'), 'chunked');
    expect(res.body, 'part one, part two');
  });

  test('requests one after the other use one connection', () async {
    for (int i = 0; i < 10; i++) {
      expect((await client.get(base)).statusCode, 200);
    }
    expect(connections.length, 1);
  });

  test('many requests at the same time', () async {
    List<DoxHttpClientResponse> all = await Future.wait(
      <Future<DoxHttpClientResponse>>[
        for (int i = 0; i < 100; i++) client.get(base.resolve('/echo?i=$i')),
      ],
    );
    for (int i = 0; i < 100; i++) {
      Map<String, dynamic> echo =
          jsonDecode(all[i].body) as Map<String, dynamic>;
      expect(echo['uri'], '/echo?i=$i');
    }
  });

  test('maxConnectionsPerHost makes requests wait', () async {
    DoxHttpClient two = DoxHttpClient(maxConnectionsPerHost: 2);
    addTearDown(two.close);
    List<DoxHttpClientResponse> all = await Future.wait(
      <Future<DoxHttpClientResponse>>[
        for (int i = 0; i < 10; i++) two.get(base.resolve('/pause')),
      ],
    );
    expect(all.every((DoxHttpClientResponse r) => r.body == 'done'), isTrue);
    expect(mostBusy, 2);
    expect(connections.length, 2);
  });

  test('a big answer and a big request body', () async {
    DoxHttpClientResponse big = await client.get(base.resolve('/big'));
    expect(big.bodyBytes.length, 3 << 20);
    expect(big.bodyBytes.every((int b) => b == 120), isTrue);
    DoxHttpClientResponse count = await client.post(
      base.resolve('/count'),
      bodyBytes: Uint8List(2 << 20),
    );
    expect(count.body, '${2 << 20}');
    // the buffers are small again and the client still works
    expect((await client.get(base)).body, 'Hello World');
  });

  test(
    'a host name works, also when its first address has no server',
    () async {
      // localhost can be ::1 first, and the server listens on 127.0.0.1 only
      Uri url = Uri.parse('http://localhost:${server.port}/echo');
      DoxHttpClientResponse res = await client.get(url);
      Map<String, dynamic> echo = jsonDecode(res.body) as Map<String, dynamic>;
      expect(echo['host'], 'localhost:${server.port}');
      expect((await client.get(url)).statusCode, 200);
    },
  );

  test('an IPv6 address', () async {
    HttpServer six;
    try {
      six = await HttpServer.bind(InternetAddress.loopbackIPv6, 0);
    } on SocketException {
      return; // no IPv6 on this machine
    }
    six.listen(handle);
    addTearDown(() => six.close(force: true));
    DoxHttpClientResponse res = await client.get(
      Uri.parse('http://[::1]:${six.port}/echo'),
    );
    Map<String, dynamic> echo = jsonDecode(res.body) as Map<String, dynamic>;
    expect(echo['host'], '[::1]:${six.port}');
  });

  test('a slow answer fails with a timeout', () async {
    DoxHttpClient fast = DoxHttpClient(
      timeout: const Duration(milliseconds: 50),
    );
    addTearDown(fast.close);
    Stopwatch clock = Stopwatch()..start();
    await expectLater(
      fast.get(base.resolve('/slow')),
      failsWith(DoxHttpFailure.timeout),
    );
    expect(clock.elapsedMilliseconds, lessThan(250));
    // the timeout of one request wins over the one of the client
    DoxHttpClientResponse res = await fast.get(
      base.resolve('/slow'),
      timeout: const Duration(seconds: 5),
    );
    expect(res.body, 'slow done');
    await expectLater(
      client.get(
        base.resolve('/slow'),
        timeout: const Duration(milliseconds: 50),
      ),
      failsWith(DoxHttpFailure.timeout),
    );
  });

  test('a timeout below 1 millisecond is a timeout, not no timeout', () async {
    await expectLater(
      client.get(
        base.resolve('/slow'),
        timeout: const Duration(microseconds: 300),
      ),
      failsWith(DoxHttpFailure.timeout),
    );
  });

  test('an idle connection closes after idleTimeout', () async {
    DoxHttpClient short = DoxHttpClient(
      idleTimeout: const Duration(milliseconds: 100),
    );
    addTearDown(short.close);
    expect((await short.get(base)).statusCode, 200);
    expect(server.connectionsInfo().total, 1);
    // no request and no call into Rust: the helper thread closes it
    await Future<void>.delayed(const Duration(milliseconds: 600));
    expect(server.connectionsInfo().total, 0);
    expect((await short.get(base)).statusCode, 200);
    expect(connections.length, 2);
    // the default keeps a connection for longer than this test
    expect((await client.get(base)).statusCode, 200);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect((await client.get(base)).statusCode, 200);
    expect(connections.length, 3);
  });

  test('a port with no server fails to connect', () async {
    ServerSocket socket = await ServerSocket.bind('127.0.0.1', 0);
    int port = socket.port;
    await socket.close();
    await expectLater(
      client.get(Uri.parse('http://127.0.0.1:$port/')),
      failsWith(DoxHttpFailure.connectionFailed),
    );
  });

  group('redirects', () {
    Uri redirect(int status, String to) {
      return base.replace(
        path: '/redirect/$status',
        queryParameters: <String, String>{'to': to},
      );
    }

    Map<String, Object?> echo(DoxHttpClientResponse res) {
      return jsonDecode(res.body) as Map<String, Object?>;
    }

    test('get goes to the new address, also a relative one', () async {
      for (int status in <int>[301, 302, 303, 307, 308]) {
        DoxHttpClientResponse res = await client.get(
          redirect(status, '/echo?a=1'),
        );
        expect(res.statusCode, 200, reason: '$status');
        expect(res.url, base.resolve('/echo?a=1'));
        expect(echo(res)['method'], 'GET');
      }
      DoxHttpClientResponse res = await client.get(
        redirect(302, redirect(302, '$base/').toString()),
      );
      expect(res.body, 'Hello World');
    });

    test('307 and 308 keep the method and the body', () async {
      for (int status in <int>[307, 308]) {
        DoxHttpClientResponse res = await client.post(
          redirect(status, '/echo'),
          headers: <String, String>{'Content-Type': 'application/json'},
          body: '{"a":1}',
        );
        Map<String, Object?> got = echo(res);
        expect(got['method'], 'POST');
        expect(got['body'], '{"a":1}');
        expect(got['type'], 'application/json');
      }
    });

    test('301, 302 and 303 change POST to GET with no body', () async {
      for (int status in <int>[301, 302, 303]) {
        DoxHttpClientResponse res = await client.post(
          redirect(status, '/echo'),
          headers: <String, String>{'Content-Type': 'application/json'},
          body: '{"a":1}',
        );
        Map<String, Object?> got = echo(res);
        expect(got['method'], 'GET', reason: '$status');
        expect(got['body'], '');
        expect(got['type'], isNull);
        expect(got['length'], isNull);
      }
      // PUT stays PUT for 301 and 302, and becomes GET for 303
      DoxHttpClientResponse res = await client.put(
        redirect(302, '/echo'),
        body: 'x',
      );
      expect(echo(res)['method'], 'PUT');
      res = await client.put(redirect(303, '/echo'), body: 'x');
      expect(echo(res)['method'], 'GET');
    });

    test('authorization stays on one server only', () async {
      HttpServer other = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      other.listen(handle);
      addTearDown(() => other.close(force: true));
      Map<String, String> headers = <String, String>{
        'Authorization': 'secret',
        'x-name': 'kept',
      };
      redirectAuth.clear();
      await client.get(
        redirect(302, redirect(200, '').toString()),
        headers: headers,
      );
      expect(redirectAuth, <String?>['secret', 'secret']);

      redirectAuth.clear();
      Uri there = Uri.parse('http://127.0.0.1:${other.port}');
      await client.get(
        redirect(302, '$there/redirect/302?to=/echo'),
        headers: headers,
      );
      expect(redirectAuth, <String?>['secret', null]);
      DoxHttpClientResponse res = await client.get(
        redirect(302, '$there/echo'),
        headers: headers,
      );
      expect(echo(res)['x-name'], 'kept');
      expect(echo(res)['host'], '127.0.0.1:${other.port}');
    });

    test('too many redirects fail', () async {
      Uri loop = base.replace(path: '/redirect/302');
      loop = loop.replace(queryParameters: <String, String>{'to': '$loop'});
      // the answer redirects to a URL with no `to`, which is no redirect
      DoxHttpClientResponse res = await client.get(loop);
      expect(res.statusCode, 302);
      expect(res.header('location'), isNull);

      Uri chain = base.resolve('/');
      for (int i = 0; i < 6; i++) {
        chain = redirect(302, chain.toString());
      }
      await expectLater(
        client.get(chain),
        failsWith(DoxHttpFailure.tooManyRedirects),
      );
      DoxHttpClient more = DoxHttpClient(maxRedirects: 6);
      addTearDown(more.close);
      expect((await more.get(chain)).body, 'Hello World');
    });

    test('followRedirects false gives the redirect answer', () async {
      DoxHttpClientResponse res = await client.send(
        'GET',
        redirect(302, '/echo'),
        followRedirects: false,
      );
      expect(res.statusCode, 302);
      expect(res.header('location'), '/echo');
      DoxHttpClient plain = DoxHttpClient(followRedirects: false);
      addTearDown(plain.close);
      expect((await plain.get(redirect(301, '/echo'))).statusCode, 301);
      // a location that is not http stays an answer
      res = await client.get(redirect(302, 'ftp://example.com/x'));
      expect(res.statusCode, 302);
      // an empty location is not the same address again
      res = await client.get(redirect(302, ''));
      expect(res.statusCode, 302);
    });

    test('the timeout includes the redirects', () async {
      await expectLater(
        client.get(
          redirect(302, '/slow'),
          timeout: const Duration(milliseconds: 100),
        ),
        failsWith(DoxHttpFailure.timeout),
      );
    });
  });

  group('compressed bodies', () {
    test('a gzip body comes decoded', () async {
      DoxHttpClientResponse res = await client.get(base.resolve('/gzip'));
      expect(res.body, 'zipped text ' * 1000);
      expect(res.header('content-encoding'), isNull);
      expect(res.header('content-length'), isNull);
      res = await client.get(base.resolve('/echo'));
      expect(
        (jsonDecode(res.body) as Map<String, Object?>)['accept-encoding'],
        'gzip, br',
      );
    });

    test(
      'an accept-encoding header of the request gives the raw body',
      () async {
        DoxHttpClientResponse res = await client.get(
          base.resolve('/gzip'),
          headers: <String, String>{'Accept-Encoding': 'gzip'},
        );
        expect(res.header('content-encoding'), 'gzip');
        expect(utf8.decode(gzip.decode(res.bodyBytes)), 'zipped text ' * 1000);
      },
    );

    test('a range request does not ask for a coded body', () async {
      DoxHttpClientResponse res = await client.get(
        base.resolve('/echo'),
        headers: <String, String>{'Range': 'bytes=0-9'},
      );
      expect(
        (jsonDecode(res.body) as Map<String, Object?>)['accept-encoding'],
        isNull,
      );
      res = await client.get(
        base.resolve('/gzip'),
        headers: <String, String>{'range': 'bytes=0-9'},
      );
      expect(res.header('content-encoding'), 'gzip');
    });

    test('maxResponseSize is for the decoded body', () async {
      DoxHttpClient small = DoxHttpClient(maxResponseSize: 11999);
      addTearDown(small.close);
      await expectLater(
        small.get(base.resolve('/gzip')),
        failsWith(DoxHttpFailure.responseTooBig),
      );
      DoxHttpClient enough = DoxHttpClient(maxResponseSize: 12000);
      addTearDown(enough.close);
      expect((await enough.get(base.resolve('/gzip'))).bodyBytes.length, 12000);
    });
  });

  test('a host name with no address fails', () async {
    await expectLater(
      client.get(Uri.parse('http://no-such-host.invalid/')),
      failsWith(DoxHttpFailure.lookupFailed),
    );
  });

  test('a server that closes with no answer', () async {
    ServerSocket socket = await ServerSocket.bind('127.0.0.1', 0);
    addTearDown(socket.close);
    socket.listen((Socket s) {
      s.listen((List<int> _) => s.destroy());
    });
    await expectLater(
      client.post(Uri.parse('http://127.0.0.1:${socket.port}/')),
      failsWith(DoxHttpFailure.connectionClosed),
    );
  });

  test('an answer that is not HTTP', () async {
    ServerSocket socket = await ServerSocket.bind('127.0.0.1', 0);
    addTearDown(socket.close);
    socket.listen((Socket s) {
      s.listen((List<int> _) => s.write('hello there\r\n\r\n'));
    });
    await expectLater(
      client.get(Uri.parse('http://127.0.0.1:${socket.port}/')),
      failsWith(DoxHttpFailure.badResponse),
    );
  });

  test('maxResponseSize stops a big answer', () async {
    DoxHttpClient small = DoxHttpClient(maxResponseSize: 1000);
    addTearDown(small.close);
    await expectLater(
      small.get(base.resolve('/big')),
      failsWith(DoxHttpFailure.responseTooBig),
    );
    expect((await small.get(base)).body, 'Hello World');
  });

  test('bad arguments', () {
    expect(
      () => client.get(Uri.parse('ftp://example.com/')),
      throwsArgumentError,
    );
    expect(() => client.get(Uri.parse('/no-host')), throwsArgumentError);
    expect(() => client.send('GE T', base), throwsArgumentError);
    expect(
      () => client.post(base, body: 'a', bodyBytes: Uint8List(1)),
      throwsArgumentError,
    );
    // no header can add a line to the request
    expect(
      () => client.get(base, headers: <String, String>{'a': 'b\r\nc: d'}),
      throwsArgumentError,
    );
    expect(
      () => client.get(base, headers: <String, String>{'a b': 'c'}),
      throwsArgumentError,
    );
    expect(
      () => client.get(base, headers: <String, String>{'Content-Length': '5'}),
      throwsArgumentError,
    );
  });

  test('close fails the open requests', () async {
    DoxHttpClient other = DoxHttpClient();
    Future<DoxHttpClientResponse> open = other.get(base.resolve('/slow'));
    unawaited(expectLater(open, failsWith(DoxHttpFailure.clientClosed)));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    other.close();
    other.close();
    await pumpEventQueue();
    expect(() => other.get(base), throwsStateError);
  });

  group('streamed bodies', () {
    /// Reads a body to its end and checks it against [pattern]. [slow]
    /// pauses the listener now and then.
    Future<int> readPattern(Stream<Uint8List> body, {bool slow = false}) async {
      int offset = 0;
      int pauses = 0;
      bool exact = true;
      Completer<void> done = Completer<void>();
      late StreamSubscription<Uint8List> sub;
      sub = body.listen(
        (Uint8List part) {
          for (int i = 0; i < part.length; i++) {
            if (part[i] != patternByte(offset + i)) exact = false;
          }
          offset += part.length;
          if (slow && offset ~/ (3 << 20) > pauses) {
            pauses++;
            sub.pause(Future<void>.delayed(const Duration(milliseconds: 40)));
          }
        },
        onDone: done.complete,
        onError: done.completeError,
      );
      await done.future;
      expect(
        exact,
        isTrue,
        reason: 'the bytes are not the bytes of the server',
      );
      return offset;
    }

    /// A body of [size] bytes of [pattern] in parts that the client asks for
    /// one by one. [made] counts the bytes made so far.
    Stream<List<int>> generate(int size, List<int> made) async* {
      while (made[0] < size) {
        int n = size - made[0] < 65536 ? size - made[0] : 65536;
        yield pattern(made[0], n);
        made[0] += n;
      }
    }

    test('a big answer comes in parts, also for a slow listener', () async {
      int size = 24 << 20;
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/stream/$size'),
      );
      expect(res.statusCode, 200);
      expect(res.header('content-length'), '$size');
      expect(res.url, base.resolve('/stream/$size'));
      expect(await readPattern(res.body, slow: true), size);
      // the connection is used again
      await client.get(base);
      expect(connections.length, 1);
    });

    test('a paused listener stops the server', () async {
      int size = 48 << 20;
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/stream/$size'),
      );
      int offset = 0;
      Completer<void> done = Completer<void>();
      StreamSubscription<Uint8List> sub = res.body.listen(
        (Uint8List part) => offset += part.length,
        onDone: done.complete,
      );
      sub.pause();
      await Future<void>.delayed(const Duration(milliseconds: 500));
      // the client did not read the socket: the buffers of the system fill
      // up, and the server waits
      expect(streamSent, lessThan(size ~/ 2));
      int before = streamSent;
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(streamSent, before);
      sub.resume();
      await done.future;
      expect(offset, size);
    });

    test('nobody listens: the same', () async {
      int size = 48 << 20;
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/stream/$size'),
      );
      await Future<void>.delayed(const Duration(milliseconds: 500));
      expect(streamSent, lessThan(size ~/ 2));
      // a late listener gets everything
      expect(await readPattern(res.body), size);
    });

    test('an answer with a gzip body is decoded in parts', () async {
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/gzip'),
      );
      expect(res.header('content-encoding'), isNull);
      BytesBuilder bytes = BytesBuilder(copy: false);
      await res.body.forEach(bytes.add);
      expect(utf8.decode(bytes.takeBytes()), 'zipped text ' * 1000);
    });

    test('a body over maxResponseSize is an error event', () async {
      DoxHttpClient small = DoxHttpClient(maxResponseSize: 1000);
      addTearDown(small.close);
      DoxHttpClientStreamedResponse res = await small.sendStreamed(
        'GET',
        base.resolve('/big'),
      );
      expect(res.statusCode, 200);
      await expectLater(
        res.body.drain<void>(),
        failsWith(DoxHttpFailure.responseTooBig),
      );
    });

    test('an upload with a length is byte exact', () async {
      int size = 5 << 20;
      holdGate.complete();
      List<int> made = <int>[0];
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'POST',
        base.resolve('/hold'),
        body: generate(size, made),
        contentLength: size,
      );
      expect(res.statusCode, 200);
      Map<String, dynamic> answer = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(answer, <String, dynamic>{'length': size, 'ok': true});
    });

    test('an upload with no length is chunked and byte exact', () async {
      int size = 3 << 20;
      holdGate.complete();
      List<int> made = <int>[0];
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'POST',
        base.resolve('/hold'),
        body: generate(size, made),
      );
      Map<String, dynamic> answer = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(answer, <String, dynamic>{'length': size, 'ok': true});
      // small parts and empty parts, and a body with no byte
      res = await client.sendStreamed(
        'POST',
        base.resolve('/echo'),
        body: Stream<List<int>>.fromIterable(<List<int>>[
          utf8.encode('one, '),
          <int>[],
          utf8.encode('two'),
        ]),
      );
      Map<String, dynamic> echo = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(echo['body'], 'one, two');
      expect(echo['length'], isNull);
      res = await client.sendStreamed(
        'POST',
        base.resolve('/echo'),
        body: const Stream<List<int>>.empty(),
      );
      echo = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(echo['body'], '');
    });

    test('a slow server pauses the body stream', () async {
      int size = 64 << 20;
      List<int> made = <int>[0];
      Future<DoxHttpClientStreamedResponse> request = client.sendStreamed(
        'POST',
        base.resolve('/hold'),
        body: generate(size, made),
        contentLength: size,
      );
      await Future<void>.delayed(const Duration(milliseconds: 500));
      // the server reads nothing: the stream stops after the buffers fill
      expect(made[0], greaterThan(0));
      expect(made[0], lessThan(size ~/ 2));
      int before = made[0];
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(made[0], before);
      holdGate.complete();
      DoxHttpClientStreamedResponse res = await request;
      Map<String, dynamic> answer = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(answer, <String, dynamic>{'length': size, 'ok': true});
      expect(made[0], size);
    });

    test('cancel closes the connection', () async {
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/stream/${32 << 20}'),
      );
      Completer<void> first = Completer<void>();
      StreamSubscription<Uint8List> sub = res.body.listen((Uint8List _) {
        if (!first.isCompleted) first.complete();
      });
      await first.future;
      await sub.cancel();
      await client.get(base);
      // the second request used another connection
      expect(connections.length, 2);
    });

    test('a connection that closes in the body is an error event', () async {
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/die'),
      );
      expect(res.statusCode, 200);
      List<Object> events = <Object>[];
      Completer<void> done = Completer<void>();
      res.body.listen(events.add, onError: events.add, onDone: done.complete);
      await done.future;
      expect(events.last, isA<DoxHttpClientException>());
      expect(
        (events.last as DoxHttpClientException).failure,
        DoxHttpFailure.connectionClosed,
      );
    });

    test('contentLength must be the length of the body', () async {
      holdGate.complete();
      // more bytes than the length
      await expectLater(
        client.sendStreamed(
          'POST',
          base.resolve('/hold'),
          body: Stream<List<int>>.value(Uint8List(10)),
          contentLength: 5,
        ),
        failsWith(DoxHttpFailure.badRequestBody),
      );
      // fewer bytes than the length
      await expectLater(
        client.sendStreamed(
          'POST',
          base.resolve('/hold'),
          body: Stream<List<int>>.value(Uint8List(3)),
          contentLength: 5,
        ),
        failsWith(DoxHttpFailure.badRequestBody),
      );
      // the client still works
      expect((await client.get(base)).body, 'Hello World');
    });

    test('an error of the body stream fails the request', () async {
      holdGate.complete();
      Stream<List<int>> broken() async* {
        yield pattern(0, 1000);
        await Future<void>.delayed(const Duration(milliseconds: 20));
        throw const FormatException('the body broke');
      }

      await expectLater(
        client.sendStreamed('POST', base.resolve('/hold'), body: broken()),
        throwsA(
          isA<FormatException>().having(
            (FormatException e) => e.message,
            'message',
            'the body broke',
          ),
        ),
      );
      await expectLater(
        client.sendStreamed(
          'POST',
          base.resolve('/hold'),
          body: Stream<List<int>>.error(const FormatException('at once')),
          contentLength: 4,
        ),
        throwsFormatException,
      );
      expect((await client.get(base)).body, 'Hello World');
    });

    test('an early answer stops the upload', () async {
      // a server that answers at once and does not read the body
      int accepted = 0;
      ServerSocket raw = await ServerSocket.bind(
        InternetAddress.loopbackIPv4,
        0,
      );
      addTearDown(raw.close);
      List<Socket> sockets = <Socket>[];
      raw.listen((Socket socket) {
        accepted++;
        sockets.add(socket);
        StringBuffer head = StringBuffer();
        socket.listen((List<int> data) {
          head.write(latin1.decode(data));
          // only the head of a request that is whole is of interest
          if (head.toString().contains('\r\n\r\n')) {
            socket.add(
              latin1.encode('HTTP/1.1 401 No\r\ncontent-length: 0\r\n\r\n'),
            );
            head.clear();
          }
        }, onError: (Object _) {});
      });
      addTearDown(() {
        for (Socket socket in sockets) {
          socket.destroy();
        }
      });
      Uri url = Uri.parse('http://127.0.0.1:${raw.port}/early');
      bool stopped = false;
      Stream<List<int>> endless() async* {
        try {
          while (true) {
            yield Uint8List(65536);
            await Future<void>.delayed(const Duration(milliseconds: 1));
          }
        } finally {
          stopped = true;
        }
      }

      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'POST',
        url,
        body: endless(),
        contentLength: 1 << 40,
      );
      expect(res.statusCode, 401);
      await res.body.drain<void>();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(stopped, isTrue);
      // the connection was not kept
      expect((await client.get(url)).statusCode, 401);
      expect(accepted, 2);
    });

    test('redirects: no body follows, a body does not', () async {
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/redirect/302?to=/echo?x=1'),
      );
      expect(res.statusCode, 200);
      expect(res.url, base.resolve('/echo?x=1'));
      await res.body.drain<void>();
      // a 307 with no body keeps the method
      res = await client.sendStreamed(
        'DELETE',
        base.resolve('/redirect/307?to=/echo'),
      );
      Map<String, dynamic> echo = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(echo['method'], 'DELETE');
      // followRedirects false
      res = await client.sendStreamed(
        'GET',
        base.resolve('/redirect/302?to=/'),
        followRedirects: false,
      );
      expect(res.statusCode, 302);
      await res.body.drain<void>();
      // a request with a body stream gets the redirect answer
      res = await client.sendStreamed(
        'POST',
        base.resolve('/redirect/307?to=/echo'),
        body: Stream<List<int>>.value(utf8.encode('data')),
        contentLength: 4,
      );
      expect(res.statusCode, 307);
      expect(res.header('location'), '/echo');
      await res.body.drain<void>();
      // too many redirects
      Uri chain = base.resolve('/');
      for (int i = 0; i < 6; i++) {
        chain = base.replace(
          path: '/redirect/302',
          queryParameters: <String, String>{'to': '$chain'},
        );
      }
      await expectLater(
        client.sendStreamed('GET', chain),
        failsWith(DoxHttpFailure.tooManyRedirects),
      );
    });

    test('the timeout is for the head', () async {
      await expectLater(
        client.sendStreamed(
          'GET',
          base.resolve('/slow'),
          timeout: const Duration(milliseconds: 50),
        ),
        failsWith(DoxHttpFailure.timeout),
      );
      // a body that comes slowly is no timeout
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/trickle'),
        timeout: const Duration(milliseconds: 100),
      );
      expect(utf8.decode(await collect(res.body)), 'trickles');
    });

    test('abort stops the request and closes the connection', () async {
      // before the head: a server that reads and never answers
      ServerSocket hung = await ServerSocket.bind('127.0.0.1', 0);
      addTearDown(hung.close);
      Completer<void> closed = Completer<void>();
      hung.listen((Socket socket) {
        socket.listen((List<int> _) {}, onDone: closed.complete);
      });
      Completer<void> abort = Completer<void>();
      Future<DoxHttpClientStreamedResponse> pending = client.sendStreamed(
        'GET',
        Uri.parse('http://127.0.0.1:${hung.port}/'),
        timeout: Duration.zero,
        abort: abort.future,
      );
      await Future<void>.delayed(const Duration(milliseconds: 100));
      abort.complete();
      await expectLater(pending, failsWith(DoxHttpFailure.aborted));
      await closed.future.timeout(const Duration(seconds: 5));

      // in the body: an error event, then the stream closes
      Completer<void> later = Completer<void>();
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'GET',
        base.resolve('/stream/3000000'),
        abort: later.future,
      );
      int length = 0;
      Object? error;
      Completer<void> done = Completer<void>();
      res.body.listen(
        (Uint8List part) {
          length += part.length;
          if (!later.isCompleted) later.complete();
        },
        onError: (Object e) => error = e,
        onDone: done.complete,
      );
      await done.future;
      expect(length, lessThan(3000000));
      expect(
        (error! as DoxHttpClientException).failure,
        DoxHttpFailure.aborted,
      );

      // after the end: nothing
      Completer<void> late = Completer<void>();
      DoxHttpClientStreamedResponse whole = await client.sendStreamed(
        'GET',
        base,
        abort: late.future,
      );
      expect(utf8.decode(await collect(whole.body)), 'Hello World');
      late.complete();
      await Future<void>.delayed(Duration.zero);

      // while the lookup runs: the request never goes out
      Completer<List<InternetAddress>> found =
          Completer<List<InternetAddress>>();
      DoxHttpClient slow = DoxHttpClient(lookup: (String _) => found.future);
      addTearDown(slow.close);
      connections.clear();
      Completer<void> early = Completer<void>();
      Future<DoxHttpClientStreamedResponse> waits = slow.sendStreamed(
        'GET',
        Uri.parse('http://app.invalid:${server.port}/'),
        abort: early.future,
      );
      early.complete();
      await expectLater(waits, failsWith(DoxHttpFailure.aborted));
      found.complete(<InternetAddress>[InternetAddress.loopbackIPv4]);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(connections, isEmpty);
      // the client still works
      expect((await client.get(base)).body, 'Hello World');
    });

    test('bad arguments', () {
      expect(
        () => client.sendStreamed('GET', base, contentLength: 3),
        throwsArgumentError,
      );
      expect(
        () => client.sendStreamed(
          'POST',
          base,
          body: const Stream<List<int>>.empty(),
          contentLength: -1,
        ),
        throwsArgumentError,
      );
      expect(
        () => client.sendStreamed(
          'POST',
          base,
          body: const Stream<List<int>>.empty(),
          headers: <String, String>{'Content-Length': '5'},
        ),
        throwsArgumentError,
      );
      expect(
        () => client.sendStreamed(
          'POST',
          base,
          body: const Stream<List<int>>.empty(),
          headers: <String, String>{'transfer-encoding': 'chunked'},
        ),
        throwsArgumentError,
      );
    });

    test('close fails the open streams', () async {
      DoxHttpClient other = DoxHttpClient();
      // a request with no head
      Future<DoxHttpClientStreamedResponse> open = other.sendStreamed(
        'GET',
        base.resolve('/slow'),
      );
      unawaited(expectLater(open, failsWith(DoxHttpFailure.clientClosed)));
      await Future<void>.delayed(const Duration(milliseconds: 20));
      // an answer body that is on its way
      DoxHttpClientStreamedResponse res = await other.sendStreamed(
        'GET',
        base.resolve('/stream/${32 << 20}'),
      );
      Completer<void> first = Completer<void>();
      Completer<Object> failed = Completer<Object>();
      StreamSubscription<Uint8List> sub = res.body.listen((Uint8List _) {
        if (!first.isCompleted) first.complete();
      }, onError: failed.complete);
      await first.future;
      // the answer stays open: the listener waits
      sub.pause();
      // an upload that waits for the server
      Future<DoxHttpClientStreamedResponse> upload = other.sendStreamed(
        'POST',
        base.resolve('/hold'),
        body: generate(64 << 20, <int>[0]),
        contentLength: 64 << 20,
      );
      unawaited(expectLater(upload, failsWith(DoxHttpFailure.clientClosed)));
      await Future<void>.delayed(const Duration(milliseconds: 100));
      other.close();
      sub.resume();
      Object error = await failed.future;
      expect(
        error,
        isA<DoxHttpClientException>().having(
          (DoxHttpClientException e) => e.failure,
          'failure',
          DoxHttpFailure.clientClosed,
        ),
      );
      await pumpEventQueue();
      expect(() => other.sendStreamed('GET', base), throwsStateError);
    });
  });

  group('https', () {
    late Directory dir;
    late String certificate;
    late HttpServer secure;
    late DoxHttpClient trusting;

    setUpAll(() {
      // a new certificate for localhost and 127.0.0.1
      dir = Directory.systemTemp.createTempSync('dox_client_test');
      ProcessResult made = Process.runSync('openssl', <String>[
        'req',
        '-x509',
        '-nodes',
        '-days',
        '2',
        '-newkey',
        'ec',
        '-pkeyopt',
        'ec_paramgen_curve:prime256v1',
        '-keyout',
        '${dir.path}/key.pem',
        '-out',
        '${dir.path}/cert.pem',
        '-subj',
        '/CN=localhost',
        '-addext',
        'subjectAltName=DNS:localhost,IP:127.0.0.1',
        '-addext',
        'basicConstraints=critical,CA:FALSE',
      ]);
      expect(made.exitCode, 0, reason: '${made.stderr}');
      certificate = File('${dir.path}/cert.pem').readAsStringSync();
    });

    tearDownAll(() => dir.deleteSync(recursive: true));

    setUp(() async {
      SecurityContext context = SecurityContext()
        ..useCertificateChain('${dir.path}/cert.pem')
        ..usePrivateKey('${dir.path}/key.pem');
      secure = await HttpServer.bindSecure(
        InternetAddress.loopbackIPv4,
        0,
        context,
      );
      secure.listen(handle);
      trusting = DoxHttpClient(trustedCertificates: certificate);
    });

    tearDown(() async {
      trusting.close();
      await secure.close(force: true);
    });

    test('get and post, on one connection', () async {
      Uri url = Uri.parse('https://localhost:${secure.port}');
      DoxHttpClientResponse res = await trusting.get(url);
      expect(res.statusCode, 200);
      expect(res.body, 'Hello World');
      DoxHttpClientResponse post = await trusting.post(
        url.resolve('/echo'),
        body: 'secret',
      );
      Map<String, dynamic> echo = jsonDecode(post.body) as Map<String, dynamic>;
      expect(echo['body'], 'secret');
      expect(echo['host'], 'localhost:${secure.port}');
      expect(connections.length, 1);
    });

    test('an IP address in the certificate', () async {
      DoxHttpClientResponse res = await trusting.get(
        Uri.parse('https://127.0.0.1:${secure.port}/'),
      );
      expect(res.body, 'Hello World');
    });

    test('a big answer and a big request body', () async {
      Uri url = Uri.parse('https://localhost:${secure.port}');
      DoxHttpClientResponse big = await trusting.get(url.resolve('/big'));
      expect(big.bodyBytes.length, 3 << 20);
      expect(big.bodyBytes.every((int b) => b == 120), isTrue);
      DoxHttpClientResponse count = await trusting.post(
        url.resolve('/count'),
        bodyBytes: Uint8List(2 << 20),
      );
      expect(count.body, '${2 << 20}');
    });

    test('many requests at the same time', () async {
      Uri url = Uri.parse('https://localhost:${secure.port}');
      List<DoxHttpClientResponse> all = await Future.wait(
        <Future<DoxHttpClientResponse>>[
          for (int i = 0; i < 30; i++) trusting.get(url.resolve('/echo?i=$i')),
        ],
      );
      for (int i = 0; i < 30; i++) {
        Map<String, dynamic> echo =
            jsonDecode(all[i].body) as Map<String, dynamic>;
        expect(echo['uri'], '/echo?i=$i');
      }
    });

    test('a certificate that is not trusted fails', () async {
      await expectLater(
        client.get(Uri.parse('https://localhost:${secure.port}/')),
        failsWith(DoxHttpFailure.tlsFailed),
      );
    });

    test('a certificate for another host name fails', () async {
      // 127.0.0.2 is this machine too, but the certificate does not name it
      HttpServer other;
      try {
        SecurityContext context = SecurityContext()
          ..useCertificateChain('${dir.path}/cert.pem')
          ..usePrivateKey('${dir.path}/key.pem');
        other = await HttpServer.bindSecure('127.0.0.2', 0, context);
      } on SocketException {
        return; // this machine has no 127.0.0.2
      }
      other.listen(handle);
      addTearDown(() => other.close(force: true));
      await expectLater(
        trusting.get(Uri.parse('https://127.0.0.2:${other.port}/')),
        failsWith(DoxHttpFailure.tlsFailed),
      );
    });

    test('an https request to a plain http server fails', () async {
      // the server closes the connection or sends plain HTTP back
      await expectLater(
        trusting.get(Uri.parse('https://127.0.0.1:${server.port}/')),
        throwsA(
          isA<DoxHttpClientException>().having(
            (DoxHttpClientException e) => e.failure,
            'failure',
            anyOf(DoxHttpFailure.tlsFailed, DoxHttpFailure.connectionClosed),
          ),
        ),
      );
    });

    test('streamed bodies', () async {
      Uri url = Uri.parse('https://localhost:${secure.port}');
      int size = 6 << 20;
      DoxHttpClientStreamedResponse res = await trusting.sendStreamed(
        'GET',
        url.resolve('/stream/$size'),
      );
      int offset = 0;
      bool exact = true;
      await res.body.forEach((Uint8List part) {
        for (int i = 0; i < part.length; i++) {
          if (part[i] != patternByte(offset + i)) exact = false;
        }
        offset += part.length;
      });
      expect(offset, size);
      expect(exact, isTrue);
      holdGate.complete();
      Stream<List<int>> body() async* {
        for (int at = 0; at < size; at += 65536) {
          yield pattern(at, 65536);
        }
      }

      res = await trusting.sendStreamed(
        'POST',
        url.resolve('/hold'),
        body: body(),
        contentLength: size,
      );
      Map<String, dynamic> answer = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(answer, <String, dynamic>{'length': size, 'ok': true});
      // chunked
      res = await trusting.sendStreamed(
        'POST',
        url.resolve('/hold'),
        body: body(),
      );
      answer = jsonDecode(
        utf8.decode(await collect(res.body)),
      ) as Map<String, dynamic>;
      expect(answer, <String, dynamic>{'length': size, 'ok': true});
    });

    test('trustedCertificates must be PEM', () {
      expect(
        () => DoxHttpClient(trustedCertificates: 'not a certificate'),
        throwsArgumentError,
      );
    });
  });
}
