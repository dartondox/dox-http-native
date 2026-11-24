import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

/// The connections the test server got (their remote ports).
final Set<int> connections = <int>{};

/// Requests in work at the same time, and the most that were.
int busy = 0;
int mostBusy = 0;

/// test app: `/` hello, `/echo` the request back as json, `/chunked` a
/// body with no length, `/slow` waits, `/big` many bytes, `/status/N`.
Future<void> handle(HttpRequest req) async {
  connections.add(req.connectionInfo!.remotePort);
  busy++;
  if (busy > mostBusy) mostBusy = busy;
  HttpResponse res = req.response;
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
      case 'status':
        res.statusCode = int.parse(path[1]);
      default:
        res.statusCode = 404;
    }
  } finally {
    busy--;
    await res.close();
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

    test('trustedCertificates must be PEM', () {
      expect(
        () => DoxHttpClient(trustedCertificates: 'not a certificate'),
        throwsArgumentError,
      );
    });
  });
}
