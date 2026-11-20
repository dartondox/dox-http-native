import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:http2/http2.dart';
import 'package:test/test.dart';

/// 3 MiB: more than the 1 MiB stream window of a client, so the answer
/// needs WINDOW_UPDATE frames.
const int bigSize = 3 << 20;

Completer<void> resetSeen = Completer<void>();

void handle(HttpRequest req) async {
  HttpResponse res = req.response;
  switch (req.uri.path) {
    case '/':
      res.write('Hello World');
    case '/big':
      res.add(List<int>.filled(bigSize, 7));
    case '/proto':
      res.write('${req.protocolVersion} ${req.persistentConnection}');
    case '/empty':
      res.statusCode = HttpStatus.noContent;
    case '/cookie':
      res.write('${req.headers.value('cookie')} ${req.headers.host}');
    case '/reset':
      // the client resets the stream: `done` completes
      unawaited(res.done.then((_) => resetSeen.complete()));
      await resetSeen.future.timeout(const Duration(seconds: 5));
      res.write('late');
    case '/stream':
      // 2b: a streamed answer is not there yet on HTTP/2
      await res.addStream(Stream<List<int>>.value(List<int>.filled(100000, 1)));
    default:
      res.statusCode = HttpStatus.notFound;
  }
  await res.close();
}

class Answer {
  Answer(this.headers, this.body);

  final Map<String, String> headers;
  final List<int> body;

  int get status => int.parse(headers[':status']!);
}

Future<ClientTransportConnection> connect(int port) async =>
    ClientTransportConnection.viaSocket(
      await Socket.connect('127.0.0.1', port),
    );

Future<Answer> request(
  ClientTransportConnection c,
  String path, {
  String method = 'GET',
  List<Header> extra = const <Header>[],
  List<int>? body,
}) async {
  ClientTransportStream s = c.makeRequest(<Header>[
    Header.ascii(':method', method),
    Header.ascii(':scheme', 'http'),
    Header.ascii(':path', path),
    Header.ascii(':authority', 'example.test'),
    ...extra,
  ], endStream: body == null);
  if (body != null) s.sendData(body, endStream: true);
  Map<String, String> headers = <String, String>{};
  List<int> data = <int>[];
  await for (StreamMessage m in s.incomingMessages) {
    if (m is HeadersStreamMessage) {
      for (Header h in m.headers) {
        headers[ascii.decode(h.name)] = ascii.decode(h.value);
      }
    } else if (m is DataStreamMessage) {
      data.addAll(m.bytes);
    }
  }
  return Answer(headers, data);
}

void main() {
  late NativeHttpServer server;
  late ClientTransportConnection c;

  setUpAll(() async {
    expect(NativeHttpServer.loadError, isNull);
    server = await NativeHttpServer.bind(0, address: '127.0.0.1', http2: true);
    server.listen(handle);
    c = await connect(server.port);
  });

  tearDownAll(() async {
    await c.finish();
    await server.close();
  });

  test('GET over h2c', () async {
    Answer a = await request(c, '/');
    expect(a.status, 200);
    expect(utf8.decode(a.body), 'Hello World');
    expect(a.headers['content-length'], '11');
    expect(a.headers['content-type'], startsWith('text/plain'));
    expect(a.headers['date'], isNotNull);
  });

  test('protocolVersion is 2.0, the connection is persistent', () async {
    Answer a = await request(c, '/proto');
    expect(utf8.decode(a.body), '2.0 true');
  });

  test('many parallel streams on one connection', () async {
    List<Answer> all = await Future.wait(<Future<Answer>>[
      for (int i = 0; i < 200; i++) request(c, '/'),
    ]);
    for (Answer a in all) {
      expect(a.status, 200);
      expect(utf8.decode(a.body), 'Hello World');
    }
  });

  test('a big answer needs window updates', () async {
    List<Answer> all = await Future.wait(<Future<Answer>>[
      request(c, '/big'),
      request(c, '/big'),
      request(c, '/'),
    ]);
    for (Answer a in all.take(2)) {
      expect(a.body.length, bigSize);
      expect(a.body.every((int b) => b == 7), isTrue);
    }
    expect(utf8.decode(all[2].body), 'Hello World');
  });

  test('HEAD and 204 send no body', () async {
    Answer head = await request(c, '/', method: 'HEAD');
    expect(head.status, 200);
    expect(head.body, isEmpty);
    Answer empty = await request(c, '/empty');
    expect(empty.status, 204);
    expect(empty.body, isEmpty);
    expect(empty.headers.containsKey('content-length'), isFalse);
  });

  test('cookies are joined, :authority is the host', () async {
    Answer a = await request(
      c,
      '/cookie',
      extra: <Header>[
        Header.ascii('cookie', 'a=1'),
        Header.ascii('cookie', 'b=2'),
      ],
    );
    expect(utf8.decode(a.body), 'a=1; b=2 example.test');
  });

  test('a client RST_STREAM tells the handler', () async {
    ClientTransportStream s = c.makeRequest(<Header>[
      Header.ascii(':method', 'GET'),
      Header.ascii(':scheme', 'http'),
      Header.ascii(':path', '/reset'),
    ], endStream: true);
    await Future<void>.delayed(const Duration(milliseconds: 100));
    s.terminate();
    await resetSeen.future.timeout(const Duration(seconds: 5));
    // the connection still works
    expect((await request(c, '/')).status, 200);
  });

  test('a request with a body gets 501 for now', () async {
    Answer a = await request(c, '/', method: 'POST', body: utf8.encode('hi'));
    expect(a.status, 501);
    expect((await request(c, '/')).status, 200);
  });

  test('a streamed answer resets the stream for now', () async {
    await expectLater(request(c, '/stream'), throwsA(anything));
    expect((await request(c, '/')).status, 200);
  });

  test('one port serves HTTP/1.1 and HTTP/2', () async {
    HttpClient client = HttpClient();
    HttpClientResponse res = await (await client.get(
      '127.0.0.1',
      server.port,
      '/proto',
    )).close();
    expect(await utf8.decodeStream(res), '1.1 true');
    client.close();
  });
}
