import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_client/dox_http_client.dart';
import 'package:http2/http2.dart';
import 'package:test/test.dart';

/// The connections the server got, and the body of its last request.
int connections = 0;
Uint8List lastBody = Uint8List(0);

/// [size] bytes that are not the same in each place.
Uint8List pattern(int size) {
  Uint8List bytes = Uint8List(size);
  for (int i = 0; i < size; i++) {
    bytes[i] = (i * 31 + (i >> 8)) & 0xFF;
  }
  return bytes;
}

/// test app: `/` hello, `/echo` the request back as json, `/big/N` N bytes
/// of [pattern], `/gzip` a gzip body, `/slow` waits, `/count` the length of
/// the request body.
Future<void> serve(ServerTransportStream stream) async {
  Map<String, String> head = <String, String>{};
  BytesBuilder body = BytesBuilder(copy: false);
  await for (StreamMessage message in stream.incomingMessages) {
    if (message is HeadersStreamMessage) {
      for (Header header in message.headers) {
        head[utf8.decode(header.name)] = utf8.decode(header.value);
      }
    } else if (message is DataStreamMessage) {
      body.add(message.bytes);
    }
  }
  lastBody = body.takeBytes();
  List<String> path = Uri.parse(head[':path']!).pathSegments;
  List<Header> answer = <Header>[
    Header.ascii(':status', '200'),
    Header.ascii('x-test', 'one'),
  ];
  List<int> bytes;
  switch (path.isEmpty ? '' : path.first) {
    case '':
      bytes = utf8.encode('Hello World');
    case 'echo':
      bytes = utf8.encode(
        jsonEncode(<String, Object?>{
          'method': head[':method'],
          'path': head[':path'],
          'authority': head[':authority'],
          'scheme': head[':scheme'],
          'x-name': head['x-name'],
          'type': head['content-type'],
          'transfer-encoding': head['transfer-encoding'],
          'body': utf8.decode(lastBody),
        }),
      );
    case 'big':
      bytes = pattern(int.parse(path[1]));
    case 'gzip':
      answer.add(Header.ascii('content-encoding', 'gzip'));
      bytes = gzip.encode(utf8.encode('zipped text ' * 1000));
    case 'slow':
      await Future<void>.delayed(const Duration(milliseconds: 400));
      bytes = utf8.encode('slow done');
    case 'count':
      bytes = utf8.encode('${lastBody.length}');
    default:
      answer[0] = Header.ascii(':status', '404');
      bytes = const <int>[];
  }
  try {
    stream.sendHeaders(answer);
    stream.sendData(bytes, endStream: true);
  } on StateError {
    // the client stopped the request (RST_STREAM)
  }
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
  late Directory dir;
  late String certificate;
  late SecureServerSocket server;
  late DoxHttpClient client;
  late Uri base;

  setUpAll(() {
    // a new certificate for localhost
    dir = Directory.systemTemp.createTempSync('dox_client_h2_test');
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
    // a server that speaks HTTP/2 only
    SecurityContext context = SecurityContext()
      ..useCertificateChain('${dir.path}/cert.pem')
      ..usePrivateKey('${dir.path}/key.pem')
      ..setAlpnProtocols(<String>['h2'], true);
    server = await SecureServerSocket.bind(
      InternetAddress.loopbackIPv4,
      0,
      context,
    );
    connections = 0;
    server.listen((SecureSocket socket) {
      connections++;
      ServerTransportConnection.viaSocket(socket).incomingStreams
          .listen(serve, onError: (Object _) {});
    }, onError: (Object _) {});
    base = Uri.parse('https://localhost:${server.port}');
    client = DoxHttpClient(trustedCertificates: certificate);
  });

  tearDown(() async {
    client.close();
    await server.close();
  });

  test('get and post', () async {
    DoxHttpClientResponse res = await client.get(base);
    expect(res.statusCode, 200);
    expect(res.body, 'Hello World');
    expect(res.header('x-test'), 'one');
    res = await client.post(
      base.resolve('/echo?a=1'),
      headers: <String, String>{'X-Name': 'Dox'},
      body: '{"a":1}',
    );
    Map<String, Object?> got = jsonDecode(res.body) as Map<String, Object?>;
    expect(got['method'], 'POST');
    expect(got['path'], '/echo?a=1');
    expect(got['authority'], 'localhost:${server.port}');
    expect(got['scheme'], 'https');
    expect(got['x-name'], 'Dox');
    expect(got['type'], 'text/plain; charset=utf-8');
    expect(got['body'], '{"a":1}');
    expect((await client.get(base.resolve('/none'))).statusCode, 404);
    expect((await client.head(base)).bodyBytes, isEmpty);
    expect(connections, 1);
  });

  test('many requests at the same time share one connection', () async {
    await client.get(base);
    List<DoxHttpClientResponse> all = await Future.wait(
      <Future<DoxHttpClientResponse>>[
        for (int i = 0; i < 50; i++)
          client.post(base.resolve('/echo?n=$i'), body: 'body $i'),
      ],
    );
    for (int i = 0; i < 50; i++) {
      Map<String, Object?> got =
          jsonDecode(all[i].body) as Map<String, Object?>;
      expect(got['path'], '/echo?n=$i');
      expect(got['body'], 'body $i');
    }
    expect(connections, 1);
  });

  test('a big request body and a big answer', () async {
    Uint8List body = pattern(3 << 20);
    DoxHttpClientResponse res = await client.post(
      base.resolve('/count'),
      bodyBytes: body,
    );
    expect(res.body, '${3 << 20}');
    expect(lastBody, body);
    res = await client.get(base.resolve('/big/${2 << 20}'));
    expect(res.bodyBytes, pattern(2 << 20));
    expect(connections, 1);
  });

  test('a gzip body comes decoded', () async {
    DoxHttpClientResponse res = await client.get(base.resolve('/gzip'));
    expect(res.body, 'zipped text ' * 1000);
    expect(res.header('content-encoding'), isNull);
    DoxHttpClientStreamedResponse streamed = await client.sendStreamed(
      'GET',
      base.resolve('/gzip'),
    );
    expect(await utf8.decodeStream(streamed.body), 'zipped text ' * 1000);
  });

  test('a streamed answer with a slow listener', () async {
    const int size = 3 << 20;
    DoxHttpClientStreamedResponse res = await client.sendStreamed(
      'GET',
      base.resolve('/big/$size'),
    );
    expect(res.statusCode, 200);
    BytesBuilder got = BytesBuilder(copy: false);
    late StreamSubscription<Uint8List> sub;
    Completer<void> done = Completer<void>();
    int pauses = 0;
    sub = res.body.listen((Uint8List part) {
      got.add(part);
      if (pauses < 5 && got.length > (pauses + 1) * 300000) {
        pauses++;
        sub.pause(Future<void>.delayed(const Duration(milliseconds: 30)));
      }
    }, onDone: done.complete);
    await done.future;
    expect(pauses, 5);
    expect(got.takeBytes(), pattern(size));
  });

  test('a request body from a stream', () async {
    const int size = 2 << 20;
    Uint8List body = pattern(size);
    Stream<List<int>> parts() async* {
      for (int at = 0; at < size; at += 100000) {
        yield Uint8List.sublistView(
          body,
          at,
          at + 100000 > size ? size : at + 100000,
        );
      }
    }

    for (int? length in <int?>[null, size]) {
      DoxHttpClientStreamedResponse res = await client.sendStreamed(
        'PUT',
        base.resolve('/count'),
        body: parts(),
        contentLength: length,
      );
      expect(await utf8.decodeStream(res.body), '$size');
      expect(lastBody, body);
    }
    // HTTP/2 has no chunked framing
    DoxHttpClientStreamedResponse res = await client.sendStreamed(
      'POST',
      base.resolve('/echo'),
      body: Stream<List<int>>.value(utf8.encode('abc')),
    );
    Map<String, Object?> got =
        jsonDecode(await utf8.decodeStream(res.body)) as Map<String, Object?>;
    expect(got['body'], 'abc');
    expect(got['transfer-encoding'], isNull);
    expect(connections, 1);
  });

  test('a timeout stops one request, not the connection', () async {
    await client.get(base);
    Future<DoxHttpClientResponse> slow = client.get(
      base.resolve('/slow'),
      timeout: const Duration(milliseconds: 100),
    );
    Future<DoxHttpClientResponse> fast = client.get(base);
    await expectLater(slow, failsWith(DoxHttpFailure.timeout));
    expect((await fast).body, 'Hello World');
    expect((await client.get(base.resolve('/slow'))).body, 'slow done');
    expect(connections, 1);
  });

  test('http2: false does not ask for HTTP/2', () async {
    DoxHttpClient old = DoxHttpClient(
      trustedCertificates: certificate,
      http2: false,
    );
    addTearDown(old.close);
    // this server has HTTP/2 only
    await expectLater(
      old.get(base),
      throwsA(
        isA<DoxHttpClientException>().having(
          (DoxHttpClientException e) => e.failure,
          'failure',
          // the server refuses the handshake, or it gets HTTP/1.1 bytes
          anyOf(
            DoxHttpFailure.tlsFailed,
            DoxHttpFailure.connectionClosed,
            DoxHttpFailure.badResponse,
          ),
        ),
      ),
    );
  });
}
