import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:http2/http2.dart';
import 'package:test/test.dart';

/// 3 MiB: more than the 1 MiB stream window of a client, so the answer
/// needs WINDOW_UPDATE frames.
const int bigSize = 3 << 20;

/// Body limits of the test server: a 5 MiB upload fits, 7 MiB does not
/// (unless it is a multipart form).
const int maxBody = 6 << 20;
const int maxMultipart = 8 << 20;

/// A streamed answer: 48 parts of 64 KiB, more than the 1 MiB window of
/// Dart and of the client.
const int parts = 48;

Completer<void> resetSeen = Completer<void>();
Completer<void> streamGone = Completer<void>();

/// gRPC handlers: `/grpc-ignore` answers once this completes.
Completer<void> release = Completer<void>();

/// `/grpc-early`: its body stream ended (`done`, no error).
Completer<bool> earlyDone = Completer<bool>();

/// `/gone-wait` heard that the client left.
Completer<void> goneSeen = Completer<void>();

/// `/gone-normal`: did `gone` complete after a normal answer?
bool goneAfterAnswer = false;

/// `/hold` (a normal route that never reads its body) answers once this
/// completes.
Completer<void> hold = Completer<void>();

/// The error the body stream of `/grpc-reset` ended with.
Completer<Object> bodyError = Completer<Object>();

/// `length hash` of bytes in order (the hash changes with the order).
String digest(int length, int hash) => '$length $hash';

int mix(int hash, List<int> part) {
  for (int b in part) {
    hash = (hash * 31 + b) & 0x3fffffff;
  }
  return hash;
}

/// The gRPC routes. The body streams in parts (`isStreamed`).
Future<void> grpc(NativeRequest req, NativeResponse res) async {
  expect(req.isStreamed, isTrue);
  switch (req.uri.path) {
    case '/grpc-slow':
      // a slow reader: pauses after each part, so the client must wait
      int length = 0;
      int hash = 0;
      Completer<void> over = Completer<void>();
      late StreamSubscription<Uint8List> sub;
      sub = req.listen((Uint8List part) {
        length += part.length;
        hash = mix(hash, part);
        sub.pause();
        Timer(const Duration(milliseconds: 2), sub.resume);
      }, onDone: over.complete);
      await over.future;
      res.write(digest(length, hash));
    case '/grpc-ignore':
      // never reads the body: the client blocks at the window
      await release.future;
      res.write('ok');
    case '/grpc-trailers':
      int length = 0;
      await for (Uint8List part in req) {
        length += part.length;
      }
      res.headers.contentType = ContentType('application', 'grpc');
      res.add(List<int>.filled(length, 1));
      res.trailers.set('grpc-status', '0');
      res.trailers.set('grpc-message', 'all good');
    case '/grpc-trailers-only':
      // an error before any message: one HEADERS with END_STREAM
      await req.drain<void>();
      res.headers.set('grpc-status', '12');
    case '/grpc-empty-trailers':
      // no body, trailers: HEADERS, then the trailers HEADERS
      await req.drain<void>();
      res.trailers.set('grpc-status', '5');
    case '/grpc-early':
      // answers before the body ends, listens with no onError
      req.listen((Uint8List _) {}, onDone: () => earlyDone.complete(true));
      res.write('early');
    case '/grpc-cancel':
      // takes one part, cancels: the rest must still be acked
      Completer<void> first = Completer<void>();
      late StreamSubscription<Uint8List> sub;
      sub = req.listen((Uint8List _) {
        if (!first.isCompleted) first.complete();
      });
      await first.future;
      await sub.cancel();
      await Future<void>.delayed(const Duration(seconds: 1));
      res.write('ok');
    case '/grpc-reset':
      try {
        await req.drain<void>();
      } catch (e) {
        bodyError.complete(e);
      }
    case '/grpc-bidi':
      // echo each part at once, while the request still streams
      await res.flush();
      await for (Uint8List part in req) {
        res.add(part);
        await res.flush();
      }
      res.trailers.set('grpc-status', '0');
  }
  await res.close();
}

void handle(HttpRequest req) async {
  NativeResponse res = req.response as NativeResponse;
  if (req.uri.path.startsWith('/grpc')) {
    return grpc(req as NativeRequest, res);
  }
  switch (req.uri.path) {
    case '/':
      res.write('Hello World');
    case '/big':
      res.add(List<int>.filled(bigSize, 7));
    case '/room':
      // hasRoom: before the stream, after a small flush, with a full
      // window, and after Rust sent the data
      List<bool> room = <bool>[res.hasRoom];
      res.write('a');
      await res.flush();
      room.add(res.hasRoom);
      res.add(Uint8List(NativeResponse.window + 10));
      room.add(res.hasRoom);
      await res.flush();
      room.add(res.hasRoom);
      res.write(room.join(','));
    case '/many-parts':
      // flush first: the stream starts, so every part goes through sendPart
      res.write('start,');
      await res.flush();
      await res.addStream(manyParts());
    case '/hold':
      await hold.future;
      res.write('held');
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
    case '/echo':
      // the length and a sum of the body bytes
      int length = 0;
      int sum = 0;
      await for (List<int> part in req) {
        length += part.length;
        for (int b in part) {
          sum = (sum + b) & 0xffffffff;
        }
      }
      res.write('$length $sum');
    case '/gone-wait':
      unawaited(res.gone.then((_) => goneSeen.complete()));
      await goneSeen.future.timeout(const Duration(seconds: 5));
      expect(res.isGone, isTrue);
    case '/gone-normal':
      unawaited(res.gone.then((_) => goneAfterAnswer = true));
      res.write('ok');
    case '/trailers-204':
      res.statusCode = HttpStatus.noContent;
      res.trailers.set('grpc-status', '0');
    case '/trailers-plain':
      res.write('hello');
      res.trailers.set('grpc-status', '0');
    case '/stream':
      await res.addStream(
        Stream<List<int>>.fromIterable(<List<int>>[
          for (int i = 0; i < parts; i++) List<int>.filled(64 * 1024, i),
        ]),
      );
    case '/forever':
      // writes until the client resets the stream
      bool over = false;
      unawaited(
        res.done.then((_) {
          over = true;
          streamGone.complete();
        }),
      );
      while (!over) {
        res.add(List<int>.filled(64 * 1024, 2));
        await res.flush();
      }
    default:
      res.statusCode = HttpStatus.notFound;
  }
  await res.close();
}

class Answer {
  Answer(this.headers, this.body, [this.trailers = const <String, String>{}]);

  final Map<String, String> headers;
  final List<int> body;

  /// the headers of a second HEADERS frame (after DATA, or after the head)
  final Map<String, String> trailers;

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
  late ClientTransportConnection c;

  setUpAll(() async {
    expect(NativeHttpServer.loadError, isNull);
    server = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      http2: true,
      maxBodySize: maxBody,
      maxMultipartSize: maxMultipart,
    );
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

  test('many parts made together arrive whole and in order', () async {
    Answer a = await request(c, '/many-parts');
    expect(a.status, 200);
    expect(utf8.decode(a.body), manyPartsText());
  });

  test('hasRoom follows the stream and the window', () async {
    Answer a = await request(c, '/room');
    String tail = utf8.decode(a.body.sublist(a.body.length - 21));
    expect(tail, 'false,true,false,true');
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

  /// `length sum` of the bytes, as `/echo` answers.
  String echoOf(List<int> body) {
    int sum = 0;
    for (int b in body) {
      sum = (sum + b) & 0xffffffff;
    }
    return '${body.length} $sum';
  }

  List<int> bytes(int n) => List<int>.generate(n, (int i) => i * 7 & 0xff);

  test('a POST with a small body', () async {
    Answer a = await request(
      c,
      '/echo',
      method: 'POST',
      body: utf8.encode('hello'),
      extra: <Header>[Header.ascii('content-length', '5')],
    );
    expect(a.status, 200);
    expect(utf8.decode(a.body), echoOf(utf8.encode('hello')));
  });

  test('a 5 MiB upload needs window updates', () async {
    List<int> body = bytes(5 << 20);
    Answer a = await request(c, '/echo', method: 'POST', body: body);
    expect(utf8.decode(a.body), echoOf(body));
  });

  test('a body over the limit gets 413, other streams go on', () async {
    List<Answer> all = await Future.wait(<Future<Answer>>[
      request(c, '/echo', method: 'POST', body: bytes(7 << 20)),
      request(c, '/'),
      request(c, '/echo', method: 'POST', body: bytes(1000)),
    ]);
    expect(all[0].status, 413);
    expect(utf8.decode(all[1].body), 'Hello World');
    expect(utf8.decode(all[2].body), echoOf(bytes(1000)));
    expect((await request(c, '/')).status, 200);
  });

  test('a multipart upload gets the multipart limit', () async {
    List<int> body = bytes(7 << 20);
    Answer a = await request(
      c,
      '/echo',
      method: 'POST',
      body: body,
      extra: <Header>[
        Header.ascii('content-type', 'multipart/form-data; boundary=x'),
      ],
    );
    expect(a.status, 200);
    expect(utf8.decode(a.body), echoOf(body));
  });

  test('a streamed answer', () async {
    List<Answer> all = await Future.wait(<Future<Answer>>[
      request(c, '/stream'),
      request(c, '/stream'),
      request(c, '/'),
    ]);
    for (Answer a in all.take(2)) {
      expect(a.status, 200);
      expect(a.headers.containsKey('content-length'), isFalse);
      expect(a.body.length, parts * 64 * 1024);
      for (int i = 0; i < parts; i++) {
        expect(a.body[i * 64 * 1024], i);
      }
    }
    expect(utf8.decode(all[2].body), 'Hello World');
  });

  test('a client RST during a streamed answer stops the handler', () async {
    ClientTransportStream s = c.makeRequest(<Header>[
      Header.ascii(':method', 'GET'),
      Header.ascii(':scheme', 'http'),
      Header.ascii(':path', '/forever'),
    ], endStream: true);
    // the handler writes until its window is full: then reset
    await s.incomingMessages.firstWhere(
      (StreamMessage m) => m is DataStreamMessage,
    );
    s.terminate();
    await streamGone.future.timeout(const Duration(seconds: 5));
    expect((await request(c, '/')).status, 200);
  });

  // the flood limits (RST_STREAM, PING, SETTINGS, empty frames) must not
  // cut a normal busy client
  test('a busy normal client is not cut by the flood limits', () async {
    ClientTransportConnection busy = await connect(server.port);
    for (int round = 0; round < 3; round++) {
      List<Future<Object?>> work = <Future<Object?>>[
        for (int i = 0; i < 100; i++) request(busy, '/'),
        for (int i = 0; i < 20; i++) busy.ping(),
        request(busy, '/big'),
        request(busy, '/stream'),
      ];
      // some streams are cancelled, as a browser does on a page change
      for (int i = 0; i < 20; i++) {
        busy.makeRequest(<Header>[
          Header.ascii(':method', 'GET'),
          Header.ascii(':scheme', 'http'),
          Header.ascii(':path', '/'),
        ], endStream: true).terminate();
      }
      await Future.wait(work);
    }
    expect((await request(busy, '/')).status, 200);
    expect(busy.isOpen, isTrue);
    await busy.finish();
  });

  /// A gRPC request: HEADERS with no END_STREAM.
  ClientTransportStream grpcCall(ClientTransportConnection c, String path) =>
      c.makeRequest(<Header>[
        Header.ascii(':method', 'POST'),
        Header.ascii(':scheme', 'http'),
        Header.ascii(':path', path),
        Header.ascii('content-type', 'application/grpc'),
        Header.ascii('te', 'trailers'),
      ]);

  /// Reads the answer of [s]: the first HEADERS, the DATA, and the
  /// headers of a later HEADERS frame as trailers.
  Future<Answer> answerOf(ClientTransportStream s) async {
    Map<String, String> headers = <String, String>{};
    Map<String, String> trailers = <String, String>{};
    List<int> data = <int>[];
    bool first = true;
    await for (StreamMessage m in s.incomingMessages) {
      if (m is HeadersStreamMessage) {
        Map<String, String> into = first ? headers : trailers;
        first = false;
        for (Header h in m.headers) {
          into[ascii.decode(h.name)] = ascii.decode(h.value);
        }
      } else if (m is DataStreamMessage) {
        data.addAll(m.bytes);
      }
    }
    return Answer(headers, data, trailers);
  }

  test('a gRPC body streams in order to a slow reader', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-slow');
    // 3 MiB in 16 KiB parts: more than the 1 MiB stream window
    List<int> body = bytes(3 << 20);
    for (int at = 0; at < body.length; at += 16 * 1024) {
      s.sendData(body.sublist(at, at + 16 * 1024));
    }
    s.sendData(<int>[], endStream: true);
    Answer a = await answerOf(s);
    expect(a.status, 200);
    expect(utf8.decode(a.body), digest(body.length, mix(0, body)));
  });

  test('a gRPC body nobody reads holds the client at the window', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-ignore');
    int pulled = 0;
    Stream<StreamMessage> source() async* {
      List<int> part = List<int>.filled(16 * 1024, 3);
      for (int i = 0; i < 256; i++) {
        pulled += part.length;
        yield DataStreamMessage(part, endStream: i == 255);
      }
    }

    unawaited(s.outgoingMessages.addStream(source()).catchError((_) {}));
    Future<Answer> answer = answerOf(s);
    await Future<void>.delayed(const Duration(milliseconds: 500));
    // 4 MiB to send, the stream window is 1 MiB: the client waits
    expect(pulled, greaterThanOrEqualTo(1 << 20));
    expect(pulled, lessThan(2 << 20));
    release.complete();
    Answer a = await answer;
    expect(a.status, 200);
    expect(utf8.decode(a.body), 'ok');
    expect((await request(c, '/')).status, 200);
  });

  test('an early answer ends the body stream without an error', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-early');
    s.sendData(bytes(100));
    Answer a = await answerOf(s);
    expect(utf8.decode(a.body), 'early');
    expect(await earlyDone.future.timeout(const Duration(seconds: 5)), isTrue);
    expect((await request(c, '/')).status, 200);
  });

  test('a cancelled body subscription still acks the rest', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-cancel');
    List<int> body = bytes(3 << 20);
    for (int at = 0; at < body.length; at += 16 * 1024) {
      s.sendData(body.sublist(at, at + 16 * 1024));
    }
    s.sendData(<int>[], endStream: true);
    Answer a = await answerOf(s).timeout(const Duration(seconds: 10));
    expect(utf8.decode(a.body), 'ok');
  });

  test(
    'gone completes when the client resets, not on a normal answer',
    () async {
      ClientTransportStream s = c.makeRequest(<Header>[
        Header.ascii(':method', 'GET'),
        Header.ascii(':scheme', 'http'),
        Header.ascii(':path', '/gone-wait'),
      ], endStream: true);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      s.terminate();
      await goneSeen.future.timeout(const Duration(seconds: 5));
      Answer a = await request(c, '/gone-normal');
      expect(utf8.decode(a.body), 'ok');
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(goneAfterAnswer, isFalse);
      // reading `gone` must not leave the response in `watched`
      expect(server.watched, isEmpty);
    },
  );

  test('trailers on a whole body: content-length, HEAD and 204', () async {
    ClientTransportStream s = c.makeRequest(<Header>[
      Header.ascii(':method', 'GET'),
      Header.ascii(':scheme', 'http'),
      Header.ascii(':path', '/trailers-plain'),
    ], endStream: true);
    Answer a = await answerOf(s);
    expect(utf8.decode(a.body), 'hello');
    expect(a.headers['content-length'], '5');
    expect(a.trailers, <String, String>{'grpc-status': '0'});
    // HEAD: no body
    s = c.makeRequest(<Header>[
      Header.ascii(':method', 'HEAD'),
      Header.ascii(':scheme', 'http'),
      Header.ascii(':path', '/trailers-plain'),
    ], endStream: true);
    a = await answerOf(s);
    expect(a.status, 200);
    expect(a.body, isEmpty);
    // 204: no body, no length
    s = c.makeRequest(<Header>[
      Header.ascii(':method', 'GET'),
      Header.ascii(':scheme', 'http'),
      Header.ascii(':path', '/trailers-204'),
    ], endStream: true);
    a = await answerOf(s);
    expect(a.status, 204);
    expect(a.body, isEmpty);
    expect(a.headers.containsKey('content-length'), isFalse);
  });

  test('trailers come after the body', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-trailers');
    s.sendData(bytes(1000), endStream: true);
    Answer a = await answerOf(s);
    expect(a.status, 200);
    expect(a.headers['content-type'], 'application/grpc');
    expect(a.body.length, 1000);
    expect(a.trailers, <String, String>{
      'grpc-status': '0',
      'grpc-message': 'all good',
    });
  });

  test('trailers-only and a head with only trailers', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-trailers-only');
    s.sendData(bytes(10), endStream: true);
    Answer a = await answerOf(s);
    expect(a.headers['grpc-status'], '12');
    expect(a.body, isEmpty);
    expect(a.trailers, isEmpty);
    s = grpcCall(c, '/grpc-empty-trailers');
    s.sendData(bytes(10), endStream: true);
    a = await answerOf(s);
    expect(a.status, 200);
    expect(a.body, isEmpty);
    expect(a.trailers, <String, String>{'grpc-status': '5'});
  });

  test('a client RST mid-body ends the body stream with an error', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-reset');
    s.sendData(bytes(100));
    await Future<void>.delayed(const Duration(milliseconds: 100));
    s.terminate();
    Object error = await bodyError.future.timeout(const Duration(seconds: 5));
    expect(error, isA<HttpException>());
    expect((await request(c, '/')).status, 200);
  });

  test('bidi: parts echo back while the request streams', () async {
    ClientTransportStream s = grpcCall(c, '/grpc-bidi');
    StreamIterator<StreamMessage> incoming = StreamIterator<StreamMessage>(
      s.incomingMessages,
    );
    expect(await incoming.moveNext(), isTrue);
    expect(incoming.current, isA<HeadersStreamMessage>());
    for (int i = 0; i < 5; i++) {
      List<int> part = List<int>.filled(100 + i, i);
      s.sendData(part);
      // the echo comes before the next part is sent
      List<int> echo = <int>[];
      while (echo.length < part.length) {
        expect(await incoming.moveNext(), isTrue);
        echo.addAll((incoming.current as DataStreamMessage).bytes);
      }
      expect(echo, part);
    }
    s.sendData(<int>[], endStream: true);
    Map<String, String> trailers = <String, String>{};
    while (await incoming.moveNext()) {
      StreamMessage m = incoming.current;
      if (m is HeadersStreamMessage) {
        for (Header h in m.headers) {
          trailers[ascii.decode(h.name)] = ascii.decode(h.value);
        }
      }
    }
    expect(trailers, <String, String>{'grpc-status': '0'});
  });

  test(
    'unread gRPC bodies on a normal route do not freeze other uploads',
    () async {
      ClientTransportConnection shared = await connect(server.port);
      List<Future<Answer>> held = <Future<Answer>>[];
      // 16 x 1 MiB with content-type application/grpc to a route that never
      // reads: they used to hold the whole 16 MiB connection window
      for (int i = 0; i < 16; i++) {
        ClientTransportStream s = shared.makeRequest(<Header>[
          Header.ascii(':method', 'POST'),
          Header.ascii(':scheme', 'http'),
          Header.ascii(':path', '/hold'),
          Header.ascii('content-type', 'application/grpc'),
        ]);
        s.sendData(bytes(1 << 20));
        held.add(answerOf(s));
      }
      await Future<void>.delayed(const Duration(milliseconds: 200));
      // a JSON POST on the same connection still goes through
      List<int> json = utf8.encode(
        '[${List<String>.filled(50000, '1').join(',')}]',
      );
      Answer a = await request(
        shared,
        '/echo',
        method: 'POST',
        extra: <Header>[Header.ascii('content-type', 'application/json')],
        body: json,
      ).timeout(const Duration(seconds: 10));
      expect(utf8.decode(a.body), startsWith('${json.length} '));
      hold.complete();
      for (Answer h in await Future.wait(held)) {
        expect(utf8.decode(h.body), 'held');
      }
      await shared.finish();
    },
  );

  test(
    'too many resets drain the connection, open requests still finish',
    () async {
      NativeHttpServer small = await NativeHttpServer.bind(
        0,
        address: '127.0.0.1',
        http2: true,
        http2ResetBurst: 30,
        http2ResetRate: 0,
      );
      Completer<void> slow = Completer<void>();
      small.listen((HttpRequest req) async {
        if (req.uri.path == '/slow') await slow.future;
        req.response.write('done ${req.uri.path}');
        await req.response.close();
      });
      ClientTransportConnection proxy = await connect(small.port);
      // another user's slow request on the same connection
      Future<Answer> sibling = request(proxy, '/slow');
      await Future<void>.delayed(const Duration(milliseconds: 100));
      // one user cancels a lot
      for (int i = 0; i < 40; i++) {
        proxy.makeRequest(<Header>[
          Header.ascii(':method', 'GET'),
          Header.ascii(':scheme', 'http'),
          Header.ascii(':path', '/'),
        ], endStream: true).terminate();
      }
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(proxy.isOpen, isFalse, reason: 'GOAWAY came');
      slow.complete();
      Answer a = await sibling.timeout(const Duration(seconds: 5));
      expect(a.status, 200);
      expect(utf8.decode(a.body), 'done /slow');
      await proxy.finish();
      await small.close();
    },
  );

  test('http2ResetBurst and http2ResetRate are checked at bind', () async {
    await expectLater(
      NativeHttpServer.bind(0, http2: true, http2ResetBurst: 0),
      throwsArgumentError,
    );
    await expectLater(
      NativeHttpServer.bind(0, http2: true, http2ResetRate: -1),
      throwsArgumentError,
    );
    // the smallest values are fine: one reset, no refill
    NativeHttpServer tiny = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      http2: true,
      http2ResetBurst: 1,
      http2ResetRate: 0,
    );
    tiny.listen((HttpRequest req) => req.response.close());
    ClientTransportConnection t = await connect(tiny.port);
    expect((await request(t, '/')).status, 200);
    await t.finish();
    await tiny.close();
  });

  test('a 20 KB header block gets 431, the connection goes on', () async {
    ClientTransportConnection big = await connect(server.port);
    Answer a = await request(
      big,
      '/',
      extra: <Header>[Header.ascii('x-big', 'a' * 20000)],
    );
    expect(a.status, 431);
    expect((await request(big, '/')).status, 200);
    await big.finish();
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
