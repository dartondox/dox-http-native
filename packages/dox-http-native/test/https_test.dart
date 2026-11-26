import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:http2/http2.dart';
import 'package:test/test.dart';

/// Makes a self-signed certificate for `localhost` and `127.0.0.1` with
/// the name `dox test <n>`: `cert<n>.pem` and `key<n>.pem` in [dir]. Short
/// lived (macOS trusts no server certificate of more than 825 days). The
/// Rust tests use the fixed ones in test/certs.
void makeCertificate(Directory dir, int n) {
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
    '${dir.path}/key$n.pem',
    '-out',
    '${dir.path}/cert$n.pem',
    '-subj',
    '/CN=dox test $n',
    '-addext',
    'subjectAltName=DNS:localhost,IP:127.0.0.1',
    '-addext',
    'basicConstraints=critical,CA:FALSE',
  ]);
  expect(made.exitCode, 0, reason: '${made.stderr}');
}

/// the folder of the made certificates
late Directory made;

/// The DER bytes of the made certificate [n].
List<int> der(int n) {
  String pem = File('${made.path}/cert$n.pem').readAsStringSync();
  return base64.decode(
    pem.split('\n').where((String l) => !l.startsWith('-----')).join(),
  );
}

/// true for the two made certificates only. The check of the system
/// fails for a self-signed one, so the clients check the bytes. A
/// `SecurityContext` that trusts them works on Linux, not on macOS.
bool trusted(X509Certificate cert) {
  String got = base64.encode(cert.der);
  return got == base64.encode(der(1)) || got == base64.encode(der(2));
}

bool trustedFor(X509Certificate cert, String host, int port) => trusted(cert);

/// `/stream` sends 64 parts of 16 KiB, `/ws` echoes WebSocket messages,
/// anything else answers with the scheme and the path.
Future<void> handle(HttpRequest req) async {
  HttpResponse res = req.response;
  switch (req.uri.path) {
    case '/stream':
      for (int i = 0; i < 64; i++) {
        res.add(List<int>.filled(16 * 1024, 0x61 + i % 26));
        await res.flush();
      }
    case '/ws':
      WebSocket ws = await NativeWebSocket.upgrade(req);
      ws.listen(ws.add);
      return;
    default:
      res.write('${req.requestedUri.scheme} ${req.uri.path}');
  }
  await res.close();
}

void main() {
  late Directory dir;

  setUpAll(() {
    made = Directory.systemTemp.createTempSync('dox_https_certs_');
    makeCertificate(made, 1);
    makeCertificate(made, 2);
  });

  tearDownAll(() => made.deleteSync(recursive: true));

  late TlsOptions tls;
  late NativeHttpServer server;
  late HttpClient client;

  setUp(() async {
    // copies, so the reload test can change them
    dir = await Directory.systemTemp.createTemp('dox_https_');
    await File('${made.path}/cert1.pem').copy('${dir.path}/cert.pem');
    await File('${made.path}/key1.pem').copy('${dir.path}/key.pem');
    tls = TlsOptions(
      certificatePath: '${dir.path}/cert.pem',
      keyPath: '${dir.path}/key.pem',
    );
    server = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      http2: true,
      tls: tls,
    );
    server.listen(handle);
    client = HttpClient()..badCertificateCallback = trustedFor;
  });

  tearDown(() async {
    client.close(force: true);
    await server.close(force: true);
    await dir.delete(recursive: true);
  });

  Future<String> get(String path) async {
    HttpClientRequest req = await client.getUrl(
      Uri.parse('https://localhost:${server.port}$path'),
    );
    HttpClientResponse res = await req.close();
    expect(res.statusCode, 200);
    return utf8.decodeStream(res);
  }

  Future<SecureSocket> open(List<String> alpn) => SecureSocket.connect(
    'localhost',
    server.port,
    onBadCertificate: trusted,
    supportedProtocols: alpn,
  );

  test('HTTP/1.1 over TLS, with the https scheme', () async {
    expect(await get('/a'), 'https /a');
  });

  test('a streamed answer over TLS', () async {
    String body = await get('/stream');
    expect(body.length, 64 * 16 * 1024);
    expect(body.codeUnitAt(16 * 1024), 0x62);
  });

  test('HTTP/2 over TLS (ALPN h2)', () async {
    SecureSocket socket = await open(<String>['h2', 'http/1.1']);
    expect(socket.selectedProtocol, 'h2');
    ClientTransportConnection c = ClientTransportConnection.viaSocket(socket);
    ClientTransportStream s = c.makeRequest(<Header>[
      Header.ascii(':method', 'GET'),
      Header.ascii(':scheme', 'https'),
      Header.ascii(':path', '/h2'),
      Header.ascii(':authority', 'localhost'),
    ], endStream: true);
    List<int> data = <int>[];
    await for (StreamMessage m in s.incomingMessages) {
      if (m is DataStreamMessage) data.addAll(m.bytes);
    }
    expect(utf8.decode(data), 'https /h2');
    await c.finish();
  });

  test('WebSocket over TLS', () async {
    WebSocket ws = await WebSocket.connect(
      'wss://localhost:${server.port}/ws',
      customClient: client,
    );
    ws.add('hi');
    expect(await ws.first, 'hi');
    await ws.close();
  });

  test('plain HTTP on the TLS port gets no HTTP answer', () async {
    Socket s = await Socket.connect('127.0.0.1', server.port);
    s.write('GET / HTTP/1.1\r\nHost: a\r\n\r\n');
    List<int> got = <int>[];
    await s.forEach(got.addAll).timeout(const Duration(seconds: 5));
    expect(latin1.decode(got).startsWith('HTTP'), isFalse);
    s.destroy();
  });

  test('reloadCertificate: new connections get the new certificate', () async {
    SecureSocket old = await open(<String>['http/1.1']);
    expect(old.peerCertificate!.subject, contains('dox test 1'));

    await File('${made.path}/cert2.pem').copy(tls.certificatePath);
    await File('${made.path}/key2.pem').copy(tls.keyPath);
    await server.reloadCertificate();
    SecureSocket fresh = await open(<String>['http/1.1']);
    expect(fresh.peerCertificate!.subject, contains('dox test 2'));

    // the open connection still works
    old.write('GET /old HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n');
    String answer = await utf8.decodeStream(old);
    expect(answer, startsWith('HTTP/1.1 200'));
    expect(answer, endsWith('https /old'));

    // a bad file throws and keeps the certificate
    await File(tls.certificatePath).writeAsString('junk');
    await expectLater(server.reloadCertificate(), throwsA(isA<TlsException>()));
    SecureSocket after = await open(<String>['http/1.1']);
    expect(after.peerCertificate!.subject, contains('dox test 2'));
    fresh.destroy();
    after.destroy();
  });

  test('a bad certificate or key file throws at bind', () async {
    Future<NativeHttpServer> bindWith(String cert, String key) =>
        NativeHttpServer.bind(
          0,
          address: '127.0.0.1',
          tls: TlsOptions(certificatePath: cert, keyPath: key),
        );
    await expectLater(
      bindWith('${dir.path}/none.pem', tls.keyPath),
      throwsA(isA<TlsException>()),
    );
    // the key of another certificate
    await expectLater(
      bindWith(tls.certificatePath, '${made.path}/key2.pem'),
      throwsA(isA<TlsException>()),
    );
    // the check alone: the same errors, no server
    tls.check();
    expect(
      TlsOptions(
        certificatePath: '${dir.path}/none.pem',
        keyPath: tls.keyPath,
      ).check,
      throwsA(isA<TlsException>()),
    );
    expect(
      TlsOptions(
        certificatePath: tls.certificatePath,
        keyPath: '${made.path}/key2.pem',
      ).check,
      throwsA(isA<TlsException>()),
    );
    // a server without tls has nothing to reload
    NativeHttpServer plain = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
    );
    await expectLater(plain.reloadCertificate(), throwsStateError);
    await plain.close(force: true);
  });
}
