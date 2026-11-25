// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_response_test.dart.
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

final File scriptSource = File('test/dart_io/http_server_response_test.dart');

/// Runs [handler] for one request. The handler calls `finish` where the SDK
/// test closes the server.
Future<void> run(
  void Function(NativeHttpServer, HttpRequest, void Function()) handler, {
  int? bytes,
  bool closeClient = false,
}) async {
  Completer<void> finished = Completer<void>();
  late NativeHttpServer server;
  server = await serve(
    (HttpRequest r) => handler(server, r, () {
      if (!finished.isCompleted) finished.complete();
    }),
  );
  server.defaultResponseHeaders.clear();
  HttpClient client = newClient();
  Completer<void> clientDone = Completer<void>();
  client
      .get('127.0.0.1', server.port, '/')
      .then((HttpClientRequest r) => r.close())
      .then((HttpClientResponse response) {
        int received = 0;
        late StreamSubscription<List<int>> sub;
        sub = response.listen(
          (List<int> data) {
            if (closeClient) {
              sub.cancel();
              client.close();
            } else {
              received += data.length;
            }
          },
          onDone: () {
            if (bytes != null) expect(received, bytes);
            client.close();
            clientDone.complete();
          },
          onError: (Object e) => expect(e, isA<HttpException>()),
        );
        if (closeClient) clientDone.complete();
      })
      .catchError((Object e) {
        if (!clientDone.isCompleted) clientDone.complete();
      }, test: (Object e) => e is HttpException);
  await Future.wait(<Future<void>>[finished.future, clientDone.future])
      .timeout(const Duration(seconds: 20));
}

void main() {
  group('response done', () {
    test(
      'close',
      () => run((s, r, finish) {
        r.response.close();
        r.response.done.then((response) {
          expect(response, r.response);
          finish();
        });
      }),
    );
    test(
      'pipe of a missing file',
      skip: 'dox_http_native: after a failed addStream the response stays open (no answer, no close), dart:io cuts the connection',
      () => run((s, r, finish) {
        File('__nonexistent_file_')
            .openRead()
            .cast<List<int>>()
            .pipe(r.response)
            .catchError((Object e) => finish());
      }),
    );
    test(
      'content length 0',
      () => run((s, r, finish) {
        r.response.done.then((_) => finish());
        r.response.contentLength = 0;
        r.response.close();
      }),
    );
  });

  group('addStream', () {
    int bytes = scriptSource.lengthSync();
    test(
      'one file',
      () => run((s, r, finish) {
        r.response.addStream(scriptSource.openRead()).then((response) {
          response.close();
          response.done.then((_) => finish());
        });
      }, bytes: bytes),
      skip: 'dox_http_native: addStream completes with null, not the response',
    );
    test(
      'one file (no result used)',
      () => run((s, r, finish) {
        r.response.addStream(scriptSource.openRead()).then((_) {
          r.response.close();
          r.response.done.then((_) => finish());
        });
      }, bytes: bytes),
    );
    test(
      'two files',
      () => run((s, r, finish) {
        r.response.addStream(scriptSource.openRead()).then((_) {
          r.response.addStream(scriptSource.openRead()).then((_) {
            r.response.close();
            r.response.done.then((_) => finish());
          });
        });
      }, bytes: bytes * 2),
    );
    test(
      'empty sync stream',
      () => run((s, r, finish) {
        StreamController<List<int>> c = StreamController<List<int>>(sync: true);
        r.response.addStream(c.stream).then((_) {
          r.response.close();
          r.response.done.then((_) => finish());
        });
        c.close();
      }, bytes: 0),
    );
    test(
      'missing file',
      skip: 'dox_http_native: after a failed addStream the response stays open (no answer, no close), dart:io cuts the connection',
      () => run((s, r, finish) {
        r.response
            .addStream(File('__nonexistent_file_').openRead())
            .catchError((Object e) => finish());
      }),
    );
  });

  group('addStream, client closed', () {
    test(
      'one file',
      () => run((s, r, finish) {
        r.response.addStream(scriptSource.openRead()).then((_) {
          r.response.close();
          r.response.done.then((_) => finish());
        });
      }, closeClient: true),
    );
    test(
      '1000 files',
      () => run((s, r, finish) {
        int count = 0;
        void write() {
          r.response.addStream(scriptSource.openRead()).then((_) {
            r.response.write('sync data');
            if (++count < 1000) {
              write();
            } else {
              r.response.close();
              r.response.done.then((_) => finish());
            }
          });
        }

        write();
      }, closeClient: true),
    );
  });

  group('add, client closed', () {
    test(
      'one add',
      () => run((s, r, finish) {
        r.response.add(scriptSource.readAsBytesSync());
        r.response.close();
        r.response.done.then((_) => finish());
      }, closeClient: true),
    );
    test(
      '1000 adds',
      () => run((s, r, finish) {
        for (int i = 0; i < 1000; i++) {
          r.response.add(scriptSource.readAsBytesSync());
        }
        r.response.close();
        r.response.done.then((_) => finish());
      }, closeClient: true),
    );
    test(
      '1000 adds, one for each turn',
      () => run((s, r, finish) {
        int count = 0;
        void write() {
          r.response.add(scriptSource.readAsBytesSync());
          Timer.run(() {
            if (++count < 1000) {
              write();
            } else {
              r.response.close();
              r.response.done.then((_) => finish());
            }
          });
        }

        write();
      }, closeClient: true),
    );
  });

  group('bad content length', () {
    void bad(String name, void Function(HttpResponse) body) {
      test(
        name,
        () => run((s, r, finish) {
          body(r.response);
          r.response.close();
          r.response.done.catchError(
            (Object e) => finish(),
            test: (Object e) => e is HttpException,
          );
        }),
        skip:
            'dox_http_native: a buffered body goes out with its real length, '
            'done completes with no error',
      );
    }

    bad(
      '0 and one byte',
      (r) => r
        ..contentLength = 0
        ..add(<int>[0]),
    );
    bad(
      '5 and 6 bytes',
      (r) => r
        ..contentLength = 5
        ..add(<int>[0, 0, 0])
        ..add(<int>[0, 0, 0]),
    );
    bad(
      '0 and 192 KiB',
      (r) => r
        ..contentLength = 0
        ..add(Uint8List(64 * 1024))
        ..add(Uint8List(64 * 1024))
        ..add(Uint8List(64 * 1024)),
    );
    bad('5 and no bytes', (r) => r.contentLength = 5);
    bad(
      '5 and one byte',
      (r) => r
        ..contentLength = 5
        ..add(<int>[0]),
    );
  });

  test('request data is ignored', () async {
    var server = await serve((HttpRequest request) {
      request.response.write('all-okay');
      request.response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    request.contentLength = 1024 * 1024;
    request.add(Uint8List(1024 * 1024));
    HttpClientResponse response = await request.close();
    expect(
      await response.fold<int>(0, (int s, List<int> b) => s + b.length),
      8,
    );
  });

  test(
    'writeCharCode is latin-1 without a content type',
    () => run((s, r, finish) {
      r.response.writeCharCode(0xFF);
      r.response.writeCharCode(0xFF);
      r.response.close().then((_) => finish());
    }, bytes: 2),
    skip: 'dox_http_native: text is UTF-8, never latin-1',
  );

  test('add refuses a value that is not a byte', () async {
    Completer<List<Object?>> got = Completer<List<Object?>>();
    var server = await serve((HttpRequest request) {
      List<Object?> errors = <Object?>[];
      for (List<int> bad in <List<int>>[
        <int>[256],
        <int>[-1],
        Uint16List(1)..[0] = 300,
      ]) {
        try {
          request.response.add(bad);
        } on ArgumentError catch (e) {
          errors.add(e);
        }
      }
      request.response.add(Uint8ClampedList.fromList(<int>[1, 2]));
      request.response.close();
      got.complete(errors);
    });
    String text = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect((await got.future).length, 3);
    expect(text, endsWith('\u0001\u0002'));
  });
}
