// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_reuse_server_port_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

Future<int> runServer(int port, int connections, bool clean) async {
  Completer<int> completer = Completer<int>();
  NativeHttpServer server = await NativeHttpServer.bind(
    port,
    address: '127.0.0.1',
  );
  addTearDown(() => server.close(force: true));
  int i = 0;
  server.listen((HttpRequest request) {
    request.cast<List<int>>().pipe(request.response);
    i++;
    if (!clean && i == 10) {
      int p = server.port;
      server.close().then((_) => completer.complete(p));
    }
  });
  HttpClient client = HttpClient();
  await Future.wait(<Future<void>>[
    for (int c = 0; c < connections; c++)
      client
          .get('127.0.0.1', server.port, '/')
          .then((HttpClientRequest r) => r.close())
          .then((HttpClientResponse r) => r.drain<void>())
          .catchError((Object e) {
            if (clean) throw e;
          }),
  ]);
  client.close(force: true);
  if (clean) {
    int p = server.port;
    await server.close();
    completer.complete(p);
  }
  return completer.future;
}

void main() {
  for (bool clean in <bool>[true, false]) {
    test('reuse a port 10 times (clean: $clean)', () async {
      int port = await runServer(0, 10, clean);
      for (int i = 0; i < 10; i++) {
        await runServer(port, 10, clean);
      }
    });
  }
}
