// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_early_client_close2_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('client closes before the file answer is read', () async {
    int answered = 0;
    NativeHttpServer server = await serve((HttpRequest request) {
      File('pubspec.yaml')
          .openRead()
          .cast<List<int>>()
          .pipe(request.response)
          .catchError((Object e) {
            // ignore
          })
          .whenComplete(() => answered++);
    });
    for (int i = 0; i < 10; i++) {
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      socket.write('GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n');
      socket.close();
      await socket.done;
      socket.destroy();
    }
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(answered, 10);
    await server.close().timeout(const Duration(seconds: 5));
  });
}
