// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_pipe_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

StreamTransformer<dynamic, dynamic> reverseStrings() {
  return StreamTransformer<dynamic, dynamic>.fromHandlers(
    handleData: (dynamic data, EventSink<dynamic> sink) {
      StringBuffer sb = StringBuffer();
      for (int i = (data as String).length - 1; i >= 0; i--) {
        sb.write(data[i]);
      }
      sink.add(sb.toString());
    },
  );
}

void main() {
  for (bool transform in <bool>[false, true]) {
    for (int messages in <int>[0, 1, 10]) {
      test('pipe (messages: $messages, transform: $transform)', () async {
        Completer<void> piped = Completer<void>();
        NativeHttpServer server = await serveWebSockets((WebSocket ws) {
          (transform ? ws.transform(reverseStrings()) : ws)
              .pipe(ws)
              .then((_) => piped.complete());
        });
        WebSocket client = await WebSocket.connect(wsUrl(server));
        int count = 0;
        void next() {
          if (count < messages) {
            client.add('Hello');
          } else {
            client.close();
          }
        }

        client.listen((dynamic data) {
          count++;
          expect(data, transform ? 'olleH' : 'Hello');
          next();
        });
        next();
        await piped.future.timeout(const Duration(seconds: 15));
      });
    }
  }
}
