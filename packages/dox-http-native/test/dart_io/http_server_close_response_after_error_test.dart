// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_close_response_after_error_test.dart.
// The SDK test starts a client process that makes a request and cuts the
// connection, and the server closes its response in `onDone`. Here the client
// is a raw socket.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('request cut short: the server survives and closes', () async {
    var server = await serve((HttpRequest request) {
      request.listen(
        null,
        onError: (Object e) {},
        onDone: () => request.response.close(),
      );
    });
    // a plain and a chunked body that the client cuts
    for (int i = 0; i < 5; i++) {
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      socket.write(
        i.isEven
            ? 'POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\nabc'
            : 'POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab',
      );
      await socket.flush();
      socket.destroy();
    }
    String ok = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(statusOf(ok), 200);
    await server.close().timeout(const Duration(seconds: 5));
  });
}
