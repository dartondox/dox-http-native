// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/https_server_test.dart.
// dox_http_native has no TLS: a proxy ends it (see the README). The TLS tests
// of the SDK are skipped. What stays: a client that sends bytes that are not
// HTTP (a TLS handshake that goes to a plain port) must not hurt the server.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test(
    'bindSecure and listenOn with a SecurityContext',
    () {},
    skip: 'dox_http_native: no TLS, a proxy ends it',
  );

  test('early client close with an invalid TLS handshake', () async {
    int requests = 0;
    var server = await serve((HttpRequest request) {
      requests++;
      request.response.close();
    });
    for (int i = 0; i < 10; i++) {
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      socket.write('Invalid TLS handshake');
      socket.close();
      await socket.done;
      socket.destroy();
    }
    expect(requests, 0);
    String ok = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(statusOf(ok), 200);
  });

  test(
    'a real TLS ClientHello on the plain port gets 400 or a close',
    () async {
      var server = await serve(
        (HttpRequest request) => request.response.close(),
      );
      List<int> hello = <int>[
        0x16,
        0x03,
        0x01,
        0x00,
        0x2f,
        0x01,
        0x00,
        0x00,
        0x2b,
        0x03,
        0x03,
        ...List<int>.filled(32, 7),
        0x00,
        0x00,
        0x02,
        0x00,
        0x2f,
        0x01,
        0x00,
      ];
      String text = await raw(server.port, hello);
      expect(statusOf(text), anyOf(400, 0));
      String ok = await raw(
        server.port,
        'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(statusOf(ok), 200);
    },
  );
}
