// Copyright (c) 2021, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_parser_header_add_test.dart.
// ignore_for_file: unawaited_futures
// The SDK test checks that the dart:io client refuses a user agent with a
// character over 255, and that a request parser accepts a null data handler.
// Server side: header bytes over 127 in a request must not break the server.
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  test(
    'a request header with UTF-8 bytes (U+2019) reaches the handler',
    () async {
      String? agent;
      NativeHttpServer server = await serve((HttpRequest request) {
        agent = request.headers.value('user-agent');
        request.response.close();
      });
      String text = await raw(server.port, <int>[
        ...'GET / HTTP/1.1\r\nUser-Agent: Bob'.codeUnits,
        0xE2, 0x80, 0x99, // U+2019 in UTF-8
        ...'s browser\r\nConnection: close\r\n\r\n'.codeUnits,
      ]);
      expect(statusOf(text), 200);
      expect(agent, isNotNull);
      expect(agent!.startsWith('Bob'), isTrue);
    },
  );

  test(
    'the client refuses a user agent with a character over 255',
    () {},
    skip: 'dox_http_native: dart:io client test',
  );
}
