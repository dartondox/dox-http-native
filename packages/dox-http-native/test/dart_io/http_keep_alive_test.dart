// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_keep_alive_test.dart.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  Future<int> getData(
    HttpClient client,
    int port,
    bool chunked,
    int length,
  ) async {
    HttpClientRequest request = await client.get(
      '127.0.0.1',
      port,
      '/?chunked=$chunked&length=$length',
    );
    HttpClientResponse response = await request.close();
    return response.fold<int>(0, (int bytes, List<int> d) => bytes + d.length);
  }

  Future<int> start() async {
    var server = await serve((HttpRequest request) {
      bool chunked = request.uri.queryParameters['chunked'] == 'true';
      int length = int.parse(request.uri.queryParameters['length']!);
      if (!chunked) request.response.contentLength = length;
      request.response.add(List<int>.filled(length, 0));
      request.response.close();
    });
    return server.port;
  }

  Future<void> run(List<bool> pattern) async {
    int port = await start();
    HttpClient client = newClient();
    for (bool chunked in pattern) {
      expect(await getData(client, port, chunked, 100), 100);
    }
  }

  test(
    'keep-alive non chunked',
    () => run(<bool>[false, false, false, false, false]),
  );
  test('keep-alive chunked', () => run(<bool>[true, true, true, true, true]));
  test(
    'keep-alive mixed',
    () => run(<bool>[true, false, true, false, true, false, true, false]),
  );
}
