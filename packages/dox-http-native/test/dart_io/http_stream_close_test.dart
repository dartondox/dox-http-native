// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_stream_close_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('server, client request and response streams all close', () async {
    Completer<void> serverClosed = Completer<void>();
    NativeHttpServer server = await serve((HttpRequest request) {
      request.listen(
        (_) {},
        onDone: () {
          request.response.done.then((_) => serverClosed.complete());
          request.response.write('hello!');
          request.response.close();
        },
      );
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.postUrl(
      Uri.parse('http://127.0.0.1:${server.port}'),
    );
    request.contentLength = 'hello!'.length;
    Future<void> requestDone = request.done.then((_) {});
    request.write('hello!');
    HttpClientResponse response = await request.close();
    await response.drain<void>();
    await requestDone;
    await serverClosed.future.timeout(const Duration(seconds: 5));
  });
}
