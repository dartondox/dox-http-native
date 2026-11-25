// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_requested_uri_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  const String sendPath = '/path?a=b#c';
  const String expectedPath = '/path?a=b';

  Future<void> check(String expected, Map<String, String?> headers) async {
    Completer<String> seen = Completer<String>();
    NativeHttpServer server = await serve((HttpRequest request) {
      seen.complete(request.requestedUri.toString());
      request.response.close();
    }, host: 'localhost');
    HttpClient client = newClient();
    HttpClientRequest request = await client.get(
      'localhost',
      server.port,
      sendPath,
    );
    headers.forEach((String k, String? v) {
      v != null ? request.headers.set(k, v) : request.headers.removeAll(k);
    });
    await (await request.close()).drain<void>();
    expect(
      await seen.future,
      '${expected.replaceAll('%PORT', server.port.toString())}$expectedPath',
    );
  }

  test(
    'requestedUri',
    () => check('http://localhost:%PORT', <String, String?>{}),
  );
  test(
    'requestedUri with x-forwarded-proto https',
    () => check('https://localhost:%PORT', <String, String?>{
      'x-forwarded-proto': 'https',
    }),
    skip: 'dox_http_native: requestedUri ignores x-forwarded-* (a client could set them)',
  );
  test(
    'requestedUri with x-forwarded-proto ws',
    () => check('ws://localhost:%PORT', <String, String?>{
      'x-forwarded-proto': 'ws',
    }),
    skip: 'dox_http_native: requestedUri ignores x-forwarded-* (a client could set them)',
  );
  test(
    'requestedUri with x-forwarded-host',
    () => check('http://my-host:321', <String, String?>{
      'x-forwarded-host': 'my-host:321',
    }),
    skip: 'dox_http_native: requestedUri ignores x-forwarded-* (a client could set them)',
  );
  test('requestedUri with no Host header', () async {
    Completer<Uri> seen = Completer<Uri>();
    NativeHttpServer server = await serve((HttpRequest request) {
      seen.complete(request.requestedUri);
      request.response.close();
    });
    await raw(server.port, 'GET /path?a=b HTTP/1.0\r\n\r\n');
    expect((await seen.future).path, '/path');
    expect((await seen.future).query, 'a=b');
  });

  test('absolute URI in the request line', () async {
    Completer<Uri> seen = Completer<Uri>();
    NativeHttpServer server = await serve((HttpRequest request) {
      seen.complete(request.requestedUri);
      request.response.close();
    });
    await raw(
      server.port,
      'GET http://google.com/ HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n',
    );
    expect(await seen.future, Uri.http('google.com', '/'));
  });
}
