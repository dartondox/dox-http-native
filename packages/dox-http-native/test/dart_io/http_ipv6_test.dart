// (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_ipv6_test.dart.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('IPv6 request', () async {
    try {
      (await ServerSocket.bind('::1', 0)).close();
    } catch (_) {
      return; // no IPv6 here
    }
    late int port;
    var server = await serve((HttpRequest request) {
      expect(request.headers['host']![0], '[::1]:$port');
      expect(request.requestedUri.host, '::1');
      request.response.close();
    }, host: '::');
    port = server.port;
    HttpClient client = newClient();
    Uri url = Uri.parse('http://[::1]:$port/xxx');
    expect(url.host, '::1');
    HttpClientRequest request = await client.openUrl('GET', url);
    HttpClientResponse response = await request.close();
    expect(response.statusCode, HttpStatus.ok);
  });
}
