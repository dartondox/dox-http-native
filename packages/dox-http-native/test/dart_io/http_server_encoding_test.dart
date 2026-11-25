// Copyright (c) 2024, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_encoding_test.dart.
// The server response body follows the defaults or the charset of the
// Content-Type header.
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  Future<(List<String>, List<int>)> answer(
    void Function(HttpResponse) write,
  ) async {
    var server = await serve((HttpRequest request) {
      write(request.response);
      request.response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    HttpClientResponse response = await request.close();
    List<int> bytes = <int>[for (List<int> c in await response.toList()) ...c];
    return (
      response.headers[HttpHeaders.contentTypeHeader] ?? <String>[],
      bytes,
    );
  }

  const List<String> plainUtf8 = <String>['text/plain; charset=utf-8'];
  ContentType utf8Type() => ContentType('text', 'plain', charset: 'utf-8');

  // Japanese, utf-8 (the only built-in encoding that supports Japanese)
  test('write without content type, Japanese', () async {
    var (List<String> ct, List<int> b) = await answer(
      (HttpResponse r) => r.write('日本語'),
    );
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日本語');
  });
  test('writeln without content type, Japanese', () async {
    var (ct, b) = await answer((HttpResponse r) => r.writeln('日本語'));
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日本語\n');
  });
  test('writeAll without content type, Japanese', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r.writeAll(<String>['日', '本', '語']),
    );
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日本語');
  });
  test('writeCharCode without content type, Japanese', () async {
    var (ct, b) = await answer((HttpResponse r) => r.writeCharCode(0x65E5));
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日');
  });
  test('write with charset, Japanese', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r
        ..headers.contentType = utf8Type()
        ..write('日本語'),
    );
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日本語');
  });
  // regression: https://github.com/dart-lang/sdk/issues/59719
  test('writeln with charset, Japanese', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r
        ..headers.contentType = utf8Type()
        ..writeln('日本語'),
    );
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日本語\n');
  });
  test('writeAll with charset, Japanese', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r
        ..headers.contentType = utf8Type()
        ..writeAll(<String>['日', '本', '語']),
    );
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日本語');
  });
  test('writeCharCode with charset, Japanese', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r
        ..headers.contentType = utf8Type()
        ..writeCharCode(0x65E5),
    );
    expect(ct, plainUtf8);
    expect(utf8.decode(b), '日');
  });

  // German
  test('write without charset, German', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r
        ..headers.contentType = ContentType('text', 'plain')
        ..write('Löscherstraße'),
    );
    expect(ct, <String>['text/plain']);
    expect(latin1.decode(b), 'Löscherstraße');
  });
  // if the charset is not recognized the text is encoded with ISO-8859-1
  test('write with an unrecognized charset, German', () async {
    var (ct, b) = await answer(
      (HttpResponse r) => r
        ..headers.contentType = ContentType('text', 'plain', charset: '123')
        ..write('Löscherstraße'),
    );
    expect(ct, <String>['text/plain; charset=123']);
    expect(latin1.decode(b), 'Löscherstraße');
  });
  test('write without content type, German', () async {
    var (ct, b) = await answer((HttpResponse r) => r.write('Löscherstraße'));
    expect(ct, plainUtf8);
    expect(utf8.decode(b), 'Löscherstraße');
  });
}
