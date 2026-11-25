// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_compression_test.dart.
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  for (bool autoUncompress in <bool>[true, false]) {
    test(
      'server compress (client autoUncompress: $autoUncompress)',
      () async {
        Uint8List long = Uint8List(1024 * 1024);
        for (int i = 0; i < long.length; i++) {
          long[i] = i & 0xFF;
        }
        for (List<int> data in <List<int>>[
          'My raw server provided data'.codeUnits,
          long,
        ]) {
          var server = await serve((HttpRequest request) {
            request.response.add(data);
            request.response.close();
          }, autoCompress: true);
          HttpClient client = newClient();
          client.autoUncompress = autoUncompress;
          HttpClientRequest request = await client.get(
            '127.0.0.1',
            server.port,
            '/',
          );
          request.headers.set(HttpHeaders.acceptEncodingHeader, 'gzip,deflate');
          HttpClientResponse response = await request.close();
          List<int> list = <int>[
            for (List<int> c in await response.toList()) ...c,
          ];
          if (autoUncompress) {
            expect(list, data);
          } else {
            expect(
              response.headers.value(HttpHeaders.contentEncodingHeader),
              'gzip',
            );
            expect(gzip.decode(list), data);
          }
        }
      },
      skip: 'dox_http_native: a body of 256 bytes or less is never compressed',
    );
  }

  test('server compress: long body', () async {
    Uint8List data = Uint8List(1024 * 1024);
    for (int i = 0; i < data.length; i++) {
      data[i] = i & 0xFF;
    }
    var server = await serve((HttpRequest request) {
      request.response.add(data);
      request.response.close();
    }, autoCompress: true);
    HttpClient client = newClient();
    client.autoUncompress = false;
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    request.headers.set(HttpHeaders.acceptEncodingHeader, 'gzip,deflate');
    HttpClientResponse response = await request.close();
    expect(response.headers.value(HttpHeaders.contentEncodingHeader), 'gzip');
    List<int> list = <int>[for (List<int> c in await response.toList()) ...c];
    expect(gzip.decode(list), data);
  });

  const Map<String, bool> encodings = <String, bool>{
    'gzip': true,
    'deflate': false,
    'gzip, deflate': true,
    'gzip ,deflate': true,
    'gzip  ,  deflate': true,
    'deflate,gzip': true,
    'deflate, gzip': true,
    'deflate ,gzip': true,
    'deflate  ,  gzip': true,
    'abc,deflate  ,  gzip,def,,,ghi  ,jkl': true,
    'xgzip': false,
    'gzipx;': false,
  };
  encodings.forEach((String encoding, bool valid) {
    test('Accept-Encoding "$encoding"', () async {
      var server = await serve((HttpRequest request) {
        request.response.write('data' * 100);
        request.response.close();
      }, autoCompress: true);
      String text = await raw(
        server.port,
        'GET / HTTP/1.1\r\nAccept-Encoding: $encoding\r\nConnection: close\r\n\r\n',
      );
      expect(text.toLowerCase().contains('content-encoding: gzip'), valid);
    });
  });

  test('autoCompress is off by default', () async {
    var server = await serve((HttpRequest request) {
      expect(request.headers.value(HttpHeaders.acceptEncodingHeader), 'gzip');
      request.response.write('data' * 100);
      request.response.close();
    });
    String text = await raw(
      server.port,
      'GET / HTTP/1.1\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n',
    );
    expect(text.toLowerCase(), isNot(contains('content-encoding')));
  });
}
