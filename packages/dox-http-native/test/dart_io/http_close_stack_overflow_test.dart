// Copyright (c) 2020, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_close_stack_overflow_test.dart.
// ignore_for_file: unawaited_futures
// The SDK test closes 10000 `ServerSocket`s that an `HttpClient` talks to.
// It tests the client only.
import 'package:test/test.dart';

void main() {
  test(
    'closing many servers does not overflow the stack',
    () {},
    skip: 'dox_http_native: client-only test (it uses plain ServerSockets)',
  );
}
