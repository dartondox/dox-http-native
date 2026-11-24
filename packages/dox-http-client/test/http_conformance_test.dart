import 'package:dox_http_client/http.dart';
import 'package:http_client_conformance_tests/http_client_conformance_tests.dart';
import 'package:test/test.dart';

/// The tests of the Dart team for clients of `package:http`, with every
/// optional group on. The default client of `package:http` runs the same
/// tests.
void main() {
  group('DoxHttpPackageClient', () {
    testAll(
      DoxHttpPackageClient.new,
      preservesMethodCase: true,
      canSendCookieHeaders: true,
      canReceiveSetCookieHeaders: true,
      supportsAbort: true,
    );
  });
}
