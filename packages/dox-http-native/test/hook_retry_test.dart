import 'package:test/test.dart';

import '../hook/build.dart';

void main() {
  test('server errors and 429 are tried again', () {
    for (int status in <int>[429, 500, 502, 503, 504]) {
      expect(shouldRetryStatus(status), isTrue, reason: '$status');
    }
  });

  test('404 and other client errors are not tried again', () {
    for (int status in <int>[301, 400, 403, 404]) {
      expect(shouldRetryStatus(status), isFalse, reason: '$status');
    }
  });

  test('the new tag form comes first, then the old one', () {
    expect(
      releaseUrls(
        '3.2.3',
        'dox_http_native-linux-x64.so',
      ).map((Uri url) => url.toString()).toList(),
      <String>[
        'https://github.com/dartondox/dox-http-native/releases/download/'
            'dox_http_native-v3.2.3/dox_http_native-linux-x64.so',
        'https://github.com/dartondox/dox-http-native/releases/download/'
            'v3.2.3/dox_http_native-linux-x64.so',
      ],
    );
  });
}
