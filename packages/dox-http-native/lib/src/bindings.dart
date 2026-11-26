import 'dart:ffi';

/// FFI calls into the Rust library. The build hook (hook/build.dart) ships
/// the library as the code asset of this file.

/// The ABI version the Dart side expects. Must match `ABI_VERSION` in Rust.
const int abiVersion = 11;

@Native<Int32 Function()>(symbol: 'dox_version', isLeaf: true)
external int doxVersion();

@Native<Int32 Function(Pointer<Void>)>(symbol: 'dox_init')
external int doxInit(Pointer<Void> dlData);

/// Binds [port] and starts the idle helper, which posts to [sendPort].
/// [family] 0 is all interfaces, 4 is IPv4 (in [addrLo]), 6 is IPv6.
/// [maxConnections] 0 is no limit. A connection with no request for
/// [idleTimeoutMs] closes. With [http2], a connection that starts with the
/// HTTP/2 preface speaks HTTP/2 (at most [maxStreams] open streams).
/// [resetBurst] (0: the default) and [resetRate] set the reset bucket of
/// one HTTP/2 connection.
/// Returns a handle for [doxRun].
@Native<
  Int64 Function(
    Int32,
    Int64,
    Int64,
    Int64,
    Int64,
    Int32,
    Uint64,
    Uint64,
    Int64,
    Int64,
    Bool,
    Int64,
    Int64,
    Int64,
  )
>(symbol: 'dox_bind')
external int doxBind(
  int port,
  int sendPort,
  int maxBody,
  int maxMultipart,
  int bodyTimeoutMs,
  int family,
  int addrHi,
  int addrLo,
  int maxConnections,
  int idleTimeoutMs,
  bool http2,
  int maxStreams,
  int resetBurst,
  int resetRate,
);

/// HTTPS: reads the PEM files at the UTF-8 paths [cert] and [key] and uses
/// them for each new connection of [handle]. Call it right after
/// [doxBind], and again to reload. Returns 0, or < 0 and nothing changes:
/// -1 the certificate file can not be read, -2 it has no certificate, -3
/// the key file can not be read, -4 it has no key, -5 the key does not fit
/// the certificate, -6 a path is not UTF-8.
@Native<Int64 Function(Int64, Pointer<Uint8>, Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dox_tls',
)
external int doxTls(
  int handle,
  Pointer<Uint8> cert,
  int certLength,
  Pointer<Uint8> key,
  int keyLength,
);

/// Only checks the two files, as [doxTls] reads them. It needs no server
/// and changes nothing. Returns 0 or the error code of [doxTls].
@Native<Int64 Function(Pointer<Uint8>, Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dox_tls_check',
)
external int doxTlsCheck(
  Pointer<Uint8> cert,
  int certLength,
  Pointer<Uint8> key,
  int keyLength,
);

@Native<Int32 Function(Int64)>(symbol: 'dox_bound_port', isLeaf: true)
external int doxBoundPort(int handle);

/// Writes the answers in [out], reads new requests into [inBuf]. Waits up
/// to [budgetUs] for them. Returns the request bytes, 0 for none (Rust
/// then posts to the port when there are some), or -size when one request
/// needs a bigger [inBuf]. Not a leaf call: it may wait on the sockets.
@Native<
  Int64 Function(Int64, Pointer<Uint8>, Int64, Pointer<Uint8>, Int64, Int64)
>(symbol: 'dox_run')
external int doxRun(
  int handle,
  Pointer<Uint8> out,
  int outLength,
  Pointer<Uint8> inBuf,
  int inCapacity,
  int budgetUs,
);

/// Stop accepting. Open connections close after their current answer,
/// WebSockets get 1001. Rust posts `[2]` to the port when all are closed.
@Native<Void Function(Int64)>(symbol: 'dox_shutdown')
external void doxShutdown(int handle);

@Native<Void Function(Int64)>(symbol: 'dox_unbind')
external void doxUnbind(int handle);

@Native<Pointer<Uint8> Function(Int64)>(symbol: 'dox_alloc', isLeaf: true)
external Pointer<Uint8> doxAlloc(int size);

@Native<Void Function(Pointer<Uint8>, Int64)>(symbol: 'dox_free', isLeaf: true)
external void doxFree(Pointer<Uint8> ptr, int size);

/// Frees an external body (the finalizer of its typed list).
@Native<Void Function(Pointer<Void>)>(symbol: 'dox_free_body', isLeaf: true)
external void doxFreeBody(Pointer<Void> token);

/// AES-256-GCM, see native/src/aead.rs. [out] gets nonce + ciphertext +
/// tag (`plainLen + 28` bytes). Returns 0, -1 for bad arguments, -2 when
/// the system random source fails.
@Native<
  Int32 Function(
    Pointer<Uint8>,
    Pointer<Uint8>,
    Int64,
    Pointer<Uint8>,
    Int64,
    Pointer<Uint8>,
  )
>(symbol: 'dox_aes_gcm_seal', isLeaf: true)
external int doxAesGcmSeal(
  Pointer<Uint8> key,
  Pointer<Uint8> aad,
  int aadLen,
  Pointer<Uint8> plain,
  int plainLen,
  Pointer<Uint8> out,
);

/// The plain text of [data] in [out] (`dataLen - 28` bytes). Returns its
/// length, or -1 when the data was changed or the key or aad is wrong.
@Native<
  Int64 Function(
    Pointer<Uint8>,
    Pointer<Uint8>,
    Int64,
    Pointer<Uint8>,
    Int64,
    Pointer<Uint8>,
  )
>(symbol: 'dox_aes_gcm_open', isLeaf: true)
external int doxAesGcmOpen(
  Pointer<Uint8> key,
  Pointer<Uint8> aad,
  int aadLen,
  Pointer<Uint8> data,
  int dataLen,
  Pointer<Uint8> out,
);
