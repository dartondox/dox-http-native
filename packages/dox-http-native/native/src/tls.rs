//! TLS (HTTPS) with rustls and its `ring` crypto provider. Off unless Dart
//! gives a certificate (`dox_tls`). A TLS port takes only TLS.
//!
//! A TLS connection keeps the read and write buffers of a plain one: the
//! loop reads plain text into `rbuf` and writes plain text from `wbuf`
//! (see `Conn::read` and `Conn::flush` in run.rs). Only these two places,
//! and the WebSocket reads, go through rustls. A plain connection has no
//! `Tls` and takes the old path.
//!
//! Certificates: all servers of this process that use the same two files
//! (and the same ALPN list) share one [Certs]. A reload in one isolate
//! thus reaches the servers of all isolates. A new connection takes the
//! certificate of that time, an open one keeps its own.
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{InconsistentKeys, ServerConfig, ServerConnection};

/// Errors of [load], as the FFI returns them.
pub(crate) const CERT_READ: i64 = -1;
pub(crate) const CERT_BAD: i64 = -2;
pub(crate) const KEY_READ: i64 = -3;
pub(crate) const KEY_BAD: i64 = -4;
/// rustls refused the pair (eg. the key is not the key of the certificate)
pub(crate) const PAIR_BAD: i64 = -5;

/// The TLS state of one connection.
pub(crate) struct Tls {
    pub(crate) conn: ServerConnection,
    /// bytes at the front of `wbuf` that rustls took, but whose records
    /// are not all on the socket yet. They stay in `wbuf` (`pending()`)
    /// until they are, so "all written" means the same as on plain TCP.
    pub(crate) held: usize,
    /// the handshake is over and ALPN picked the protocol
    pub(crate) picked: bool,
    /// all TLS bytes written to the socket: progress for the send stall
    /// and idle timers, which `held` shows only per rustls buffer
    pub(crate) sent: u64,
    /// raw bytes [read] may still take from the socket in this turn. The
    /// caller sets it before a turn: records with no plain text (eg.
    /// KeyUpdates) must not hold the loop, as the plain text limits do
    /// not see them.
    pub(crate) left: usize,
    /// [read] stopped at `left`: bytes may wait, queue the connection
    /// again (mio gives no new event for them)
    pub(crate) more: bool,
}

impl Tls {
    pub(crate) fn new(conn: ServerConnection) -> Box<Self> {
        Box::new(Tls {
            conn,
            held: 0,
            picked: false,
            sent: 0,
            left: usize::MAX,
            more: false,
        })
    }

    /// Once, after the handshake: true when ALPN picked `h2`.
    pub(crate) fn pick(&mut self) -> Option<bool> {
        if self.picked || self.conn.is_handshaking() {
            return None;
        }
        self.picked = true;
        Some(self.conn.alpn_protocol() == Some(b"h2"))
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A certificate chain and its key from PEM bytes.
pub(crate) fn certified(cert: &[u8], key: &[u8]) -> Result<Arc<CertifiedKey>, i64> {
    let certs = CertificateDer::pem_slice_iter(cert)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CERT_BAD)?;
    if certs.is_empty() {
        return Err(CERT_BAD);
    }
    let key = PrivateKeyDer::from_pem_slice(key).map_err(|_| KEY_BAD)?;
    let signer = provider()
        .key_provider
        .load_private_key(key)
        .map_err(|_| PAIR_BAD)?;
    let ck = CertifiedKey::new(certs, signer);
    // the same check as `with_single_cert`: a key whose public key rustls
    // can not get passes
    match ck.keys_match() {
        Ok(()) | Err(rustls::Error::InconsistentKeys(InconsistentKeys::Unknown)) => {}
        Err(_) => return Err(PAIR_BAD),
    }
    Ok(Arc::new(ck))
}

/// A certificate chain and its key from the two PEM files.
pub(crate) fn load(cert: &Path, key: &Path) -> Result<Arc<CertifiedKey>, i64> {
    let cert = std::fs::read(cert).map_err(|_| CERT_READ)?;
    let key = std::fs::read(key).map_err(|_| KEY_READ)?;
    certified(&cert, &key)
}

/// A server config that takes its certificate from `keys` at each
/// handshake, so a reload of `keys` reaches it.
pub(crate) fn config(keys: Arc<Keys>, http2: bool) -> Arc<ServerConfig> {
    // TLS 1.2 and 1.3, the rustls defaults. No cipher options.
    let mut cfg = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .expect("ring has TLS 1.2 and 1.3")
        .with_no_client_auth()
        .with_cert_resolver(keys);
    cfg.alpn_protocols = if http2 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"http/1.1".to_vec()]
    };
    Arc::new(cfg)
}

/// The certificate of all TLS servers of this process that use one pair
/// of files, also with other `http2` values (ALPN is in [Certs]).
pub(crate) struct Keys {
    /// the paths as [same_file] gives them: the key in [ALL]
    files: (PathBuf, PathBuf),
    now: Mutex<Arc<CertifiedKey>>,
}

impl Keys {
    /// Keys that are in no list (fuzz).
    #[cfg(fuzzing)]
    pub(crate) fn alone(key: Arc<CertifiedKey>) -> Arc<Keys> {
        Arc::new(Keys {
            files: (PathBuf::new(), PathBuf::new()),
            now: Mutex::new(key),
        })
    }
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys").field("files", &self.files).finish()
    }
}

impl ResolvesServerCert for Keys {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.now.lock().unwrap().clone())
    }
}

/// One path for one file: `a/../cert.pem`, a relative path and an
/// absolute path give the same. The folder is resolved, not the file name:
/// certbot's `live/x/cert.pem` is a link to a new file after each renewal,
/// and it must stay the same key.
fn same_file(p: &Path) -> PathBuf {
    let dir = match p.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    match (dir.canonicalize(), p.file_name()) {
        (Ok(d), Some(name)) => d.join(name),
        _ => p.to_path_buf(),
    }
}

/// All [Keys] of this process that a server still uses.
static ALL: Mutex<Vec<Weak<Keys>>> = Mutex::new(Vec::new());

/// One read and store at a time, so an older read never overwrites a
/// newer one. Not `ALL` and not `Keys::now`: a handshake (`resolve`) never
/// waits for a file read.
static RELOAD: Mutex<()> = Mutex::new(());

/// The TLS config of one server.
pub(crate) struct Certs {
    /// the config holds it too: here only for the tests
    #[cfg(test)]
    pub(crate) keys: Arc<Keys>,
    cfg: Arc<ServerConfig>,
}

impl Certs {
    /// Reads the files. Servers that use the same files share their
    /// [Keys], so this is also the reload for all of them. On an error
    /// nothing changes.
    pub(crate) fn share(cert: &Path, key: &Path, http2: bool) -> Result<Arc<Certs>, i64> {
        let _one = RELOAD.lock().unwrap();
        let now = load(cert, key)?;
        let files = (same_file(cert), same_file(key));
        let mut all = ALL.lock().unwrap();
        all.retain(|w| w.strong_count() > 0);
        let same = all
            .iter()
            .filter_map(Weak::upgrade)
            .find(|k| k.files == files);
        let keys = match same {
            Some(k) => {
                *k.now.lock().unwrap() = now;
                k
            }
            None => {
                let k = Arc::new(Keys {
                    files,
                    now: Mutex::new(now),
                });
                all.push(Arc::downgrade(&k));
                k
            }
        };
        let cfg = config(keys.clone(), http2);
        Ok(Arc::new(Certs {
            #[cfg(test)]
            keys,
            cfg,
        }))
    }

    /// The TLS state of a new connection. It gets the certificate of the
    /// time of its handshake.
    pub(crate) fn accept(&self) -> Option<Box<Tls>> {
        ServerConnection::new(self.cfg.clone()).ok().map(Tls::new)
    }
}

/// Reads plain text into `buf` (not empty). Reads the socket until rustls
/// has plain text, or until the socket has no more bytes: then
/// `WouldBlock`, as on plain TCP. So the edge of mio is never lost: either
/// bytes come back (the caller reads again), or the socket is empty.
/// Ok(0): the client closed (with or without `close_notify`). A TLS error
/// queues an alert, writes it and fails. Over `left` raw bytes it stops
/// with `WouldBlock` and sets `more`.
pub(crate) fn read<S: Read + Write>(
    t: &mut Tls,
    sock: &mut S,
    buf: &mut [u8],
) -> io::Result<usize> {
    let tls = &mut t.conn;
    loop {
        match tls.reader().read(buf) {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            // the socket closed without `close_notify`: an end, like on TCP
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(0),
            Err(e) => return Err(e),
        }
        if t.left == 0 {
            t.more = true;
            return Err(ErrorKind::WouldBlock.into());
        }
        // Ok(0) is the end of the socket: the next reader call says so
        let k = tls.read_tls(sock)?;
        t.left = t.left.saturating_sub(k);
        if let Err(e) = tls.process_new_packets() {
            // eg. plain HTTP on the TLS port: the client gets an alert
            let _ = tls.write_tls(sock);
            return Err(io::Error::new(ErrorKind::InvalidData, e));
        }
    }
}

/// Writes `data` (the unwritten part of `wbuf`) through rustls. rustls
/// takes at most its buffer limit (64 KiB) at a time, and only after all
/// earlier records are on the socket, so its buffer stays small also for
/// a big or streamed answer. Returns how many bytes of `data` are written
/// (all their records are on the socket), or None on a socket error.
pub(crate) fn write<S: Write>(t: &mut Tls, sock: &mut S, data: &[u8]) -> Option<usize> {
    let mut done = 0;
    loop {
        while t.conn.wants_write() {
            match t.conn.write_tls(sock) {
                Ok(0) => return None,
                Ok(k) => t.sent += k as u64,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Some(done),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return None,
            }
        }
        done += std::mem::take(&mut t.held);
        if done == data.len() {
            return Some(done);
        }
        match t.conn.writer().write(&data[done..]) {
            Ok(0) => return Some(done),
            Ok(k) => t.held = k,
            Err(_) => return None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const CA: &[u8] = include_bytes!("../../test/certs/ca.pem");
    pub(crate) const CERT1: &[u8] = include_bytes!("../../test/certs/cert1.pem");
    pub(crate) const KEY1: &[u8] = include_bytes!("../../test/certs/key1.pem");
    pub(crate) const KEY2: &[u8] = include_bytes!("../../test/certs/key2.pem");

    /// A path in `test/certs`.
    pub(crate) fn path(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test/certs")
            .join(name)
    }

    #[test]
    fn bad_files_fail() {
        let none = path("none.pem");
        assert_eq!(load(&none, &path("key1.pem")).err(), Some(CERT_READ));
        assert_eq!(load(&path("cert1.pem"), &none).err(), Some(KEY_READ));
        assert_eq!(certified(b"junk", KEY1).err(), Some(CERT_BAD));
        assert_eq!(certified(CERT1, b"junk").err(), Some(KEY_BAD));
        // a key that is not the key of the certificate
        assert_eq!(certified(CERT1, KEY2).err(), Some(PAIR_BAD));
        assert!(certified(CERT1, KEY1).is_ok());
    }

    /// Same files share one [Keys]: also with another `http2` value, and
    /// with a relative path that goes another way to the same file.
    #[test]
    fn same_files_share_one_certificate() {
        let (c, k) = (path("cert1.pem"), path("key1.pem"));
        let a = Certs::share(&c, &k, false).unwrap();
        let b = Certs::share(&c, &k, true).unwrap();
        assert!(Arc::ptr_eq(&a.keys, &b.keys));
        let cwd = std::env::current_dir().unwrap();
        let rel = |p: &Path| {
            let up = cwd.components().count() - 1;
            let mut r = PathBuf::from("../".repeat(up));
            r.push(p.strip_prefix("/").unwrap());
            r
        };
        let d = Certs::share(&rel(&c), &rel(&k), false).unwrap();
        assert!(Arc::ptr_eq(&a.keys, &d.keys));
    }
}
