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

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection};

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
}

impl Tls {
    pub(crate) fn new(conn: ServerConnection) -> Box<Self> {
        Box::new(Tls {
            conn,
            held: 0,
            picked: false,
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

    /// Queues `close_notify` and tries to write it (and what waits before
    /// it). Only for a clean end: a cut connection must not look whole.
    pub(crate) fn close_notify(&mut self, sock: &mut impl Write) {
        self.conn.send_close_notify();
        while self.conn.wants_write() {
            match self.conn.write_tls(sock) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }
}

/// A server config from PEM bytes: a certificate chain and its key.
pub(crate) fn config(cert: &[u8], key: &[u8], http2: bool) -> Result<Arc<ServerConfig>, i64> {
    let certs = CertificateDer::pem_slice_iter(cert)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CERT_BAD)?;
    if certs.is_empty() {
        return Err(CERT_BAD);
    }
    let key = PrivateKeyDer::from_pem_slice(key).map_err(|_| KEY_BAD)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // TLS 1.2 and 1.3, the rustls defaults. No cipher options.
    let mut cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| PAIR_BAD)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|_| PAIR_BAD)?;
    cfg.alpn_protocols = if http2 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"http/1.1".to_vec()]
    };
    Ok(Arc::new(cfg))
}

/// A server config from the two PEM files.
pub(crate) fn load(cert: &Path, key: &Path, http2: bool) -> Result<Arc<ServerConfig>, i64> {
    let cert = std::fs::read(cert).map_err(|_| CERT_READ)?;
    let key = std::fs::read(key).map_err(|_| KEY_READ)?;
    config(&cert, &key, http2)
}

/// The certificate of the TLS servers that use one pair of files.
pub(crate) struct Certs {
    cert: PathBuf,
    key: PathBuf,
    http2: bool,
    now: Mutex<Arc<ServerConfig>>,
}

/// All [Certs] of this process that a server still uses.
static ALL: Mutex<Vec<Weak<Certs>>> = Mutex::new(Vec::new());

impl Certs {
    /// Reads the files. A server that uses the same files gets the same
    /// [Certs], and the new certificate is used for both. On an error
    /// nothing changes.
    pub(crate) fn share(cert: &Path, key: &Path, http2: bool) -> Result<Arc<Certs>, i64> {
        let cfg = load(cert, key, http2)?;
        let mut all = ALL.lock().unwrap();
        all.retain(|w| w.strong_count() > 0);
        let same = all
            .iter()
            .filter_map(Weak::upgrade)
            .find(|c| c.cert == cert && c.key == key && c.http2 == http2);
        if let Some(c) = same {
            *c.now.lock().unwrap() = cfg;
            return Ok(c);
        }
        let c = Arc::new(Certs {
            cert: cert.to_path_buf(),
            key: key.to_path_buf(),
            http2,
            now: Mutex::new(cfg),
        });
        all.push(Arc::downgrade(&c));
        Ok(c)
    }

    /// The TLS state of a new connection, with the current certificate.
    pub(crate) fn accept(&self) -> Option<Box<Tls>> {
        let cfg = self.now.lock().unwrap().clone();
        ServerConnection::new(cfg).ok().map(Tls::new)
    }
}

/// Reads plain text into `buf` (not empty). Reads the socket until rustls
/// has plain text, or until the socket has no more bytes: then
/// `WouldBlock`, as on plain TCP. So the edge of mio is never lost: either
/// bytes come back (the caller reads again), or the socket is empty.
/// Ok(0): the client closed (with or without `close_notify`). A TLS error
/// queues an alert, writes it and fails.
pub(crate) fn read<S: Read + Write>(
    tls: &mut ServerConnection,
    sock: &mut S,
    buf: &mut [u8],
) -> io::Result<usize> {
    loop {
        match tls.reader().read(buf) {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            // the socket closed without `close_notify`: an end, like on TCP
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(0),
            Err(e) => return Err(e),
        }
        // Ok(0) is the end of the socket: the next reader call says so
        tls.read_tls(sock)?;
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
                Ok(_) => {}
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
        assert_eq!(load(&none, &path("key1.pem"), false).err(), Some(CERT_READ));
        assert_eq!(load(&path("cert1.pem"), &none, false).err(), Some(KEY_READ));
        assert_eq!(config(b"junk", KEY1, false).err(), Some(CERT_BAD));
        assert_eq!(config(CERT1, b"junk", false).err(), Some(KEY_BAD));
        // a key that is not the key of the certificate
        assert_eq!(config(CERT1, KEY2, false).err(), Some(PAIR_BAD));
        assert!(config(CERT1, KEY1, true).is_ok());
    }

    #[test]
    fn same_files_share_one_certificate() {
        let (c, k) = (path("cert1.pem"), path("key1.pem"));
        let a = Certs::share(&c, &k, false).unwrap();
        let b = Certs::share(&c, &k, false).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        // another ALPN list is another config
        let h2 = Certs::share(&c, &k, true).unwrap();
        assert!(!Arc::ptr_eq(&a, &h2));
    }
}
