use std::time::Duration;
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

use may::net::TcpStream;
#[cfg(unix)]
use may::os::unix::net::UnixStream;

/// this is just a wrapper to unify TcpStream and UnixStream
#[derive(Debug)]
pub enum CoSocket {
    Tcp(TcpStream),
    #[cfg(unix)] Unix(UnixStream),
}

use self::CoSocket::*;

impl CoSocket {
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match *self {
            Tcp(ref s) => s.set_read_timeout(timeout),
            #[cfg(unix)]
            Unix(ref s) => s.set_read_timeout(timeout),
        }
    }

    pub fn set_nonblocking(&self, nonblock: bool) -> io::Result<()> {
        match *self {
            Tcp(ref s) => s.set_nonblocking(nonblock),
            #[cfg(unix)]
            Unix(ref s) => s.set_nonblocking(nonblock),
        }
    }
}

impl Read for CoSocket {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Tcp(ref mut s) => s.read(buf),
            #[cfg(unix)]
            Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for CoSocket {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Tcp(ref mut s) => s.write(buf),
            #[cfg(unix)]
            Unix(ref mut s) => s.write(buf),
        }
    }
    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Tcp(ref mut s) => s.flush(),
            #[cfg(unix)]
            Unix(ref mut s) => s.flush(),
        }
    }
}

#[cfg(unix)]
impl AsRawFd for CoSocket {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        match *self {
            Tcp(ref s) => s.as_raw_fd(),
            Unix(ref s) => s.as_raw_fd(),
        }
    }
}

#[cfg(windows)]
impl AsRawSocket for CoSocket {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        match *self {
            Tcp(ref s) => s.as_raw_socket(),
        }
    }
}

impl From<TcpStream> for CoSocket {
    fn from(s: TcpStream) -> Self {
        Tcp(s)
    }
}

#[cfg(unix)]
impl From<UnixStream> for CoSocket {
    fn from(s: UnixStream) -> Self {
        Unix(s)
    }
}
