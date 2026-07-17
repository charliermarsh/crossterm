use std::io;

#[cfg(feature = "libc")]
use libc::size_t;
#[cfg(not(feature = "libc"))]
use rustix::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
#[cfg(feature = "libc")]
use std::{
    fs,
    marker::PhantomData,
    os::unix::{
        io::{IntoRawFd, RawFd},
        prelude::AsRawFd,
    },
};

/// A file descriptor wrapper.
///
/// It allows to retrieve raw file descriptor, write to the file descriptor and
/// mainly it closes the file descriptor once dropped.
#[derive(Debug)]
#[cfg(feature = "libc")]
pub struct FileDesc<'a> {
    fd: RawFd,
    close_on_drop: bool,
    phantom: PhantomData<&'a ()>,
}

#[cfg(not(feature = "libc"))]
pub enum FileDesc<'a> {
    Owned(OwnedFd),
    Borrowed(BorrowedFd<'a>),
}

pub(crate) struct NonblockingGuard<'a, 'fd> {
    fd: &'a FileDesc<'fd>,
    #[cfg(feature = "libc")]
    original_flags: libc::c_int,
    #[cfg(not(feature = "libc"))]
    original_flags: rustix::fs::OFlags,
    changed: bool,
}

impl<'a, 'fd> NonblockingGuard<'a, 'fd> {
    pub(crate) fn new(fd: &'a FileDesc<'fd>) -> io::Result<Self> {
        #[cfg(feature = "libc")]
        let original_flags = {
            // Safety: F_GETFL only inspects the supplied file descriptor.
            let flags = unsafe { libc::fcntl(fd.raw_fd(), libc::F_GETFL) };
            if flags < 0 {
                return Err(io::Error::last_os_error());
            }
            flags
        };
        #[cfg(not(feature = "libc"))]
        let original_flags = rustix::fs::fcntl_getfl(fd)?;

        #[cfg(feature = "libc")]
        let changed = original_flags & libc::O_NONBLOCK == 0;
        #[cfg(not(feature = "libc"))]
        let changed = !original_flags.contains(rustix::fs::OFlags::NONBLOCK);

        if changed {
            #[cfg(feature = "libc")]
            {
                // Safety: F_SETFL updates the supplied file descriptor's status flags.
                if unsafe {
                    libc::fcntl(
                        fd.raw_fd(),
                        libc::F_SETFL,
                        original_flags | libc::O_NONBLOCK,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            #[cfg(not(feature = "libc"))]
            rustix::fs::fcntl_setfl(fd, original_flags | rustix::fs::OFlags::NONBLOCK)?;
        }

        Ok(Self {
            fd,
            original_flags,
            changed,
        })
    }
}

impl Drop for NonblockingGuard<'_, '_> {
    fn drop(&mut self) {
        if !self.changed {
            return;
        }

        #[cfg(feature = "libc")]
        // Safety: F_SETFL restores the status flags saved for this descriptor.
        unsafe {
            libc::fcntl(self.fd.raw_fd(), libc::F_SETFL, self.original_flags);
        }
        #[cfg(not(feature = "libc"))]
        let _ = rustix::fs::fcntl_setfl(self.fd, self.original_flags);
    }
}

#[cfg(feature = "libc")]
impl FileDesc<'_> {
    /// Constructs a new `FileDesc` with the given `RawFd`.
    ///
    /// # Arguments
    ///
    /// * `fd` - raw file descriptor
    /// * `close_on_drop` - specify if the raw file descriptor should be closed once the `FileDesc` is dropped
    pub fn new(fd: RawFd, close_on_drop: bool) -> FileDesc<'static> {
        FileDesc {
            fd,
            close_on_drop,
            phantom: PhantomData,
        }
    }

    pub fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        let result = unsafe {
            libc::read(
                self.fd,
                buffer.as_mut_ptr() as *mut libc::c_void,
                buffer.len() as size_t,
            )
        };

        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    /// Returns the underlying file descriptor.
    pub fn raw_fd(&self) -> RawFd {
        self.fd
    }
}

#[cfg(not(feature = "libc"))]
impl FileDesc<'_> {
    pub fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        let fd = match self {
            FileDesc::Owned(fd) => fd.as_fd(),
            FileDesc::Borrowed(fd) => fd.as_fd(),
        };
        let result = rustix::io::read(fd, buffer)?;
        Ok(result)
    }

    pub fn raw_fd(&self) -> RawFd {
        match self {
            FileDesc::Owned(fd) => fd.as_raw_fd(),
            FileDesc::Borrowed(fd) => fd.as_raw_fd(),
        }
    }
}

#[cfg(feature = "libc")]
impl Drop for FileDesc<'_> {
    fn drop(&mut self) {
        if self.close_on_drop {
            // Note that errors are ignored when closing a file descriptor. The
            // reason for this is that if an error occurs we don't actually know if
            // the file descriptor was closed or not, and if we retried (for
            // something like EINTR), we might close another valid file descriptor
            // opened after we closed ours.
            let _ = unsafe { libc::close(self.fd) };
        }
    }
}

impl AsRawFd for FileDesc<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.raw_fd()
    }
}

impl io::Write for &FileDesc<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        #[cfg(feature = "libc")]
        {
            // Safety: `buffer` is valid for reads of `buffer.len()` bytes.
            let result = unsafe {
                libc::write(
                    self.raw_fd(),
                    buffer.as_ptr().cast::<libc::c_void>(),
                    buffer.len(),
                )
            };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(result as usize)
        }
        #[cfg(not(feature = "libc"))]
        Ok(rustix::io::write(*self, buffer)?)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(not(feature = "libc"))]
impl AsFd for FileDesc<'_> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            FileDesc::Owned(fd) => fd.as_fd(),
            FileDesc::Borrowed(fd) => fd.as_fd(),
        }
    }
}

#[cfg(feature = "libc")]
/// Creates a file descriptor pointing to the standard input or `/dev/tty`.
pub fn tty_fd() -> io::Result<FileDesc<'static>> {
    let (fd, close_on_drop) = if unsafe { libc::isatty(libc::STDIN_FILENO) == 1 } {
        (libc::STDIN_FILENO, false)
    } else {
        (
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/tty")?
                .into_raw_fd(),
            true,
        )
    };

    Ok(FileDesc::new(fd, close_on_drop))
}

#[cfg(not(feature = "libc"))]
/// Creates a file descriptor pointing to the standard input or `/dev/tty`.
pub fn tty_fd() -> io::Result<FileDesc<'static>> {
    use std::fs::File;

    let stdin = rustix::stdio::stdin();
    let fd = if rustix::termios::isatty(stdin) {
        FileDesc::Borrowed(stdin)
    } else {
        let dev_tty = File::options().read(true).write(true).open("/dev/tty")?;
        FileDesc::Owned(dev_tty.into())
    };
    Ok(fd)
}

#[cfg(feature = "libc")]
/// Creates an independent descriptor for the terminal that supplies input when available.
pub(crate) fn tty_input_fd() -> io::Result<FileDesc<'static>> {
    if let Some(path) = tty_input_path() {
        if let Ok(file) = fs::OpenOptions::new().read(true).open(path) {
            return Ok(FileDesc::new(file.into_raw_fd(), true));
        }
    }
    tty_fd()
}

#[cfg(not(feature = "libc"))]
/// Creates an independent descriptor for the terminal that supplies input when available.
pub(crate) fn tty_input_fd() -> io::Result<FileDesc<'static>> {
    use std::fs::File;

    if let Some(path) = tty_input_path() {
        if let Ok(file) = File::options().read(true).open(path) {
            return Ok(FileDesc::Owned(file.into()));
        }
    }
    tty_fd()
}

#[cfg(target_os = "fuchsia")]
fn tty_input_path() -> Option<std::path::PathBuf> {
    None
}

#[cfg(not(target_os = "fuchsia"))]
fn tty_input_path() -> Option<std::path::PathBuf> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let stdin = rustix::stdio::stdin();
    if rustix::termios::isatty(stdin) {
        let name = rustix::termios::ttyname(stdin, Vec::new()).ok()?;
        Some(OsStr::from_bytes(name.as_bytes()).into())
    } else {
        Some("/dev/tty".into())
    }
}
