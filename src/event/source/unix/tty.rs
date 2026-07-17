#[cfg(feature = "libc")]
use std::os::unix::prelude::AsRawFd;
use std::{collections::VecDeque, io, os::unix::net::UnixStream, time::Duration};

#[cfg(not(feature = "libc"))]
use rustix::fd::{AsFd, AsRawFd};

use signal_hook::low_level::pipe;

use crate::event::timeout::PollTimeout;
use crate::event::Event;
use filedescriptor::{poll, pollfd, POLLIN};

#[cfg(feature = "event-stream")]
use crate::event::sys::Waker;
use crate::event::{
    source::EventSource,
    sys::unix::parse::{osc_kind, osc_terminated, parse_event, OscKind, MAX_OSC_SEQUENCE_LEN},
    InternalEvent,
};
use crate::terminal::sys::file_descriptor::{tty_input_fd, FileDesc, NonblockingGuard};

/// Holds a prototypical Waker and a receiver we can wait on when doing select().
#[cfg(feature = "event-stream")]
struct WakePipe {
    receiver: UnixStream,
    waker: Waker,
}

#[cfg(feature = "event-stream")]
impl WakePipe {
    fn new() -> io::Result<Self> {
        let (receiver, sender) = nonblocking_unix_pair()?;
        Ok(WakePipe {
            receiver,
            waker: Waker::new(sender),
        })
    }
}

// I (@zrzka) wasn't able to read more than 1_022 bytes when testing
// reading on macOS/Linux -> we don't need bigger buffer and 1k of bytes
// is enough.
const TTY_BUFFER_SIZE: usize = 1_024;

pub(crate) struct UnixInternalEventSource {
    parser: Parser,
    tty_buffer: [u8; TTY_BUFFER_SIZE],
    tty: FileDesc<'static>,
    winch_signal_receiver: UnixStream,
    pending_resize: bool,
    #[cfg(feature = "event-stream")]
    wake_pipe: WakePipe,
}

enum ResizeQuery {
    Bounded,
    Unbounded,
}

fn nonblocking_unix_pair() -> io::Result<(UnixStream, UnixStream)> {
    let (receiver, sender) = UnixStream::pair()?;
    receiver.set_nonblocking(true)?;
    sender.set_nonblocking(true)?;
    Ok((receiver, sender))
}

impl UnixInternalEventSource {
    pub fn new() -> io::Result<Self> {
        UnixInternalEventSource::from_file_descriptor(tty_input_fd()?)
    }

    pub(crate) fn from_file_descriptor(input_fd: FileDesc<'static>) -> io::Result<Self> {
        Ok(UnixInternalEventSource {
            parser: Parser::default(),
            tty_buffer: [0u8; TTY_BUFFER_SIZE],
            tty: input_fd,
            winch_signal_receiver: {
                let (receiver, sender) = nonblocking_unix_pair()?;
                // Unregistering is unnecessary because EventSource is a singleton
                #[cfg(feature = "libc")]
                pipe::register(libc::SIGWINCH, sender)?;
                #[cfg(not(feature = "libc"))]
                pipe::register(rustix::process::Signal::Winch as i32, sender)?;
                receiver
            },
            pending_resize: false,
            #[cfg(feature = "event-stream")]
            wake_pipe: WakePipe::new()?,
        })
    }

    fn read_resize(&mut self, query: ResizeQuery) -> io::Result<Option<InternalEvent>> {
        let (columns, rows) = match query {
            ResizeQuery::Bounded => match crate::terminal::window_size() {
                Ok(size) => (size.columns, size.rows),
                Err(_) => {
                    self.pending_resize = true;
                    return Ok(None);
                }
            },
            ResizeQuery::Unbounded => crate::terminal::size()?,
        };

        self.pending_resize = false;
        Ok(Some(InternalEvent::Event(Event::Resize(columns, rows))))
    }
}

/// read_complete reads from a non-blocking file descriptor
/// until the buffer is full or it would block.
///
/// Similar to `std::io::Read::read_to_end`, except this function
/// only fills the given buffer and does not read beyond that.
fn read_complete(fd: &FileDesc, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match fd.read(buf) {
            Ok(x) => return Ok(x),
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock => return Ok(0),
                io::ErrorKind::Interrupted => continue,
                _ => return Err(e),
            },
        }
    }
}

impl EventSource for UnixInternalEventSource {
    fn try_read(&mut self, timeout: Option<Duration>) -> io::Result<Option<InternalEvent>> {
        if let Some(event) = self.parser.next() {
            return Ok(Some(event));
        }

        if self.pending_resize {
            let query = if timeout.is_some() {
                ResizeQuery::Bounded
            } else {
                ResizeQuery::Unbounded
            };
            if let Some(event) = self.read_resize(query)? {
                return Ok(Some(event));
            }
        }

        let bounded = timeout.is_some();
        let timeout = PollTimeout::new(timeout);

        fn make_pollfd<F: AsRawFd>(fd: &F) -> pollfd {
            pollfd {
                fd: fd.as_raw_fd(),
                events: POLLIN,
                revents: 0,
            }
        }

        #[cfg(not(feature = "event-stream"))]
        let mut fds = [
            make_pollfd(&self.tty),
            make_pollfd(&self.winch_signal_receiver),
        ];

        #[cfg(feature = "event-stream")]
        let mut fds = [
            make_pollfd(&self.tty),
            make_pollfd(&self.winch_signal_receiver),
            make_pollfd(&self.wake_pipe.receiver),
        ];

        while timeout.leftover().map_or(true, |t| !t.is_zero()) {
            // check if there are buffered events from the last read
            if let Some(event) = self.parser.next() {
                return Ok(Some(event));
            }
            match poll(&mut fds, timeout.leftover()) {
                Err(filedescriptor::Error::Poll(e)) | Err(filedescriptor::Error::Io(e)) => {
                    match e.kind() {
                        // retry on EINTR
                        io::ErrorKind::Interrupted => continue,
                        _ => return Err(e),
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("got unexpected error while polling: {:?}", e),
                    ))
                }
                Ok(_) => (),
            };
            if fds[0].revents & POLLIN != 0 {
                // Another reader can consume the bytes after `poll` reports readiness. Keep the
                // descriptor nonblocking only for this read so a stale notification cannot hang
                // a bounded poll and the terminal's original flags remain intact.
                let read_result = {
                    let _nonblocking = NonblockingGuard::new(&self.tty)?;
                    self.tty.read(&mut self.tty_buffer)
                };
                let read_count = match read_result {
                    Ok(read_count) => read_count,
                    Err(e)
                        if e.kind() == io::ErrorKind::WouldBlock
                            || e.kind() == io::ErrorKind::Interrupted =>
                    {
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                if read_count > 0 {
                    self.parser.advance(
                        &self.tty_buffer[..read_count],
                        read_count == TTY_BUFFER_SIZE,
                    );
                }

                if let Some(event) = self.parser.next() {
                    return Ok(Some(event));
                }
            }
            if fds[1].revents & POLLIN != 0 {
                #[cfg(feature = "libc")]
                let fd = FileDesc::new(self.winch_signal_receiver.as_raw_fd(), false);
                #[cfg(not(feature = "libc"))]
                let fd = FileDesc::Borrowed(self.winch_signal_receiver.as_fd());
                // drain the pipe
                while read_complete(&fd, &mut [0; 1024])? != 0 {}
                let query = if bounded {
                    ResizeQuery::Bounded
                } else {
                    ResizeQuery::Unbounded
                };
                if let Some(event) = self.read_resize(query)? {
                    return Ok(Some(event));
                }
                continue;
            }

            #[cfg(feature = "event-stream")]
            if fds[2].revents & POLLIN != 0 {
                #[cfg(feature = "libc")]
                let fd = FileDesc::new(self.wake_pipe.receiver.as_raw_fd(), false);
                #[cfg(not(feature = "libc"))]
                let fd = FileDesc::Borrowed(self.wake_pipe.receiver.as_fd());
                // drain the pipe
                while read_complete(&fd, &mut [0; 1024])? != 0 {}

                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "Poll operation was woken up by `Waker::wake`",
                ));
            }
        }
        Ok(None)
    }

    fn clear(&mut self) {
        self.parser.buffer.clear();
        self.parser.internal_events.clear();
        self.parser.discarding_osc = None;
        self.parser.discarding_osc_esc = false;
        self.pending_resize = false;

        #[cfg(feature = "libc")]
        let fd = FileDesc::new(self.winch_signal_receiver.as_raw_fd(), false);
        #[cfg(not(feature = "libc"))]
        let fd = FileDesc::Borrowed(self.winch_signal_receiver.as_fd());
        while matches!(read_complete(&fd, &mut [0; 1024]), Ok(read_count) if read_count != 0) {}
    }

    #[cfg(feature = "event-stream")]
    fn waker(&self) -> Waker {
        self.wake_pipe.waker.clone()
    }
}

//
// Following `Parser` structure exists for two reasons:
//
//  * mimic anes Parser interface
//  * move the advancing, parsing, ... stuff out of the `try_read` method
//
#[derive(Debug)]
struct Parser {
    buffer: Vec<u8>,
    internal_events: VecDeque<InternalEvent>,
    discarding_osc: Option<OscKind>,
    discarding_osc_esc: bool,
}

impl Default for Parser {
    fn default() -> Self {
        Parser {
            // This buffer is used for -> 1 <- ANSI escape sequence. Are we
            // aware of any ANSI escape sequence that is bigger? Can we make
            // it smaller?
            //
            // Probably not worth spending more time on this as "there's a plan"
            // to use the anes crate parser.
            buffer: Vec::with_capacity(256),
            // TTY_BUFFER_SIZE is 1_024 bytes. How many ANSI escape sequences can
            // fit? What is an average sequence length? Let's guess here
            // and say that the average ANSI escape sequence length is 8 bytes. Thus
            // the buffer size should be 1024/8=128 to avoid additional allocations
            // when processing large amounts of data.
            //
            // There's no need to make it bigger, because when you look at the `try_read`
            // method implementation, all events are consumed before the next TTY_BUFFER
            // is processed -> events pushed.
            internal_events: VecDeque::with_capacity(128),
            discarding_osc: None,
            discarding_osc_esc: false,
        }
    }
}

impl Parser {
    fn advance(&mut self, buffer: &[u8], more: bool) {
        for (idx, byte) in buffer.iter().enumerate() {
            let more = idx + 1 < buffer.len() || more;

            if let Some(kind) = self.discarding_osc {
                let terminated = *byte == b'\x07'
                    || matches!(kind, OscKind::C1) && *byte == 0x9C
                    || self.discarding_osc_esc && *byte == b'\\';
                self.discarding_osc_esc = *byte == b'\x1B';
                if terminated {
                    self.discarding_osc = None;
                    self.discarding_osc_esc = false;
                }
                continue;
            }

            self.buffer.push(*byte);

            if let Some(kind) = osc_kind(&self.buffer) {
                if self.buffer.len() >= MAX_OSC_SEQUENCE_LEN && !osc_terminated(&self.buffer, kind)
                {
                    self.discarding_osc = Some(kind);
                    self.discarding_osc_esc = self.buffer.ends_with(b"\x1B");
                    self.buffer.clear();
                    continue;
                }
                if !osc_terminated(&self.buffer, kind) {
                    continue;
                }
            }

            match parse_event(&self.buffer, more) {
                Ok(Some(ie)) => {
                    self.internal_events.push_back(ie);
                    self.buffer.clear();
                }
                Ok(None) => {
                    // Event can't be parsed, because we don't have enough bytes for
                    // the current sequence. Keep the buffer and process next bytes.
                }
                Err(_) => {
                    // Event can't be parsed (not enough parameters, parameter is not a number, ...).
                    // Clear the buffer and continue with another sequence.
                    self.buffer.clear();
                }
            }
        }
    }
}

impl Iterator for Parser {
    type Item = InternalEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.internal_events.pop_front()
    }
}

#[cfg(test)]
#[path = "bounded_read_tests.rs"]
mod tests;
