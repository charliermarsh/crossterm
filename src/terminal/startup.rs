use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::time::{Duration, Instant};

use crate::event::{
    filter::{CursorPositionFilter, TerminalStartupProbeFilter},
    read::InternalEventReader,
    try_lock_internal_event_reader_for, InternalEvent, OscColorPayload,
};
use crate::style::Color;
use crate::terminal::disable_raw_mode;
use crate::terminal::sys::file_descriptor::{FileDesc, NonblockingGuard};

struct RawModeGuard {
    active: bool,
}

impl RawModeGuard {
    fn enable() -> io::Result<Self> {
        Ok(Self {
            active: crate::terminal::sys::enable_raw_mode_if_needed()?,
        })
    }

    fn restore(mut self) -> io::Result<()> {
        if self.active {
            disable_raw_mode()?;
            self.active = false;
        }
        Ok(())
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = disable_raw_mode();
        }
    }
}

/// Controls whether the terminal startup probe queries keyboard enhancement support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyboardEnhancementProbe {
    /// Query keyboard enhancement support and its primary-device-attributes fallback.
    Query,
    /// Do not query keyboard enhancement support.
    Skip,
}

/// Responses collected by [`query_terminal_startup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalStartupProbe {
    /// Zero-based cursor position (`column`, `row`).
    pub cursor_position: Option<(u16, u16)>,
    /// Default foreground color reported with OSC 10.
    pub foreground_color: Option<Color>,
    /// Default background color reported with OSC 11.
    pub background_color: Option<Color>,
    /// Whether the terminal reports keyboard enhancement support.
    pub keyboard_enhancement_supported: Option<bool>,
}

#[derive(Debug, Default)]
struct ProbeState {
    cursor_position: Option<(u16, u16)>,
    foreground_color: Option<Option<Color>>,
    background_color: Option<Option<Color>>,
    saw_keyboard_enhancement_flags: bool,
    saw_primary_device_attributes: bool,
}

impl ProbeState {
    fn record(&mut self, event: InternalEvent) {
        match event {
            InternalEvent::CursorPosition(x, y) | InternalEvent::ExtendedCursorPosition(x, y) => {
                self.cursor_position = Some((x, y));
            }
            InternalEvent::OscColor { slot: 10, payload } => {
                if self.foreground_color.is_none() {
                    self.foreground_color = Some(color_from_payload(payload));
                }
            }
            InternalEvent::OscColor { slot: 11, payload } => {
                if self.background_color.is_none() {
                    self.background_color = Some(color_from_payload(payload));
                }
            }
            InternalEvent::KeyboardEnhancementFlags(_) => {
                self.saw_keyboard_enhancement_flags = true;
            }
            InternalEvent::PrimaryDeviceAttributes => {
                self.saw_primary_device_attributes = true;
            }
            InternalEvent::Event(_)
            | InternalEvent::CursorPositionOrF3(_, _, _)
            | InternalEvent::OscColor { .. } => {}
        }
    }

    fn is_complete(&self, keyboard_probe: KeyboardEnhancementProbe) -> bool {
        self.cursor_position.is_some()
            && self.foreground_color.is_some()
            && self.background_color.is_some()
            && (keyboard_probe == KeyboardEnhancementProbe::Skip
                || self.saw_primary_device_attributes)
    }

    fn only_cursor_is_missing(&self, keyboard_probe: KeyboardEnhancementProbe) -> bool {
        self.cursor_position.is_none()
            && self.foreground_color.is_some()
            && self.background_color.is_some()
            && (keyboard_probe == KeyboardEnhancementProbe::Skip
                || self.saw_primary_device_attributes)
    }

    fn into_probe(self) -> TerminalStartupProbe {
        TerminalStartupProbe {
            cursor_position: self.cursor_position,
            foreground_color: self.foreground_color.flatten(),
            background_color: self.background_color.flatten(),
            keyboard_enhancement_supported: self
                .saw_keyboard_enhancement_flags
                .then_some(true)
                .or_else(|| self.saw_primary_device_attributes.then_some(false)),
        }
    }
}

fn color_from_payload(payload: OscColorPayload) -> Option<Color> {
    match payload {
        OscColorPayload::Rgb { r, g, b } => Some(Color::Rgb { r, g, b }),
        OscColorPayload::Unrecognized(_) => None,
    }
}

/// Query the cursor position, default colors, and optional keyboard support under one terminal-I/O
/// deadline.
///
/// The query uses crossterm's internal event reader, so key presses, bracketed pastes, and other
/// non-query events remain queued for the next [`crate::event::read`] or event stream poll.
/// The timeout bounds reader-lock acquisition, query I/O retries, and response polling under a
/// single deadline. First-use event-source initialization and terminal mode setup or restoration
/// use synchronous platform APIs and are not interruptible by this timeout.
/// Callers must flush any buffered stdout output before probing; the query writes directly to the
/// terminal descriptor so a contended stdout mutex cannot extend the deadline.
/// When keyboard support is queried, primary device attributes complete the unsupported fallback
/// immediately. If enhancement flags arrive first, the fallback is drained before returning.
/// Terminals that do not implement the extended cursor-position request receive one standard CPR
/// retry within the same deadline. Already-queued modified-F3 input is preserved before the retry;
/// a first-row CPR response that collides with the legacy F3 encoding is consumed exactly once.
pub fn query_terminal_startup(
    timeout: Duration,
    keyboard_probe: KeyboardEnhancementProbe,
) -> io::Result<TerminalStartupProbe> {
    let started_at = Instant::now();
    let Some(mut reader) = try_lock_internal_event_reader_for(timeout) else {
        return Ok(ProbeState::default().into_probe());
    };
    if remaining_timeout(started_at, timeout).is_zero() {
        return Ok(ProbeState::default().into_probe());
    }

    let raw_mode_guard = RawModeGuard::enable()?;
    let result = query_terminal_startup_raw(timeout, keyboard_probe, started_at, &mut reader);
    raw_mode_guard.restore()?;
    result
}

fn query_terminal_startup_raw(
    timeout: Duration,
    keyboard_probe: KeyboardEnhancementProbe,
    started_at: Instant,
    reader: &mut InternalEventReader,
) -> io::Result<TerminalStartupProbe> {
    if remaining_timeout(started_at, timeout).is_zero() {
        return Ok(ProbeState::default().into_probe());
    }

    let query: &[u8] = match keyboard_probe {
        KeyboardEnhancementProbe::Query => b"\x1B]10;?\x1B\\\x1B]11;?\x1B\\\x1B[?u\x1B[c\x1B[?6n",
        KeyboardEnhancementProbe::Skip => b"\x1B]10;?\x1B\\\x1B]11;?\x1B\\\x1B[?6n",
    };
    send_query(query, started_at, timeout)?;

    let filter = TerminalStartupProbeFilter {
        query_keyboard: keyboard_probe == KeyboardEnhancementProbe::Query,
    };
    let mut state = ProbeState::default();
    let mut standard_cursor_attempted = false;
    let standard_cursor_after = timeout / 2;
    loop {
        if state.is_complete(keyboard_probe) {
            while reader.poll(Some(Duration::ZERO), &filter)? {
                state.record(reader.read(&filter)?);
            }
            return Ok(state.into_probe());
        }

        let remaining = remaining_timeout(started_at, timeout);
        if remaining.is_zero() {
            return Ok(state.into_probe());
        }

        if !standard_cursor_attempted
            && state.cursor_position.is_none()
            && (state.only_cursor_is_missing(keyboard_probe)
                || started_at.elapsed() >= standard_cursor_after)
        {
            standard_cursor_attempted = true;
            if let Some(position) = query_standard_cursor_position(reader, started_at, timeout)? {
                state.cursor_position = Some(position);
            }
            continue;
        }

        let poll_timeout = if !standard_cursor_attempted && state.cursor_position.is_none() {
            remaining.min(standard_cursor_after.saturating_sub(started_at.elapsed()))
        } else {
            remaining
        };
        if poll_timeout.is_zero() || !reader.poll(Some(poll_timeout), &filter)? {
            continue;
        }
        state.record(reader.read(&filter)?);
    }
}

fn query_standard_cursor_position(
    reader: &mut InternalEventReader,
    started_at: Instant,
    timeout: Duration,
) -> io::Result<Option<(u16, u16)>> {
    let queued_events = reader.take_queued_events();
    let result = (|| {
        send_query(b"\x1B[6n", started_at, timeout)?;
        let remaining = remaining_timeout(started_at, timeout);
        if remaining.is_zero() || !reader.poll(Some(remaining), &CursorPositionFilter)? {
            return Ok(None);
        }
        match reader.read(&CursorPositionFilter)? {
            InternalEvent::CursorPosition(x, y) | InternalEvent::CursorPositionOrF3(x, y, _) => {
                Ok(Some((x, y)))
            }
            _ => unreachable!("cursor-position filter returned a non-cursor event"),
        }
    })();
    reader.prepend_queued_events(queued_events);
    result
}

fn remaining_timeout(started_at: Instant, timeout: Duration) -> Duration {
    timeout.saturating_sub(started_at.elapsed())
}

fn send_query(query: &[u8], started_at: Instant, timeout: Duration) -> io::Result<()> {
    if remaining_timeout(started_at, timeout).is_zero() {
        return Err(query_write_timeout());
    }

    #[cfg(feature = "libc")]
    // Safety: `isatty` only inspects the supplied file descriptor.
    let stdin_is_terminal = unsafe { libc::isatty(libc::STDIN_FILENO) == 1 };
    #[cfg(not(feature = "libc"))]
    let stdin_is_terminal = rustix::termios::isatty(rustix::stdio::stdin());

    if stdin_is_terminal {
        #[cfg(feature = "libc")]
        let stdout_fd = FileDesc::new(libc::STDOUT_FILENO, false);
        #[cfg(not(feature = "libc"))]
        let stdout_fd = FileDesc::Borrowed(rustix::stdio::stdout());
        let _nonblocking = NonblockingGuard::new(&stdout_fd)?;
        let mut stdout = &stdout_fd;
        return write_query(query, &mut stdout, started_at, timeout);
    }

    #[cfg(feature = "libc")]
    let nonblocking = libc::O_NONBLOCK;
    #[cfg(not(feature = "libc"))]
    let nonblocking = rustix::fs::OFlags::NONBLOCK.bits() as i32;

    let mut tty = OpenOptions::new()
        .write(true)
        .custom_flags(nonblocking)
        .open("/dev/tty")?;
    write_query(query, &mut tty, started_at, timeout)
}

fn write_query(
    mut query: &[u8],
    writer: &mut impl Write,
    started_at: Instant,
    timeout: Duration,
) -> io::Result<()> {
    while !query.is_empty() {
        if remaining_timeout(started_at, timeout).is_zero() {
            return Err(query_write_timeout());
        }
        match writer.write(query) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write the terminal startup query",
                ));
            }
            Ok(written) => query = &query[written..],
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted =>
            {
                wait_for_query_write(started_at, timeout)?;
            }
            Err(err) => return Err(err),
        }
    }

    loop {
        if remaining_timeout(started_at, timeout).is_zero() {
            return Err(query_write_timeout());
        }
        match writer.flush() {
            Ok(()) => return Ok(()),
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted =>
            {
                wait_for_query_write(started_at, timeout)?;
            }
            Err(err) => return Err(err),
        }
    }
}

fn wait_for_query_write(started_at: Instant, timeout: Duration) -> io::Result<()> {
    let remaining = remaining_timeout(started_at, timeout);
    if remaining.is_zero() {
        return Err(query_write_timeout());
    }
    std::thread::sleep(remaining.min(Duration::from_millis(1)));
    Ok(())
}

fn query_write_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out while writing the terminal startup query",
    )
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::time::{Duration, Instant};

    use super::{
        query_terminal_startup, remaining_timeout, write_query, KeyboardEnhancementProbe,
        ProbeState, TerminalStartupProbe,
    };
    use crate::event::{InternalEvent, KeyModifiers, KeyboardEnhancementFlags, OscColorPayload};
    use crate::style::Color;

    #[test]
    fn records_out_of_order_startup_responses() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::OscColor {
            slot: 11,
            payload: OscColorPayload::Rgb { r: 1, g: 2, b: 3 },
        });
        state.record(InternalEvent::PrimaryDeviceAttributes);
        state.record(InternalEvent::ExtendedCursorPosition(9, 19));
        state.record(InternalEvent::OscColor {
            slot: 10,
            payload: OscColorPayload::Rgb { r: 4, g: 5, b: 6 },
        });
        state.record(InternalEvent::KeyboardEnhancementFlags(
            KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES,
        ));

        assert!(state.is_complete(KeyboardEnhancementProbe::Query));
        assert_eq!(
            state.into_probe(),
            TerminalStartupProbe {
                cursor_position: Some((9, 19)),
                foreground_color: Some(Color::Rgb { r: 4, g: 5, b: 6 }),
                background_color: Some(Color::Rgb { r: 1, g: 2, b: 3 }),
                keyboard_enhancement_supported: Some(true),
            }
        );
    }

    #[test]
    fn treats_unrecognized_colors_as_responses() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::ExtendedCursorPosition(0, 0));
        state.record(InternalEvent::OscColor {
            slot: 10,
            payload: OscColorPayload::Unrecognized("?".to_string()),
        });
        state.record(InternalEvent::OscColor {
            slot: 11,
            payload: OscColorPayload::Unrecognized("?".to_string()),
        });

        assert!(state.is_complete(KeyboardEnhancementProbe::Skip));
        assert_eq!(
            state.into_probe(),
            TerminalStartupProbe {
                cursor_position: Some((0, 0)),
                foreground_color: None,
                background_color: None,
                keyboard_enhancement_supported: None,
            }
        );
    }

    #[test]
    fn uses_the_latest_extended_cursor_position_response() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::ExtendedCursorPosition(1, 0));
        state.record(InternalEvent::ExtendedCursorPosition(9, 19));

        assert_eq!(state.into_probe().cursor_position, Some((9, 19)));
    }

    #[test]
    fn accepts_a_normal_cursor_response_but_ignores_ambiguous_f3() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::CursorPosition(9, 19));
        state.record(InternalEvent::CursorPositionOrF3(1, 0, KeyModifiers::SHIFT));

        assert_eq!(state.into_probe().cursor_position, Some((9, 19)));
    }

    #[test]
    fn waits_for_the_keyboard_fallback_after_supported_flags() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::ExtendedCursorPosition(0, 0));
        state.record(InternalEvent::OscColor {
            slot: 10,
            payload: OscColorPayload::Rgb { r: 1, g: 2, b: 3 },
        });
        state.record(InternalEvent::OscColor {
            slot: 11,
            payload: OscColorPayload::Rgb { r: 4, g: 5, b: 6 },
        });
        state.record(InternalEvent::KeyboardEnhancementFlags(
            KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES,
        ));

        assert!(!state.is_complete(KeyboardEnhancementProbe::Query));
        state.record(InternalEvent::PrimaryDeviceAttributes);
        assert!(state.is_complete(KeyboardEnhancementProbe::Query));
        assert_eq!(
            state.into_probe().keyboard_enhancement_supported,
            Some(true)
        );
    }

    #[test]
    fn completes_when_the_keyboard_fallback_arrives_first() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::ExtendedCursorPosition(0, 0));
        state.record(InternalEvent::OscColor {
            slot: 10,
            payload: OscColorPayload::Rgb { r: 1, g: 2, b: 3 },
        });
        state.record(InternalEvent::OscColor {
            slot: 11,
            payload: OscColorPayload::Rgb { r: 4, g: 5, b: 6 },
        });
        state.record(InternalEvent::PrimaryDeviceAttributes);

        assert!(state.is_complete(KeyboardEnhancementProbe::Query));
        assert_eq!(
            state.into_probe().keyboard_enhancement_supported,
            Some(false)
        );
    }

    #[test]
    fn remaining_timeout_handles_the_largest_duration() {
        assert!(!remaining_timeout(Instant::now(), Duration::MAX).is_zero());
    }

    #[test]
    fn remaining_timeout_saturates_after_the_deadline() {
        let started_at = Instant::now() - Duration::from_millis(10);

        assert_eq!(
            remaining_timeout(started_at, Duration::from_millis(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn does_not_enable_raw_mode_when_the_event_reader_is_locked() {
        let _reader = crate::event::lock_internal_event_reader();

        assert_eq!(
            query_terminal_startup(Duration::ZERO, KeyboardEnhancementProbe::Skip).unwrap(),
            TerminalStartupProbe {
                cursor_position: None,
                foreground_color: None,
                background_color: None,
                keyboard_enhancement_supported: None,
            }
        );
    }

    #[test]
    fn does_not_enable_raw_mode_or_send_a_query_for_a_zero_timeout() {
        assert_eq!(
            query_terminal_startup(Duration::ZERO, KeyboardEnhancementProbe::Skip).unwrap(),
            TerminalStartupProbe {
                cursor_position: None,
                foreground_color: None,
                background_color: None,
                keyboard_enhancement_supported: None,
            }
        );
    }

    #[test]
    fn writes_and_flushes_the_complete_startup_query() {
        #[derive(Default)]
        struct RecordingWriter {
            bytes: Vec<u8>,
            flushed: bool,
        }

        impl Write for RecordingWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                self.flushed = true;
                Ok(())
            }
        }

        let mut writer = RecordingWriter::default();
        let query = b"\x1B[6n\x1B]10;?\x1B\\\x1B]11;?\x1B\\";

        write_query(query, &mut writer, Instant::now(), Duration::from_secs(1)).unwrap();

        assert_eq!(writer.bytes, query);
        assert!(writer.flushed);
    }

    #[test]
    fn terminal_startup_query_does_not_block_on_a_full_output_buffer() {
        use std::os::unix::net::UnixStream;

        let (mut writer, _reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        let chunk = [0_u8; 4096];
        loop {
            match writer.write(&chunk) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed to fill test output buffer: {err}"),
            }
        }
        writer.set_nonblocking(false).unwrap();
        let flags_handle = writer.try_clone().unwrap();
        #[cfg(feature = "libc")]
        let writer_fd = {
            use std::os::unix::io::AsRawFd;
            crate::terminal::sys::file_descriptor::FileDesc::new(flags_handle.as_raw_fd(), false)
        };
        #[cfg(not(feature = "libc"))]
        let writer_fd = {
            use rustix::fd::AsFd;
            crate::terminal::sys::file_descriptor::FileDesc::Borrowed(flags_handle.as_fd())
        };

        let started_at = Instant::now();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let stdout_lock_thread = std::thread::spawn(move || {
            let _stdout_lock = io::stdout().lock();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();
        let err = {
            let _nonblocking =
                crate::terminal::sys::file_descriptor::NonblockingGuard::new(&writer_fd).unwrap();
            let mut raw_writer = &writer_fd;
            write_query(
                b"\x1B[?6n",
                &mut raw_writer,
                started_at,
                Duration::from_millis(5),
            )
            .unwrap_err()
        };
        release_tx.send(()).unwrap();
        stdout_lock_thread.join().unwrap();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(started_at.elapsed() < Duration::from_millis(100));
        #[cfg(feature = "libc")]
        {
            use std::os::unix::io::AsRawFd;
            // Safety: F_GETFL only inspects the supplied file descriptor.
            let flags = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) };
            assert!(flags >= 0);
            assert_eq!(flags & libc::O_NONBLOCK, 0);
        }
        #[cfg(not(feature = "libc"))]
        assert!(!rustix::fs::fcntl_getfl(&writer)
            .unwrap()
            .contains(rustix::fs::OFlags::NONBLOCK));
    }

    #[test]
    fn stops_a_partial_terminal_startup_query_when_the_deadline_expires() {
        #[derive(Default)]
        struct SlowPartialWriter {
            writes: usize,
        }

        impl Write for SlowPartialWriter {
            fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
                self.writes += 1;
                std::thread::sleep(Duration::from_millis(10));
                Ok(1)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = SlowPartialWriter::default();
        let err = write_query(
            b"\x1B[?6n",
            &mut writer,
            Instant::now(),
            Duration::from_millis(1),
        )
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(writer.writes, 1);
    }
}
