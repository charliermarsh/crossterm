use std::fs::OpenOptions;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use crate::event::{
    filter::TerminalStartupProbeFilter, poll_internal, read_internal, InternalEvent,
    OscColorPayload,
};
use crate::style::Color;
use crate::terminal::{disable_raw_mode, enable_raw_mode};

struct RawModeGuard {
    active: bool,
}

impl RawModeGuard {
    fn enable() -> io::Result<Self> {
        enable_raw_mode()?;
        Ok(Self { active: true })
    }

    fn restore(mut self) -> io::Result<()> {
        disable_raw_mode()?;
        self.active = false;
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
            InternalEvent::CursorPosition(x, y) => {
                if self.cursor_position.is_none() {
                    self.cursor_position = Some((x, y));
                }
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
            InternalEvent::Event(_) | InternalEvent::OscColor { .. } => {}
        }
    }

    fn is_complete(&self, keyboard_probe: KeyboardEnhancementProbe) -> bool {
        self.cursor_position.is_some()
            && self.foreground_color.is_some()
            && self.background_color.is_some()
            && (keyboard_probe == KeyboardEnhancementProbe::Skip
                || self.saw_keyboard_enhancement_flags && self.saw_primary_device_attributes)
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

/// Query the cursor position, default colors, and optional keyboard support under one deadline.
///
/// The query uses crossterm's internal event reader, so key presses, bracketed pastes, and other
/// non-query events remain queued for the next [`crate::event::read`] or event stream poll.
/// When keyboard support is queried, both the enhancement-flags and fallback responses are
/// drained when available; an out-of-order fallback therefore uses the full timeout before it is
/// treated as unsupported.
pub fn query_terminal_startup(
    timeout: Duration,
    keyboard_probe: KeyboardEnhancementProbe,
) -> io::Result<TerminalStartupProbe> {
    if crate::terminal::sys::is_raw_mode_enabled() {
        query_terminal_startup_raw(timeout, keyboard_probe)
    } else {
        let raw_mode_guard = RawModeGuard::enable()?;
        let result = query_terminal_startup_raw(timeout, keyboard_probe);
        raw_mode_guard.restore()?;
        result
    }
}

fn query_terminal_startup_raw(
    timeout: Duration,
    keyboard_probe: KeyboardEnhancementProbe,
) -> io::Result<TerminalStartupProbe> {
    let query: &[u8] = match keyboard_probe {
        KeyboardEnhancementProbe::Query => b"\x1B[6n\x1B]10;?\x1B\\\x1B]11;?\x1B\\\x1B[?u\x1B[c",
        KeyboardEnhancementProbe::Skip => b"\x1B[6n\x1B]10;?\x1B\\\x1B]11;?\x1B\\",
    };
    send_query(query)?;

    let started_at = Instant::now();
    let filter = TerminalStartupProbeFilter {
        query_keyboard: keyboard_probe == KeyboardEnhancementProbe::Query,
    };
    let mut state = ProbeState::default();
    loop {
        let remaining = remaining_timeout(started_at, timeout);
        if remaining.is_zero() || !poll_internal(Some(remaining), &filter)? {
            return Ok(state.into_probe());
        }
        state.record(read_internal(&filter)?);
        if state.is_complete(keyboard_probe) {
            return Ok(state.into_probe());
        }
    }
}

fn remaining_timeout(started_at: Instant, timeout: Duration) -> Duration {
    timeout.saturating_sub(started_at.elapsed())
}

fn send_query(query: &[u8]) -> io::Result<()> {
    #[cfg(feature = "libc")]
    // Safety: `isatty` only inspects the supplied file descriptor.
    let stdin_is_terminal = unsafe { libc::isatty(libc::STDIN_FILENO) == 1 };
    #[cfg(not(feature = "libc"))]
    let stdin_is_terminal = rustix::termios::isatty(rustix::stdio::stdin());

    if stdin_is_terminal {
        let mut stdout = io::stdout();
        write_query(query, &mut stdout)
    } else {
        let mut tty = OpenOptions::new().write(true).open("/dev/tty")?;
        write_query(query, &mut tty)
    }
}

fn write_query(query: &[u8], writer: &mut impl Write) -> io::Result<()> {
    writer.write_all(query)?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::time::{Duration, Instant};

    use super::{
        remaining_timeout, write_query, KeyboardEnhancementProbe, ProbeState, TerminalStartupProbe,
    };
    use crate::event::{InternalEvent, KeyboardEnhancementFlags, OscColorPayload};
    use crate::style::Color;

    #[test]
    fn records_out_of_order_startup_responses() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::OscColor {
            slot: 11,
            payload: OscColorPayload::Rgb { r: 1, g: 2, b: 3 },
        });
        state.record(InternalEvent::PrimaryDeviceAttributes);
        state.record(InternalEvent::CursorPosition(9, 19));
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
        state.record(InternalEvent::CursorPosition(0, 0));
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
    fn waits_for_the_keyboard_fallback_after_supported_flags() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::CursorPosition(0, 0));
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
        assert_eq!(
            state.into_probe().keyboard_enhancement_supported,
            Some(true)
        );
    }

    #[test]
    fn waits_for_supported_flags_when_the_fallback_arrives_first() {
        let mut state = ProbeState::default();
        state.record(InternalEvent::CursorPosition(0, 0));
        state.record(InternalEvent::OscColor {
            slot: 10,
            payload: OscColorPayload::Rgb { r: 1, g: 2, b: 3 },
        });
        state.record(InternalEvent::OscColor {
            slot: 11,
            payload: OscColorPayload::Rgb { r: 4, g: 5, b: 6 },
        });
        state.record(InternalEvent::PrimaryDeviceAttributes);

        assert!(!state.is_complete(KeyboardEnhancementProbe::Query));
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

        write_query(query, &mut writer).unwrap();

        assert_eq!(writer.bytes, query);
        assert!(writer.flushed);
    }
}
