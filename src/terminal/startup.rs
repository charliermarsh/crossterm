use std::fs::OpenOptions;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use crate::event::{
    filter::TerminalStartupProbeFilter, poll_internal, read_internal, InternalEvent,
    OscColorPayload,
};
use crate::style::Color;
use crate::terminal::{disable_raw_mode, enable_raw_mode};

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
pub fn query_terminal_startup(
    timeout: Duration,
    keyboard_probe: KeyboardEnhancementProbe,
) -> io::Result<TerminalStartupProbe> {
    if crate::terminal::sys::is_raw_mode_enabled() {
        query_terminal_startup_raw(timeout, keyboard_probe)
    } else {
        enable_raw_mode()?;
        let result = query_terminal_startup_raw(timeout, keyboard_probe);
        disable_raw_mode()?;
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

    let deadline = Instant::now() + timeout;
    let filter = TerminalStartupProbeFilter {
        query_keyboard: keyboard_probe == KeyboardEnhancementProbe::Query,
    };
    let mut state = ProbeState::default();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || !poll_internal(Some(remaining), &filter)? {
            return Ok(state.into_probe());
        }
        state.record(read_internal(&filter)?);
        if state.is_complete(keyboard_probe) {
            return Ok(state.into_probe());
        }
    }
}

fn send_query(query: &[u8]) -> io::Result<()> {
    let sent = OpenOptions::new()
        .write(true)
        .open("/dev/tty")
        .and_then(|mut tty| {
            tty.write_all(query)?;
            tty.flush()
        });

    if sent.is_err() {
        let mut stdout = io::stdout();
        stdout.write_all(query)?;
        stdout.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{KeyboardEnhancementProbe, ProbeState, TerminalStartupProbe};
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
}
