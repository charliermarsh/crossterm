use super::*;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

fn bounded_read_with_incomplete_prefix(prefix: &[u8]) -> (UnixInternalEventSource, UnixStream) {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    writer.set_nonblocking(true).unwrap();
    writer.write_all(prefix).unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    let writing = Arc::new(AtomicBool::new(true));
    let keep_writing = Arc::clone(&writing);
    let writer_thread = std::thread::spawn(move || {
        let chunk = [b'1'; TTY_BUFFER_SIZE];
        let deadline = Instant::now() + Duration::from_secs(1);
        while keep_writing.load(Ordering::Relaxed) && Instant::now() < deadline {
            match writer.write(&chunk) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
                Err(err) => panic!("failed to write incomplete input: {err}"),
            }
        }
        writer
    });

    let started = Instant::now();
    let event = source.try_read(Some(Duration::from_millis(20))).unwrap();
    let elapsed = started.elapsed();
    writing.store(false, Ordering::Relaxed);
    let writer = writer_thread.join().unwrap();

    assert!(event.is_none());
    assert!(
        elapsed < Duration::from_millis(250),
        "bounded read took {elapsed:?} while input stayed readable"
    );
    if osc_kind(prefix).is_some() {
        assert!(source.parser.buffer.is_empty());
        assert_eq!(source.parser.discarding_osc, osc_kind(prefix));
    } else {
        assert!(source.parser.buffer.starts_with(prefix));
    }

    (source, writer)
}

#[test]
fn bounded_read_preserves_continuously_readable_incomplete_input() {
    let (mut source, mut writer) = bounded_read_with_incomplete_prefix(b"\x1B[?");

    let completion = std::thread::spawn(move || loop {
        match writer.write(b"c") {
            Ok(1) => break,
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
            Err(err) => panic!("failed to complete input sequence: {err}"),
        }
    });
    assert_eq!(
        source.try_read(Some(Duration::from_secs(1))).unwrap(),
        Some(InternalEvent::PrimaryDeviceAttributes)
    );
    completion.join().unwrap();
}

#[test]
fn bounded_read_honors_deadline_for_an_incomplete_osc_payload() {
    let (source, writer) = bounded_read_with_incomplete_prefix(b"\x1B]12;");
    assert_discarded_osc_tail_does_not_leak_input(source, writer, b"\x1B\\h");
}

#[test]
fn bounded_read_honors_deadline_for_an_incomplete_c1_osc_payload() {
    let (source, writer) = bounded_read_with_incomplete_prefix(b"\x9D12;");
    assert_discarded_osc_tail_does_not_leak_input(source, writer, b"\x9Ch");
}

#[test]
fn bounded_read_honors_deadline_for_continuously_readable_incomplete_c1_csi() {
    let _ = bounded_read_with_incomplete_prefix(b"\x9B?");
}

#[test]
fn bounded_resize_query_uses_ioctl_or_defers_without_tput() {
    let (reader, _writer) = UnixStream::pair().unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    let expected_size = crate::terminal::window_size().ok();

    let started = Instant::now();
    let event = source.read_resize(ResizeQuery::Bounded).unwrap();
    assert!(started.elapsed() < Duration::from_millis(250));

    if let Some(ref size) = expected_size {
        assert_eq!(
            event,
            Some(InternalEvent::Event(Event::Resize(size.columns, size.rows)))
        );
        assert!(!source.pending_resize);
    } else {
        assert!(event.is_none());
        assert!(source.pending_resize);
    }

    source.clear();
    assert!(!source.pending_resize);
}

#[test]
fn clear_discards_a_queued_resize_signal() {
    let (reader, _writer) = UnixStream::pair().unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    source.pending_resize = true;
    #[cfg(not(feature = "use-dev-tty"))]
    {
        source
            .signals
            .add_signal(signal_hook::consts::SIGUSR1)
            .unwrap();
        signal_hook::low_level::raise(signal_hook::consts::SIGUSR1).unwrap();
    }
    #[cfg(feature = "use-dev-tty")]
    {
        let (receiver, mut sender) = nonblocking_unix_pair().unwrap();
        source.winch_signal_receiver = receiver;
        sender.write_all(&[1]).unwrap();
    }

    source.clear();

    assert!(!source.pending_resize);
    #[cfg(not(feature = "use-dev-tty"))]
    {
        assert!(source.events.is_empty());
        assert!(source.signals.pending().next().is_none());
    }
    #[cfg(feature = "use-dev-tty")]
    {
        #[cfg(feature = "libc")]
        let fd = FileDesc::new(source.winch_signal_receiver.as_raw_fd(), false);
        #[cfg(not(feature = "libc"))]
        let fd = FileDesc::Borrowed(source.winch_signal_receiver.as_fd());
        assert_eq!(read_complete(&fd, &mut [0; 1024]).unwrap(), 0);
    }
}

#[cfg(all(not(feature = "use-dev-tty"), not(target_os = "fuchsia")))]
#[test]
fn normal_mio_source_does_not_toggle_borrowed_stdin_flags() {
    use std::ffi::OsStr;
    use std::io::IsTerminal;
    use std::os::unix::ffi::OsStrExt;

    if !std::io::stdin().is_terminal() {
        return;
    }

    let Ok(stdin_tty) = rustix::termios::ttyname(rustix::stdio::stdin(), Vec::new()) else {
        return;
    };
    if std::fs::OpenOptions::new()
        .read(true)
        .open(OsStr::from_bytes(stdin_tty.as_bytes()))
        .is_err()
    {
        return;
    }

    let source = UnixInternalEventSource::new().unwrap();
    assert_ne!(source.tty_fd.raw_fd(), 0);

    #[cfg(feature = "libc")]
    {
        // Safety: F_GETFL only inspects the supplied file descriptor.
        let before = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_GETFL) };
        assert!(before >= 0);
        let _nonblocking = NonblockingGuard::new(&source.tty_fd).unwrap();
        // Safety: F_GETFL only inspects the supplied file descriptor.
        let during = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_GETFL) };
        assert_eq!(during, before);
    }
    #[cfg(not(feature = "libc"))]
    {
        let before = rustix::fs::fcntl_getfl(rustix::stdio::stdin()).unwrap();
        let _nonblocking = NonblockingGuard::new(&source.tty_fd).unwrap();
        let during = rustix::fs::fcntl_getfl(rustix::stdio::stdin()).unwrap();
        assert_eq!(during, before);
    }
}

#[test]
fn bounded_read_does_not_block_on_a_partial_sequence() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    let observer = reader.try_clone().unwrap();
    writer.write_all(b"\x1B[?").unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let writer_thread = std::thread::spawn(move || {
        if cancel_rx.recv_timeout(Duration::from_millis(500)).is_err() {
            writer.write_all(b"c").unwrap();
        }
        writer
    });

    let started = Instant::now();
    let event = source.try_read(Some(Duration::from_millis(20))).unwrap();
    let elapsed = started.elapsed();
    let _ = cancel_tx.send(());
    let mut writer = writer_thread.join().unwrap();

    assert!(event.is_none());
    assert!(
        elapsed < Duration::from_millis(250),
        "bounded read blocked for {elapsed:?} after a partial sequence"
    );
    assert_eq!(source.parser.buffer, b"\x1B[?");
    assert_blocking(&observer);

    writer.write_all(b"c").unwrap();
    assert_eq!(
        source.try_read(Some(Duration::from_secs(1))).unwrap(),
        Some(InternalEvent::PrimaryDeviceAttributes)
    );
}

#[test]
fn bounded_read_completes_a_multi_chunk_sequence() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    let observer = reader.try_clone().unwrap();
    let mut partial = b"\x1B[?".to_vec();
    partial.resize(TTY_BUFFER_SIZE * 2 + 17, b'1');
    writer.write_all(&partial).unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    assert!(source
        .try_read(Some(Duration::from_millis(20)))
        .unwrap()
        .is_none());
    assert_eq!(source.parser.buffer, partial);
    assert_blocking(&observer);

    writer.write_all(b"c").unwrap();
    assert_eq!(
        source.try_read(Some(Duration::from_secs(1))).unwrap(),
        Some(InternalEvent::PrimaryDeviceAttributes)
    );
    assert_blocking(&observer);
}

#[test]
fn idle_read_leaves_the_terminal_descriptor_blocking() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    let observer = reader.try_clone().unwrap();
    writer.write_all(b"\x1B[?").unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    let reader_thread = std::thread::spawn(move || source.try_read(None).unwrap());
    std::thread::sleep(Duration::from_millis(50));

    assert_blocking(&observer);
    writer.write_all(b"c").unwrap();
    assert_eq!(
        reader_thread.join().unwrap(),
        Some(InternalEvent::PrimaryDeviceAttributes)
    );
    assert_blocking(&observer);
}

#[cfg(feature = "use-dev-tty")]
#[test]
fn stale_terminal_readiness_does_not_block_the_scoped_read() {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;

    let (reader, mut writer) = UnixStream::pair().unwrap();
    let mut competing_reader = reader.try_clone().unwrap();
    writer.write_all(b"x").unwrap();

    let mut fds = [filedescriptor::pollfd {
        fd: reader.as_raw_fd(),
        events: filedescriptor::POLLIN,
        revents: 0,
    }];
    filedescriptor::poll(&mut fds, Some(Duration::from_secs(1))).unwrap();
    assert_ne!(fds[0].revents & filedescriptor::POLLIN, 0);

    let mut consumed = [0; 1];
    competing_reader.read_exact(&mut consumed).unwrap();
    assert_eq!(consumed, *b"x");

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut buffer = [0; 1];
    let started = Instant::now();
    let read_result = {
        let _nonblocking = NonblockingGuard::new(&input_fd).unwrap();
        input_fd.read(&mut buffer)
    };

    assert_eq!(read_result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert!(started.elapsed() < Duration::from_millis(100));
    assert_blocking(&competing_reader);
}

fn assert_blocking(stream: &UnixStream) {
    #[cfg(feature = "libc")]
    {
        use std::os::unix::io::AsRawFd;
        // Safety: F_GETFL only inspects the supplied file descriptor.
        let flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(flags & libc::O_NONBLOCK, 0);
    }
    #[cfg(not(feature = "libc"))]
    assert!(!rustix::fs::fcntl_getfl(stream)
        .unwrap()
        .contains(rustix::fs::OFlags::NONBLOCK));
}

fn assert_clear_discards_partial_input(prefix: &[u8]) {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    writer.write_all(prefix).unwrap();

    #[cfg(feature = "libc")]
    let input_fd = {
        use std::os::unix::io::IntoRawFd;
        FileDesc::new(reader.into_raw_fd(), true)
    };
    #[cfg(not(feature = "libc"))]
    let input_fd = FileDesc::Owned(reader.into());

    let mut source = UnixInternalEventSource::from_file_descriptor(input_fd).unwrap();
    assert!(source
        .try_read(Some(Duration::from_millis(20)))
        .unwrap()
        .is_none());
    assert_eq!(source.parser.buffer, prefix);
    source
        .parser
        .internal_events
        .push_back(InternalEvent::Event(Event::Key(
            crate::event::KeyCode::Char('x').into(),
        )));

    source.clear();

    assert!(source.parser.buffer.is_empty());
    assert!(source.parser.internal_events.is_empty());
    writer.write_all(b"h").unwrap();
    assert_eq!(
        source.try_read(Some(Duration::from_secs(1))).unwrap(),
        Some(InternalEvent::Event(Event::Key(
            crate::event::KeyCode::Char('h').into(),
        )))
    );
}

#[test]
fn clear_discards_a_partial_csi_and_queued_parser_events() {
    assert_clear_discards_partial_input(b"\x1B[?");
}

fn assert_discarded_osc_tail_does_not_leak_input(
    mut source: UnixInternalEventSource,
    mut writer: UnixStream,
    completion: &'static [u8],
) {
    let completion_thread = std::thread::spawn(move || {
        let mut remaining = completion;
        while !remaining.is_empty() {
            match writer.write(remaining) {
                Ok(written) => remaining = &remaining[written..],
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
                Err(err) => panic!("failed to terminate discarded OSC input: {err}"),
            }
        }
    });

    assert_eq!(
        source.try_read(Some(Duration::from_secs(1))).unwrap(),
        Some(InternalEvent::Event(Event::Key(
            crate::event::KeyCode::Char('h').into(),
        )))
    );
    assert!(source.parser.buffer.is_empty());
    assert!(source.parser.discarding_osc.is_none());
    assert!(!source.parser.discarding_osc_esc);
    completion_thread.join().unwrap();
}

#[test]
fn clear_discards_an_oversized_osc_state() {
    let (mut source, _writer) = bounded_read_with_incomplete_prefix(b"\x1B]12;");

    source.clear();

    assert!(source.parser.buffer.is_empty());
    assert!(source.parser.discarding_osc.is_none());
    assert!(!source.parser.discarding_osc_esc);
}

#[cfg(feature = "bracketed-paste")]
#[test]
fn clear_discards_a_partial_bracketed_paste() {
    assert_clear_discards_partial_input(b"\x1B[200~partial paste");
}
