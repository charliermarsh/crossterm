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
        elapsed < Duration::from_millis(500),
        "bounded read took {elapsed:?} while input stayed readable"
    );
    assert!(source.parser.buffer.starts_with(prefix));

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
    let _ = bounded_read_with_incomplete_prefix(b"\x1B]12;");
}

#[test]
fn bounded_read_does_not_block_on_a_partial_sequence() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
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

    writer.write_all(b"c").unwrap();
    assert_eq!(
        source.try_read(Some(Duration::from_secs(1))).unwrap(),
        Some(InternalEvent::PrimaryDeviceAttributes)
    );
}
