//! IPC transport end to end, selected through a YAML config.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(unix)]
use deaddrop::TrustedOwner;
use deaddrop::{ConsumerConfig, DetectionEvent, EventConsumer, EventProducer};

const WAIT: Duration = Duration::from_secs(10);

fn unique_name() -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "com.deaddrop.test.{}.{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

fn write_config(dir: &Path, name: &str, extra: &str) -> PathBuf {
    let path = dir.join("deaddrop.yaml");
    std::fs::write(
        &path,
        format!("transport: ipc\nipc:\n  name: {name}\n  flush_timeout_ms: 3000\n{extra}"),
    )
    .unwrap();
    path
}

fn trusted_config(dir: &Path) -> PathBuf {
    write_config(dir, &unique_name(), "  trusted_peer: current_user\n")
}

fn start_consumer(cfg: &Path) -> (EventConsumer, mpsc::Receiver<DetectionEvent>) {
    start_consumer_with(cfg, |_| {})
}

fn start_consumer_with(
    cfg: &Path,
    tune: impl FnOnce(&mut ConsumerConfig),
) -> (EventConsumer, mpsc::Receiver<DetectionEvent>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let mut c = EventConsumer::init_with(cfg, tune).unwrap();
    c.register_event_listener(move |e| {
        let _ = tx.lock().unwrap().send(e);
    })
    .unwrap();
    c.start_watching().unwrap();
    (c, rx)
}

#[test]
fn roundtrip_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = trusted_config(tmp.path());
    let (mut consumer, rx) = start_consumer(&cfg);
    let producer = EventProducer::init(&cfg).unwrap();
    assert!(
        producer.endpoint().starts_with("ipc: "),
        "{}",
        producer.endpoint()
    );
    assert!(
        consumer.endpoint().starts_with("ipc: "),
        "{}",
        consumer.endpoint()
    );

    for seq in 0..200 {
        producer
            .publish_event(&DetectionEvent::new(seq, "TargetApp").with_metadata("n", seq))
            .unwrap();
    }
    let got: Vec<u64> = (0..200)
        .map(|_| rx.recv_timeout(WAIT).unwrap().seq_id)
        .collect();
    assert_eq!(got, (0..200).collect::<Vec<_>>());
    producer.shutdown().unwrap();
    consumer.shutdown().unwrap();
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "no duplicates"
    );
}

#[test]
fn producer_first_events_are_held_until_consumer_starts() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = trusted_config(tmp.path());
    let producer = EventProducer::init(&cfg).unwrap();
    for seq in 1..=5 {
        producer
            .publish_event(&DetectionEvent::new(seq, "A"))
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(300));
    let (_consumer, rx) = start_consumer(&cfg);
    let got: Vec<u64> = (0..5)
        .map(|_| rx.recv_timeout(WAIT).unwrap().seq_id)
        .collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5]);
}

#[test]
fn producer_reconnects_after_consumer_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = trusted_config(tmp.path());
    let producer = EventProducer::init(&cfg).unwrap();

    let (mut first, rx1) = start_consumer(&cfg);
    producer
        .publish_event(&DetectionEvent::new(1, "A"))
        .unwrap();
    assert_eq!(rx1.recv_timeout(WAIT).unwrap().seq_id, 1);
    first.shutdown().unwrap();

    let (_second, rx2) = start_consumer(&cfg);
    for seq in 2..=4 {
        producer
            .publish_event(&DetectionEvent::new(seq, "A"))
            .unwrap();
    }
    let mut got: Vec<u64> = (0..3)
        .map(|_| rx2.recv_timeout(WAIT).unwrap().seq_id)
        .collect();
    got.dedup();
    assert_eq!(got, vec![2, 3, 4]);
}

#[test]
fn shutdown_flushes_queued_events() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = trusted_config(tmp.path());
    let (_consumer, rx) = start_consumer(&cfg);
    let producer = EventProducer::init(&cfg).unwrap();
    for seq in 0..50 {
        producer
            .publish_event(&DetectionEvent::new(seq, "A"))
            .unwrap();
    }
    producer.shutdown().unwrap(); // returns once the queue is drained
    assert!(producer
        .publish_event(&DetectionEvent::new(99, "A"))
        .is_err());
    assert_eq!(
        (0..50).filter(|_| rx.recv_timeout(WAIT).is_ok()).count(),
        50
    );
}

#[test]
fn second_consumer_on_same_endpoint_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = trusted_config(tmp.path());
    let (_first, _rx) = start_consumer(&cfg);
    let mut second = EventConsumer::init(&cfg).unwrap();
    second.register_event_listener(|_| {}).unwrap();
    assert!(second.start_watching().is_err());
}

#[cfg(unix)]
#[test]
fn consumer_rejects_untrusted_producer() {
    let tmp = tempfile::tempdir().unwrap();
    // No trusted_peer in YAML: the consumer's own policy applies.
    let cfg = write_config(tmp.path(), &unique_name(), "");
    let (_consumer, rx) = start_consumer_with(&cfg, |c| {
        c.trusted_owner = TrustedOwner::Uid(unsafe_euid().wrapping_add(4242));
    });
    let producer = EventProducer::init(&cfg).unwrap();
    producer
        .publish_event(&DetectionEvent::new(1, "A"))
        .unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(800)).is_err());
}

#[cfg(unix)]
fn unsafe_euid() -> u32 {
    extern "C" {
        fn geteuid() -> u32;
    }
    // SAFETY: trivial libc call.
    unsafe { geteuid() }
}
