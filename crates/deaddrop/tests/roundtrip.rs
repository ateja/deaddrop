use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deaddrop::file::event_file_name;
use deaddrop::{
    ConsumerConfig, DetectionEvent, Error, EventConsumer, EventProducer, ProducerConfig,
    TrustedOwner,
};
use serde_json::json;

const WAIT: Duration = Duration::from_secs(5);

fn consumer_cfg(dir: &Path) -> ConsumerConfig {
    let mut c = ConsumerConfig::new(dir);
    c.trusted_owner = TrustedOwner::CurrentUser;
    c.rescan_interval = None; // prove the OS watcher alone delivers
    c
}

fn channel_consumer(cfg: ConsumerConfig) -> (EventConsumer, mpsc::Receiver<DetectionEvent>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let mut consumer = EventConsumer::with_config(cfg).unwrap();
    consumer
        .register_event_listener(move |e| {
            let _ = tx.lock().unwrap().send(e);
        })
        .unwrap();
    consumer.start_watching().unwrap();
    (consumer, rx)
}

fn event(seq: u64) -> DetectionEvent {
    DetectionEvent::new(seq, "TargetApp")
        .with_metadata("vendor", "ExampleCorp")
        .with_metadata("confidence", 98)
}

fn event_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

#[test]
fn delivers_every_event_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let producer = EventProducer::init(&dir).unwrap();
    let (mut consumer, rx) = channel_consumer(consumer_cfg(&dir));

    for seq in 1..=200 {
        producer.publish_event(&event(seq)).unwrap();
    }
    let got: Vec<u64> = (0..200)
        .map(|_| rx.recv_timeout(WAIT).unwrap().seq_id)
        .collect();
    assert_eq!(got, (1..=200).collect::<Vec<_>>());
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "no duplicates"
    );

    let first = event(1);
    assert_eq!(first.metadata["confidence"], json!(98));
    consumer.shutdown().unwrap();
    producer.shutdown().unwrap();
}

#[test]
fn replays_backlog_unless_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let producer = EventProducer::init(&dir).unwrap();
    producer.publish_event(&event(1)).unwrap();
    producer.publish_event(&event(2)).unwrap();

    let (_c1, rx1) = channel_consumer(consumer_cfg(&dir));
    assert_eq!(rx1.recv_timeout(WAIT).unwrap().seq_id, 1);
    assert_eq!(rx1.recv_timeout(WAIT).unwrap().seq_id, 2);

    let mut cfg = consumer_cfg(&dir);
    cfg.replay_existing = false;
    let (_c2, rx2) = channel_consumer(cfg);
    producer.publish_event(&event(3)).unwrap();
    assert_eq!(rx2.recv_timeout(WAIT).unwrap().seq_id, 3);
    assert_eq!(
        rx1.recv_timeout(WAIT).unwrap().seq_id,
        3,
        "fan out to both consumers"
    );
}

#[test]
fn ignores_temp_foreign_and_malformed_files() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let producer = EventProducer::init(&dir).unwrap();
    let (_c, rx) = channel_consumer(consumer_cfg(&dir));

    fs::write(dir.join(".tmp-00000000000000aa.part"), b"{half a reco").unwrap();
    fs::write(dir.join("README.txt"), b"hello").unwrap();
    fs::write(dir.join(event_file_name(50, 1)), b"not json").unwrap();
    // Valid JSON whose seq_id disagrees with the file name.
    fs::write(
        dir.join(event_file_name(51, 2)),
        event(999).to_json().unwrap(),
    )
    .unwrap();
    let mut wrong_major = event(52);
    wrong_major.schema_version = "2.0".into();
    fs::write(
        dir.join(event_file_name(52, 3)),
        serde_json::to_vec(&wrong_major).unwrap(),
    )
    .unwrap();

    producer.publish_event(&event(60)).unwrap();
    assert_eq!(rx.recv_timeout(WAIT).unwrap().seq_id, 60);
    assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());
}

#[test]
fn duplicate_seq_ids_never_clobber() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let producer = EventProducer::init(&dir).unwrap();
    let a = event(7).with_metadata("copy", "a");
    let b = event(7).with_metadata("copy", "b");
    producer.publish_event(&a).unwrap();
    producer.publish_event(&b).unwrap();
    assert_eq!(event_files(&dir).len(), 2);

    let (_c, rx) = channel_consumer(consumer_cfg(&dir));
    let mut copies: Vec<String> = (0..2)
        .map(|_| {
            rx.recv_timeout(WAIT).unwrap().metadata["copy"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    copies.sort();
    assert_eq!(copies, ["a", "b"]);
}

#[test]
fn concurrent_publishers_lose_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let mut pc = ProducerConfig::new(&dir);
    pc.max_files = None;
    let producer = Arc::new(EventProducer::with_config(pc).unwrap());
    let (_c, rx) = channel_consumer(consumer_cfg(&dir));

    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let p = Arc::clone(&producer);
            std::thread::spawn(move || {
                for i in 0..50 {
                    p.publish_event(&event(t * 1000 + i)).unwrap();
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());

    let mut got: Vec<u64> = (0..400)
        .map(|_| rx.recv_timeout(WAIT).unwrap().seq_id)
        .collect();
    got.sort();
    let mut want: Vec<u64> = (0..8)
        .flat_map(|t| (0..50).map(move |i| t * 1000 + i))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn retention_caps_file_count() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let mut pc = ProducerConfig::new(&dir);
    pc.max_files = Some(10);
    pc.durable = false;
    let producer = EventProducer::with_config(pc).unwrap();
    for seq in 0..200 {
        producer.publish_event(&event(seq)).unwrap();
        // Amortized: never more than max_files + PRUNE_EVERY on disk.
        assert!(event_files(&dir).len() <= 10 + 64);
    }
    producer.shutdown().unwrap();
    let files = event_files(&dir);
    assert_eq!(files.len(), 10, "shutdown applies retention");
    assert!(files.iter().all(|f| f.starts_with("evt-")));
}

#[test]
fn rejects_oversized_and_invalid_events() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let mut pc = ProducerConfig::new(&dir);
    pc.max_event_bytes = 512;
    let producer = EventProducer::with_config(pc).unwrap();

    let big = event(1).with_metadata("blob", "x".repeat(1000));
    assert!(matches!(
        producer.publish_event(&big),
        Err(Error::EventTooLarge { .. })
    ));

    let mut bad = event(2);
    bad.timestamp_utc = "last tuesday".into();
    assert!(matches!(
        producer.publish_event(&bad),
        Err(Error::InvalidEvent(_))
    ));
    assert!(
        event_files(&dir).is_empty(),
        "nothing written, no temp left behind"
    );

    producer.shutdown().unwrap();
    assert!(matches!(
        producer.publish_event(&event(3)),
        Err(Error::ShutDown)
    ));
}

#[test]
fn listener_panic_does_not_stop_delivery() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let producer = EventProducer::init(&dir).unwrap();

    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let mut consumer = EventConsumer::with_config(consumer_cfg(&dir)).unwrap();
    consumer
        .register_event_listener(|e| {
            if e.seq_id == 1 {
                panic!("listener bug");
            }
        })
        .unwrap();
    consumer
        .register_event_listener(move |e| {
            let _ = tx.lock().unwrap().send(e.seq_id);
        })
        .unwrap();
    consumer.start_watching().unwrap();

    producer.publish_event(&event(1)).unwrap();
    producer.publish_event(&event(2)).unwrap();
    assert_eq!(rx.recv_timeout(WAIT).unwrap(), 1);
    assert_eq!(rx.recv_timeout(WAIT).unwrap(), 2);
}

#[test]
fn shutdown_is_idempotent_and_prompt() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let _producer = EventProducer::init(&dir).unwrap();
    let (mut consumer, _rx) = channel_consumer(consumer_cfg(&dir));
    assert!(matches!(
        consumer.start_watching(),
        Err(Error::AlreadyWatching)
    ));
    let t = Instant::now();
    consumer.shutdown().unwrap();
    consumer.shutdown().unwrap();
    assert!(t.elapsed() < Duration::from_secs(1));
    assert!(matches!(consumer.start_watching(), Err(Error::ShutDown)));
}

#[cfg(unix)]
mod unix_security {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[test]
    fn producer_creates_dir_with_exact_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("events");
        let mut pc = ProducerConfig::new(&dir);
        pc.dir_mode = 0o750;
        pc.file_mode = 0o640;
        let producer = EventProducer::with_config(pc).unwrap();
        producer.publish_event(&event(1)).unwrap();
        let dmode = fs::metadata(&dir).unwrap().permissions().mode() & 0o7777;
        assert_eq!(dmode, 0o750);
        let f = dir.join(&event_files(&dir)[0]);
        assert_eq!(fs::metadata(f).unwrap().permissions().mode() & 0o777, 0o640);
    }

    #[test]
    fn rejects_group_or_world_writable_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("events");
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o777)).unwrap();

        assert!(matches!(
            EventProducer::init(&dir),
            Err(Error::InsecureDirectory { .. })
        ));
        let mut c = EventConsumer::with_config(consumer_cfg(&dir)).unwrap();
        assert!(matches!(
            c.start_watching(),
            Err(Error::InsecureDirectory { .. })
        ));
    }

    #[test]
    fn rejects_symlinked_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = tmp.path().join("events");
        symlink(&real, &link).unwrap();
        let mut c = EventConsumer::with_config(consumer_cfg(&link)).unwrap();
        assert!(matches!(
            c.start_watching(),
            Err(Error::InsecureDirectory { .. })
        ));
    }

    #[test]
    fn enforces_trusted_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("events");
        let _p = EventProducer::init(&dir).unwrap();
        // SAFETY: trivial libc call.
        let me = unsafe { libc_geteuid() };
        let mut cfg = consumer_cfg(&dir);
        cfg.trusted_owner = TrustedOwner::Uid(me.wrapping_add(4242));
        let mut c = EventConsumer::with_config(cfg).unwrap();
        assert!(matches!(
            c.start_watching(),
            Err(Error::InsecureDirectory { .. })
        ));
    }

    #[test]
    fn rejects_bad_modes_in_config() {
        let tmp = tempfile::tempdir().unwrap();
        let mut pc = ProducerConfig::new(tmp.path().join("events"));
        pc.file_mode = 0o666;
        assert!(matches!(
            EventProducer::with_config(pc),
            Err(Error::InvalidConfig(_))
        ));
    }

    extern "C" {
        #[link_name = "geteuid"]
        fn libc_geteuid() -> u32;
    }
}
