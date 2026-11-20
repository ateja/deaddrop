//! DEADDROP_CONFIG selection. One test per binary: it changes the process
//! environment.

use deaddrop::{DetectionEvent, EventConsumer, EventProducer, TrustedOwner};

#[test]
fn env_var_switches_transport_without_code_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("events");
    let cfg = tmp.path().join("deaddrop.yaml");

    // file transport, directory taken from the init() argument
    std::fs::write(&cfg, "transport: file\n").unwrap();
    std::env::set_var("DEADDROP_CONFIG", &cfg);
    let p = EventProducer::init(&dir).unwrap();
    assert_eq!(p.endpoint(), format!("file: {}", dir.display()));
    p.publish_event(&DetectionEvent::new(1, "A")).unwrap();
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    p.shutdown().unwrap();

    // same code, ipc transport
    let name = format!("com.deaddrop.envtest.{}", std::process::id());
    std::fs::write(
        &cfg,
        format!("transport: ipc\nipc:\n  name: {name}\n  trusted_peer: current_user\n"),
    )
    .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let tx = std::sync::Mutex::new(tx);
    let mut c =
        EventConsumer::init_with(&dir, |c| c.trusted_owner = TrustedOwner::CurrentUser).unwrap();
    assert!(c.endpoint().starts_with("ipc: "));
    c.register_event_listener(move |e| {
        let _ = tx.lock().unwrap().send(e.seq_id);
    })
    .unwrap();
    c.start_watching().unwrap();
    let p = EventProducer::init(&dir).unwrap();
    assert!(p.endpoint().starts_with("ipc: "));
    p.publish_event(&DetectionEvent::new(2, "A")).unwrap();
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap(),
        2
    );

    // bad config is a clear error, not a silent fallback
    std::fs::write(&cfg, "transport: pigeons\n").unwrap();
    let err = EventProducer::init(&dir).err().unwrap().to_string();
    assert!(err.contains("deaddrop.yaml"), "{err}");
    std::env::remove_var("DEADDROP_CONFIG");
}
