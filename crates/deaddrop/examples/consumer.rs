//! Native consumer from the proposal, section 3.
//!
//! cargo run --example consumer -- /var/run/vendor_ipc/events [--any-owner]

use std::path::PathBuf;
use std::sync::mpsc;

use deaddrop::{ConsumerConfig, DetectionEvent, EventConsumer, Result, TrustedOwner};

fn run_consumer() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "/var/run/vendor_ipc/events".into()),
    );
    let mut config = ConsumerConfig::new(&dir);
    if args.any(|a| a == "--any-owner") {
        config.trusted_owner = TrustedOwner::Any; // dev only
    }

    // 1. Initialize targeting the agreed-upon directory
    let mut consumer = EventConsumer::with_config(config)?;

    // 2. Register the handler triggered asynchronously by the OS watcher
    consumer.register_event_listener(|event: DetectionEvent| {
        println!("Received event {}: {}", event.seq_id, event.app_name);
    })?;

    // 3. Start the OS-native watcher (returns immediately; events arrive on a
    //    background thread)
    consumer.start_watching()?;
    println!("watching {} — press Enter to stop", dir.display());

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = std::io::stdin().read_line(&mut String::new());
        let _ = tx.send(());
    });
    let _ = rx.recv();

    // 4. On application shutdown, stop the watcher and release handles
    consumer.shutdown()
}

fn main() {
    if let Err(e) = run_consumer() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
