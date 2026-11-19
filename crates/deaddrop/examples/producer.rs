//! Rust equivalent of the Python producer from the proposal.
//!
//! cargo run --example producer -- /var/run/vendor_ipc/events [count]

use deaddrop::{DetectionEvent, EventProducer, Result};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .unwrap_or_else(|| "/var/run/vendor_ipc/events".into());
    let count: u64 = args.next().and_then(|c| c.parse().ok()).unwrap_or(3);

    // 1. Initialize with the agreed-upon secure directory path
    let producer = EventProducer::init(&dir)?;

    // 2. Construct and publish events (atomic write + rename)
    for seq_id in 10492..10492 + count {
        let event = DetectionEvent::new(seq_id, "TargetApp")
            .with_metadata("vendor", "ExampleCorp")
            .with_metadata("confidence", 98);
        producer.publish_event(&event)?;
        println!("published {seq_id}");
    }

    // 3. Release handles gracefully on application exit
    producer.shutdown()
}
