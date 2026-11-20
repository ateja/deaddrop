//! Measures publish -> callback latency through the real OS watcher.
//!
//! cargo run --release --example latency -- [samples] [--no-fsync] [--ipc]
//!
//! --ipc measures the IPC transport (named pipe / Unix socket; on macOS the
//! XPC listener needs launchd, so --ipc is not supported there).

use std::sync::mpsc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use deaddrop::{
    ConsumerConfig, DetectionEvent, EventConsumer, EventProducer, ProducerConfig, TrustedOwner,
};

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i].as_secs_f64() * 1e6
}

fn main() -> deaddrop::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let samples: usize = args.iter().find_map(|a| a.parse().ok()).unwrap_or(2000);
    let durable = !args.iter().any(|a| a == "--no-fsync");

    let ipc = args.iter().any(|a| a == "--ipc");
    let tmp = std::env::temp_dir().join(format!("deaddrop-latency-{}", std::process::id()));
    std::fs::create_dir_all(&tmp)?;
    let (producer, mut consumer) = if ipc {
        let cfg = tmp.join("deaddrop.yaml");
        std::fs::write(
            &cfg,
            format!(
                "transport: ipc\nipc:\n  name: com.deaddrop.latency.{}\n  trusted_peer: current_user\n",
                std::process::id()
            ),
        )?;
        let consumer = EventConsumer::init(&cfg)?;
        (None, (consumer, Some(cfg)))
    } else {
        let mut cc = ConsumerConfig::new(tmp.join("events"));
        cc.trusted_owner = TrustedOwner::CurrentUser;
        cc.rescan_interval = None;
        let mut pc = ProducerConfig::new(tmp.join("events"));
        pc.durable = durable;
        let producer = EventProducer::with_config(pc)?;
        (Some(producer), (EventConsumer::with_config(cc)?, None))
    };
    let (tx, rx) = mpsc::channel::<Instant>();
    let tx = Mutex::new(tx);
    consumer.0.register_event_listener(move |_e| {
        let _ = tx.lock().unwrap().send(Instant::now());
    })?;
    consumer.0.start_watching()?;
    let producer = match (producer, &consumer.1) {
        (Some(p), _) => p,
        (None, Some(cfg)) => EventProducer::init(cfg)?,
        (None, None) => unreachable!(),
    };
    let mut consumer = consumer.0;
    let transport = producer.endpoint();

    let mut publish = Vec::with_capacity(samples);
    let mut end_to_end = Vec::with_capacity(samples);
    for seq in 0..samples as u64 {
        let ev = DetectionEvent::new(seq, "TargetApp").with_metadata("confidence", 98);
        let t0 = Instant::now();
        producer.publish_event(&ev)?;
        let t1 = Instant::now();
        let t2 = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("event not delivered");
        publish.push(t1 - t0);
        end_to_end.push(t2 - t0);
    }
    consumer.shutdown()?;
    producer.shutdown()?;
    let _ = std::fs::remove_dir_all(&tmp);

    publish.sort();
    end_to_end.sort();
    println!(
        "samples={samples} transport={transport} fsync={} os={}",
        if ipc {
            "n/a".to_owned()
        } else {
            durable.to_string()
        },
        std::env::consts::OS
    );
    for (name, v) in [
        ("publish_event", &publish),
        ("publish->callback", &end_to_end),
    ] {
        println!(
            "{name:<18} p50={:>8.1}us p95={:>8.1}us p99={:>8.1}us max={:>8.1}us",
            pct(v, 0.50),
            pct(v, 0.95),
            pct(v, 0.99),
            pct(v, 1.0)
        );
    }
    Ok(())
}
