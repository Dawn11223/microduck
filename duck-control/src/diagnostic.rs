//! Opt-in bench trace. No policy inputs or motor parameters are changed by this module.
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

use crate::{ACTION_LEN, NUM_JOINTS, OBS_LEN};

#[derive(Clone, Copy, Debug)]
pub enum Event {
    BusRead { start_ns: u64, end_ns: u64, ok: bool, raw: [[u8; 12]; NUM_JOINTS + 1] },
    KneeRead { start_ns: u64, end_ns: u64, ok: bool, raw: [[u8; 32]; 2] },
    Policy { observation: [f32; OBS_LEN], action: [f32; ACTION_LEN], scale: f64, gain: u16 },
    InferenceInput { observation: [f32; OBS_LEN] },
    RawAction { bits: [u32; ACTION_LEN], len: usize },
    InferenceResult { ok: bool },
    Write { start_ns: u64, end_ns: u64, ok: bool, positions: [f64; NUM_JOINTS] },
}

#[derive(Clone, Copy, Debug)]
struct Stamped { tick_ns: u64, event_ns: u64, event: Event }

struct Sink {
    tx: SyncSender<Stamped>,
    dropped: AtomicU64,
    active: AtomicBool,
    knees: AtomicBool,
}

impl Sink {
    fn try_record(&self, event: Stamped) {
        if !self.active.load(Ordering::Relaxed) { return; }
        match self.tx.try_send(event) {
            Ok(()) => {},
            Err(TrySendError::Full(_)) => { self.dropped.fetch_add(1, Ordering::Relaxed); },
            Err(TrySendError::Disconnected(_)) => { self.active.store(false, Ordering::Relaxed); },
        }
    }
}

fn write_record(out: &mut impl Write, record: &Stamped) -> std::io::Result<()> {
    use serde_json::json;
    let event = match &record.event {
        Event::BusRead { start_ns, end_ns, ok, raw } => json!({
            "kind":"bus_read", "start_ns":start_ns,"end_ns":end_ns,"ok":ok,"raw":raw }),
        Event::KneeRead { start_ns, end_ns, ok, raw } => json!({
            "kind":"knee_read", "start_ns":start_ns,"end_ns":end_ns,"ok":ok,"raw":raw }),
        Event::Policy { observation, action, scale, gain } => json!({
            "kind":"policy", "observation":observation.as_slice(),"action":action,
            "scale":scale,"gain":gain }),
        Event::InferenceInput { observation } => json!({
            "kind":"inference_input", "observation":observation.as_slice(),
            "observation_bits":observation.iter().map(|v| v.to_bits()).collect::<Vec<_>>() }),
        Event::RawAction { bits, len } => json!({"kind":"raw_action","bits":bits,"len":len}),
        Event::InferenceResult { ok } => json!({"kind":"inference_result","ok":ok}),
        Event::Write { start_ns, end_ns, ok, positions } => json!({
            "kind":"write", "start_ns":start_ns,"end_ns":end_ns,"ok":ok,"positions":positions }),
    };
    serde_json::to_writer(&mut *out, &json!({
        "tick_ns":record.tick_ns,"event_ns":record.event_ns,"event":event
    }))?;
    writeln!(out)
}

static SINK: OnceLock<Arc<Sink>> = OnceLock::new();
static TICK_NS: AtomicU64 = AtomicU64::new(0);

pub fn enabled() -> bool {
    SINK.get().is_some_and(|s| s.active.load(Ordering::Relaxed))
}

pub fn knee_read_enabled() -> bool {
    enabled() && SINK.get().is_some_and(|s| s.knees.load(Ordering::Relaxed))
}

/// A failed optional transaction is recorded once, then disabled to avoid repeated timeouts.
pub fn disable_knee_reads() {
    if let Some(s) = SINK.get() { s.knees.store(false, Ordering::Relaxed); }
}

pub fn begin_tick(tick_ns: u64) {
    if enabled() { TICK_NS.store(tick_ns, Ordering::Relaxed); }
}

pub fn now_ns() -> u64 { duck_ipc_proto::clock::monotonic_ns() }

pub fn record(event: Event) {
    if let Some(sink) = SINK.get() {
        sink.try_record(Stamped { tick_ns: TICK_NS.load(Ordering::Relaxed), event_ns: now_ns(), event });
    }
}

pub struct Capture {
    sink: Arc<Sink>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Capture {
    fn drop(&mut self) {
        // main retains this guard until after the control thread has joined. No more
        // producers remain, so draining after the flag is cleared includes the final tick.
        self.sink.active.store(false, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
    }
}

/// Called before hardware is opened. Absent DUCK_TRACE_PATH is the ordinary production path.
/// Files must be new; an old capture is never overwritten. Serialization and disk writes all
/// happen on the worker. The control thread only copies fixed arrays into a bounded queue.
pub fn start_from_env() -> std::io::Result<Option<Capture>> {
    let Some(path) = std::env::var_os("DUCK_TRACE_PATH") else { return Ok(None); };
    let seconds = std::env::var("DUCK_TRACE_SECONDS").ok()
        .and_then(|s| s.parse::<u64>().ok()).unwrap_or(900).clamp(1, 3600);
    let knees = std::env::var("DUCK_TRACE_KNEES").is_ok_and(|v| v == "1");
    let (capture, sink) = start_capture(std::path::Path::new(&path), seconds, knees)?;
    SINK.set(sink).map_err(|_| std::io::Error::other("diagnostic capture already started"))?;
    Ok(Some(capture))
}

fn start_capture(path: &std::path::Path, seconds: u64, knees: bool) -> std::io::Result<(Capture, Arc<Sink>)> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut out = BufWriter::with_capacity(256 * 1024, file);
    serde_json::to_writer(&mut out, &serde_json::json!({
        "kind":"meta", "schema":1, "monotonic_ns":now_ns(),
        "realtime_ns":duck_ipc_proto::clock::realtime_ns(),
        "joint_ids":crate::JOINT_IDS, "joint_names":crate::JOINT_NAMES,
        "raw_bus_ids":std::iter::once(200).chain(crate::JOINT_IDS).collect::<Vec<_>>(),
        "raw_bus_address":124,"raw_bus_length":12,
        "knee_ids":[12,22],"knee_address":116,"knee_length":32,
        "knees_enabled":knees, "duration_s":seconds,
        "write_ok_means":"host transmission succeeded; knee Goal is independent readback",
        "policy_gain_means":"requested gain; retain IPC and journal for apply errors",
        "raw_action_bits_means":"first 14 f32 bit patterns before validation; len is true output length"
    }))?;
    writeln!(out)?;
    out.flush()?;
    let (tx, rx) = sync_channel(1024);
    let sink = Arc::new(Sink { tx, dropped: AtomicU64::new(0),
        active: AtomicBool::new(true), knees: AtomicBool::new(knees) });
    let worker_sink = Arc::clone(&sink);
    let worker = std::thread::Builder::new().name("duck-diagnostic".into()).spawn(move || {
        let result = (|| -> std::io::Result<()> {
            let started = Instant::now();
            let mut last_flush = Instant::now();
            let mut records = 0u64;
            while worker_sink.active.load(Ordering::Relaxed) && started.elapsed() < Duration::from_secs(seconds) {
                if let Ok(event) = rx.recv_timeout(Duration::from_millis(200)) {
                    write_record(&mut out, &event)?;
                    records += 1;
                }
                if last_flush.elapsed() >= Duration::from_millis(200) {
                    serde_json::to_writer(&mut out, &serde_json::json!({
                        "kind":"trace_health", "monotonic_ns":now_ns(),"records":records,
                        "dropped":worker_sink.dropped.load(Ordering::Relaxed)
                    }))?;
                    writeln!(out)?;
                    out.flush()?;
                    last_flush = Instant::now();
                }
            }
            worker_sink.active.store(false, Ordering::Relaxed);
            for event in rx.try_iter() { write_record(&mut out, &event)?; records += 1; }
            serde_json::to_writer(&mut out, &serde_json::json!({
                "kind":"end", "records":records,"dropped":worker_sink.dropped.load(Ordering::Relaxed)
            }))?;
            writeln!(out)?;
            out.flush()
        })();
        worker_sink.active.store(false, Ordering::Relaxed);
        if let Err(error) = result { tracing::error!(%error, "diagnostic writer stopped; capture is incomplete"); }
    })?;
    Ok((Capture { sink: Arc::clone(&sink), worker: Some(worker) }, sink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::TryRecvError;

    #[test]
    fn shutdown_drains_queued_records_and_finalizes_before_returning() {
        let path = std::env::temp_dir().join(format!("duck-trace-{}-{}.jsonl", std::process::id(), now_ns()));
        let (capture, sink) = start_capture(&path, 60, false).unwrap();
        sink.try_record(Stamped { tick_ns: 12, event_ns: 34, event: Event::Write {
            start_ns: 1, end_ns: 2, ok: true, positions: [0.25; NUM_JOINTS] } });
        drop(capture);
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&path).unwrap().lines()
            .map(|line| serde_json::from_str(line).unwrap()).collect();
        std::fs::remove_file(path).unwrap();
        assert_eq!(rows.last().unwrap()["kind"], "end");
        assert_eq!(rows.last().unwrap()["records"], 1);
        assert_eq!(rows[1]["event"]["positions"][14], 0.25);
    }

    fn value(event: Event) -> serde_json::Value {
        let mut bytes = Vec::new();
        write_record(&mut bytes, &Stamped { tick_ns: 123, event_ns: 456, event }).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn policy_trace_preserves_full_observation_and_unclipped_action() {
        let mut observation = [0.0; OBS_LEN];
        observation[0] = -1.25;
        observation[60] = 7.5;
        let mut action = [0.0; ACTION_LEN];
        action[13] = -9.75;
        let v = value(Event::Policy { observation, action, scale: 0.9, gain: 200 });
        assert_eq!(v["event"]["observation"].as_array().unwrap().len(), 61);
        assert_eq!(v["event"]["observation"][0], -1.25);
        assert_eq!(v["event"]["observation"][60], 7.5);
        assert_eq!(v["event"]["action"][13], -9.75);
        assert_eq!(v["tick_ns"], 123);
        assert_eq!(v["event_ns"], 456);
    }

    #[test]
    fn bus_trace_keeps_raw_signed_words_and_both_read_timestamps() {
        let mut raw = [[0; 12]; NUM_JOINTS + 1];
        raw[0][11] = 128;
        raw[15][0] = 255;
        raw[15][1] = 255;
        let v = value(Event::BusRead { start_ns: 100, end_ns: 120, ok: true, raw });
        assert_eq!(v["event"]["raw"][0][11], 128);
        assert_eq!(v["event"]["raw"][15][0], 255);
        assert_eq!(v["event"]["raw"][15][1], 255);
        assert_eq!(v["event"]["start_ns"], 100);
        assert_eq!(v["event"]["end_ns"], 120);
    }

    #[test]
    fn failed_write_is_distinct_from_successful_transmission() {
        let v = value(Event::Write { start_ns: 1, end_ns: 2, ok: false, positions: [0.25; NUM_JOINTS] });
        assert_eq!(v["event"]["ok"], false);
        assert_eq!(v["event"]["positions"][14], 0.25);
    }

    #[test]
    fn full_queue_drops_new_record_without_replacing_previous_record() {
        let (tx, rx) = sync_channel(1);
        let sink = Sink { tx, dropped: AtomicU64::new(0), active: AtomicBool::new(true), knees: AtomicBool::new(false) };
        let sample = Stamped { tick_ns: 1, event_ns: 2, event: Event::Write {
            start_ns: 1, end_ns: 2, ok: true, positions: [0.0; NUM_JOINTS] } };
        sink.try_record(sample);
        sink.try_record(Stamped { tick_ns: 3, ..sample });
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(rx.try_recv().unwrap().tick_ns, 1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }
}
