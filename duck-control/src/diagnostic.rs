//! Opt-in bench trace. No policy inputs or motor parameters are changed by this module.
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};

use crate::{ACTION_LEN, NUM_JOINTS, OBS_LEN};

#[derive(Clone, Copy, Debug)]
pub enum Event {
    BusRead { start_ns: u64, end_ns: u64, ok: bool, raw: [[u8; 12]; NUM_JOINTS + 1] },
    KneeRead { start_ns: u64, end_ns: u64, ok: bool, raw: [[u8; 32]; 2] },
    Policy { observation: [f32; OBS_LEN], action: [f32; ACTION_LEN], scale: f64, gain: u16 },
    Write { start_ns: u64, end_ns: u64, ok: bool, positions: [f64; NUM_JOINTS] },
}

#[derive(Clone, Copy, Debug)]
struct Stamped { tick_ns: u64, event_ns: u64, event: Event }

struct Sink {
    tx: SyncSender<Stamped>,
    dropped: AtomicU64,
    active: AtomicBool,
}

impl Sink {
    fn try_record(&self, _event: Stamped) {}
}

fn write_record(out: &mut impl Write, _record: &Stamped) -> std::io::Result<()> {
    writeln!(out, "{{}}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::TryRecvError;

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
        let sink = Sink { tx, dropped: AtomicU64::new(0), active: AtomicBool::new(true) };
        let sample = Stamped { tick_ns: 1, event_ns: 2, event: Event::Write {
            start_ns: 1, end_ns: 2, ok: true, positions: [0.0; NUM_JOINTS] } };
        sink.try_record(sample);
        sink.try_record(Stamped { tick_ns: 3, ..sample });
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(rx.try_recv().unwrap().tick_ns, 1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }
}
