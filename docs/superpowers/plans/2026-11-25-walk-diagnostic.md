# One-second walking capture

Keep commit 936bee3's calibration and all policy/safety parameters unchanged. The operator runs
one `duckwalk 0.2 0 0 1` only after capture validation; software preparation never enables motion.

1. Add opt-in diagnostic events at the existing combined bus read, actual policy inference,
   and final bus write. Preserve raw 12-byte IMU and all 15 motor blocks, 61 float observations,
   14 raw actions, applied scale/gain and final radian targets with write result. Use bounded
   nonblocking enqueue and a background JSONL writer; log drops and failures explicitly.
2. With a separate opt-in, read registers 116–147 for knees 12 and 22 after each primary read.
   Record Goal, PWM/current/velocity/position, trajectories, voltage and temperature as raw bytes.
   Timestamp both transactions separately; an optional read failure must not invalidate the
   primary sensor sample. No second process opens the serial bus.
3. Verify serialization fidelity and queue overflow behavior with failing tests first, run
   relevant crate tests and the workspace check via the existing GitHub Actions build path.
   Build the aarch64 binary without changing homing offsets or policy behavior.
4. Once the operator confirms the body is supported, back up the current executable and deploy
   the diagnostic build. Validate held-only logging, timestamps, no lost records, no new missed
   ticks, primary read gaps and temperatures. Do not run if diagnostics disturb the 20-ms loop.
5. Record IPC state/system health alongside the internal trace and operator's side-view video.
   Stop sequence: speed zero, operator supports weight, then enable --off. Preserve at least
   several seconds before/after motion; download artifacts and analyze the earliest divergence.

Limits: PWM/current are measured for every servo at control rate; extra internal trajectory
readbacks cover both knees, not all joints. Servo supply/temperature for other joints remain
the ordinary slow samples. A successful write reports host transmission, not servo receipt;
knee Goal readback is independent evidence of receipt. Historical captures have a different
binary/calibration and are contextual evidence rather than identical-condition comparisons.
