pub mod engine;
pub mod sequence;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager};

use crate::audio::{AudioCue, AudioManager};
use crate::db::{queries, DbState};
use crate::settings::Settings;
use crate::tray::{self, TrayState};
use crate::websocket::{self, WsState};

use engine::{EngineHandle, TimerCommand, TimerEvent};
use sequence::{RoundType, SequenceState};

// ---------------------------------------------------------------------------
// Snapshot — serialized to JSON for the frontend
// ---------------------------------------------------------------------------

/// Full timer state snapshot. Sent as the payload of Tauri events and
/// returned by the `timer_get_state` IPC command.
#[derive(Debug, Clone, Serialize)]
pub struct TimerSnapshot {
    /// "work" | "short-break" | "long-break"
    pub round_type: String,
    /// Round type that was active before this one. Empty string on the first round of a session.
    pub previous_round_type: String,
    pub elapsed_secs: u32,
    pub total_secs: u32,
    pub is_running: bool,
    /// Also true when paused before the first tick or suspended by the system.
    pub is_paused: bool,
    pub work_round_number: u32,
    pub work_rounds_total: u32,
    pub can_go_back: bool,
    /// Session position lets the UI keep counting when long breaks are disabled.
    pub session_work_count: u32,
}

// ---------------------------------------------------------------------------
// Shared mutable state between the controller and the event-listener thread
// ---------------------------------------------------------------------------

struct TimerShared {
    elapsed_secs: u32,
    total_secs: u32,
    is_running: bool,
    is_paused: bool,
}

// ---------------------------------------------------------------------------
// TimerController — public API registered as Tauri state
// ---------------------------------------------------------------------------

pub struct TimerController {
    engine: EngineHandle,
    sequence: Arc<Mutex<SequenceState>>,
    settings: Arc<Mutex<Settings>>,
    shared: Arc<Mutex<TimerShared>>,
    /// Kept alive so TrayState is not dropped if lib.rs forgets its copy.
    #[allow(dead_code)]
    tray: Arc<TrayState>,
}

impl TimerController {
    /// Construct and start the background threads.
    /// Call once from `lib.rs` during Tauri `setup`.
    pub fn new(
        app: AppHandle,
        settings: Settings,
        tray: Arc<TrayState>,
        db: DbState,
    ) -> Self {
        let seq = SequenceState::new(settings.long_break_interval);
        let duration = seq.current_duration_secs(&settings);

        let (engine, event_rx) = engine::spawn(duration, Duration::from_secs(1));

        let sequence = Arc::new(Mutex::new(seq));
        let settings_arc = Arc::new(Mutex::new(settings));
        let shared = Arc::new(Mutex::new(TimerShared {
            elapsed_secs: 0,
            total_secs: duration,
            is_running: false,
            is_paused: false,
        }));

        // Clone handles for the event-listener thread.
        let seq_thread = Arc::clone(&sequence);
        let settings_thread = Arc::clone(&settings_arc);
        let shared_thread = Arc::clone(&shared);
        let engine_thread = engine.clone();
        let tray_thread = Arc::clone(&tray);

        std::thread::Builder::new()
            .name("timer-events".to_string())
            .spawn(move || {
                listen_events(
                    app,
                    event_rx,
                    ListenContext {
                        sequence: seq_thread,
                        settings: settings_thread,
                        shared: shared_thread,
                        engine: engine_thread,
                        tray: tray_thread,
                        db,
                    },
                );
            })
            .expect("failed to spawn timer event listener");

        Self {
            engine,
            sequence,
            settings: settings_arc,
            shared,
            tray,
        }
    }

    // --- Commands ---

    /// Toggle: start a fresh timer if idle, resume if paused, pause if running.
    pub fn toggle(&self) {
        let s = self.shared.lock().unwrap();
        if s.is_running {
            log::info!("[timer] pause");
            self.engine.send(TimerCommand::Pause);
        } else if s.is_paused {
            log::info!("[timer] resume");
            self.engine.send(TimerCommand::Resume);
        } else {
            log::info!("[timer] start");
            self.engine.send(TimerCommand::Start);
        }
    }

    pub fn reset(&self) {
        log::info!("[timer] reset");
        self.sequence.lock().unwrap().reset();
        // Send only Reset — the event listener's Reset handler will follow up
        // with Prime once the engine is confirmed Idle. Sending a duration
        // update here first would race the Reset and can leave the UI stale.
        self.engine.send(TimerCommand::Reset);
    }

    /// Restart only the current round's timer without touching the sequence.
    /// Round type, round number, and position in the work/break cycle are all
    /// preserved — only the elapsed time is zeroed.
    pub fn restart_round(&self) {
        log::info!("[timer] restart round");
        self.engine.send(TimerCommand::Reset);
    }

    pub fn skip(&self) {
        log::info!("[timer] skip");
        self.engine.send(TimerCommand::Skip);
    }

    pub fn previous(&self) {
        log::info!("[timer] previous round");
        self.engine.send(TimerCommand::Previous);
    }

    pub fn adjust_time(&self, delta_secs: i32) {
        self.engine.send(TimerCommand::Adjust { delta_secs });
    }

    pub fn suspend(&self) {
        self.engine.send(TimerCommand::Suspend);
    }

    pub fn wake_resume(&self) {
        self.engine.send(TimerCommand::WakeResume);
    }

    /// Update the duration for the current round when settings change.
    /// Only takes effect after the next Start/Resume (current countdown is not interrupted).
    pub fn reconfigure(&self) {
        let duration = {
            let seq = self.sequence.lock().unwrap();
            let settings = self.settings.lock().unwrap();
            seq.current_duration_secs(&settings)
        };
        self.engine.send(TimerCommand::Reconfigure { duration_secs: duration });
    }

    // --- Query ---

    pub fn get_snapshot(&self) -> TimerSnapshot {
        build_snapshot(&self.sequence, &self.shared)
    }

    /// Apply new settings values. Updates the in-memory copy and, if the
    /// timer is idle (not running and no elapsed progress), reconfigures the
    /// engine so the next Start uses the new duration.
    ///
    /// When the timer is running or paused, the current countdown is left
    /// untouched; the new duration takes effect at the start of the next
    /// round or after a manual reset.  Sending Reconfigure to a running
    /// engine transitions it to Idle, which would freeze the timer.
    pub fn apply_settings(&self, new: Settings) {
        // Sync work_rounds_total so the round counter and advance() logic both
        // reflect the new long_break_interval immediately.
        let duration_changed = {
            let mut seq = self.sequence.lock().unwrap();
            let mut settings = self.settings.lock().unwrap();
            let changed = seq.current_duration_secs(&settings) != seq.current_duration_secs(&new);
            seq.work_rounds_total = new.long_break_interval;
            *settings = new;
            changed
        };
        let s = self.shared.lock().unwrap();
        let is_idle = !s.is_running && !s.is_paused;
        drop(s);
        // Volume or theme changes must not discard an idle round's extra minutes.
        if is_idle && duration_changed {
            self.reconfigure();
        }
    }
}

// ---------------------------------------------------------------------------
// Background event listener thread
// ---------------------------------------------------------------------------

struct ListenContext {
    sequence: Arc<Mutex<SequenceState>>,
    settings: Arc<Mutex<Settings>>,
    shared: Arc<Mutex<TimerShared>>,
    engine: EngineHandle,
    tray: Arc<TrayState>,
    db: DbState,
}

fn listen_events(
    app: AppHandle,
    event_rx: std::sync::mpsc::Receiver<TimerEvent>,
    ctx: ListenContext,
) {
    let ListenContext { sequence, settings, shared, engine, tray, db } = ctx;
    // Track last tray progress to throttle redraws to ≥ 1% delta.
    let mut last_tray_progress: f32 = -1.0;
    // Active session row ID for recording (None = not started yet).
    let mut current_session_id: Option<i64> = None;

    while let Ok(event) = event_rx.recv() {
        match event {
            TimerEvent::Started { total_secs } => {
                log::info!("[timer] started total={total_secs}s");
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = 0;
                    s.total_secs = total_secs;
                    s.is_running = true;
                    s.is_paused = false;
                }
                let _ = app.emit("timer:started", serde_json::json!({ "total_secs": total_secs }));
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_started(&ws, total_secs);
                }
                tray::update_menu_items(&tray, true, false);
            }

            TimerEvent::Tick { elapsed_secs, total_secs } => {
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = elapsed_secs;
                    s.total_secs = total_secs;
                    s.is_running = true;
                    s.is_paused = false;
                }
                let _ = app.emit(
                    "timer:tick",
                    serde_json::json!({ "elapsed_secs": elapsed_secs, "total_secs": total_secs }),
                );

                // --- Session recording: start on first tick of a new round ---
                if elapsed_secs == 1 && current_session_id.is_none() {
                    let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                    if let Ok(conn) = db.lock() {
                        match queries::insert_session(&conn, &rt, total_secs) {
                            Ok(id) => current_session_id = Some(id),
                            Err(e) => log::error!("[timer] failed to record session: {e}"),
                        }
                    }
                }

                // --- Tick sound ---
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                if let Some(audio) = app.try_state::<Arc<AudioManager>>() {
                    if audio.tick_enabled_for(&rt) {
                        audio.play_cue(AudioCue::Tick);
                    }
                }

                // Update tray arc — throttle to 1% visual change.
                let progress = if total_secs > 0 {
                    elapsed_secs as f32 / total_secs as f32
                } else {
                    0.0
                };
                if (progress - last_tray_progress).abs() >= 0.01 {
                    tray::update_icon(&tray, &rt, false, progress);
                    last_tray_progress = progress;
                }
            }

            TimerEvent::DurationChanged { elapsed_secs, total_secs } => {
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = elapsed_secs;
                    s.total_secs = total_secs;
                }
                if let Some(id) = current_session_id {
                    if let Ok(conn) = db.lock() {
                        if let Err(e) = queries::update_session_duration(&conn, id, total_secs) {
                            log::error!("[timer] failed to update session duration: {e}");
                        }
                    }
                }
                let snapshot = build_snapshot(&sequence, &shared);
                let progress = if total_secs > 0 { elapsed_secs as f32 / total_secs as f32 } else { 0.0 };
                tray::update_icon(&tray, &snapshot.round_type, snapshot.is_paused, progress);
                last_tray_progress = progress;
                let _ = app.emit("timer:duration-changed", &snapshot);
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_duration_changed(&ws, snapshot);
                }
            }

            TimerEvent::Complete { .. } | TimerEvent::Previous { .. } => {
                let (was_skipped, confirmation) = match event {
                    TimerEvent::Complete { skipped } => (skipped, None),
                    TimerEvent::Previous { confirm } => (true, Some(confirm)),
                    _ => unreachable!(),
                };
                let completed_round = sequence.lock().unwrap().current_round.as_str().to_string();

                let transition = {
                    let mut seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    let next = if confirmation.is_some() {
                        seq.retreat(&s)
                    } else {
                        Some(seq.advance(&s))
                    };
                    next.map(|(round, duration)| {
                        (round, duration, should_auto_start(round, was_skipped, &s))
                    })
                };
                let Some((next_round, next_duration, should_auto)) = transition else {
                    if let Some(confirm) = confirmation {
                        let _ = confirm.send(false);
                    }
                    continue;
                };
                log::info!(
                    "[timer] round complete type={completed_round} skipped={was_skipped}"
                );

                // --- Session recording: mark the completed round ---
                if let Some(session_id) = current_session_id.take() {
                    if let Ok(conn) = db.lock() {
                        let _ = queries::complete_session(&conn, session_id, !was_skipped);
                    }
                }

                // Reset shared state for the new round.
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = 0;
                    s.total_secs = next_duration;
                    s.is_running = should_auto;
                    s.is_paused = false;
                }

                // Arm the next round's duration without risking a late
                // reconfigure that kicks a freshly-started timer back to Idle.
                engine.send(TimerCommand::Prime {
                    duration_secs: next_duration,
                });

                // Emit round-change with the new snapshot.
                let snapshot = build_snapshot(&sequence, &shared);
                let _ = app.emit("timer:round-change", snapshot);

                // Desktop notifications are dispatched by the frontend via the
                // notification_show command after receiving the timer:round-change
                // event, so translated strings can be used.

                // Audio alert for the new round.
                if let Some(audio) = app.try_state::<Arc<AudioManager>>() {
                    let cue = match next_round {
                        RoundType::Work => AudioCue::WorkAlert,
                        RoundType::ShortBreak => AudioCue::ShortBreakAlert,
                        RoundType::LongBreak => AudioCue::LongBreakAlert,
                    };
                    audio.play_cue(cue);
                }

                // Lower-priority-during-breaks: when always_on_top is on and
                // break_always_on_top is enabled, disable always-on-top for
                // breaks and restore it when work resumes.
                let (always_on_top, break_always_on_top) = {
                    let s = settings.lock().unwrap();
                    (s.always_on_top, s.break_always_on_top)
                };
                if always_on_top {
                    if let Some(window) = app.get_webview_window("main") {
                        let is_break = next_round != RoundType::Work;
                        let _ = window.set_always_on_top(!(break_always_on_top && is_break));
                    }
                }

                // Update tray to reflect new round type and reset progress.
                // Use -1.0 (same as initialisation and Reset) so the very
                // first tick of the new round always passes the ≥1% threshold,
                // regardless of how long the round is.  Using 0.0 here caused
                // a ≥15-second blank period before the arc started animating.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                tray::update_icon(&tray, &rt, false, 0.0);
                last_tray_progress = -1.0;

                // Broadcast round-change to any connected WebSocket clients.
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    let snap = build_snapshot(&sequence, &shared);
                    websocket::broadcast_round_change(&ws, snap);
                }

                if should_auto {
                    log::debug!("[timer] auto-starting {}", next_round.as_str());
                    engine.send(TimerCommand::Start);
                } else {
                    // Timer is idle waiting for the user to start the new round.
                    // Reset the tray menu to "Start" so it doesn't keep showing
                    // "Pause" from the round that just completed.
                    tray::update_menu_items(&tray, false, false);
                }
                if let Some(confirm) = confirmation {
                    let _ = confirm.send(true);
                }
            }

            TimerEvent::Paused { elapsed_secs } => {
                log::info!("[timer] paused elapsed={elapsed_secs}s");
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = elapsed_secs;
                    s.is_running = false;
                    s.is_paused = true;
                }
                let _ = app.emit("timer:paused", serde_json::json!({ "elapsed_secs": elapsed_secs }));
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_paused(&ws, elapsed_secs);
                }

                // Show pause bars in tray.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                let total = shared.lock().unwrap().total_secs;
                let progress = if total > 0 { elapsed_secs as f32 / total as f32 } else { 0.0 };
                tray::update_icon(&tray, &rt, true, progress);
                tray::update_menu_items(&tray, false, true);
            }

            TimerEvent::Resumed { elapsed_secs } => {
                log::info!("[timer] resumed elapsed={elapsed_secs}s");
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = elapsed_secs;
                    s.is_running = true;
                    s.is_paused = false;
                }
                let _ = app.emit("timer:resumed", serde_json::json!({ "elapsed_secs": elapsed_secs }));
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_resumed(&ws, elapsed_secs);
                }

                // Restore arc in tray.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                let total = shared.lock().unwrap().total_secs;
                let progress = if total > 0 { elapsed_secs as f32 / total as f32 } else { 0.0 };
                tray::update_icon(&tray, &rt, false, progress);
                last_tray_progress = progress;
                tray::update_menu_items(&tray, true, false);
            }

            TimerEvent::Reset => {
                log::debug!("[timer] idle");
                // Abandon the active session (leave DB row as-is).
                current_session_id = None;

                let duration = {
                    let seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    seq.current_duration_secs(&s)
                };
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = 0;
                    s.total_secs = duration;
                    s.is_running = false;
                    s.is_paused = false;
                }
                let snapshot = build_snapshot(&sequence, &shared);
                let _ = app.emit("timer:reset", snapshot);
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_reset(&ws);
                }

                // Prime the engine with the current round's duration so the
                // next Start uses the correct (possibly settings-updated)
                // total. Using the lighter-weight command here avoids a race
                // where a fast user click on Start is immediately clobbered by
                // a late follow-up duration update.
                engine.send(TimerCommand::Prime { duration_secs: duration });

                // Reset tray to idle (empty arc).
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                tray::update_icon(&tray, &rt, false, 0.0);
                last_tray_progress = -1.0;
                tray::update_menu_items(&tray, false, false);
            }

            TimerEvent::Suspended { elapsed_secs } => {
                log::info!("[timer] suspended by system elapsed={elapsed_secs}s");
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = elapsed_secs;
                    s.is_running = false;
                    s.is_paused = true;
                }
                let _ = app.emit(
                    "timer:suspended",
                    serde_json::json!({ "elapsed_secs": elapsed_secs }),
                );

                // Show pause bars while suspended.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                let total = shared.lock().unwrap().total_secs;
                let progress = if total > 0 { elapsed_secs as f32 / total as f32 } else { 0.0 };
                tray::update_icon(&tray, &rt, true, progress);
            }
        }
    }
}

fn should_auto_start(round: RoundType, manual: bool, settings: &Settings) -> bool {
    manual
        || match round {
            RoundType::Work => settings.auto_start_work,
            _ => settings.auto_start_break,
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjustments_survive_unrelated_settings_and_pause_before_first_tick() {
        let settings = Settings::default();
        let base_duration = settings.time_work_secs;
        let (engine, rx) = engine::spawn(base_duration + 120, Duration::from_secs(1));
        let controller = TimerController {
            engine,
            sequence: Arc::new(Mutex::new(SequenceState::new(settings.long_break_interval))),
            settings: Arc::new(Mutex::new(settings.clone())),
            shared: Arc::new(Mutex::new(TimerShared {
                elapsed_secs: 0,
                total_secs: base_duration + 120,
                is_running: false,
                is_paused: false,
            })),
            tray: TrayState::new(),
        };
        let mut updated = settings.clone();
        updated.volume = 0.2;
        controller.apply_settings(updated.clone());
        assert!(rx.recv_timeout(Duration::from_millis(40)).is_err());
        assert_eq!(controller.get_snapshot().total_secs, base_duration + 120);
        controller.toggle();
        assert!(matches!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), TimerEvent::Started { total_secs } if total_secs == base_duration + 120));
        controller.engine.send(TimerCommand::Pause);
        assert!(matches!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), TimerEvent::Paused { elapsed_secs: 0 }));
        controller.shared.lock().unwrap().is_paused = true;
        assert!(controller.get_snapshot().is_paused);
        updated.time_work_secs += 60;
        controller.apply_settings(updated);
        assert!(rx.recv_timeout(Duration::from_millis(40)).is_err());
        controller.toggle();
        assert!(matches!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), TimerEvent::Resumed { elapsed_secs: 0 }));
        assert_eq!(controller.get_snapshot().total_secs, base_duration + 120);
        controller.engine.send(TimerCommand::Shutdown);
    }

    #[test]
    fn manual_navigation_starts_but_completion_respects_preferences() {
        for auto_start_work in [false, true] {
            for auto_start_break in [false, true] {
                let settings = Settings {
                    auto_start_work,
                    auto_start_break,
                    ..Settings::default()
                };
                for round in [RoundType::Work, RoundType::ShortBreak, RoundType::LongBreak] {
                    assert!(should_auto_start(round, true, &settings));
                    let expected = if round == RoundType::Work {
                        auto_start_work
                    } else {
                        auto_start_break
                    };
                    assert_eq!(should_auto_start(round, false, &settings), expected);
                }
            }
        }
    }

    #[test]
    fn restart_current_round_preserves_position_and_navigation_history() {
        let settings = Settings::default();
        let mut seq = SequenceState::new(4);
        for _ in 0..3 {
            seq.advance(&settings);
        }
        let duration = seq.current_duration_secs(&settings);
        let (engine, rx) = engine::spawn(duration, Duration::from_millis(20));
        let controller = TimerController {
            engine,
            sequence: Arc::new(Mutex::new(seq)),
            settings: Arc::new(Mutex::new(settings)),
            shared: Arc::new(Mutex::new(TimerShared {
                elapsed_secs: 10,
                total_secs: duration,
                is_running: true,
                is_paused: false,
            })),
            tray: TrayState::new(),
        };
        controller.engine.send(TimerCommand::Start);
        while !matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            TimerEvent::Tick { .. }
        ) {}
        controller.restart_round();
        loop {
            match rx.recv_timeout(Duration::from_secs(2)).unwrap() {
                TimerEvent::Reset => break,
                TimerEvent::Tick { .. } => {}
                event => panic!("unexpected event on restart: {event:?}"),
            }
        }
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        let snap = controller.get_snapshot();
        assert_eq!(snap.round_type, "short-break");
        assert_eq!(snap.work_round_number, 2);
        assert_eq!(snap.session_work_count, 2);
        assert!(snap.can_go_back);
        controller.engine.send(TimerCommand::Start);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            TimerEvent::Started { total_secs } if total_secs == duration
        ));
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            TimerEvent::Tick { elapsed_secs: 1, .. }
        ));
        controller.engine.send(TimerCommand::Shutdown);
    }
}

fn build_snapshot(
    sequence: &Arc<Mutex<SequenceState>>,
    shared: &Arc<Mutex<TimerShared>>,
) -> TimerSnapshot {
    let seq = sequence.lock().unwrap();
    let sh = shared.lock().unwrap();

    TimerSnapshot {
        round_type: seq.current_round.as_str().to_string(),
        previous_round_type: seq.previous_round.map(|r| r.as_str().to_string()).unwrap_or_default(),
        elapsed_secs: sh.elapsed_secs,
        total_secs: sh.total_secs,
        is_running: sh.is_running,
        is_paused: sh.is_paused,
        work_round_number: seq.work_round_number,
        work_rounds_total: seq.work_rounds_total,
        can_go_back: seq.can_go_back(),
        session_work_count: seq.session_work_count,
    }
}
