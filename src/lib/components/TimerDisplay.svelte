<script lang="ts">
  // Displays the remaining time (MM:SS).
  import type { TimerState } from '$lib/types';
  import { timerAdjustTime } from '$lib/ipc';
  import Tooltip from './Tooltip.svelte';
  import * as m from '$paraglide/messages.js';

  interface Props {
    state: TimerState;
  }

  let { state }: Props = $props();

  let remaining = $derived(Math.max(0, state.total_secs - state.elapsed_secs));
  let minutes = $derived(Math.floor(remaining / 60));
  let seconds = $derived(remaining % 60);
  let display = $derived(`${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`);
</script>

<div class="display">
  <span class="time">{display}</span>
  <div class="time-adjustments">
    <Tooltip text={m.tooltip_subtract_minute()} placement="below">
      <button onclick={() => timerAdjustTime(-60)} aria-label={m.tooltip_subtract_minute()}>
        {m.timer_subtract_minute()}
      </button>
    </Tooltip>
    <Tooltip text={m.tooltip_add_minute()} placement="below">
      <button onclick={() => timerAdjustTime(60)} aria-label={m.tooltip_add_minute()}>
        {m.timer_add_minute()}
      </button>
    </Tooltip>
  </div>
</div>

<style>
  .display {
    /* Fill the dial-stack and flex-center the time. */
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    pointer-events: none;
  }

  .time {
    font-family: 'Mona Sans Mono', monospace;
    font-size: 2.8rem;
    font-weight: 300;
    font-stretch: 85%;
    letter-spacing: -0.02em;
    color: var(--color-foreground);
  }

  .time-adjustments {
    position: absolute;
    top: calc(50% + 34px);
    display: flex;
    gap: 8px;
    pointer-events: auto;
  }

  .time-adjustments button {
    min-width: 64px;
    height: 32px;
    padding: 0 8px;
    border: none;
    border-radius: 5px;
    background: none;
    color: var(--color-foreground);
    font: inherit;
    font-size: 0.8125rem;
    font-weight: 400;
    cursor: pointer;
    transition:
      color var(--transition-default),
      background var(--transition-default);
  }

  .time-adjustments button:hover {
    color: var(--color-current-round);
    background: var(--color-hover);
  }

  .time-adjustments button:focus-visible {
    outline: 2px solid var(--color-current-round);
    outline-offset: 2px;
  }
</style>
