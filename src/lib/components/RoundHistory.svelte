<script lang="ts">
  import { timerState } from '$lib/stores/timer';
  import * as m from '$paraglide/messages.js';

  let { isCompact = false }: { isCompact?: boolean } = $props();

  function formatTime(timestamp: number): string {
    const date = new Date(timestamp);
    return `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
  }
</script>

{#if $timerState.recent_round_changes.length > 0}
  <ol class="round-history" class:compact={isCompact}>
    {#each $timerState.recent_round_changes as change}
      <li>
        <span
          aria-label={change.round_type === 'work'
            ? m.round_label_work()
            : change.round_type === 'short-break'
              ? m.round_label_short_break()
              : m.round_label_long_break()}>{change.round_type === 'work' ? 'W' : 'R'}:</span
        >
        <time datetime={new Date(change.changed_at_ms).toISOString()}
          >{formatTime(change.changed_at_ms)}</time
        >
      </li>
    {/each}
  </ol>
{/if}

<style>
  .round-history {
    position: absolute;
    top: 4px;
    left: 12px;
    list-style: none;
    font-size: 11px;
    line-height: 16px;
    font-variant-numeric: tabular-nums;
    color: var(--color-foreground-darker, var(--color-foreground));
    pointer-events: none;
  }

  li {
    display: grid;
    grid-template-columns: 1.25em max-content;
    gap: 4px;
    white-space: nowrap;
  }

  li > span {
    text-align: right;
  }

  .compact {
    top: 0;
    left: 8px;
    font-size: 10px;
    line-height: 12px;
  }

  li:last-child {
    color: var(--color-foreground);
  }
</style>
