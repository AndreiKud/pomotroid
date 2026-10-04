<script lang="ts">
  import { onDestroy, tick, type Snippet } from 'svelte';

  interface Props {
    text: string;
    delay?: number;
    placement?: 'above' | 'below';
    children: Snippet;
  }

  let { text, delay = 600, placement = 'above', children }: Props = $props();

  let visible = $state(false);
  let positioned = $state(false);
  let actualPlacement = $state<'above' | 'below'>('above');
  let timer: ReturnType<typeof setTimeout> | undefined;
  let wrapper = $state<HTMLSpanElement | undefined>(undefined);
  let tooltipEl = $state<HTMLSpanElement | undefined>(undefined);
  const tooltipId = `tooltip-${Math.random().toString(36).slice(2, 9)}`;

  async function show() {
    // Already showing or timer running — nothing to do.
    if (visible || timer !== undefined) return;
    const run = async () => {
      timer = undefined;
      visible = true;
      await tick();
      const element = tooltipEl;
      // Font substitution can change both the line count and the anchor offset.
      await document.fonts.ready;
      if (!visible || tooltipEl !== element) return;
      updatePosition();
    };
    if (delay === 0) {
      await run();
    } else {
      timer = setTimeout(run, delay);
    }
  }

  function hide() {
    clearTimeout(timer);
    timer = undefined;
    visible = false;
    positioned = false;
  }

  onDestroy(hide);

  function attachTooltip(node: HTMLSpanElement) {
    // Keep viewport coordinates independent of the timer's zoom and transforms.
    document.body.appendChild(node);
    const observer = new ResizeObserver(() => {
      if (positioned) updatePosition();
    });
    observer.observe(node);
    if (wrapper) observer.observe(wrapper);
    window.addEventListener('resize', updatePosition);
    window.addEventListener('scroll', updatePosition, true);
    return {
      destroy() {
        observer.disconnect();
        window.removeEventListener('resize', updatePosition);
        window.removeEventListener('scroll', updatePosition, true);
        node.remove();
      },
    };
  }

  function updatePosition() {
    if (!visible || !wrapper || !tooltipEl) return;
    const wRect = wrapper.getBoundingClientRect();
    const centerX = wRect.left + wRect.width / 2;
    const pad = 8;
    const gap = 8;

    // Only wrap labels that exceed the available width, avoiding an intrinsic
    // text-width rounding difference turning a short label into two lines.
    const maxWidth = Math.min(240, window.innerWidth - pad * 2);
    tooltipEl.style.maxWidth = 'none';
    tooltipEl.style.width = 'max-content';
    tooltipEl.style.whiteSpace = 'nowrap';
    const naturalWidth = Math.ceil(tooltipEl.getBoundingClientRect().width);
    tooltipEl.style.width = `${Math.min(naturalWidth, maxWidth)}px`;
    tooltipEl.style.maxWidth = `${maxWidth}px`;
    tooltipEl.style.whiteSpace = naturalWidth > maxWidth ? 'normal' : 'nowrap';
    const tRect = tooltipEl.getBoundingClientRect();

    const roomAbove = wRect.top - gap - pad;
    const roomBelow = window.innerHeight - wRect.bottom - gap - pad;
    actualPlacement = placement;
    if (placement === 'above' && tRect.height > roomAbove && roomBelow > roomAbove) {
      actualPlacement = 'below';
    } else if (placement === 'below' && tRect.height > roomBelow && roomAbove > roomBelow) {
      actualPlacement = 'above';
    }

    let top: number;
    if (actualPlacement === 'below') {
      top = wRect.bottom + gap;
    } else {
      top = wRect.top - gap - tRect.height;
    }
    top = Math.max(pad, Math.min(top, window.innerHeight - pad - tRect.height));

    // Horizontal: center on trigger, clamped within viewport.
    let left = centerX - tRect.width / 2;
    if (left < pad) left = pad;
    if (left + tRect.width > window.innerWidth - pad) {
      left = window.innerWidth - pad - tRect.width;
    }

    // Arrow offset: always points at the trigger's horizontal center.
    const arrowLeft = Math.max(pad, Math.min(centerX - left, tRect.width - pad));
    tooltipEl.style.top = `${top}px`;
    tooltipEl.style.left = `${left}px`;
    tooltipEl.style.setProperty('--arrow-left', `${arrowLeft}px`);
    positioned = true;
  }
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<span
  class="tooltip-wrapper"
  bind:this={wrapper}
  onpointerenter={show}
  onpointermove={show}
  onpointerleave={hide}
  aria-describedby={visible ? tooltipId : undefined}
>
  {@render children()}
  {#if visible}
    <span
      bind:this={tooltipEl}
      use:attachTooltip
      class="tooltip"
      class:below={actualPlacement === 'below'}
      class:positioned
      id={tooltipId}
      role="tooltip">{text}</span
    >
  {/if}
</span>

<style>
  .tooltip-wrapper {
    position: relative;
    display: inline-flex;
    align-items: center;
    justify-content: center;
  }

  .tooltip {
    --tooltip-bg: var(
      --color-background-light,
      color-mix(in oklch, var(--color-foreground) 10%, var(--color-background))
    );
    position: fixed;
    top: 0;
    left: 0;
    background: var(--tooltip-bg);
    color: var(--color-foreground);
    font-size: 0.72rem;
    line-height: 1.4;
    padding: 5px 9px;
    border-radius: 4px;
    width: max-content;
    max-width: min(240px, calc(100vw - 16px));
    white-space: nowrap;
    overflow-wrap: anywhere;
    text-align: center;
    pointer-events: none;
    z-index: 9999;
    box-shadow: 0 2px 8px color-mix(in oklch, black 30%, transparent);
    border: 1px solid color-mix(in oklch, var(--color-foreground) 12%, transparent);
    /* Hidden until JS positions it to avoid a 1-frame flash at top-left. */
    visibility: hidden;
  }

  .tooltip.positioned {
    visibility: visible;
  }

  /* Arrow pointing down — tooltip is above the trigger */
  .tooltip::after {
    content: '';
    position: absolute;
    top: 100%;
    left: var(--arrow-left, 50%);
    transform: translateX(-50%);
    border: 5px solid transparent;
    border-top-color: var(--tooltip-bg);
  }

  /* Arrow pointing up — tooltip is below the trigger */
  .tooltip.below::after {
    top: auto;
    bottom: 100%;
    border-top-color: transparent;
    border-bottom-color: var(--tooltip-bg);
  }
</style>
