import { afterEach, describe, expect, test } from "bun:test";
import { NEAR_VIEWPORT_MARGIN, whenNearViewport } from "./viewport";

type ObserverCallback = (entries: { isIntersecting: boolean }[]) => void;

// The test runner has no DOM, so stand in a minimal IntersectionObserver we can drive by hand.
class FakeObserver {
  static last: FakeObserver | undefined;
  observed: Element[] = [];
  disconnects = 0;

  constructor(
    private readonly callback: ObserverCallback,
    readonly options: { root?: Element | null; rootMargin?: string },
  ) {
    FakeObserver.last = this;
  }

  observe(element: Element) {
    this.observed.push(element);
  }

  disconnect() {
    this.disconnects += 1;
  }

  report(isIntersecting: boolean) {
    this.callback([{ isIntersecting }]);
  }
}

const globals = globalThis as { IntersectionObserver?: unknown };
const element = {} as Element;

function install() {
  globals.IntersectionObserver = FakeObserver;
  FakeObserver.last = undefined;
}

afterEach(() => {
  delete globals.IntersectionObserver;
});

describe("whenNearViewport", () => {
  test("waits for the element to come near the viewport, then stops observing", () => {
    install();
    let entered = 0;
    whenNearViewport(element, () => (entered += 1));
    const observer = FakeObserver.last;
    expect(observer?.observed).toEqual([element]);
    expect(observer?.options.rootMargin).toBe(NEAR_VIEWPORT_MARGIN);

    observer?.report(false);
    expect(entered).toBe(0);

    observer?.report(true);
    expect(entered).toBe(1);
    expect(observer?.disconnects).toBe(1);
  });

  test("disposing before the element arrives cancels the observation", () => {
    install();
    let entered = 0;
    const dispose = whenNearViewport(element, () => (entered += 1));
    dispose();
    expect(FakeObserver.last?.disconnects).toBe(1);
    expect(entered).toBe(0);
  });

  test("enters immediately where IntersectionObserver is unavailable", () => {
    let entered = 0;
    whenNearViewport(element, () => (entered += 1));
    expect(entered).toBe(1);
  });
});
