import { useSyncExternalStore } from "react";

// Tiny global store tracking in-flight queries so the top bar can show a live "querying" status.
const store = { active: 0, listeners: new Set<() => void>() };

function emit() {
  for (const listener of store.listeners) listener();
}

export function beginQuery(): void {
  store.active += 1;
  emit();
}

export function endQuery(): void {
  store.active = Math.max(0, store.active - 1);
  emit();
}

export function useQueryActive(): boolean {
  return useSyncExternalStore(
    (callback) => {
      store.listeners.add(callback);
      return () => store.listeners.delete(callback);
    },
    () => store.active > 0,
    () => false,
  );
}
