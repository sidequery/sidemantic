import { Component, type ReactNode } from "react";

type State = { error?: Error };

/** Keeps a render error in one view from blanking the whole app. */
export class ErrorBoundary extends Component<{ children: ReactNode }, State> {
  state: State = {};

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  render() {
    if (this.state.error) {
      return (
        <div className="p-4">
          <div className="border border-danger/40 bg-surface p-4">
            <p className="text-sm font-semibold text-danger">Something went wrong rendering this view.</p>
            <p className="mt-1 break-words text-xs text-muted">{this.state.error.message}</p>
            <div className="mt-3 flex gap-2">
              <button
                type="button"
                onClick={() => this.setState({ error: undefined })}
                className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
              >
                Retry
              </button>
              <button
                type="button"
                onClick={() => window.location.reload()}
                className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
              >
                Reload
              </button>
            </div>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
