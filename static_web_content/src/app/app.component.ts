import { Component, OnDestroy, OnInit, signal } from '@angular/core';
import { environment } from '../environments/environment';

interface StatusEntry {
  status?: string;
  as_of?: number;
}

interface WebpageStatus {
  openai_text_generator_status?: StatusEntry;
  qwen_tts_speaker_status?: StatusEntry;
}

@Component({
  selector: 'app-root',
  standalone: true,
  styles: [`
    .container {
      width: 100%;
      max-width: 520px;
      padding: 2.5rem 2rem;
      text-align: center;
    }
    h1 {
      font-size: 1.5rem;
      font-weight: 700;
      color: var(--accent);
      letter-spacing: -0.02em;
      margin-bottom: 0.25rem;
    }
    .status-waiting {
      display: block;
      color: #888;
      font-size: 0.85rem;
      font-family: ui-monospace, monospace;
      margin-bottom: 1rem;
    }
    .player-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 1.5rem;
      margin-bottom: 1.5rem;
    }
    audio {
      width: 100%;
      border-radius: 10px;
      outline: none;
    }
    .footer {
      margin-top: 1.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.72rem;
      color: #555;
    }
    .footer .source {
      color: var(--accent);
    }
    .keepalive-status {
      font-size: 0.7rem;
      color: var(--accent);
      min-height: 1rem;
    }
    .keepalive-status .active {
      color: #22c55e;
    }
    .section-divider {
      display: flex;
      align-items: center;
      margin: 2rem 0;
      padding: 0 0.25rem;
    }
    .section-divider::before,
    .section-divider::after {
      content: '';
      flex: 1;
      height: 1px;
      background: var(--border);
    }
    .section-divider-line {
      display: inline-block;
      padding: 0 1rem;
      color: var(--text-muted);
      font-size: 0.85rem;
      text-transform: uppercase;
      letter-spacing: 0.15em;
      user-select: none;
    }
    .status-messages-section {
      text-align: left;
    }
    .status-card {
      background: var(--card);
      border: none;
      border-radius: 20px;
      padding: 2rem 1.75rem;
      margin-bottom: 1.5rem;
    }
    .status-card h2 {
      font-size: 0.95rem;
      font-weight: 600;
      color: var(--accent);
      margin-bottom: 0.5rem;
      letter-spacing: 0.01em;
    }
    .status-list {
      list-style: none;
      padding: 0;
    }
    .status-item {
      list-style: none;
      padding: 1rem 0;
      background: none;
      border: none;
      border-radius: 0;
    }
    .status-text {
      font-family: ui-monospace, monospace;
      font-size: 0.75rem;
      color: #888;
    }
  `],
  template: `
    <div class="container">
      <h1>TTStream Audio</h1>
      <p class="subtitle">Powered by icecast+HLS</p>
      <div class="player-card">
        <audio controls preload="auto" [src]="streamUrl"></audio>
      </div>
      <div class="section-divider">
        <span class="section-divider-line">Component Status</span>
      </div>
      <div class="status-messages-section">
        <div class="status-card">
          <h2>OpenAI Text Generator Status</h2>
          @if (status().openai_text_generator_status?.status; as _s) {
            <ul class="status-list">
              @let entry = status().openai_text_generator_status!;
              <li class="status-item status-card">
                <span class="status-text">
                  {{ entry.status }}
                  @if (entry.as_of !== undefined) {
                    <span class="status-text time-ago">{{ timeAgo(entry.as_of) }}</span>
                  }
                </span>
              </li>
            </ul>
          } @else {
            <span class="status-waiting">Waiting for status...</span>
          }
        </div>
        <div class="status-card">
          <h2>Qwen TTS Speaker Status</h2>
          @if (status().qwen_tts_speaker_status?.status; as _s) {
            <ul class="status-list">
              @let entry = status().qwen_tts_speaker_status!;
              <li class="status-item status-card">
                <span class="status-text">
                  {{ entry.status }}
                  @if (entry.as_of !== undefined) {
                    <span class="status-text time-ago">{{ timeAgo(entry.as_of) }}</span>
                  }
                </span>
              </li>
            </ul>
          } @else {
            <span class="status-waiting">Waiting for status...</span>
          }
        </div>
      </div>
      <div class="footer">
        <span>Stream: <span class="source">{{ streamUrl }}</span></span>
        <span class="keepalive-status">
          <span [class.active]="keepaliveActive()">●</span>
          Keepalive: active
        </span>
      </div>
    </div>
  `,
})
export class AppComponent implements OnInit, OnDestroy {
  readonly streamUrl = environment.streamUrl;

  readonly status = signal<WebpageStatus>({});
  readonly keepaliveActive = signal(true);
  private statusTimer: ReturnType<typeof setInterval> | undefined;
  private keepaliveTimer: ReturnType<typeof setInterval> | undefined;

  timeAgo(asOf: number): string {
    const secondsAgo = Math.max(0, Math.floor(Date.now() / 1000 - asOf));
    return secondsAgo === 0 ? ' · Now' : ` · ${secondsAgo}s ago`;
  }

  ngOnInit(): void {
    this.statusTimer = setInterval(() => this.refreshStatus(), environment.statusIntervalMs);
    this.keepaliveTimer = setInterval(() => this.pingOrchestrator(), environment.keepaliveIntervalMs);
    this.pingOrchestrator();
  }

  ngOnDestroy(): void {
    if (this.statusTimer) clearInterval(this.statusTimer);
    if (this.keepaliveTimer) clearInterval(this.keepaliveTimer);
  }

  private async refreshStatus(): Promise<void> {
    try {
      const response = await fetch(`${environment.orchestratorUrl}/webpage_status.json`, { method: 'GET' });
      const json = (await response.json()) as WebpageStatus;
      this.status.set(json);
    } catch {
      this.status.set({});
    }
  }

  private pingOrchestrator(): void {
    fetch(`${environment.orchestratorUrl}/webpage-keepalive`, { method: 'POST' })
      .then(() => this.keepaliveActive.set(true))
      .catch(() => this.keepaliveActive.set(false));
  }
}
