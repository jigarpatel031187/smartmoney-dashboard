# Smart Money Ledger — Midcap 150 · Smallcap 250

Rule-based EOD dashboard: top-10 stocks from the Nifty Midcap 150 + Smallcap 250
universe, ranked by a deterministic composite of trailing fundamental growth (50%),
smart-money accumulation (40%: bulk/block deals, delivery % spikes, RVol with
same-day price confirmation, U/D volume ratio) and technical context (10%).
Auto-updates every trading evening via GitHub Actions. No servers, no cost, no
AI-sourced numbers.

**Honest framing:** the fundamental score is TRAILING reported growth, not a
forward projection. Free forward analyst estimates do not exist for this universe;
nothing is fabricated to fill the gap. Stocks with missing quarterly data are
excluded and counted, never guessed.

## One-time setup (~10 minutes)

1. **Create the repo.** On github.com click **+ → New repository**. Name it
   (e.g. `smartmoney-dashboard`), set it **Public** (required for free Pages),
   click **Create repository**.
2. **Upload the files.** Click **uploading an existing file**, drag the entire
   contents of this folder in (keep the folder structure: `.github/workflows/`,
   `scripts/`, `docs/`, `data/` if present, `requirements.txt`, `README.md`),
   then **Commit changes**.
   - If the `.github` folder doesn't upload via drag-and-drop, create the file
     manually: **Add file → Create new file**, type
     `.github/workflows/daily.yml` as the name, paste the file's contents, commit.
3. **Enable Actions.** Go to the **Actions** tab → click **"I understand my
   workflows, enable them"** if prompted.
4. **Allow the bot to commit.** **Settings → Actions → General → Workflow
   permissions** → select **Read and write permissions** → Save.
5. **Enable Pages.** **Settings → Pages** → Source: **Deploy from a branch** →
   Branch: **main**, folder: **/docs** → Save. Your dashboard URL appears at the
   top of that page after a minute (`https://<username>.github.io/<repo>/`).
6. **First run.** **Actions tab → Daily smart-money update → Run workflow**.
   The first run takes 30–60+ minutes (it builds the full fundamentals cache for
   ~400 stocks). Subsequent daily runs are much faster; fundamentals refresh
   weekly on Saturdays.
7. Open your Pages URL. Until the first successful run, it shows clearly-marked
   **SAMPLE DATA**; after the run it shows real output.

## Schedule

- **Mon–Fri 19:00 IST** — prices, delivery %, bulk/block deals, smart-money
  re-score, re-rank. (GitHub cron can start 5–30 min late; that's normal.)
- **Saturday 09:00 IST** — fundamentals refresh + full weekly re-rank.

## Data warm-up

Two components need rolling history that builds up from the first run:
delivery-spike (needs 5+ sessions, uses 20) and the bulk/block 5-session window.
Cards show "warming up" until enough sessions accumulate — expect the smart-money
scores to become fully meaningful after ~4 weeks of runs. Everything computed
from yfinance history (RVol, U/D, 200DMA, 6-month veto) works from day one.

## Failure behaviour (by design)

- NSE bhavcopy unreachable → prices fall back to Yahoo; delivery data marked
  unavailable for the day; amber banner on the dashboard. Never silently stale.
- Bulk/block files unreachable → component scored from prior sessions only, flagged.
- A stock without 5 usable quarters of financials → V2 excluded, counted in the
  provenance strip.

## Disclaimer

Educational purposes only. Not investment advice. Not SEBI-registered research.
Data from public NSE archives and Yahoo Finance; verify independently before acting.
