"""
utils/run_logger.py
═══════════════════
Rich-console logger that mirrors output to terminal AND saves a
styled HTML log file to ``logs/``.

Usage
─────
    from utils.run_logger import RunLogger

    log = RunLogger("test_portfolio")   # creates timestamped HTML log
    log.h1("STEP 1: Fetching data")
    log.print("Benchmark (SPY): 501 rows")
    log.success("All tickers loaded")
    log.warning("Sector XLE has < 100 rows")
    log.error("Something went wrong", exc_info=True)
    log.table(title="Portfolio", columns=[...], rows=[...])
    log.kv("Breadth regime", "strong")
    log.divider()
    log.save()                          # writes HTML to logs/
"""

from __future__ import annotations

import datetime as dt
import re
import traceback as tb
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
import io

from common.config import LOGS_DIR


class RunLogger:
    """Dual-output logger: live terminal + recorded HTML file."""

    def __init__(self, run_name: str = "run", width: int = 120):
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filename = f"{run_name}_{ts}.html"
        self.filepath = LOGS_DIR / self.filename
        self.run_name = run_name
        self.started = dt.datetime.now()

        # Terminal console (live output)
        self._term = Console(width=width, force_terminal=True)
        # Recording console (captures everything for HTML export)
        self._rec = Console(
            width=width, record=True, force_terminal=True, file=io.StringIO(),
        )

    # ── Primitives ────────────────────────────────────────────

    def _echo(self, *args, **kwargs):
        """Print to both terminal and recording console."""
        self._term.print(*args, **kwargs)
        self._rec.print(*args, **kwargs)

    def print(self, *args, **kwargs):
        """Plain print (supports rich markup like [bold], [green], etc.)."""
        self._echo(*args, **kwargs)

    def log(self, *args, **kwargs):
        """Print with automatic timestamp prefix."""
        self._term.log(*args, **kwargs)
        self._rec.log(*args, **kwargs)

    # ── Headings ──────────────────────────────────────────────

    def h1(self, title: str):
        """Major section heading."""
        self._echo()
        self._echo(Panel(
            Text(title, style="bold white"),
            border_style="bright_cyan",
            box=box.DOUBLE_EDGE,
            expand=True,
        ))

    def h2(self, title: str):
        """Sub-section heading."""
        self._echo()
        self._echo(f"[bold bright_yellow]── {title} ──[/]")

    def divider(self, style: str = "dim"):
        """Horizontal rule."""
        self._echo(f"[{style}]{'─' * 80}[/]")

    # ── Semantic helpers ──────────────────────────────────────

    def success(self, msg: str):
        self._echo(f"  [bold green]✓[/] {msg}")

    def warning(self, msg: str):
        self._echo(f"  [bold yellow]⚠[/] {msg}")

    def error(self, msg: str, exc_info: bool = False):
        self._echo(f"  [bold red]✗[/] {msg}")
        if exc_info:
            txt = tb.format_exc()
            self._echo(f"[dim red]{txt}[/]")

    def info(self, msg: str):
        self._echo(f"  [dim cyan]ℹ[/] {msg}")

    def kv(self, key: str, value, key_width: int = 22):
        """Key-value pair, neatly aligned."""
        self._echo(
            f"  [bold]{key + ':':<{key_width}}[/] {value}"
        )

    def bullet(self, msg: str, style: str = "green"):
        """Bullet point."""
        self._echo(f"  [{style}]•[/] {msg}")

    # ── Sector / regime badges ────────────────────────────────

    def regime_badge(self, regime: str) -> str:
        """Return a rich-markup badge for a regime string."""
        badges = {
            "leading":   "[bold green]🟢 leading[/]",
            "weakening": "[bold yellow]🟡 weakening[/]",
            "improving": "[bold blue]🔵 improving[/]",
            "lagging":   "[bold red]🔴 lagging[/]",
            "strong":    "[bold green]● strong[/]",
            "neutral":   "[bold yellow]● neutral[/]",
            "weak":      "[bold red]● weak[/]",
            "crisis":    "[bold magenta]● crisis[/]",
        }
        return badges.get(regime, f"[dim]{regime}[/]")

    # ── Tables ────────────────────────────────────────────────

    def table(
        self,
        title: str,
        columns: list[dict],
        rows: list[list],
        box_style=box.SIMPLE_HEAVY,
    ):
        """
        Render a rich table.

        Parameters
        ----------
        columns : list of dict
            Each dict has keys: ``header``, and optionally
            ``justify`` ("left"/"right"/"center"), ``style``.
        rows : list of list
            Each inner list is one row of cell values (str).
        """
        tbl = Table(title=title, box=box_style, show_lines=False)
        for col in columns:
            tbl.add_column(
                col["header"],
                justify=col.get("justify", "left"),
                style=col.get("style", ""),
            )
        for row in rows:
            tbl.add_row(*[str(c) for c in row])
        self._echo(tbl)

    def sector_rankings(self, sector_rs_latest: list[dict]):
        """
        Pretty-print sector rankings.

        Expects list of dicts with keys:
        sector, rank, pctrank, regime
        """
        cols = [
            {"header": "Sector", "style": "bold"},
            {"header": "PctRank", "justify": "right"},
            {"header": "Regime", "justify": "center"},
        ]
        rows = []
        for s in sector_rs_latest:
            regime = s.get("regime", "")
            badge = self.regime_badge(regime)
            rows.append([
                s["sector"],
                f"{s.get('pctrank', 0):.2f}",
                badge,
            ])
        self.table("Sector Relative Strength", cols, rows)

    def portfolio_table(self, holdings: list[dict]):
        """
        Pretty-print portfolio holdings.

        Expects list of dicts with keys:
        ticker, weight, sector, score, signal
        """
        if not holdings:
            self.warning("No holdings")
            return

        cols = [
            {"header": "Ticker", "style": "bold cyan"},
            {"header": "Weight", "justify": "right"},
            {"header": "Sector"},
            {"header": "Score", "justify": "right"},
            {"header": "Signal", "justify": "center"},
        ]
        rows = []
        for h in holdings:
            signal = h.get("signal", "")
            if signal == "BUY":
                sig_styled = "[bold green]BUY[/]"
            elif signal == "SELL":
                sig_styled = "[bold red]SELL[/]"
            else:
                sig_styled = signal
            rows.append([
                h["ticker"],
                f"{h.get('weight', 0):.1%}",
                h.get("sector", ""),
                f"{h.get('score', 0):.3f}",
                sig_styled,
            ])
        self.table("Portfolio Holdings", cols, rows)

    def rebalance_table(self, actions: list[dict]):
        """
        Pretty-print rebalance actions.

        Expects list of dicts with keys:
        ticker, current_weight, target_weight, delta, action
        """
        cols = [
            {"header": "Ticker", "style": "bold"},
            {"header": "Current", "justify": "right"},
            {"header": "Target", "justify": "right"},
            {"header": "Delta", "justify": "right"},
            {"header": "Action", "justify": "center"},
        ]
        rows = []
        for a in actions:
            action = a.get("action", "")
            if action == "BUY":
                act_styled = "[bold green]BUY[/]"
            elif action == "SELL":
                act_styled = "[bold red]SELL[/]"
            elif action == "REDUCE":
                act_styled = "[bold yellow]REDUCE[/]"
            elif action == "TRIM":
                act_styled = "[bold yellow]TRIM[/]"
            elif action == "ADD":
                act_styled = "[bold cyan]ADD[/]"
            else:
                act_styled = action
            rows.append([
                a["ticker"],
                f"{a.get('current_weight', 0):.1%}",
                f"{a.get('target_weight', 0):.1%}",
                f"{a.get('delta', 0):+.1%}",
                act_styled,
            ])
        self.table("Rebalance Orders", cols, rows)

    # ── Breadth summary ───────────────────────────────────────

    def breadth_summary(self, breadth: dict):
        """
        Pretty-print breadth data from a breadth dict/row.
        """
        self.h2("Market Breadth")
        regime = breadth.get("regime", "unknown")
        self.kv("Regime", self.regime_badge(regime))
        self.kv("Breadth score", f"{breadth.get('score', 0):.3f}")
        self.kv("Smooth score", f"{breadth.get('score_smooth', 0):.3f}")
        self.kv("A-D line", breadth.get("ad_line", "N/A"))
        self.kv("% above 50d", f"{breadth.get('pct_above_50', 0):.1%}")
        self.kv("% above 200d", f"{breadth.get('pct_above_200', 0):.1%}")
        thrust = breadth.get("thrust_active", False)
        if thrust:
            self.kv("Thrust", "[bold magenta]⚡ ACTIVE[/]")
        else:
            self.kv("Thrust", "[dim]inactive[/]")

    # ── Save / export ─────────────────────────────────────────

    def save(self) -> Path:
        """
        Write the recorded output to an HTML file and return the path.
        """
        elapsed = dt.datetime.now() - self.started
        self._echo()
        self.divider()
        self._echo(
            f"[dim]Run completed in {elapsed.total_seconds():.1f}s  •  "
            f"Log saved to {self.filepath.name}[/]"
        )

        raw_html = self._rec.export_html(
            inline_styles=True,
            theme=_DARK_THEME,
        )

        # ── Extract just the <pre>...</pre> block ─────────────
        # Rich's export_html() returns a full HTML document.
        # We only want the <pre><code>...</code></pre> fragment
        # to embed inside our own styled wrapper.
        match = re.search(
            r"(<pre\b.*?</pre>)", raw_html, re.DOTALL
        )
        if match:
            body_fragment = match.group(1)
        else:
            # Fallback: strip outer html/head/body tags manually
            body_fragment = raw_html
            for tag in (
                "<!DOCTYPE html>", "<html>", "</html>",
                "<head>", "</head>", "<body>", "</body>",
            ):
                body_fragment = body_fragment.replace(tag, "")
            # Also strip any <meta> and <style> blocks
            body_fragment = re.sub(
                r"<meta[^>]*>", "", body_fragment
            )
            body_fragment = re.sub(
                r"<style>.*?</style>", "", body_fragment,
                flags=re.DOTALL,
            )

        full_html = _HTML_WRAPPER.format(
            title=f"SMR – {self.run_name}",
            timestamp=self.started.strftime("%Y-%m-%d %H:%M:%S"),
            elapsed=f"{elapsed.total_seconds():.1f}s",
            body=body_fragment,
        )

        self.filepath.write_text(full_html, encoding="utf-8")
        return self.filepath


# ── HTML template ─────────────────────────────────────────────

_HTML_WRAPPER = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  body {{
    background: #1a1b26;
    color: #c0caf5;
    font-family: 'Cascadia Code', 'Fira Code', 'JetBrains Mono',
                 'Consolas', monospace;
    font-size: 14px;
    line-height: 1.5;
    padding: 24px 32px;
    max-width: 1200px;
    margin: 0 auto;
  }}
  .header {{
    border-bottom: 2px solid #3b4261;
    padding-bottom: 12px;
    margin-bottom: 24px;
    color: #7aa2f7;
  }}
  .header h1 {{ margin: 0; font-size: 20px; }}
  .header .meta {{ color: #565f89; font-size: 12px; margin-top: 4px; }}
  pre {{
    background: transparent !important;
    color: inherit !important;
    margin: 0;
    padding: 0;
    white-space: pre-wrap;
    word-wrap: break-word;
  }}
  code {{
    font-family: inherit;
    background: transparent !important;
  }}
  ::selection {{
    background: #33467c;
    color: #c0caf5;
  }}
</style>
</head>
<body>
<div class="header">
  <h1>🔄 Smart Money Rotation</h1>
  <div class="meta">{timestamp}  •  elapsed {elapsed}</div>
</div>
{body}
</body>
</html>
"""

# Rich's built-in MONOKAI theme works well; override if desired
from rich.terminal_theme import MONOKAI as _DARK_THEME