# backtest/phase2/stop_loss.py

import numpy as np
from dataclasses import dataclass, field


@dataclass
class PositionTracker:
    """Tracks stop levels for a single position."""
    ticker: str
    entry_price: float
    entry_date: str
    initial_stop: float          # ATR-based initial stop
    high_water_mark: float = 0.0
    trailing_active: bool = False
    current_stop: float = 0.0

    def __post_init__(self):
        self.high_water_mark = self.entry_price
        self.current_stop = self.initial_stop


def update_stops(
    position: PositionTracker,
    current_price: float,
    current_atr: float,
    params: dict,
) -> tuple[PositionTracker, bool]:
    """
    Update stop levels. Returns (updated_position, should_exit).
    """
    entry = position.entry_price
    max_loss_pct = params.get("max_loss_pct", 0.20)
    trail_activation = params.get("trail_activation_pct", 0.10)
    trail_atr_mult = params.get("trail_atr_multiplier", 2.5)
    trail_pct_fallback = params.get("trail_pct_fallback", 0.15)

    # ── Hard max loss check ──────────────────────────────────
    if params.get("max_loss_enabled", True):
        hard_stop = entry * (1.0 - max_loss_pct)
        if current_price <= hard_stop:
            return position, True

    # ── Update high-water mark ───────────────────────────────
    if current_price > position.high_water_mark:
        position.high_water_mark = current_price

    # ── Activate trailing if gain threshold reached ──────────
    gain_pct = (position.high_water_mark - entry) / entry
    if gain_pct >= trail_activation and params.get("trailing_enabled", True):
        position.trailing_active = True

    # ── Compute trailing stop level ──────────────────────────
    if position.trailing_active:
        if current_atr > 0:
            trail_stop = position.high_water_mark - (trail_atr_mult * current_atr)
        else:
            trail_stop = position.high_water_mark * (1.0 - trail_pct_fallback)

        # Trail only goes UP, never down
        position.current_stop = max(position.current_stop, trail_stop)

    # ── Ratchet profit lock-in ───────────────────────────────
    if params.get("ratchet_enabled", True):
        for threshold, lock_pct in params.get("ratchet_levels", []):
            if gain_pct >= threshold:
                ratchet_stop = entry * (1.0 + lock_pct)
                position.current_stop = max(position.current_stop, ratchet_stop)

    # ── Check if stopped out ─────────────────────────────────
    if current_price <= position.current_stop:
        return position, True

    return position, False