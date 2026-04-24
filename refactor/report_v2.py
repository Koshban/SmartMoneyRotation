from __future__ import annotations
import pandas as pd


def build_report_v2(result: dict) -> dict:
    portfolio = result.get('portfolio', {})
    selected = portfolio.get('selected', pd.DataFrame())
    actions = result.get('action_table', pd.DataFrame())
    review = result.get('review_table', pd.DataFrame())
    meta = portfolio.get('meta', {})
    latest = result.get('latest', pd.DataFrame())
    market = result.get('market', 'UNKNOWN')
    leadership = result.get('leadership_universe', [])
    tradable = result.get('tradable_universe', [])

    top = []
    if review is not None and not review.empty:
        top = review.head(10).to_dict(orient='records')

    action_summary = {'STRONG_BUY': 0, 'BUY': 0, 'HOLD': 0, 'SELL': 0}
    if actions is not None and not actions.empty and 'action_v2' in actions.columns:
        counts = actions['action_v2'].value_counts().to_dict()
        for k in action_summary:
            action_summary[k] = int(counts.get(k, 0))

    return {
        'header': {
            'market': market,
            'tradable_universe_size': len(tradable),
            'leadership_universe_size': len(leadership),
            'processed_names': 0 if latest is None else len(latest),
            'rsi_field': 'rsi14',
        },
        'regime': {
            'breadth_regime': meta.get('breadth_regime', 'unknown'),
            'vol_regime': meta.get('vol_regime', 'unknown'),
            'target_exposure': meta.get('target_exposure', 0.0),
        },
        'actions': action_summary,
        'portfolio': {
            'selected_count': meta.get('selected_count', 0),
            'candidate_count': meta.get('candidate_count', 0),
            'top_picks': top,
        }
    }


def to_text_v2(report: dict) -> str:
    h = report['header']; r = report['regime']; a = report['actions']; p = report['portfolio']
    lines = []
    lines.append('CASH V2 REPORT')
    lines.append('-' * 90)
    lines.append(f"Market                  : {h['market']}")
    lines.append(f"Tradable universe size  : {h['tradable_universe_size']}")
    lines.append(f"Leadership universe size: {h['leadership_universe_size']}")
    lines.append(f"Processed names         : {h['processed_names']}")
    lines.append(f"RSI field used          : {h['rsi_field']} (RSI 14)")
    lines.append(f"Breadth regime          : {r['breadth_regime']}")
    lines.append(f"Vol regime              : {r['vol_regime']}")
    lines.append(f"Target exposure         : {r['target_exposure']:.2%}")
    lines.append(f"Action counts           : STRONG_BUY={a['STRONG_BUY']} BUY={a['BUY']} HOLD={a['HOLD']} SELL={a['SELL']}")
    lines.append(f"Candidates              : {p['candidate_count']}")
    lines.append(f"Selected                : {p['selected_count']}")
    lines.append('')
    lines.append('REVIEW TABLE')
    lines.append('-' * 90)
    if not p['top_picks']:
        lines.append('No signals')
    else:
        for i, row in enumerate(p['top_picks'], 1):
            lines.append(
                f"{i:>2}. {row.get('ticker','?'):12s} {row.get('recommendation','?'):10s} "
                f"score={row.get('composite_score',0):.3f} pct={row.get('score_percentile',0):.0%} "
                f"rsi14={row.get('rsi_14',0):.1f} adx14={row.get('adx_14',0):.1f} rv={row.get('relative_volume',0):.2f} "
                f"lead={row.get('leadership_strength',0):.2f} sector={row.get('sector','Unknown')} theme={row.get('theme','Unknown')}"
            )
    return '\n'.join(lines)
