# Tuning 24 Sep 2026

Local commit: `357ea0a`

## Changes applied in config.py (already working on local bot)

| Parameter | Value |
|-----------|-------|
| MAX_CONCURRENT_POSITIONS | 4 |
| SCANNER_MAX_ACTIVE_PAIRS_OKX | 4 |
| SCANNER_MIN_SCORE | 55.0 |
| RISK_PER_TRADE_PCT | 2.0 |
| MAX_POSITION_PCT_FUTURES | 40.0 |
| ADX_MIN_TREND / FUTURES | 20.0 |
| COOLDOWN_AFTER_SL_MIN | 20 |
| EMA_CROSS_SPREAD_MIN_PCT | 0.15 |
| REVERSE_EMA_SPREAD_MIN_PCT | 0.30 |
| TP_STALL_MIN_PROFIT_PCT | 1.2 |

**Note:** `config.py` on GitHub may still need a full push from local machine if placeholder remains. From your PC (where bot already runs with tuned config):

```bash
git add config.py
git commit -m "Tune strategy from 24 Sep log"
git push origin HEAD:main
```
