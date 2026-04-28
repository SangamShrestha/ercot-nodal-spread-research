import numpy as np
import pandas as pd


def daily_metrics(df: pd.DataFrame, label: str) -> dict:
    daily = df.groupby("Date")["Net_PnL"].sum().sort_index()
    cum = daily.cumsum()
    dd = cum - cum.cummax()

    daily_std = daily.std()

    if daily_std == 0 or np.isnan(daily_std):
        daily_sharpe = np.nan
        annualized_sharpe = np.nan
    else:
        daily_sharpe = daily.mean() / daily_std
        annualized_sharpe = daily_sharpe * np.sqrt(252)

    return {
        "label": label,
        "rows": len(df),
        "total_net_pnl": df["Net_PnL"].sum(),
        "mean_trade_pnl": df["Net_PnL"].mean(),
        "win_rate": (df["Net_PnL"] > 0).mean(),
        "daily_mean": daily.mean(),
        "daily_std": daily_std,
        "daily_sharpe": daily_sharpe,
        "annualized_sharpe": annualized_sharpe,
        "max_drawdown": dd.min(),
        "num_days": len(daily),
    }


def clean_metrics_table(df: pd.DataFrame) -> pd.DataFrame:
    display_cols = [
        "label",
        "rows",
        "total_net_pnl",
        "mean_trade_pnl",
        "win_rate",
        "annualized_sharpe",
        "max_drawdown",
        "num_days",
    ]

    out = df[display_cols].copy()

    out["total_net_pnl"] = out["total_net_pnl"].round(2)
    out["mean_trade_pnl"] = out["mean_trade_pnl"].round(2)
    out["win_rate"] = (out["win_rate"] * 100).round(1)
    out["annualized_sharpe"] = out["annualized_sharpe"].round(2)
    out["max_drawdown"] = out["max_drawdown"].round(2)

    return out
