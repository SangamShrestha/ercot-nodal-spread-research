import pandas as pd

from .backtest import build_lag_strategy_strict
from .metrics import daily_metrics


def chronological_train_test_split(
    df: pd.DataFrame,
    train_frac: float = 0.5,
):
    all_dates = sorted(df["Date"].unique())
    split_idx = int(len(all_dates) * train_frac)

    train_dates = all_dates[:split_idx]
    test_dates = all_dates[split_idx:]

    train_raw = df[df["Date"].isin(train_dates)].copy()
    test_raw = df[df["Date"].isin(test_dates)].copy()

    return train_raw, test_raw, train_dates, test_dates


def train_rule_search(
    train_raw: pd.DataFrame,
    hour_windows=None,
    thresholds=None,
    min_rows: int = 300,
) -> tuple[pd.DataFrame, pd.Series]:
    if hour_windows is None:
        hour_windows = [
            (8, 20),
            (9, 20),
            (10, 20),
            (8, 22),
            (9, 22),
        ]

    if thresholds is None:
        thresholds = [-20, -10, 0, 5, 10, 15, 20]

    rule_tests = []

    for start_hour, end_hour in hour_windows:
        for threshold in thresholds:
            temp = build_lag_strategy_strict(
                train_raw,
                start_hour=start_hour,
                end_hour=end_hour,
                min_lag_rtm=threshold,
            )

            if temp.empty:
                continue

            m = daily_metrics(
                temp,
                f"Train: Hour {start_hour}-{end_hour}, Lag1_RTM >= {threshold}",
            )

            m["start_hour"] = start_hour
            m["end_hour"] = end_hour
            m["threshold"] = threshold
            rule_tests.append(m)

    rule_search = pd.DataFrame(rule_tests)

    rule_search_filtered = rule_search[rule_search["rows"] >= min_rows].copy()

    if rule_search_filtered.empty:
        raise ValueError(
            "No candidate rules met min_rows requirement. Lower min_rows."
        )

    best_rule = rule_search_filtered.sort_values(
        "annualized_sharpe",
        ascending=False,
    ).iloc[0]

    return rule_search, best_rule


def evaluate_selected_rule(
    train_raw: pd.DataFrame,
    test_raw: pd.DataFrame,
    start_hour: int,
    end_hour: int,
    threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_strategy = build_lag_strategy_strict(
        train_raw,
        start_hour=start_hour,
        end_hour=end_hour,
        min_lag_rtm=threshold,
    )

    test_strategy = build_lag_strategy_strict(
        test_raw,
        start_hour=start_hour,
        end_hour=end_hour,
        min_lag_rtm=threshold,
    )

    comparison = pd.DataFrame([
        daily_metrics(train_raw, "Train baseline: all hours"),
        daily_metrics(train_strategy, "Train selected strict-lag strategy"),
        daily_metrics(test_raw, "Test baseline: all hours"),
        daily_metrics(test_strategy, "Test selected strict-lag strategy"),
    ])

    return comparison, train_strategy, test_strategy


def rank_nodes_on_train(
    train_raw: pd.DataFrame,
    start_hour: int,
    end_hour: int,
    threshold: float,
) -> pd.DataFrame:
    train_signal = build_lag_strategy_strict(
        train_raw,
        start_hour=start_hour,
        end_hour=end_hour,
        min_lag_rtm=threshold,
    )

    node_rank = (
        train_signal
        .groupby("Node")
        .agg(
            rows=("Net_PnL", "count"),
            total_net_pnl=("Net_PnL", "sum"),
            mean_net_pnl=("Net_PnL", "mean"),
            win_rate=("Net_PnL", lambda x: (x > 0).mean()),
            std_net_pnl=("Net_PnL", "std"),
        )
        .sort_values("total_net_pnl", ascending=False)
    )

    return node_rank


def evaluate_train_selected_node_baskets(
    train_raw: pd.DataFrame,
    test_raw: pd.DataFrame,
    ranked_nodes: list[str],
    basket_sizes=None,
    start_hour: int = 10,
    end_hour: int = 20,
    threshold: float = 0,
) -> pd.DataFrame:
    if basket_sizes is None:
        basket_sizes = [3, 5, 10, 15, 20]

    basket_results = []

    for n in basket_sizes:
        selected_nodes = ranked_nodes[:n]

        train_basket_raw = train_raw[
            train_raw["Node"].isin(selected_nodes)
        ].copy()

        test_basket_raw = test_raw[
            test_raw["Node"].isin(selected_nodes)
        ].copy()

        train_basket_strategy = build_lag_strategy_strict(
            train_basket_raw,
            start_hour=start_hour,
            end_hour=end_hour,
            min_lag_rtm=threshold,
        )

        test_basket_strategy = build_lag_strategy_strict(
            test_basket_raw,
            start_hour=start_hour,
            end_hour=end_hour,
            min_lag_rtm=threshold,
        )

        train_metrics = daily_metrics(
            train_basket_strategy,
            f"Train top {n} nodes"
        )
        train_metrics["split"] = "train"
        train_metrics["basket_size"] = n
        train_metrics["selected_nodes"] = ",".join(selected_nodes)

        test_metrics = daily_metrics(
            test_basket_strategy,
            f"Test top {n} train-selected nodes"
        )
        test_metrics["split"] = "test"
        test_metrics["basket_size"] = n
        test_metrics["selected_nodes"] = ",".join(selected_nodes)

        basket_results.append(train_metrics)
        basket_results.append(test_metrics)

    return pd.DataFrame(basket_results)
