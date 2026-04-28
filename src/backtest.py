import pandas as pd

from .config import SINK_HUB, DAM_HUB_NAME, TRANSACTION_COST, NODE_MAPPING
from .data_loader import fetch_rtm_cached, fetch_dam_cached


def run_cached_targeted_backtest(
    ercot_api,
    dates,
    candidate_nodes,
    transaction_cost: float = TRANSACTION_COST,
    sink_hub: str = SINK_HUB,
    dam_hub_name: str = DAM_HUB_NAME,
    node_mapping: dict | None = None,
) -> pd.DataFrame:
    """
    Runs a cached targeted ERCOT DAM/RTM hub-node spread backtest.

    Spread definition:
        RTM_Spread = RTM hub LMP - RTM node LMP
        DAM_Spread = DAM hub LMP - DAM node LMP
        Alpha_PnL = RTM_Spread - DAM_Spread
        Net_PnL = Alpha_PnL - transaction_cost
    """
    if node_mapping is None:
        node_mapping = NODE_MAPPING

    all_results = []
    settlement_points_needed = [sink_hub] + list(candidate_nodes)

    for date in dates:
        print("\n" + "=" * 80)
        print(f"Processing {date}")
        print("=" * 80)

        try:
            rtm_parts = []

            for sp in settlement_points_needed:
                try:
                    df_sp = fetch_rtm_cached(
                        ercot_api=ercot_api,
                        date=date,
                        settlement_point=sp,
                    )

                    if df_sp.empty:
                        print(f"RTM empty for settlementPoint={sp}")
                        continue

                    rtm_parts.append(df_sp)

                except Exception as e:
                    print(f"RTM fetch failed for settlementPoint={sp}: {e}")
                    continue

            if not rtm_parts:
                print("No RTM data fetched. Skipping date.")
                continue

            df_rtm = pd.concat(rtm_parts, ignore_index=True)

            required_rtm_cols = {"SCEDTimestamp", "SettlementPoint", "LMP"}
            missing_rtm_cols = required_rtm_cols - set(df_rtm.columns)

            if missing_rtm_cols:
                print("Missing RTM columns:", missing_rtm_cols)
                continue

            df_rtm["SCEDTimestamp"] = pd.to_datetime(df_rtm["SCEDTimestamp"])

            df_pivoted = df_rtm.pivot_table(
                index="SCEDTimestamp",
                columns="SettlementPoint",
                values="LMP",
                aggfunc="mean",
            )

            if sink_hub not in df_pivoted.columns:
                print(f"{sink_hub} not found in RTM data. Skipping date.")
                continue

            available_nodes = [
                node for node in candidate_nodes
                if node in df_pivoted.columns
            ]

            if not available_nodes:
                print("No candidate nodes found in RTM data. Skipping date.")
                continue

            spreads_df = pd.DataFrame(index=df_pivoted.index)

            for node in available_nodes:
                spreads_df[node] = df_pivoted[sink_hub] - df_pivoted[node]

            dam_buses_needed = [dam_hub_name]

            for node in available_nodes:
                dam_bus = node_mapping.get(
                    node,
                    node.replace("_UNIT", "_").replace("UNIT", ""),
                )
                dam_buses_needed.append(dam_bus)

            dam_buses_needed = sorted(set(dam_buses_needed))

            dam_parts = []

            for bus_name in dam_buses_needed:
                try:
                    df_bus = fetch_dam_cached(
                        ercot_api=ercot_api,
                        date=date,
                        bus_name=bus_name,
                    )

                    if df_bus.empty:
                        print(f"DAM empty for busName={bus_name}")
                        continue

                    dam_parts.append(df_bus)

                except Exception as e:
                    print(f"DAM fetch failed for busName={bus_name}: {e}")
                    continue

            if not dam_parts:
                print("No DAM data fetched. Skipping date.")
                continue

            df_dam = pd.concat(dam_parts, ignore_index=True)

            required_dam_cols = {"DeliveryDate", "HourEnding", "BusName", "LMP"}
            missing_dam_cols = required_dam_cols - set(df_dam.columns)

            if missing_dam_cols:
                print("Missing DAM columns:", missing_dam_cols)
                continue

            for node in available_nodes:
                dam_bus = node_mapping.get(
                    node,
                    node.replace("_UNIT", "_").replace("UNIT", ""),
                )

                node_dam = df_dam[df_dam["BusName"] == dam_bus].copy()
                hub_dam = df_dam[df_dam["BusName"] == dam_hub_name].copy()

                if node_dam.empty:
                    print(f"node_dam is empty for node={node}, dam_bus={dam_bus}")
                    continue

                if hub_dam.empty:
                    print(f"hub_dam is empty for dam_hub_name={dam_hub_name}")
                    continue

                node_dam["HourInt"] = node_dam["HourEnding"].str[:2].astype(int)
                hub_dam["HourInt"] = hub_dam["HourEnding"].str[:2].astype(int)

                dam_merged = pd.merge(
                    node_dam,
                    hub_dam,
                    on=["DeliveryDate", "HourInt"],
                    suffixes=("_node", "_hub"),
                )

                if dam_merged.empty:
                    print(f"DAM merge empty for node={node}. Skipping.")
                    continue

                dam_merged["DAM_Spread"] = (
                    dam_merged["LMP_hub"] - dam_merged["LMP_node"]
                )

                rtm_series = spreads_df[node].copy()
                rtm_series.index = pd.to_datetime(rtm_series.index)

                rtm_hourly = (
                    rtm_series
                    .resample("h")
                    .mean()
                    .to_frame(name="RTM_Spread_Avg")
                )

                # RTM hour 00 maps to DAM HourEnding 1.
                # RTM hour 23 maps to DAM HourEnding 24.
                rtm_hourly["HourInt"] = rtm_hourly.index.hour + 1

                backtest = pd.merge(
                    rtm_hourly,
                    dam_merged,
                    on="HourInt",
                    how="inner",
                )

                if backtest.empty:
                    print(f"Backtest merge empty for node={node}. Skipping.")
                    continue

                backtest["Alpha_PnL"] = (
                    backtest["RTM_Spread_Avg"] - backtest["DAM_Spread"]
                )

                backtest["Net_PnL"] = backtest["Alpha_PnL"] - transaction_cost
                backtest["Node"] = node
                backtest["DAM_Bus"] = dam_bus
                backtest["DAM_Hub"] = dam_hub_name
                backtest["Date"] = date

                all_results.append(backtest)

        except Exception as e:
            print(f"Technical error on {date}: {e}")

    if all_results:
        return pd.concat(all_results, ignore_index=True)

    return pd.DataFrame()


def build_lag_strategy(
    df: pd.DataFrame,
    start_hour: int = 10,
    end_hour: int = 20,
    min_lag_rtm: float = 10,
) -> pd.DataFrame:
    """
    Non-strict lag strategy. Uses previous available row by Node and HourInt.
    Kept for diagnostics only.

    For official results, prefer build_lag_strategy_strict().
    """
    lag_df = df.copy()
    lag_df["Date_dt"] = pd.to_datetime(lag_df["Date"])
    lag_df = lag_df.sort_values(["Node", "HourInt", "Date_dt"])

    lag_df["Lag1_RTM_Spread"] = (
        lag_df
        .groupby(["Node", "HourInt"])["RTM_Spread_Avg"]
        .shift(1)
    )

    lag_df = lag_df.dropna(subset=["Lag1_RTM_Spread"]).copy()

    strategy_df = lag_df[
        (lag_df["HourInt"] >= start_hour) &
        (lag_df["HourInt"] <= end_hour) &
        (lag_df["Lag1_RTM_Spread"] >= min_lag_rtm)
    ].copy()

    return strategy_df


def build_lag_strategy_strict(
    df: pd.DataFrame,
    start_hour: int = 10,
    end_hour: int = 20,
    min_lag_rtm: float = 10,
) -> pd.DataFrame:
    """
    Official strict-lag strategy.

    Signal:
        Lag1_RTM_Spread = previous calendar day's same-node same-hour
        RTM hub-node spread.

    Rule:
        HourInt between start_hour and end_hour
        Lag1_RTM_Spread >= min_lag_rtm

    This avoids using stale lag values from multiple days ago.
    """
    lag_df = df.copy()
    lag_df["Date_dt"] = pd.to_datetime(lag_df["Date"])
    lag_df = lag_df.sort_values(["Node", "HourInt", "Date_dt"])

    lag_df["Lag1_RTM_Spread"] = (
        lag_df
        .groupby(["Node", "HourInt"])["RTM_Spread_Avg"]
        .shift(1)
    )

    lag_df["Lag1_Date"] = (
        lag_df
        .groupby(["Node", "HourInt"])["Date_dt"]
        .shift(1)
    )

    lag_df["Lag_Days"] = (
        lag_df["Date_dt"] - lag_df["Lag1_Date"]
    ).dt.days

    lag_df = lag_df[lag_df["Lag_Days"] == 1].copy()

    strategy_df = lag_df[
        (lag_df["HourInt"] >= start_hour) &
        (lag_df["HourInt"] <= end_hour) &
        (lag_df["Lag1_RTM_Spread"] >= min_lag_rtm)
    ].copy()

    return strategy_df
