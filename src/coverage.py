import pandas as pd


def date_coverage(df: pd.DataFrame, nodes: list[str]) -> pd.DataFrame:
    expected_rows_per_day = len(nodes) * 24

    out = (
        df
        .groupby("Date")
        .size()
        .rename("rows")
        .reset_index()
    )

    out["expected_rows"] = expected_rows_per_day
    out["coverage_ratio"] = out["rows"] / expected_rows_per_day
    out["complete"] = out["rows"] == expected_rows_per_day

    return out


def node_coverage(df: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    expected_rows_per_node = len(dates) * 24

    out = (
        df
        .groupby("Node")
        .size()
        .rename("rows")
        .reset_index()
    )

    out["expected_rows"] = expected_rows_per_node
    out["coverage_ratio"] = out["rows"] / expected_rows_per_node

    return out


def high_coverage_nodes(
    node_coverage_df: pd.DataFrame,
    min_coverage: float = 0.75,
) -> list[str]:
    return (
        node_coverage_df
        .loc[node_coverage_df["coverage_ratio"] >= min_coverage, "Node"]
        .tolist()
    )
