import time
from pathlib import Path

import pandas as pd

from .config import RTM_RAW_DIR, DAM_RAW_DIR


def safe_name(name: str) -> str:
    return str(name).replace("/", "_").replace(" ", "_")


def fetch_rtm_cached(
    ercot_api,
    date: str,
    settlement_point: str,
    sleep_seconds: float = 1.0,
    page_size: int = 1000,
    max_pages: int = 5,
) -> pd.DataFrame:
    """
    Fetch RTM LMP for one date and one settlement point.
    Uses local CSV cache if available.
    """
    RTM_RAW_DIR.mkdir(parents=True, exist_ok=True)

    filename = RTM_RAW_DIR / f"rtm_{date}_{safe_name(settlement_point)}.csv"

    if filename.exists():
        return pd.read_csv(filename)

    print(f"Fetching RTM from API: date={date}, settlementPoint={settlement_point}")

    time.sleep(sleep_seconds)

    df = ercot_api.hit_ercot_api(
        "/np6-788-cd/lmp_node_zone_hub",
        page_size=page_size,
        max_pages=max_pages,
        verbose=True,
        SCEDTimestampFrom=f"{date}T00:00:00",
        SCEDTimestampTo=f"{date}T23:59:59",
        settlementPoint=settlement_point,
    )

    df.to_csv(filename, index=False)
    return df


def fetch_dam_cached(
    ercot_api,
    date: str,
    bus_name: str,
    sleep_seconds: float = 1.0,
    page_size: int = 100,
    max_pages: int = 1,
) -> pd.DataFrame:
    """
    Fetch DAM hourly LMP for one date and one bus.
    Uses local CSV cache if available.
    """
    DAM_RAW_DIR.mkdir(parents=True, exist_ok=True)

    filename = DAM_RAW_DIR / f"dam_{date}_{safe_name(bus_name)}.csv"

    if filename.exists():
        return pd.read_csv(filename)

    print(f"Fetching DAM from API: date={date}, busName={bus_name}")

    time.sleep(sleep_seconds)

    df = ercot_api.hit_ercot_api(
        "/np4-183-cd/dam_hourly_lmp",
        page_size=page_size,
        max_pages=max_pages,
        verbose=True,
        deliveryDateFrom=date,
        deliveryDateTo=date,
        busName=bus_name,
    )

    df.to_csv(filename, index=False)
    return df
