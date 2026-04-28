from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RTM_RAW_DIR = RAW_DIR / "rtm"
DAM_RAW_DIR = RAW_DIR / "dam"
PROCESSED_DIR = DATA_DIR / "processed"

RESULTS_DIR = PROJECT_DIR / "results"
FIGURES_DIR = PROJECT_DIR / "figures"

SINK_HUB = "HB_NORTH"
DAM_HUB_NAME = "NORTH"

TRANSACTION_COST = 1.00

TOP_NODES = [
    "BRTSW_BCW1",
    "BRAUNIG_VHB1",
    "BRA_AVR1_CT2",
    "BRAUNIG_VHB2",
    "BRAUNIG_VHB3",
]

NODE_MAPPING = {node: node for node in TOP_NODES}
