"""Export the gold table as the CSV file the web dashboard loads."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "gold" / "constructor_monthly.parquet"
OUT = ROOT / "docs" / "f1_data.csv"


def build_dashboard_table(gold: pd.DataFrame) -> pd.DataFrame:
    month = pd.to_datetime(gold["m"])
    return pd.DataFrame({
        "Season": month.dt.year,
        "Date": month.dt.strftime("%Y-%m-%d"),
        "ConstructorName": gold["constructor_name"],
        "Points": gold["points_m"],
    })


def main() -> None:
    table = build_dashboard_table(pd.read_parquet(GOLD))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUT, index=False)
    print(f"Rows: {len(table)}")
    print("Saved ->", OUT)


if __name__ == "__main__":
    main()
