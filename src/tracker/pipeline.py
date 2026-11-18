"""Run every data stage in order: raw CSV -> bronze -> silver -> gold -> dashboard CSV."""
from . import add_month, aggregate, clean, export, ingest


def main() -> None:
    for stage in (ingest, add_month, clean, aggregate, export):
        print(f"\n=== {stage.__name__.rsplit('.', 1)[-1]} ===")
        stage.main()


if __name__ == "__main__":
    main()
