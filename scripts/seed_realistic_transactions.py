"""Seeds realistic commercial transactions and double-entry ledger accounts into PayFlow."""

import argparse
import sys
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from payflow.common.db import default_engine, init_db
from payflow.data.real_dataset import seed_realistic_database_records


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed realistic transactions into PayFlow")
    parser.add_argument("--count", type=int, default=100, help="Number of transactions to seed")
    parser.add_argument("--force", action="store_true", help="Force seeding even if records exist")
    args = parser.parse_args()

    print(f"Initializing database and seeding {args.count} realistic transactions (force={args.force})...")
    init_db(default_engine)
    seeded = seed_realistic_database_records(default_engine, count=args.count, force=args.force)
    print(f"Successfully seeded {seeded} authentic commercial payment transactions with balanced ledger accounts!")


if __name__ == "__main__":
    main()
