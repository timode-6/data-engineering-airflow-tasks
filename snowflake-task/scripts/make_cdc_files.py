"""Split Airline_Dataset.csv into the two files that test the CDC load.

The source system is simulated as of 1 December 2022. Everything departing
before then is the initial load; what follows is one batch of changes.

    airline_001_initial.csv
        every booking departing before 1 December
    airline_002_changes.csv
        the batch of changes, in the order a CDC feed sends them:
        1. new bookings     every December departure
        2. status updates   earlier bookings whose row number is a multiple of 25
                            move one step: On Time -> Delayed, Delayed -> Cancelled.
                            Cancelled ones stay as they are.
        3. second updates   those that were On Time and whose row number is a
                            multiple of 250 change again, Delayed -> Cancelled.
                            Both lines are in the same file, so the later one
                            has to win.
        4. a rename         Turkey was renamed Türkiye in June 2022, so every line
                            for an airport in Turkey carries the new country name

The numbers in the names put the files in delivery order when sorted. That
matters when one COPY loads several files at once: their rows share
LOADED_AT, so the file name decides which line is newer.

Both files keep the source's header, columns and CSV format, so the pipeline
loads them unchanged. Run from the snowflake-task folder:

    python3 scripts/make_cdc_files.py
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from pathlib import Path

INCOMING_DIR = Path(__file__).resolve().parent.parent / "data" / "incoming"
INITIAL_FILE = "airline_001_initial.csv"
CHANGES_FILE = "airline_002_changes.csv"
CUTOFF = date(2022, 12, 1)
UPDATE_EVERY = 25
SECOND_UPDATE_EVERY = 250
NEXT_STATUS = {"On Time": "Delayed", "Delayed": "Cancelled"}
RENAMED_COUNTRIES = {"TR": "Türkiye"}


def read(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        return header, list(reader)


def write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n")
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source", type=Path, default=INCOMING_DIR / "Airline_Dataset.csv")
    parser.add_argument("--out-dir", type=Path, default=INCOMING_DIR)
    args = parser.parse_args()

    header, rows = read(args.source)
    column = {name: i for i, name in enumerate(header)}
    status = column["Flight Status"]

    def with_status(row: list[str], new_status: str) -> list[str]:
        changed = list(row)
        changed[status] = new_status
        return changed

    initial, new_bookings, updates, second_updates = [], [], [], []
    for row in rows:
        departure = datetime.strptime(row[column["Departure Date"]], "%m/%d/%Y").date()
        if departure >= CUTOFF:
            new_bookings.append(row)
            continue
        initial.append(row)
        number = int(row[0])
        if number % UPDATE_EVERY == 0 and row[status] in NEXT_STATUS:
            updates.append(with_status(row, NEXT_STATUS[row[status]]))
            if number % SECOND_UPDATE_EVERY == 0 and row[status] == "On Time":
                second_updates.append(with_status(row, "Cancelled"))

    changes = new_bookings + updates + second_updates
    renamed = 0
    for row in changes:
        new_name = RENAMED_COUNTRIES.get(row[column["Airport Country Code"]])
        if new_name:
            row[column["Country Name"]] = new_name
            renamed += 1

    write(args.out_dir / INITIAL_FILE, header, initial)
    write(args.out_dir / CHANGES_FILE, header, changes)

    print(f"{INITIAL_FILE}  {len(initial):>6,} bookings departing before {CUTOFF}")
    print(f"{CHANGES_FILE}  {len(changes):>6,} lines")
    print(f"  new bookings           {len(new_bookings):>6,}")
    print(f"  status updates         {len(updates):>6,}")
    print(f"  second updates         {len(second_updates):>6,}")
    print(f"  renamed lines          {renamed:>6,}  (lines for Turkish airports, among the above)")


if __name__ == "__main__":
    main()
