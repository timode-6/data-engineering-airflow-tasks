"""Command line entry point: python -m log_alerts <command>."""

from __future__ import annotations

import argparse

from log_alerts import processor, replayer


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m log_alerts",
        description="Alerts on fatal errors in mobile app log journals.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("process", help="watch the incoming folder and raise alerts")
    commands.add_parser("replay", help="send the dataset to the incoming folder as journals")
    arguments = parser.parse_args(argv)

    if arguments.command == "process":
        processor.main()
    else:
        replayer.main()


if __name__ == "__main__":
    main()
