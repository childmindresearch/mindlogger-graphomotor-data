"""Main command-line interface for the Graphomotor MindLogger package."""

import argparse
import logging
from pathlib import Path

from bidsi import BidsConfig, BidsWriter

from .graphomotor import GraphomotorReport


def main(
    mindlogger_export_dir: Path,
    bids_root: Path,
    config: Path,
) -> None:
    """Main method for command-line interface."""
    logging.basicConfig(level=logging.DEBUG)
    # TODO: Watch Folder
    # TODO: Handle multiple reports
    report = GraphomotorReport.create(mindlogger_export_dir)
    writer = BidsWriter(bids_root, BidsConfig.from_file(config), report.bids_model())
    print("=========== Writing BIDS ===========")
    writer.write()
    # TODO: Report email
    # TODO: Move processed report to separate directory


def cli() -> None:
    """Command-line interface for Graphomotor MindLogger package."""
    parser = argparse.ArgumentParser(
        prog="Graphomotor BIDS",
        description="Converts Graphomotor export data from MindLogger to BIDS format.",
    )
    parser.add_argument(
        "--export",
        "-e",
        type=Path,
        required=True,
        help="Path to input data directory.",
    )
    parser.add_argument(
        "--bids_root",
        "-b",
        type=Path,
        required=True,
        help="Path to output BIDS directory.",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        help="Config file for Bidsi.",
    )
    args = parser.parse_args()

    main(args.export, args.bids_root, args.config)


if __name__ == "__main__":
    cli()
