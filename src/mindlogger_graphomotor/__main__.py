"""Main command-line interface for the Oak MindLogger package."""

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
    report = GraphomotorReport.create(mindlogger_export_dir)
    writer = BidsWriter(bids_root, BidsConfig.from_file(config), report.bids_model())
    print("=========== Writing BIDS ===========")
    writer.write()


def cli() -> None:
    """Command-line interface for Oak MindLogger package."""
    parser = argparse.ArgumentParser(
        prog="Oak BIDS",
        description="Converts Oak export data from MindLogger to BIDS format.",
    )
    parser.add_argument(
        "--mindlogger_export_dir",
        "-m",
        type=Path,
        required=True,
        help="Path to input data directory.",
    )
    parser.add_argument(
        "--bids_root_dir",
        "-b",
        type=Path,
        required=True,
        help="Path to output BIDS directory.",
    )
    parser.add_argument(
        "--bids_config",
        "-c",
        type=Path,
        help="Config file for Bidsi.",
    )
    args = parser.parse_args()

    main(args.mindlogger_export_dir, args.bids_root_dir, args.bids_config)


if __name__ == "__main__":
    cli()
