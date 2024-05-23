"""Main command-line interface for the Oak MindLogger package."""

import argparse
from pathlib import Path

from bidsi import BidsWriter, MergeStrategy

from .oak_report import OakReport


def main(
    mindlogger_export_dir: Path, bids_root: Path, merge_strategy: MergeStrategy
) -> None:
    """Main method for command-line interface."""
    report = OakReport.create(mindlogger_export_dir)
    writer = BidsWriter(bids_root, merge_strategy, report.bids_model())
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
        "--merge_strategy",
        "-s",
        type=MergeStrategy.argparse,
        default=MergeStrategy.OVERWRITE,
        choices=list(MergeStrategy),
        help="Merge strategy for BIDS entities.",
    )
    args = parser.parse_args()

    main(args.mindlogger_export_dir, args.bids_root_dir, args.merge_strategy)


if __name__ == "__main__":
    cli()
