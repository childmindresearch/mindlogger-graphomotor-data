"""Model for Oak report of MindLogger data."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Tuple
from zipfile import ZipFile

import pandas as pd
from bidsi import BidsBuilder, BidsModel

LOG = logging.getLogger(__name__)


class OakReport:
    """Model of Oak report data and import/export methods."""

    __DATE_REGEX = r"(\w{3}\s\w{3}\s\d{1,2}\s\d{4})"
    _ACTIVITY_USER_JOURNEY_FILENAME = "activity_user_journey.csv"
    _REPORT_FILENAME = "report.csv"
    _DRAWING_RESPONSES_PATTERN = "drawing-responses-*.zip"
    _MEDIA_RESPONSES_PATTERN = "media-responses-*.zip"
    _TRAILS_RESPONSES_PATTERN = "trails-responses-*.zip"
    __ID_REGEX = r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
    _TRAILS_FILENAME_PATTERN = f"(?P<report_id>{__ID_REGEX})" r"-trail(?P<run>\d).csv"
    _DRAWING_FILENAME_PATTERN = (
        f"(?P<report_id>{__ID_REGEX})"
        f"-(?P<user_id>{__ID_REGEX})"
        r"-(?P<item>\w+?(?P<run>\d)\w*).(?P<filetype>csv|svg)"
    )

    def __init__(
        self,
        data_directory: Path,
        activity_user_journey: pd.DataFrame,
        report: pd.DataFrame,
        drawing_dir: Path,
        media_dir: Path,
        trails_dir: Path,
        datetime_format: Optional[str] = None,
    ) -> None:
        """Initialize Oak report."""
        self._data_directory = data_directory
        self._activity_user_journey = activity_user_journey
        self._report = report
        self._drawing_dir = drawing_dir
        self._drawings = list(self._drawing_dir.glob("*"))
        self._media_dir = media_dir
        self._media = list(self._media_dir.glob("*"))
        self._trails_dir = trails_dir
        self._trails = list(self._trails_dir.glob("*"))
        self._datetime_format = datetime_format

    def user_ids(self) -> pd.Series:
        """Return unique users in report."""
        return self._report["userId"].unique()

    def activity_ids(self) -> pd.Series:
        """Return unique activities in report."""
        return self._report["activity_id"].unique()

    def activity_flow_ids(self) -> pd.Series:
        """Return unique activity flows in report."""
        return self._report["activity_flow_id"].unique()

    def item_ids(self) -> pd.Series:
        """Return unique items in report."""
        return self._report["item_id"].unique()

    def _find_response_artifact_path(self, response: str) -> Path:
        """Find Path to response artifact."""
        media_responses = self._search_media(response)
        drawing_responses = self._search_drawings(response)
        trails_responses = self._search_trails(response)
        if len(media_responses) + len(drawing_responses) + len(trails_responses) > 1:
            raise ValueError(
                f"{self.__class__}::_find_response_artifact_path: "
                "Multiple responses found in different zip files: {response}"
            )
        if media_responses:
            if len(media_responses) > 1:
                raise ValueError(
                    f"{self.__class__}::_find_response_artifact: "
                    "Multiple media responses found: {response}"
                )
            return Path(media_responses[0])
        if drawing_responses:
            if len(drawing_responses) > 1:
                raise ValueError(
                    f"{self.__class__}::_find_response_artifact: "
                    "Multiple drawing responses found: {response}"
                )
            return Path(drawing_responses[0])
        if trails_responses:
            if len(trails_responses) > 1:
                raise ValueError(
                    f"{self.__class__}::_find_response_artifact: "
                    "Multiple trails responses found: {response}"
                )
            return Path(trails_responses[0])
        raise ValueError(
            f"{self.__class__}::_find_response_artifact_path: "
            f"No responses found: {response}"
        )

    def _search_media(self, search: str) -> List[Path]:
        """Filter media files matching search string."""
        return [file for file in self._media_dir.iterdir() if file.match(search)]

    def _search_drawings(self, search: str) -> List[Path]:
        """Filter drawing files matching search string."""
        return [file for file in self._drawing_dir.iterdir() if file.match(search)]

    def _search_trails(self, search: str) -> List[Path]:
        """Filter trail files matching search string."""
        return [file for file in self._trails_dir.iterdir() if file.match(search)]

    def _parse_response(self, response: str) -> Tuple[pd.DataFrame | Path, str]:
        """Parse resource string to Path."""
        # If response is a value, return a DataFrame with the value
        if response.startswith("value:"):
            return (
                pd.DataFrame([["value"], [response.split(":")[1].strip()]]),
                ".tsv",
            )
        # If response is a CSV file, read so that it is converted to TSV
        elif response.endswith(".csv"):
            return (
                pd.read_csv(
                    self._data_directory / self._find_response_artifact_path(response)
                ),
                ".tsv",
            )
        # If response is a different filetype, return the file path
        return (
            self._data_directory / self._find_response_artifact_path(response),
            response[-4:],
        )

    def bids_model(self) -> BidsModel:
        """Construct BIDS Model from current data model."""
        builder = BidsBuilder()
        for row in self._report.itertuples(name="Row"):
            if row.item == "study_id":
                continue
            (resource, suffix) = self._parse_response(row.response)
            print(f"resource: {resource}, suffix: {suffix}")
            builder.add(
                subject_id=row.study_id,
                datatype="beh",
                task_name=row.item,
                suffix=suffix,
                resource=resource,
                metadata={
                    "mindlogger_id": row.id,
                    "activity_start_time": row.activity_start_time.strftime(
                        self._datetime_format
                    )
                    if self._datetime_format is not None
                    else row.activity_start_time.isoformat(),
                    "activity_end_time": row.activity_end_time.strftime(
                        self._datetime_format
                    )
                    if self._datetime_format is not None
                    else row.activity_start_time.isoformat(),
                    "activity_scheduled_time": row.activity_scheduled_time,
                    "flag": row.flag,
                    "secret_user_id": row.secret_user_id,
                    "user_id": row.userId,
                    "activity_id": row.activity_id,
                    "activity_name": row.activity_name,
                    "activity_flow_id": row.activity_flow_id,
                    "activity_flow_name": row.activity_flow_name,
                    "item_id": row.item_id,
                    "item": row.item,
                    "response": row.response,
                    "prompt": row.prompt,
                    "options": row.options,
                    "version": row.version,
                    "rawScore": row.rawScore,
                    "reviewing_id": row.reviewing_id,
                    "event_id": row.event_id,
                    "timezone_offset": row.timezone_offset,
                },
            )
        return builder.build()

    @classmethod
    def create(cls, data_directory: Path) -> OakReport:
        """Collect files from data directory."""
        if not data_directory.is_dir():
            raise FileNotFoundError(f"Directory {data_directory} does not exist.")

        activity_path = data_directory / cls._ACTIVITY_USER_JOURNEY_FILENAME
        if not activity_path.is_file():
            raise FileNotFoundError(f"File {activity_path} does not exist.")
        activity_user_journey = pd.read_csv(activity_path)
        activity_user_journey.fillna("", inplace=True)

        report_path = data_directory / cls._REPORT_FILENAME
        if not report_path.is_file():
            raise FileNotFoundError(f"File {report_path} does not exist.")
        report = pd.read_csv(report_path)
        report.fillna("", inplace=True)

        # Construct column for study_id
        report["study_id"] = report["response"].where(report["item"] == "study_id")
        # Set study_id for cursive_q to the row above, backfill other values
        report["study_id"] = (
            report["study_id"].fillna(report["study_id"].shift(1)).bfill()
        )
        # Convert timestamps to datetime
        report["activity_end_time"] = pd.to_datetime(
            report["activity_end_time"], unit="ms", utc=True
        ).dt.tz_convert("America/New_York")
        report["activity_start_time"] = pd.to_datetime(
            report["activity_start_time"], unit="ms", utc=True
        ).dt.tz_convert("America/New_York")

        try:
            drawing_responses_path = next(
                data_directory.glob(cls._DRAWING_RESPONSES_PATTERN)
            )
        except StopIteration:
            raise FileNotFoundError(f"File {drawing_responses_path} does not exist.")
        drawing_responses_dir = drawing_responses_path.with_suffix("")
        drawing_responses_dir.mkdir(exist_ok=True)
        ZipFile(drawing_responses_path).extractall(drawing_responses_dir)

        try:
            media_responses_path = next(
                data_directory.glob(cls._MEDIA_RESPONSES_PATTERN)
            )
        except StopIteration:
            raise FileNotFoundError(f"File {media_responses_path} does not exist.")
        media_responses_dir = media_responses_path.with_suffix("")
        media_responses_dir.mkdir(exist_ok=True)
        ZipFile(media_responses_path).extractall(media_responses_dir)

        try:
            trails_responses_path = next(
                data_directory.glob(cls._TRAILS_RESPONSES_PATTERN)
            )
        except StopIteration:
            raise FileNotFoundError(f"File {trails_responses_path} does not exist.")
        trails_responses_dir = trails_responses_path.with_suffix("")
        trails_responses_dir.mkdir(exist_ok=True)
        ZipFile(trails_responses_path).extractall(trails_responses_dir)

        return cls(
            data_directory,
            activity_user_journey,
            report,
            drawing_responses_dir,
            media_responses_dir,
            trails_responses_dir,
        )
