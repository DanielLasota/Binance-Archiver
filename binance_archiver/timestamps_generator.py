from __future__ import annotations

from datetime import datetime, timezone


class TimestampsGenerator:
    __slots__ = ()

    @staticmethod
    def get_utc_formatted_timestamp_for_file_name() -> str:
        return datetime.utcnow().strftime("%d-%m-%YT%H-%M-%SZ")

    @staticmethod
    def get_utc_timestamp_epoch_milliseconds() -> int:
        return round(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def get_utc_timestamp_epoch_seconds() -> int:
        return round(datetime.now(timezone.utc).timestamp())
