from __future__ import annotations
from datetime import datetime, timedelta
from bson import json_util
import json


class Datapoint(object):
    DEFAULT_COLUMNS = [
        "ts_datetime",
        "hh_datetime",
        "ts_frc",
        "hh_frc",
        "ts_wattemp",
        "ts_cond",
    ]

    DEFAULT_MAPPING = {
        "ts_datetime": "ts_date",
        "hh_datetime": "hh_date",
        "ts_frc": "ts_frc",
        "hh_frc": "hh_frc",
        "ts_wattemp": "ts_temp",
        "ts_cond": "ts_cond",
    }

    def __init__(
        self,
        ts_date: datetime,
        hh_date: datetime,
        ts_frc: float,
        hh_frc: float,
        ts_cond: int,
        ts_temp: int,
    ) -> Datapoint:
        self.ts_date = ts_date
        self.hh_date = hh_date
        self.ts_frc = ts_frc
        self.hh_frc = hh_frc
        self.ts_cond = ts_cond
        self.ts_temp = ts_temp

    def to_document(self, **kwargs) -> dict:
        return {
            "tsDate": self.ts_date,
            "tsFrc": self.ts_frc,
            "tsCond": self.ts_cond,
            "tsTemp": self.ts_temp,
            "hhDate": self.hh_date,
            "hhFrc": self.hh_frc,
            **kwargs,
        }

    def to_csv_line(self) -> str:
        values = []
        for column in self.DEFAULT_MAPPING.values():
            val = getattr(self, column)
            if isinstance(val, datetime):
                val = val.isoformat()
            elif not val:
                val = ""
            values.append(str(val))

        return ",".join(values)

    def __eq__(self, other: Datapoint) -> bool:
        return self.ts_date == other.ts_date and self.hh_date == other.hh_date

    def __gt__(self, other: Datapoint) -> bool:
        return self.ts_date > other.ts_date and self.hh_date > other.hh_date

    def __ge__(self, other: Datapoint) -> bool:
        return self.__eq__(other) or self.__gt__(other)

    def __str__(self):
        return self.to_csv_line()

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_document(cls: Datapoint, document: dict) -> Datapoint:
        return cls(
            ts_date=document["tsDate"],
            hh_date=document["hhDate"],
            ts_frc=document["tsFrc"],
            hh_frc=document["hhFrc"],
            ts_cond=document["tsCond"],
            ts_temp=document["tsTemp"],
        )

    def get_csv_lines(datapoints: list[Datapoint]) -> list[str]:
        lines = []
        lines.append(",".join(Datapoint.DEFAULT_MAPPING.keys()))
        for datapoint in datapoints:
            lines.append(str(datapoint))
        return lines


def round_time(dt: datetime):
    return dt - timedelta(microseconds=int(dt.strftime("%f")))


def extract(filename: str) -> list[Datapoint]:
    datapoints = []

    file = open(filename, "r")
    header_line = file.readline().rstrip("\n")
    header_line = header_line.split(",")
    columns = Datapoint.DEFAULT_COLUMNS
    indices = {}
    for col in columns:
        indices[col] = [i for i, x in enumerate(header_line) if col in x][0]

    for l in file:
        # Skip over lines without six elements and empty lines
        l = l.rstrip('\n')
        if not l:
            continue

        line = l.strip().split(",")

        try:
            ts_date = round_time(
                datetime(1900, 1, 1)
                + timedelta(days=float(line[indices["ts_datetime"]]))
            )
        except ValueError:
            try:
                ts_date = datetime.strptime(
                    line[indices["ts_datetime"]], "%Y-%m-%dT%H:%M"
                )
            except ValueError:
                try:
                    ts_date = datetime.strptime(
                        line[indices["ts_datetime"]], "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                except ValueError:
                    ts_date = None

        try:
            hh_date = round_time(
                datetime(1900, 1, 1)
                + timedelta(days=float(line[indices["hh_datetime"]]))
            )
        except ValueError:
            try:
                hh_date = datetime.strptime(
                    line[indices["hh_datetime"]], "%Y-%m-%dT%H:%M"
                )
            except ValueError:
                try:
                    hh_date = datetime.strptime(
                        line[indices["hh_datetime"]], "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                except ValueError:
                    hh_date = None

        try:
            ts_frc = float(line[indices["ts_frc"]])
        except ValueError:
            ts_frc = None

        try:
            hh_frc = float(line[indices["hh_frc"]])
        except ValueError:
            hh_frc = None

        try:
            ts_cond = int(line[indices["ts_cond"]])
        except ValueError:
            ts_cond = None

        try:
            ts_temp = int(round(float(line[indices["ts_wattemp"]])))
        except ValueError:
            ts_temp = None

        datapoints.append(Datapoint(ts_date, hh_date, ts_frc, hh_frc, ts_cond, ts_temp))

    file.close()
    return datapoints
