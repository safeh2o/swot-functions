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


def format_unknown_date(date_string: str):
    try:
        return round_time(datetime(1900, 1, 1) + timedelta(days=float(date_string)))
    except ValueError:
        return format_plain_date(date_string)


def format_plain_date(date_string: str):
    spacer = " "
    if "T" in date_string:
        spacer = "T"
    if len(date_string) == 16:
        date_format = f"%Y-%m-%d{spacer}%H:%M"
    elif len(date_string) == 19:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S"
    elif len(date_string) == 23:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S.%f"
    elif len(date_string) == 29:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S.%f%z"
    else:
        return None

    return datetime.strptime(date_string, date_format)


def try_format(num_string: str, cast_type):
    try:
        return cast_type(num_string)
    except:
        return None


def extract(filename: str) -> list[Datapoint]:
    datapoints = []

    file = open(filename, "r")
    header_line = file.readline().rstrip("\n")
    header_line = header_line.split(",")
    columns = Datapoint.DEFAULT_COLUMNS
    indices = {}
    for col in columns:
        indices[col] = [i for i, x in enumerate(header_line) if col in x][0]

    for row_num, l in enumerate(file, 1):
        # Skip over lines without six elements and empty lines
        l = l.rstrip("\n")
        if not l:
            continue

        line = l.strip().split(",")

        ts_date = format_unknown_date(line[indices["ts_datetime"]])
        hh_date = format_unknown_date(line[indices["hh_datetime"]])
        ts_frc = try_format(line[indices["ts_frc"]], float)
        hh_frc = try_format(line[indices["hh_frc"]], float)
        ts_cond = try_format(line[indices["ts_cond"]], int)
        ts_temp = try_format(line[indices["ts_wattemp"]], float)

        datapoint = Datapoint(ts_date, hh_date, ts_frc, hh_frc, ts_cond, ts_temp)
        datapoints.append(datapoint)

    file.close()
    return datapoints
