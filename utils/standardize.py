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

    def to_json(self) -> dict:
        if self.ts_date:
            ts_date = datetime.isoformat(self.ts_date)
        else:
            ts_date = None

        if self.hh_date:
            hh_date = datetime.isoformat(self.hh_date)
        else:
            hh_date = None

        return {
            "tsDate": ts_date,
            "hhDate": hh_date,
            "tsFrc": self.ts_frc,
            "hhFrc": self.hh_frc,
            "tsCond": self.ts_cond,
            "tsTemp": self.ts_temp,
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


def get_bad_columns(datapoint: Datapoint):
    TWO_DAYS = 48 * 3600  # two days in seconds
    bad_columns = set()
    if not datapoint.ts_date or datapoint.ts_date > datetime.now():
        bad_columns.add("ts_date")
    if not datapoint.hh_date or datapoint.hh_date > datetime.now():
        bad_columns.add("hh_date")

    if (
        datapoint.ts_date != None
        and datapoint.hh_date != None
        and (
            datapoint.ts_date > datapoint.hh_date  # if tapstand is later than household
            or (datapoint.hh_date - datapoint.ts_date).total_seconds()
            >= TWO_DAYS  # if time difference is two days or greater
        )
    ):
        bad_columns.update({"ts_date", "hh_date"})  # highlight both ts and hh dates

    if datapoint.ts_frc == None or datapoint.ts_frc <= 0:
        bad_columns.add("ts_frc")
    if datapoint.hh_frc == None or datapoint.hh_frc <= 0:
        bad_columns.add("hh_frc")

    if (
        datapoint.hh_frc
        and datapoint.ts_frc
        and datapoint.hh_frc - datapoint.ts_frc > 0.06
    ):
        bad_columns.update({"ts_frc", "hh_frc"})

    return list(bad_columns)


def extract(filename: str) -> list[Datapoint]:
    datapoints = []
    errors = []

    file = open(filename, "r")
    header_line = file.readline().rstrip("\n")
    header_line = header_line.split(",")
    columns = Datapoint.DEFAULT_COLUMNS
    indices = {}
    for col in columns:
        indices[col] = [i for i, x in enumerate(header_line) if col in x][0]

    for row_number, l in enumerate(file, 1):
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
        errors_in_datapoint = get_bad_columns(datapoint)

        if errors_in_datapoint:
            errors.append(
                {
                    "row_number": row_number,
                    "datapoint": datapoint.to_json(),
                    "bad_columns": errors_in_datapoint,
                }
            )
        else:
            datapoints.append(datapoint)

    file.close()
    return datapoints, errors
