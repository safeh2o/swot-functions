from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Type, TypedDict


class JSONDatapoint(TypedDict):
    tsDate: str | None
    hhDate: str | None
    tsFrc: float
    hhFrc: float
    tsCond: int
    tsTemp: int
    timezoneOffset: int


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
        timezone_offset: int,
    ):
        self.ts_date = ts_date
        self.hh_date = hh_date
        self.ts_frc = ts_frc
        self.hh_frc = hh_frc
        self.ts_cond = ts_cond
        self.ts_temp = ts_temp
        self.timezone_offset = timezone_offset

    def to_document(self, **kwargs) -> dict:
        return {
            "tsDate": self.ts_date,
            "tsFrc": self.ts_frc,
            "tsCond": self.ts_cond,
            "tsTemp": self.ts_temp,
            "hhDate": self.hh_date,
            "hhFrc": self.hh_frc,
            "timezoneOffset": self.timezone_offset,
            **kwargs,
        }

    def to_json(self) -> JSONDatapoint:
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
            "timezoneOffset": self.timezone_offset,
        }

    def to_csv_line(self) -> str:
        values = []
        for column in self.DEFAULT_MAPPING.values():
            val = self.get_str_attr(column)
            values.append(val)

        return ",".join(values)

    def get_str_attr(self, attr) -> str:
        val = getattr(self, attr)
        if isinstance(val, datetime):
            val = val.isoformat()
        elif val == None:
            val = ""
        return str(val)

    def copy(self) -> Datapoint:
        return Datapoint(
            self.ts_date,
            self.hh_date,
            self.ts_frc,
            self.hh_frc,
            self.ts_cond,
            self.ts_temp,
            self.timezone_offset,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, Datapoint):
            return False
        return self.ts_date == other.ts_date and self.hh_date == other.hh_date

    def __gt__(self, other) -> bool:
        if not isinstance(other, Datapoint):
            return False
        return self.ts_date > other.ts_date and self.hh_date > other.hh_date

    def __ge__(self, other) -> bool:
        if not isinstance(other, Datapoint):
            return False
        return self.__eq__(other) or self.__gt__(other)

    def __str__(self):
        return self.to_csv_line()

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def header_line() -> str:
        return ",".join(Datapoint.DEFAULT_MAPPING.keys())

    @classmethod
    def from_document(cls: Type[Datapoint], document: dict) -> Datapoint:
        return cls(
            ts_date=document["tsDate"],
            hh_date=document["hhDate"],
            ts_frc=document["tsFrc"],
            hh_frc=document["hhFrc"],
            ts_cond=document["tsCond"],
            ts_temp=document["tsTemp"],
            timezone_offset=document["timezoneOffset"],
        )

    @staticmethod
    def get_csv_lines(datapoints: list[Datapoint]) -> list[str]:
        lines = [Datapoint.header_line()]
        for datapoint in datapoints:
            lines.append(str(datapoint))
        return lines

    @staticmethod
    def add_timezones(datapoints: list[Datapoint]):
        for datapoint in datapoints:
            timezone_offset = datapoint.timezone_offset
            if timezone_offset:
                tzinfo = timezone(offset=timedelta(seconds=timezone_offset))
                datapoint.ts_date = datapoint.ts_date.replace(
                    tzinfo=timezone.utc
                ).astimezone(tzinfo)
                datapoint.hh_date = datapoint.hh_date.replace(
                    tzinfo=timezone.utc
                ).astimezone(tzinfo)


class StandardizationError:
    def __init__(self, row_number: int, datapoint: Datapoint, bad_columns: set[str]):
        self.row_number = row_number
        self.datapoint = datapoint
        self.bad_columns = bad_columns

    def to_csv_line(self):
        marked_datapoint = self.datapoint.copy()
        values = []
        for col in Datapoint.DEFAULT_MAPPING.values():
            val = marked_datapoint.get_str_attr(col)
            if col in self.bad_columns:
                val += "*"
            values.append(val)
        row = ",".join(values)

        return f"{self.row_number},{row}"


class UploadedFileSummary:
    def __init__(self, filename: str, errors: list[StandardizationError]):
        self.filename = filename
        self.errors = errors


def round_time(dt: datetime):
    return dt - timedelta(microseconds=int(dt.strftime("%f")))


def format_unknown_date(date_string: str):
    try:
        return round_time(
            datetime(1900, 1, 1, tzinfo=timezone.utc)
            + timedelta(days=float(date_string))
        )
    except ValueError:
        pass

    try:
        return format_plain_date(date_string)
    except:
        return None


def format_plain_date(date_string: str):
    date_string_len = len(date_string)
    spacer = " "
    if "T" in date_string:
        spacer = "T"
    if date_string_len == 16:
        date_format = f"%Y-%m-%d{spacer}%H:%M"
    elif date_string_len == 19:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S"
    elif date_string_len == 23:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S.%f"
    elif date_string_len == 25:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S%z"
    elif date_string_len >= 26 and date_string_len < 29:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S.%f%z"
        date_string = date_string[:26] + ":00"
    elif date_string_len == 29:
        date_format = f"%Y-%m-%d{spacer}%H:%M:%S.%f%z"
    else:
        return None

    raw_date = datetime.strptime(date_string, date_format)
    if not raw_date.tzinfo:
        res = raw_date.replace(tzinfo=timezone.utc)
    else:
        res = raw_date
    return res


def get_timezone_offset(ts_date: datetime, hh_date: datetime) -> int:
    if not ts_date or not hh_date or ts_date.tzinfo != hh_date.tzinfo:
        return 0
    assert isinstance(ts_date, datetime)
    offset = ts_date.utcoffset()
    assert isinstance(offset, timedelta)
    return int(offset.total_seconds())


def try_format(num_string: str, cast_type):
    try:
        return cast_type(num_string)
    except:
        return None


def get_bad_columns(datapoint: Datapoint):
    TWO_DAYS = 48 * 3600  # two days in seconds
    bad_columns = set()
    now = datetime.now(timezone.utc)
    # if ts date is null or in the future
    if not datapoint.ts_date or datapoint.ts_date > now:
        bad_columns.add("ts_date")
    # if hh date is null or in the future
    if not datapoint.hh_date or datapoint.hh_date > now:
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

    # if ts frc is null
    if datapoint.ts_frc == None or datapoint.ts_frc <= 0:
        bad_columns.add("ts_frc")
    # if hh frc is null or negative
    if datapoint.hh_frc == None or datapoint.hh_frc < 0:
        bad_columns.add("hh_frc")

    # if hh frc is 0.06 greater than ts frc
    if (
        datapoint.hh_frc
        and datapoint.ts_frc
        and datapoint.hh_frc - datapoint.ts_frc > 0.06
    ):
        bad_columns.update({"ts_frc", "hh_frc"})

    # if there is no timezone offset found or inferred
    if datapoint.timezone_offset == None:
        bad_columns.update({"ts_date", "hh_date"})

    return bad_columns


def extract(filename: str) -> tuple[list[Datapoint], list]:
    datapoints = []
    errors: list[StandardizationError] = []

    file = open(filename, "r")
    first_line = file.readline().rstrip("\n")
    first_line_columns = first_line.split(",")
    indices = {}
    for col in Datapoint.DEFAULT_COLUMNS:
        indices[col] = [
            i for i, column_name in enumerate(first_line_columns) if col in column_name
        ][0]

    for row_number, l in enumerate(file, 2):
        # Skip over lines without six elements and empty lines
        l = l.rstrip("\n")
        if not l or re.match("^,*$", l):
            continue

        line = l.strip().split(",")

        ts_date = format_unknown_date(line[indices["ts_datetime"]])
        hh_date = format_unknown_date(line[indices["hh_datetime"]])
        ts_frc = try_format(line[indices["ts_frc"]], float)
        hh_frc = try_format(line[indices["hh_frc"]], float)
        ts_cond = try_format(line[indices["ts_cond"]], float)
        ts_temp = try_format(line[indices["ts_wattemp"]], float)
        timezone_offset = get_timezone_offset(ts_date, hh_date)

        datapoint = Datapoint(
            ts_date, hh_date, ts_frc, hh_frc, ts_cond, ts_temp, timezone_offset
        )
        bad_columns_in_datapoint = get_bad_columns(datapoint)

        if bad_columns_in_datapoint:
            err = StandardizationError(row_number, datapoint, bad_columns_in_datapoint)
            errors.append(err)
        else:
            datapoints.append(datapoint)

    file.close()
    return datapoints, errors
