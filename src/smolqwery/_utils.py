import datetime
import re
from importlib import import_module
from typing import Any, Iterator, Mapping, Sequence, Type, Union

from dateutil.relativedelta import relativedelta
from django.db.models import QuerySet

try:
    from django.utils.timezone import localtime
except ImportError:
    localtime = datetime.datetime.now


IS_CAMEL_WORD = re.compile(r"[A-Z][a-z]*")
IS_NUM = re.compile(r"\d+")
IS_ABBREV = re.compile(r"[A-Z]{2,}")
IS_SNAKE_WORD = re.compile(r"[a-z]+")


def smart_split_name(s: str) -> Sequence[str]:
    """
    Smart name splitting. Understand both CamelCase and snake_case, including
    camel case abbreviations like `setIBAN`. Returns the lowercase parts of the
    name. Everything that isn't letters or numbers will be discarded.
    """

    out = []
    pos = 0

    while pos < len(s):
        m_camel_word = IS_CAMEL_WORD.search(s, pos)
        m_num = IS_NUM.search(s, pos)
        m_abbrev = IS_ABBREV.search(s, pos)
        m_snake_word = IS_SNAKE_WORD.search(s, pos)

        match_pos = min(
            (x.start() for x in [m_camel_word, m_num, m_abbrev, m_snake_word] if x),
            default=-1,
        )

        if match_pos < 0:
            break

        if m_abbrev and m_abbrev.start() == match_pos:
            if m_abbrev.end() == len(s):
                out.append(m_abbrev.group(0))
                pos = m_abbrev.end()
            else:
                next_c = s[m_abbrev.end()]

                if next_c.islower():
                    out.append(m_abbrev.group(0)[0:-1])
                    pos = m_abbrev.end() - 1
                else:
                    out.append(m_abbrev.group(0))
                    pos = m_abbrev.end()
        elif m_num and m_num.start() == match_pos:
            out.append(m_num.group(0))
            pos = m_num.end()
        elif m_camel_word and m_camel_word.start() == match_pos:
            out.append(m_camel_word.group(0))
            pos = m_camel_word.end()
        elif m_snake_word and m_snake_word.start() == match_pos:
            out.append(m_snake_word.group(0))
            pos = m_snake_word.end()
        else:
            pos += 1

    return tuple(x.lower() for x in out)


def to_snake(seq: Sequence[str]) -> str:
    """
    Converts the output of smart_split_name() into a snake case variable
    name.

    Parameters
    ----------
    seq
        Output of smart_split_name()
    """

    return "_".join(seq)


def import_class(class_fqn: str) -> Type:
    """
    Imports a class from its FQN (technically it imports anything that is
    inside a module, actually).

    Parameters
    ----------
    class_fqn
        FQN of the class, like "datetime.datetime"
    """

    module_name, class_name = class_fqn.rsplit(".", maxsplit=1)
    module = import_module(module_name)
    return getattr(module, class_name)


DateOrTime = Union[datetime.date, datetime.datetime]


def zero_date(date: DateOrTime) -> datetime.datetime:
    """
    First "instant" of a date (on current time zone).

    Parameters
    ----------
    date
        Date for which we want the first instant
    """

    return localtime() + relativedelta(
        year=date.year,
        month=date.month,
        day=date.day,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def exclusive_date(date: DateOrTime) -> datetime.datetime:
    """
    For a given naive date generates the date on which you can do a __lt filter
    to get everything before and up to this date in configured time zone.

    Parameters
    ----------
    date
        Date to transform
    """

    return zero_date(date) + relativedelta(days=1)


def date_range(
    exclusive_first_day: DateOrTime, exclusive_last_day: DateOrTime
) -> Iterator[datetime.date]:
    """
    Generates a date range that go from the first day to the last day without
    going through the days themselves.

    For example if exclusive_first_day is 1st of January at 3pm and
    exclusive_last_day is 4th of January at 1am, then the dates of the range
    will be 2nd and 3rd of January.

    Parameters
    ----------
    exclusive_first_day
        First date/timestamp. Exclusive bound, that date won't be in the range.
    exclusive_last_day
        Last date/timestamp. Exclusive bound, that date won't be in the range.
    """

    current = exclusive_date(exclusive_first_day)
    last = zero_date(exclusive_last_day)

    while current < last:
        yield current.date()
        current += relativedelta(days=1)


class ChunkIterator:
    """
    This iterator cuts down an iterator into several chunks. By example, you
    can iterate over a very long list and do chunk creates every 1000 entry
    using this.
    """

    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.iterating = True
        self._next = None
        self.iterated = 0

        self.next()

    def next(self):
        nxt = self._next

        try:
            self._next = next(self.iterator)
        except StopIteration:
            self.iterating = False

        return nxt

    def chunks(self, size):
        """
        Call this method to return the chunks iterator

        :param size: int, size of a chunk
        :return:
        """

        def iter_chunk():
            for i in range(0, size):
                yield self.next()
                self.iterated += 1

                if not self.iterating:
                    break

        while self.iterating:
            yield iter_chunk()


def json_dates(data: Any) -> Any:
    """
    Hunts down dates in the provided structure in oder to replace them with
    their ISO format with the intent of having something that can be turned
    into JSON easily.

    Parameters
    ----------
    data
        Data to transform
    """

    if isinstance(data, (str, bytes)):
        return data
    elif isinstance(data, Sequence):
        return [*map(json_dates, data)]
    elif isinstance(data, Mapping):
        return {k: json_dates(v) for k, v in data.items()}
    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()
    else:
        return data
