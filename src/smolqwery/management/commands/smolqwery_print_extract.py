import datetime
from argparse import ArgumentParser

from dateutil.relativedelta import relativedelta
from django.core.management import BaseCommand
from django.utils.dateparse import parse_datetime
from django.utils.timezone import get_current_timezone, localtime
from rich.console import Console
from rich.table import Table

from smolqwery import ExtractionManager
from smolqwery._utils import date_range
from smolqwery.config import default_settings


class Command(BaseCommand):
    def add_arguments(self, parser: ArgumentParser):
        now = localtime() - relativedelta(days=1)
        parser.add_argument("-f", "--first-date", type=parse_datetime, default=None)
        parser.add_argument("-l", "--last-date", type=parse_datetime, default=now)

    def handle(self, first_date, last_date, *args, **options):
        em = ExtractionManager(default_settings)
        console = Console()

        if first_date is None:
            first_date = last_date

        first_date = first_date.astimezone(get_current_timezone())
        last_date = last_date.astimezone(get_current_timezone())

        first_date -= relativedelta(days=1)
        last_date += relativedelta(days=1)

        for extractor_class in default_settings.get_extractors():
            extractor = extractor_class(em.bq, default_settings)
            table = Table(title=extractor.get_table_name())
            schema = extractor.get_schema()

            for field in schema["regular_fields"]:
                table.add_column(
                    field["name"],
                    style=(
                        "magenta"
                        if field["name"] == extractor.get_date_field()
                        else None
                    ),
                )

            for today in date_range(first_date, last_date):
                for e in em.extract_at_date(today, [extractor_class]):
                    for row in e.generator:
                        table.add_row(
                            *(f'{row[f["name"]]}' for f in schema["regular_fields"])
                        )

            console.print(table)
