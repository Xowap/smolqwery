from core.models import Contract, EmailMessage, User
from core.smolqwery import UserExtractor
from dateutil.relativedelta import relativedelta
from django.test import TransactionTestCase
from django.utils.dateparse import parse_datetime

from smolqwery import ExtractionManager, default_settings
from smolqwery._utils import date_range


def flatten_steps(steps):
    for step in steps:
        yield from step.generator


class ExtractorTest(TransactionTestCase):
    def dataset_1(self) -> None:
        User.objects.bulk_create(
            [
                User(
                    date_create=parse_datetime("2022-01-01T00:42:00+0100"),
                    personal_info="a",
                ),
                User(
                    date_create=parse_datetime("2022-01-02T00:00:00+0100"),
                    personal_info="b",
                ),
                User(
                    date_create=parse_datetime("2022-01-02T13:00:00+0100"),
                    personal_info="c",
                ),
                User(
                    date_create=parse_datetime("2022-01-02T14:42:00+0100"),
                    personal_info="d",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T08:09:10+0100"),
                    personal_info="e",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T09:08:07+0100"),
                    personal_info="f",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T23:59:59.999+0100"),
                    personal_info="g",
                ),
                User(
                    date_create=parse_datetime("2022-01-04T11:11:11+0100"),
                    personal_info="h",
                ),
                User(
                    date_create=parse_datetime("2022-01-05T17:02:42+0100"),
                    personal_info="i",
                ),
            ]
        )

        Contract.objects.bulk_create(
            [
                Contract(
                    date_create=parse_datetime("2022-01-01T00:42:10+0100"),
                    date_validate=None,
                    state=Contract.State.created,
                    user=User.objects.get(personal_info="a"),
                    more_personal_info="1",
                    value=10,
                ),
                Contract(
                    date_create=parse_datetime("2022-01-02T14:00:00+0100"),
                    date_validate=parse_datetime("2022-01-04T14:00:00+0100"),
                    state=Contract.State.validated,
                    user=User.objects.get(personal_info="c"),
                    more_personal_info="2",
                    value=20,
                ),
                Contract(
                    date_create=parse_datetime("2022-01-02T15:42:00+0100"),
                    date_validate=parse_datetime("2022-01-03T15:49:00+0100"),
                    state=Contract.State.validated,
                    user=User.objects.get(personal_info="d"),
                    more_personal_info="2",
                    value=20,
                ),
                Contract(
                    date_create=parse_datetime("2022-01-04T00:42:41+0100"),
                    date_validate=parse_datetime("2022-01-05T18:40:12+0100"),
                    state=Contract.State.validated,
                    user=User.objects.get(personal_info="g"),
                    more_personal_info="2",
                    value=30,
                ),
            ]
        )

        for user in User.objects.all():
            EmailMessage.objects.create(
                user=user,
                date_sent=user.date_create + relativedelta(seconds=1),
                type=EmailMessage.Type.registration,
                content="welcome",
            )

        for contract in Contract.objects.all():
            EmailMessage.objects.create(
                user=contract.user,
                date_sent=contract.date_create + relativedelta(seconds=1),
                type=EmailMessage.Type.contract_created,
                content="contract created",
            )

            if contract.date_validate:
                EmailMessage.objects.create(
                    user=contract.user,
                    date_sent=contract.date_create + relativedelta(seconds=1),
                    type=EmailMessage.Type.contract_validated,
                    content="contract validated",
                )

    def setUp(self) -> None:
        self.dataset_1()

    def test_date_range(self):
        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01T03:40:14+0100"),
                    parse_datetime("2022-01-04T00:00:00+0100"),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01T03:40:14+0100"),
                    parse_datetime("2022-01-04T08:00:00+0100"),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01"),
                    parse_datetime("2022-01-04"),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01").date(),
                    parse_datetime("2022-01-04").date(),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

    def test_email_extractor(self):
        em = ExtractionManager(default_settings)

        e = em.extract_at_date(parse_datetime("2022-01-01").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 1, "prospects": 1, "clients": 0, "date": "2022-01-01"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-02").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 4, "prospects": 3, "clients": 0, "date": "2022-01-02"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-03").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 7, "prospects": 3, "clients": 1, "date": "2022-01-03"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-04").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 8, "prospects": 4, "clients": 2, "date": "2022-01-04"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-05").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 9, "prospects": 4, "clients": 3, "date": "2022-01-05"}], data
        )
