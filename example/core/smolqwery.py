import datetime
from dataclasses import dataclass
from typing import Iterator, Optional, Type

from django.db.models import Case, Count, IntegerField, OuterRef, Sum, Value, When
from django.db.models.functions import Coalesce

from smolqwery import BaseExtractor, D, ExtractorType, sq_field

from .models import Contract as ContractModel
from .models import EmailMessage
from .models import User as UserModel


@dataclass
class User:
    users: int = sq_field(differentiate=True)
    prospects: int = sq_field(differentiate=True)
    clients: int = sq_field(differentiate=True)


@dataclass
class Email:
    timestamp: datetime.datetime
    type: str


@dataclass
class Contract:
    average_value: Optional[float]
    count: int = sq_field(differentiate=True)


class UserExtractor(BaseExtractor[User]):
    def get_dataclass(self) -> Type[User]:
        return User

    def get_extractor_type(self) -> ExtractorType:
        return ExtractorType.date_aggregated

    def extract(
        self, date_start: datetime.datetime, date_end: datetime.datetime
    ) -> Iterator[User]:
        qs = UserModel.objects.filter(date_create__lt=date_end)

        contracts = ContractModel.objects.filter(
            user=OuterRef("pk"), date_create__lt=date_end
        )
        contracts_valid = contracts.filter(date_validate__lt=date_end)

        yield User(
            **qs.annotate(
                created_contracts=Count(contracts.values("pk")),
                valid_contracts=Count(contracts_valid.values("pk")),
            ).aggregate(
                users=Count("pk"),
                prospects=Coalesce(
                    Sum(
                        Case(
                            When(created_contracts__gt=0, then=1),
                            default=0,
                            output_field=IntegerField(),
                        )
                    ),
                    Value(0),
                    output_field=IntegerField(),
                ),
                clients=Coalesce(
                    Sum(
                        Case(
                            When(valid_contracts__gt=0, then=1),
                            default=0,
                            output_field=IntegerField(),
                        )
                    ),
                    Value(0),
                    output_field=IntegerField(),
                ),
            )
        )


class EmailExtractor(BaseExtractor[Email]):
    def get_dataclass(self) -> Type[D]:
        return Email

    def get_extractor_type(self) -> ExtractorType:
        return ExtractorType.individual_rows

    def extract(
        self, date_start: datetime.datetime, date_end: datetime.datetime
    ) -> Iterator[Email]:
        for email in EmailMessage.objects.filter(
            date_sent__gte=date_start, date_sent__lt=date_end
        ):
            yield Email(
                timestamp=email.date_sent,
                type=email.type,
            )
