from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Sequence, Type

from smolqwery._utils import import_class

if TYPE_CHECKING:
    from smolqwery import BaseExtractor

__all__ = ["BaseConfigProvider", "DjangoConfigProvider", "default_settings"]

GOOGLE_KEYS = [
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
    "auth_provider_x509_cert_url",
    "client_x509_cert_url",
]
CONFIG_KEYS = [
    "extractors",
    "dataset",
    "dataset_location",
    "acl_groups",
    "first_date",
    *(f"google_{x}" for x in GOOGLE_KEYS),
]


class BaseConfigProvider(ABC):
    """
    Base interface + some basic utilities to provide configuration to
    Smolqwery. It is abstracted so it's not too hard to adapt it for non-Django
    purposes, however for now the abstraction is not very useful (which is
    a sin but well).
    """

    @abstractmethod
    def __getattr__(self, item):
        """
        That's up to the implementer to allow getting the configuration keys
        as getattr()

        Parameters
        ----------
        item
            Name of the configuration key
        """

        raise NotImplementedError

    def get_mandatory_config_keys(self):
        """
        Returns the list of fields that are mandatory. Django implementation
        adds some fields on top of that, for example.
        """

        return CONFIG_KEYS

    def ensure_config(self):
        """
        Ensures that all required config keys are defined, otherwise crashes.
        Let's note that it's not validating the schema of the config, just the
        presence of said keys. If you put trash on them then it's on you.
        """

        missing = [x for x in self.get_mandatory_config_keys() if not hasattr(self, x)]

        if missing:
            self.raise_config_error(missing)

    def raise_config_error(self, missing_keys: Sequence[str]):
        """
        Raises an error because some config keys are missing

        Parameters
        ----------
        missing_keys
            List of missing keys
        """

        raise Exception(f'Missing config keys: {", ".join(missing_keys)}')

    def get_extractors(self) -> Sequence[Type["BaseExtractor"]]:
        """
        Imports all configured extractor classes
        """

        return [*map(import_class, self.extractors)]

    def get_credentials_data(self):
        """
        Returns Google Cloud credential JSON
        """

        return {k: getattr(self, f"google_{k}") for k in GOOGLE_KEYS}


class DjangoConfigProvider(BaseConfigProvider):
    """
    Django-specific configuration provider, takes care mostly of checking some
    extra keys and providing the access to django.conf.settings (while
    auto-prefixing keys).

    If you get smolqwery_settings.foo it'll fetch django_settings.SMOLQWERY_FOO
    """

    def __getattr__(self, item: str):
        from django.conf import settings

        return getattr(settings, f"SMOLQWERY_{item.upper()}")

    def get_mandatory_config_keys(self):
        return ["django_app", *super().get_mandatory_config_keys()]

    def raise_config_error(self, missing_keys: Sequence[str]):
        from django.core.exceptions import ImproperlyConfigured

        raise ImproperlyConfigured(
            f'Missing config keys: {", ".join(f"SMOLQWERY_{x.upper()}" for x in missing_keys)}'
        )


default_settings = DjangoConfigProvider()
