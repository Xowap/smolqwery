from django.apps import AppConfig

from .config import default_settings


class SmolQweryConfig(AppConfig):
    name = "smolqwery"
    verbose_name = "SmolQwery"

    def ready(self):
        default_settings.ensure_config()
