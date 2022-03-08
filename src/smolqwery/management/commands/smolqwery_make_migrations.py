from logging import WARNING, getLogger

from django.core.management import BaseCommand

from smolqwery.swallow import Swallow


class Command(BaseCommand):
    """
    Generates the migrations for the new extractors that have been created (if
    any of course). This creates migrations different from Django's default one
    but still is quite compatible with the system.
    """

    def handle(self, *args, **options):
        getLogger("smolqwery.swallow").setLevel(WARNING)

        s = Swallow()
        changed = False

        self.stdout.write(
            self.style.MIGRATE_HEADING(f"Migrations for '{s.settings.django_app}':")
            + "\n"
        )

        for migration in s.make_migrations():
            self.stdout.write(f"  {self.style.MIGRATE_LABEL(migration.name)}\n")
            changed = True

            table_name: str

            # noinspection PyUnresolvedReferences
            for table_name in migration.coconut.list_created_tables():
                _, table_name_short = table_name.rsplit(".", maxsplit=1)
                self.stdout.write(f"    - Created '{table_name_short}'\n")

        if not changed:
            self.stdout.write("  No changes detected")
