# Smolqwery

A Django-oriented micro-framework to help structuring BigQuery exports with Data
Studio and analytics in mind.

It lets you write minimal extractors that will generate the statistics you need
and it will manage everything around them (creating the tables on BigQuery,
running the exports, etc).

## Integration guide

We'll review here how you can integrate Smolqwery in your project.

### Installation

First step is (obviously to add it to the dependencies). You just need to
specify that you need the `smolqwery` package (use `pip`, `poetry` or whatever).

### Django configuration

You can thank Google for that, the configuration is a bit complex. The first
thing to do is to add it in your installed apps:

```python
INSTALLED_APPS = [
    # your stuff
    "smolqwery",
    # more of your stuff
]
```

Then there is _a few_ variables to configure:

-   Things specific to your project
    -   `SMOLQWERY_EXTRACTORS` &mdash; List of extractors, more on this below
    -   `SMOLQWERY_DJANGO_APP` &mdash; App in which migrations must be created
    -   `SMOLQWERY_FIRST_DATE` &mdash; First date of data to include in exports
        (all days between this date and now will be exported)
-   Things that you want to specify about BigQuery
    -   `SMOLQWERY_DATASET` &mdash; Name of the dataset (database if you compare
        to PostgreSQL for example). It will be created automatically by the
        migrations, you don't need to create it yourself.
    -   `SMOLQWERY_DATASET_LOCATION` &mdash; Location of the data. Unless you
        want to be specific you can just say "EU" or "US"
    -   `SMOLQWERY_ACL_GROUPS` &mdash; List of IAM groups emails that will
        receive the read/write permissions on the created dataset
-   Google credentials (more on this later)
    -   `SMOLQWERY_GOOGLE_TYPE`
    -   `SMOLQWERY_GOOGLE_PROJECT_ID`
    -   `SMOLQWERY_GOOGLE_PRIVATE_KEY_ID`
    -   `SMOLQWERY_GOOGLE_PRIVATE_KEY`
    -   `SMOLQWERY_GOOGLE_CLIENT_EMAIL`
    -   `SMOLQWERY_GOOGLE_CLIENT_ID`
    -   `SMOLQWERY_GOOGLE_AUTH_URI`
    -   `SMOLQWERY_GOOGLE_TOKEN_URI`
    -   `SMOLQWERY_GOOGLE_AUTH_PROVIDER_X509_CERT_URL`
    -   `SMOLQWERY_GOOGLE_CLIENT_X509_CERT_URL`

### Getting Google credentials

So. There is a bunch of credentials to get. Roughly, the steps to get them is:

-   Log into the Google Cloud Console
-   Create a project (you can re-use this same project between several instances
    of your code as long as you let each instance use a different dataset name)
-   Create a service account within this project
-   Download the credentials of this service account (as JSON file) and recopy
    the contents of this file into the configuration. For example in the JSON
    file there is a `project_id` entry, it matches the
    `SMOLQWERY_GOOGLE_PROJECT_ID` setting

### Creating extractors

You can now start creating extractors. It's the way your code will convert your
data into the statistics you want to store.

#### Extractor type

The first step is to decide what kind of extractor you're going to do:

-   Date-aggregated &mdash; Each row is a single date. By example "today there
    was 345 new users and 23 new contracts"
-   Individual rows &mdash; Each row is one entry. By example, each row can
    represent one email that has been sent.

How to choose? If you're dealing with personal data, the only way you can use it
for statistics without consent is to aggregate it.

> _Note_ &mdash; As long as an ID represents a single or a few users (hash,
> fingerprint, etc) then it's considered to be a personal data. There is no such
> thing as anonymization. The only way out is aggregation.

The way to handle dates is going to be different depending on the type:

-   Date-aggregated extractors don't have to include a date in their data model,
    since it's the system that decides which date range corresponds to which
    date "row". For that matter, you need need to reply with the data you deduce
    from the range you're asked to extract and that's it.
-   Individual rows must contain a timestamp field which indicates to which date
    this field is related. It can be a timestamp (date/time + timezone) or a
    simple date. It will not be added automatically but it is expected to be
    named `timestamp` (see below how to define fields). You can override this
    name in the extractor if you need to.

#### Dataclass

You declare what you're going to return using a dataclass. It's a bit like
Django models, it's used to define what are the fields.

Here is type types mapping (in relation to Big Query's types):

-   `int` &rarr; `INT64`
-   `float` &rarr; `FLOAT64`
-   `bool` &rarr; `BOOL`
-   `str` &rarr; `STRING`
-   `bytes` &rarr; `BYTES`
-   `datetime.date` &rarr; `DATE`
-   `datetime.datetime` &rarr; `TIMESTAMP` (make sure to use time-zone-aware
    datetime instances, there is no check of this)

If you need your field to be nullable you can use the `Optional` type
annotation.

Another thing is the "differentiated" fields. For example let's say that you
have a strictly growing metric ("number of users that ever registered") and you
want to easily know the difference from one date to the other ("number of users
that registered during the time range") in the data studio. You can mark a field
as "differentiated", which will trigger the creation of a view where the metric
is differentiated day-by-day.

You would have the `user` table with this data:

| Date       | Registered |
| ---------- | ---------- |
| 2022-01-01 | 10         |
| 2022-01-02 | 20         |
| 2022-01-03 | 35         |

And then the `user_delta` view with:

| Date       | Registered |
| ---------- | ---------- |
| 2022-01-01 | 10         |
| 2022-01-02 | 10         |
| 2022-01-03 | 15         |

This can be done using the `sq_field(diffrentiate=True)` field (which is a
shortcut to `dataclasses`'s `field` created for our purpose).

Here is an example of both a date-aggregated and individual-rows dataclasses:

```python
from dataclasses import dataclass
from smolqwery import sq_field
import datetime

@dataclass
class User:
    users: int = sq_field(differentiate=True)
    prospects: int = sq_field(differentiate=True)
    clients: int = sq_field(differentiate=True)


@dataclass
class Email:
    timestamp: datetime.datetime
    type: str
```

#### Extractor

The extractors themselves are a simple interface that you need to implement to
reach your needs. For example:

```python
class EmailExtractor(BaseExtractor[Email]):
    def get_dataclass(self) -> Type[Email]:
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
```

Let's note here that `date_start` is inclusive and `date_end` is exclusive.

You need to declare your extractors in the settings, for example:

```python
SMOLQWERY_EXTRACTORS = [
    "core.smolqwery.UserExtractor",
    "core.smolqwery.EmailExtractor",
]
```

#### Migrations

Like Django, and using Django's system in part, Smolqwery will manage its table
within BigQuery using migrations. Those schema are automatically created from
the dataclasses that you've defined.

However unlike Django you cannot change a data model once it has been created,
because it would complicate things a lot for something that you don't really
need.

See it that way: you want to add a bunch of statistics to your BigQuery `user`
table. If your statistics can be computed from observing the data in your
database, you could just create a `user2` table and re-compute statistics from
the beginning into this table. This will provide the warranty that all fields
present indeed have a value (as opposed to having to keep all new columns at
NULL).

It's quite flexible this way: you can either create a new table and put just the
new fields there, either forget about the old table and re-compute everything
from the start in a new table, etc.

So, how do you migrate?

```
./manage.py smolqwery_make_migrations
./manage.py migrate
```

Let's note that if you (or the next person) doesn't have the Google credentials
configured, then you'll run into an issue.

### Exporting

Now you can run the export.

Either with a test run

```
./manage.py smolqwery_print_extract -f 2022-01-01 -l 2022-01-05
```

Either with a real run, that will really insert the data

```
./manage.py smolqwery_extract
```

The extract will look up for the date of the last extract table by table. If the
extract was never done then it will fall back to the `SMOLQWERY_FIRST_DATE`
setting.

All the days between the last extract and the last revolute day (in Django's
time zone) will be extracted.

You can safely run this as a cron, several times a day if you want to.

### Exploiting the data

Now all your data is in BigQuery and you can start using it from Data Studio or
other sources!
