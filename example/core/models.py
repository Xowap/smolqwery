from django.db import models


class User(models.Model):
    date_create = models.DateTimeField()
    personal_info = models.TextField()


class Contract(models.Model):
    class State(models.TextChoices):
        created = "created"
        validated = "validated"

    date_create = models.DateTimeField()
    date_validate = models.DateTimeField(null=True)
    state = models.CharField(choices=State.choices, max_length=100)
    user = models.ForeignKey("User", on_delete=models.CASCADE)
    more_personal_info = models.TextField()
    value = models.FloatField()


class EmailMessage(models.Model):
    class Type(models.TextChoices):
        registration = "registration"
        contract_created = "contract_created"
        contract_validated = "contract_validated"

    user = models.ForeignKey("User", on_delete=models.CASCADE)
    date_sent = models.DateTimeField()
    type = models.CharField(choices=Type.choices, max_length=100)
    content = models.TextField()
