from datetime import datetime, timedelta

from example.models.person import Person
from example.models.batch import Batch, Recipe


t = [datetime(2021, 1, 1) + timedelta(hours=1) for i in range(100)]

persons = [
    Person(id_="1", name="Alice", email="alice@example.org"),
    Person(id_="2", name="Bob", email="bob@example.org"),
]

batches = [
    Batch(
        batch_number=1,
        date="2021-01-01",
        assignee_id=persons[0].id_,
        recipe=Recipe.SIMPLE,
    ),
    Batch(
        batch_number=2,
        date="2021-01-01",
        assignee_id=persons[1].id_,
        recipe=Recipe.SIMPLE,
    ),
    Batch(
        batch_number=1,
        date="2021-01-02",
        assignee_id=persons[0].id_,
        recipe=Recipe.SIMPLE,
    ),
    Batch(
        batch_number=2,
        date="2021-01-02",
        assignee_id=persons[1].id_,
        recipe=Recipe.SIMPLE,
    ),
]
