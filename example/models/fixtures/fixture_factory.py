from typing import List

from aleph.models import RecordSet

from example.constants import Namespace
from example.models.user import User


class FixtureFactory:

    def generate(self, key: str) -> RecordSet:
        if key == Namespace.USERS:
            return self.users()
        raise ValueError(f"Unknown fixture key: {key}")

    def users(self) -> List[User]:
        fake_user = User(name="John Doe", email="johndoe@example.org")
        return [fake_user]
