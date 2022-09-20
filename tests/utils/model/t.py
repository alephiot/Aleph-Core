from aleph_core.utils.model import Model


class MyModel(Model, key="this.is.my.key"):
    field1: int
    field2: str


m = MyModel(field1=1, field2="hello")

print(m)