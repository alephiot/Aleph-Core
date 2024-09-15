from enum import Enum
from aleph.models import Model


class PLCStage(Enum):
    PROCESSING = 0
    AWAITING = 1
    CLEANING = 2
    OFF = 3


class PLC(Model):
    temperature: float  # °C
    flow: float  # m3/s
    stage: PLCStage

    @property
    def temperature_fahrenheit(self):
        return self.temperature * 9 / 5 + 32
