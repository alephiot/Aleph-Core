import pycomm3
from aleph_core import Connection
from enum import Enum


class AllenBradleyModels(Enum):
    MicroLogix = "MicroLogix"
    ControlLogix = "ControlLogix"
    CompactLogix = "CompactLogix"
    Micro800 = "Micro800"
    SLC500 = "MicroLogix"


class AllenBradleyConnection(Connection):
    """
    Connect to an Allen Bradley PLC
    """

    ip_address = "localhost"
    controller: AllenBradleyModels = AllenBradleyModels.MicroLogix
    plc = None

    report_by_exception = True

    def open(self):
        if self.controller in [
            AllenBradleyModels.ControlLogix,
            AllenBradleyModels.CompactLogix,
            AllenBradleyModels.Micro800,
        ]:
            plc = pycomm3.LogixDriver(self.ip_address)

        elif self.controller in [
            AllenBradleyModels.SLC500,
            AllenBradleyModels.MicroLogix,
        ]:
            plc = pycomm3.SLCDriver(self.ip_address)

        else:
            raise Exception(
                f"Invalid PLC Model: '{self.controller}'. Valid models are 'ControlLogix', "
                f"'CompactLogix', 'Micro800', 'SLC500' and 'MicroLogix'"
            )

        self.plc = plc
        self.plc.open()

    def close(self):
        if self.plc is None: return
        self.plc.close()
        self.plc = None

    def is_connected(self):
        if self.plc is None: return False
        return self.plc.connected

    def read(self, key, **kwargs):
        """
        Use the pycomm3 connection object. For example: self.plc.read('F8:0')
        See https://github.com/ottowayi/pycomm3
        """
        tag = key

        # tag = X:I{L}
        if "{" not in tag:
            tag = tag + "{1}"
        tag_ = tag.split(":")
        X = tag_[0]
        tag_ = tag_[1].replace("}", "").split("{")
        I = int(tag_[0])
        L = int(tag_[1])

        values = self.plc.read(tag).value
        if not isinstance(values, list): values = [values]

        result = {}
        for i in range(0, len(values)):
            result[X + ":" + str(I + i)] = values[i]

        return result

    def write(self, key, data):
        """
        Use the pycomm3 connection object. For example: self.plc.write('F8:0', 21)
        See https://github.com/ottowayi/pycomm3
        """
        return
