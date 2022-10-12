from aleph_core import Connection
import snap7


class SiemensS7Connection(Connection):
    ip_address = "localhost"
    port = 102
    rack = 0
    slot = 1

    plc: snap7.client.Client = None

    def open(self):
        self.plc = snap7.client.Client()
        self.plc.connect(self.ip_address, rack=self.rack, slot=self.slot, tcpport=self.port)

    def close(self):
        if self.plc is not None:
            self.plc.disconnect()

    def is_open(self):
        if self.plc is None:
            return False
        return self.plc.get_connected()

    def read(self, key, **kwargs):
        return []

    def write(self, key, data):
        return
