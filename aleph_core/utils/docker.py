import time
from typing import Optional
from abc import ABC
import docker


class DockerContainer(ABC):
    IMAGE: str = None
    COMMAND: str = None
    VOLUMES = {}
    PORTS = {}
    WAIT_SECONDS = 5

    def __init__(self):
        self.client = docker.from_env()
        self.container = None

    def run(self):
        if self.IMAGE is None:
            raise NotImplementedError

        self.container = self.client.containers.run(
            self.IMAGE,
            command=self.COMMAND,
            detach=True,
            volumes=self.VOLUMES,
            ports=self.PORTS,
        )

        if self.WAIT_SECONDS:
            time.sleep(self.WAIT_SECONDS)

    def stop(self):
        if self.container is not None:
            self.container.stop()


class MosquittoContainer(DockerContainer):
    IMAGE = "eclipse-mosquitto:2.0"
    COMMAND = "mosquitto -c /mosquitto-no-auth.conf"
    PORTS = {1883: 1883}


class RedisContainer(DockerContainer):
    IMAGE = "redis"
    PORTS = {6379: 6379}
