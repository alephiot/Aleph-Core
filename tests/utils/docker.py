from typing import Optional
from abc import ABC
import docker
import time


class DockerContainer(ABC):
    IMAGE: str = None
    COMMAND: str = None
    VOLUMES = {}
    PORTS = {}
    ENVIRONMENT = {}
    WAIT_SECONDS = 4

    def __init__(self):
        self.client = docker.from_env()
        self.container = None

        running_containers = self.client.containers.list(all=True)
        for container in running_containers:
            if container.name == self.name:
                self.container = container
                break

    @property
    def name(self) -> str:
        return f'dk-{self.IMAGE.replace(":", "-").replace(".", "-")}'

    @property
    def running(self) -> bool:
        self.container.reload()
        if self.container is not None:
            return self.container.attrs["State"]["Running"]
        return False

    def run(self):
        if self.IMAGE is None:
            raise NotImplementedError

        if self.container is not None:
            if self.running:
                self.container.restart()
                time.sleep(1)

        else:
            self.container = self.client.containers.run(
                self.IMAGE,
                command=self.COMMAND,
                name=self.name,
                detach=True,
                volumes=self.VOLUMES,
                ports=self.PORTS,
                environment=self.ENVIRONMENT,
            )

            if self.WAIT_SECONDS:
                time.sleep(self.WAIT_SECONDS)

    def stop(self):
        if self.container is not None and self.running:
            self.container.stop()


class MosquittoContainer(DockerContainer):
    IMAGE = "eclipse-mosquitto:2.0"
    COMMAND = "mosquitto -c /mosquitto-no-auth.conf"
    PORTS = {1883: 1883}


class RedisContainer(DockerContainer):
    IMAGE = "redis"
    PORTS = {6379: 6379}


class MariaDBContainer(DockerContainer):
    IMAGE = "mariadb"
    PORTS = {3306: 3306}
    WAIT_SECONDS = 7
    ENVIRONMENT = {
        "MARIADB_USER": "user",
        "MARIADB_PASSWORD": "1234",
        "MARIADB_DATABASE": "main",
        "MARIADB_ROOT_PASSWORD": "0000",
    }


class MongoDBContainer(DockerContainer):
    IMAGE = "mongo"
    PORTS = {27017: 27017}
