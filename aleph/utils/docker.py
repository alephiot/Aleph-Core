import logging
import docker
from docker.errors import NotFound


class DockerManager:

    def __init__(self):
        self.client = docker.from_env()

    def run(self, name: str, image: str, **kwargs) -> None:
        """
        Run a docker container if it's not already running
        """
        try:
            container = self.client.containers.get(name)
            if container.status != "running":
                logging.debug(f"Restarting container '{name}'")
                container.start()

        except NotFound:
            logging.debug(f"Starting container '{name}'")
            self.client.containers.run(image, name=name, detach=True, **kwargs)

    def run_mosquitto_server(self) -> None:
        """
        Run a mosquitto server
        """
        name = "MOSQUITTO"
        image = "eclipse-mosquitto"
        ports = {'1883': 1883}
        self.run(name, image, ports=ports)
