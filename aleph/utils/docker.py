import docker
import logging
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
        ports = {"1883": 1883}
        config_file = __file__.replace("docker.py", "mosquitto.conf")
        volumes = {config_file: {"bind": "/mosquitto/config/mosquitto.conf", "mode": "rw"}}
        self.run(name, image, ports=ports, volumes=volumes)
