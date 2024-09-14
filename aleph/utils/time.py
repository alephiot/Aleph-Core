from time import time, sleep


def loop() -> None:
    """
    Run an infinite loop
    """
    while True:
        sleep(0.1)


def current_timestamp() -> int:
    """
    Returns the current time in milliseconds
    """
    return int(time() * 1000)
