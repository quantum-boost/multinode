import time


def current_time() -> int:
    return int(time.time())


def pause(seconds: int) -> None:
    time.sleep(seconds)
