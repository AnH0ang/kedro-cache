import time
from typing import Any


def delayed_passthrough(*args: Any) -> Any:  # pragma: no cover
    """A passthrough function that sleeps for 3 second.

    Returns:
        Returns the input arguments.
    """
    time.sleep(3)
    return args
