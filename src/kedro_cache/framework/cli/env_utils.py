import contextlib
import os
from typing import Generator

from kedro_cache.framework.hooks.cache_hook import RERUN_ENV_VAR


@contextlib.contextmanager
def rerun_env_context() -> Generator[None, None, None]:
    """Set the environment variable to rerun the pipeline.

    Yields:
        None
    """
    env = os.environ

    orig_rerun_value = env.get(RERUN_ENV_VAR, "0")
    try:
        env[RERUN_ENV_VAR] = "1"
        yield
    finally:
        env[RERUN_ENV_VAR] = orig_rerun_value
