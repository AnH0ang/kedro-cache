from pathlib import Path
from typing import List

from pydantic import BaseModel, Extra, Field

DEFAULT_CACHEDB_DIR = Path(".cachdb.sqlite")


class KedroCacheConfig(BaseModel):
    """The pydantic model for the `cache.yml` file which configures this plugin."""

    class Config:
        extra = Extra.forbid

    ignore_nodes: List[str] = Field(default_factory=list)
    cachedb_dir: Path = DEFAULT_CACHEDB_DIR
    rerun_all: bool = False
