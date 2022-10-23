import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Generator

import pandas as pd
import pytest
from contexttimer import Timer
from kedro.extras.datasets.pickle import PickleDataSet
from kedro.framework.context import KedroContext
from kedro.framework.project import _ProjectPipelines  # type: ignore
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline.node import node
from kedro.pipeline.pipeline import Pipeline
from pytest import MonkeyPatch
from pytest_mock import MockerFixture

from kedro_cache.framework.hooks.cache_hook import RERUN_ENV_VAR
from kedro_cache.framework.hooks.hash import hash_datadict


@pytest.fixture
def set_rerun_env_variable() -> Generator[None, None, None]:
    """Set the KEDRO_CACHE_RERUN environment variable.

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


@pytest.fixture
def mock_catalog(mocker: MockerFixture) -> None:
    """Mock a the data catalog."""
    dummy_catalog = DataCatalog(
        {
            "input": MemoryDataSet(pd.DataFrame({"a": [1, 2, 3]})),  # type: ignore
            "output": PickleDataSet("output.pkl"),
        }
    )

    mocker.patch.object(
        KedroContext,
        "_get_catalog",
        return_value=dummy_catalog,
    )


@pytest.fixture
def mock_pipeline(mocker: MockerFixture) -> None:
    """Mock a pipeline to cache."""

    def foo(x: Any) -> Any:
        time.sleep(1)
        return x

    def bar(x: Any) -> Any:
        time.sleep(1)
        return x

    def mocked_register_pipelines() -> Dict[str, Pipeline]:
        test_pipeline = pipeline(
            [
                node(
                    func=foo,
                    inputs=["input"],
                    outputs="interim",
                ),
                node(
                    func=bar,
                    inputs=["interim"],
                    outputs="output",
                ),
            ],
        )
        return {"__default__": test_pipeline}

    mocker.patch.object(
        _ProjectPipelines,
        "_get_pipelines_registry_callable",
        return_value=mocked_register_pipelines,
    )


@pytest.mark.usefixtures("mock_catalog")
@pytest.mark.usefixtures("mock_pipeline")
def test_disable_caching_with_config(
    monkeypatch: MonkeyPatch, kedro_project: Path, datadir: Path
) -> None:
    """Check that caching is disabled when the config is set to False."""
    session: KedroSession

    # change dir
    monkeypatch.chdir(kedro_project)

    # copy the catalog with the artifact configuration
    source_cfg = datadir / "cache.yml"
    destination_cfg = kedro_project / "conf" / "local" / "cache.yml"
    shutil.copy(source_cfg, destination_cfg)

    # set up project
    bootstrap_project(kedro_project)

    # first run
    with Timer() as first_t:
        with KedroSession.create(project_path=kedro_project) as session:
            first_res = session.run()

    # second run
    with Timer() as second_t:
        with KedroSession.create(project_path=kedro_project) as session:
            second_res = session.run()

    # check that the second run is two seconds faster (with 0.3 seconds tolerance)
    diff = abs(first_t.elapsed - second_t.elapsed)
    assert 1 - 0.2 < diff < 1 + 0.2, "Only foo should be cached"

    # check that the results are identical
    first_res_hash = hash_datadict(first_res)
    second_res_hash = hash_datadict(second_res)
    assert first_res_hash == second_res_hash, "Caching did not work"


@pytest.mark.usefixtures("set_rerun_env_variable")
@pytest.mark.usefixtures("mock_catalog")
@pytest.mark.usefixtures("mock_pipeline")
def test_disable_caching_with_env_variable(
    monkeypatch: MonkeyPatch, kedro_project: Path, datadir: Path
) -> None:
    """Check that caching is disabled when the environment variable is set to True."""
    session: KedroSession

    # change dir
    monkeypatch.chdir(kedro_project)

    # copy the catalog with the artifact configuration
    source_cfg = datadir / "cache.yml"
    destination_cfg = kedro_project / "conf" / "local" / "cache.yml"
    shutil.copy(source_cfg, destination_cfg)

    # set up project
    bootstrap_project(kedro_project)

    # first run
    with Timer() as first_t:
        with KedroSession.create(project_path=kedro_project) as session:
            first_res = session.run()

    # second run
    with Timer() as second_t:
        with KedroSession.create(project_path=kedro_project) as session:
            second_res = session.run()

    # check that the second run is two seconds faster (with 0.3 seconds tolerance)
    diff = abs(first_t.elapsed - second_t.elapsed)
    assert diff < 2 + 0.2, "There should be no caching"

    # check that the results are identical
    first_res_hash = hash_datadict(first_res)
    second_res_hash = hash_datadict(second_res)
    assert first_res_hash == second_res_hash, "Caching did not work"
