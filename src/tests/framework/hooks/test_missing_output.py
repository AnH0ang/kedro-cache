import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest
from contexttimer import Timer
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

from kedro_cache.framework.hooks.hash import hash_datadict


@pytest.fixture
def mock_catalog(mocker: MockerFixture) -> None:
    """Mock a the data catalog."""
    dummy_catalog = DataCatalog(
        {
            "input": MemoryDataSet(pd.DataFrame({"a": [1, 2, 3]})),  # type: ignore
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

    def mocked_register_pipelines() -> Dict[str, Pipeline]:
        test_pipeline = pipeline(
            [
                node(
                    func=foo,
                    inputs=["input"],
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
def test_disable_caching_with_missing_output(
    monkeypatch: MonkeyPatch,
    kedro_project_with_cache_config: Path,
) -> None:
    """Check that the cache is not used when the output is missing."""
    session: KedroSession

    # change dir
    monkeypatch.chdir(kedro_project_with_cache_config)

    # set up project
    bootstrap_project(kedro_project_with_cache_config)

    # first run
    with Timer() as first_t:
        with KedroSession.create(
            project_path=kedro_project_with_cache_config
        ) as session:
            first_res = session.run()

    # second run
    with Timer() as second_t:
        with KedroSession.create(
            project_path=kedro_project_with_cache_config
        ) as session:
            second_res = session.run()

    # check that there was no caching
    time_diff = abs(first_t.elapsed - second_t.elapsed)
    assert time_diff < 0.3, "The node was cached, but it shouldn't have been."

    # check that the results are identical
    first_res_hash = hash_datadict(first_res)
    second_res_hash = hash_datadict(second_res)
    assert first_res_hash == second_res_hash, "Caching did not work"
