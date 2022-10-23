import time
from pathlib import Path
from typing import Any, Dict

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

from kedro_cache.framework.hooks.hash import hash_datadict


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
        time.sleep(2)
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
def test_caching_in_single_node(
    monkeypatch: MonkeyPatch,
    kedro_project_with_cache_config: Path,
) -> None:
    """Check that caching works in a single node pipeline."""
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

    # check that the second run is two seconds faster (with 0.3 seconds tolerance)
    assert first_t.elapsed > second_t.elapsed + 2 - 0.3, "Caching did not work"

    # check that the results are identical
    first_res_hash = hash_datadict(first_res)
    second_res_hash = hash_datadict(second_res)
    assert first_res_hash == second_res_hash, "Caching did not work"
