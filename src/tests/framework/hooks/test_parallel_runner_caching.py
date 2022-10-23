from multiprocessing import cpu_count
from pathlib import Path
from typing import Dict

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
from kedro.runner import ParallelRunner
from pytest import MonkeyPatch
from pytest_mock import MockerFixture

from kedro_cache.framework.hooks.hash import hash_datadict
from kedro_cache.utils.testing import delayed_passthrough

N_NODES = cpu_count() * 3


@pytest.fixture
def mock_catalog(mocker: MockerFixture) -> None:
    """Mock a the data catalog."""
    datasets = [
        {
            f"input_{i}": MemoryDataSet(pd.DataFrame({"a_{i}": range(i, i + 10)})),
            f"interim_{i}": PickleDataSet(filepath=f"interim_{i}.pkl"),
        }
        for i in range(N_NODES)
    ]
    dummy_catalog = DataCatalog({k: v for d in datasets for k, v in d.items()})

    mocker.patch.object(
        KedroContext,
        "_get_catalog",
        return_value=dummy_catalog,
    )


@pytest.fixture
def mock_pipeline(mocker: MockerFixture) -> None:
    """Mock a pipeline to cache."""

    def mocked_register_pipelines() -> Dict[str, Pipeline]:
        test_pipeline = pipeline(
            [
                node(
                    func=delayed_passthrough,
                    inputs=f"input_{i}",
                    outputs=f"interim_{i}",
                )
                for i in range(N_NODES)
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
def test_caching_with_parallel_runner(
    monkeypatch: MonkeyPatch,
    kedro_project_with_cache_config: Path,
) -> None:
    """Check that caching works with the parallel runner."""
    # change dir
    monkeypatch.chdir(kedro_project_with_cache_config)

    # set up project
    bootstrap_project(kedro_project_with_cache_config)

    # set up runner
    runner = ParallelRunner()

    # first run
    with Timer() as first_t:
        with KedroSession.create(
            project_path=kedro_project_with_cache_config
        ) as session:
            first_res = session.run(runner=runner)

    # second run
    with Timer() as second_t:
        with KedroSession.create(
            project_path=kedro_project_with_cache_config
        ) as session:
            second_res = session.run(runner=runner)

    # check that the second run is nine seconds faster (with 1 seconds tolerance)
    assert first_t.elapsed > second_t.elapsed + 3 * 3 - 1, "Caching did not work"

    # check that the results are identical
    first_res_hash = hash_datadict(first_res)
    second_res_hash = hash_datadict(second_res)
    assert first_res_hash == second_res_hash, "Caching did not work"
