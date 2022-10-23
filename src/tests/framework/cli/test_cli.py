import re
import subprocess
import sys
from pathlib import Path
from typing import List

import pytest
import yaml
from click.testing import CliRunner
from kedro.framework.cli.cli import info
from kedro.framework.project import _ProjectSettings
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pytest import MonkeyPatch
from pytest_mock import MockerFixture

from kedro_cache.config import KedroCacheConfig
from kedro_cache.framework.cli.cli import cache_commands as cli_cache
from kedro_cache.framework.cli.cli import init as cli_init
from kedro_cache.framework.cli.cli import rerun as cli_rerun


def extract_cmd_from_help(msg: str) -> List[str]:
    """Extract the commands from the help message.

    Args:
        msg: The help message.

    Returns:
        The list of commands.
    """
    # [\s\S] is used instead of "." to match any character including new lines
    cmd_txt = re.search((r"(?<=Commands:)([\s\S]+)$"), msg).group(1)  # type: ignore
    cmd_list_detailed = cmd_txt.split("\n")

    cmd_list = []
    for cmd_detailed in cmd_list_detailed:
        cmd_match = re.search(r"\w+(?=  )", string=cmd_detailed)
        if cmd_match is not None:
            cmd_list.append(cmd_match.group(0))
    return cmd_list


@pytest.fixture(autouse=True)
def mock_validate_settings(mocker: MockerFixture) -> None:
    """Mock the validate_settings function."""
    # KedroSession eagerly validates that a project's settings.py is correct by
    # importing it. settings.py does not actually exists as part of this test suite
    # since we are testing session in isolation, so the validation is patched.
    mocker.patch("kedro.framework.session.session.validate_settings")


def _mock_imported_settings_paths(
    mocker: MockerFixture, mock_settings: _ProjectSettings
) -> _ProjectSettings:
    """Mock the imported settings paths from kedro.

    Args:
        mocker: A pytest-mock fixture.
        mock_settings: The settings that are used instead.

    Returns:
        The mocked settings.
    """
    for path in [
        "kedro.framework.project.settings",
        "kedro.framework.session.session.settings",
    ]:
        mocker.patch(path, mock_settings)
    return mock_settings


@pytest.fixture
def mock_settings_fake_project(mocker: MockerFixture) -> _ProjectSettings:
    """Mock the settings for a fake project.

    Returns:
        The mocked settings.
    """
    return _mock_imported_settings_paths(mocker, _ProjectSettings())  # type: ignore


def test_cli_global_discovered(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Check that the global commands are discovered."""
    # Change the current working directory to the temporary directory
    monkeypatch.chdir(tmp_path)

    # Run the info command to trigger the discovery of the global CLI
    cli_runner = CliRunner()
    result = cli_runner.invoke(info)
    assert result.exit_code == 0

    # Check that the plugin is listed in the output
    plugin_line = next(
        (ln for ln in result.output.splitlines() if "kedro_cache" in ln), None
    )
    assert plugin_line is not None, "Plugin not found in the output of `kedro info`"

    # Check that the plugin hooks are listed in the output
    assert "hooks,project" in plugin_line, "Hooks are not correctly installed"


def test_cache_commands_inside_kedro_project(
    monkeypatch: MonkeyPatch, kedro_project: Path
) -> None:
    """Check that the aim commands are discovered when inside a kedro project."""
    # Change the current working directory to the kedro project
    monkeypatch.chdir(kedro_project)

    # launch the command to initialize the project
    cli_runner = CliRunner()
    result = cli_runner.invoke(cli_cache)
    assert {"init"} == set(extract_cmd_from_help(result.output))
    assert "You have not updated your template yet" not in result.output


def test_cli_init(monkeypatch: MonkeyPatch, kedro_project: Path) -> None:
    """Check that this `cache init` creates an `cache.yml` file."""
    # "kedro_project" is a pytest.fixture declared in conftest
    monkeypatch.chdir(kedro_project)
    cli_runner = CliRunner()
    result = cli_runner.invoke(cli_init, catch_exceptions=False)  # type: ignore

    # FIRST TEST:
    # the command should have executed propery
    assert result.exit_code == 0

    # check cache.yml file
    assert "'conf/local/cache.yml' successfully updated." in result.output
    assert (kedro_project / "conf" / "local" / "cache.yml").is_file()


def test_cli_init_existing_config(
    monkeypatch: MonkeyPatch,
    kedro_project_with_cache_config: Path,
    mock_settings_fake_project: _ProjectSettings,
) -> None:
    """Check that `cache init` does not overwrite an existing `cache.yml` file."""
    # "kedro_project" is a pytest.fixture declared in conftest
    cli_runner = CliRunner()
    monkeypatch.chdir(kedro_project_with_cache_config)
    bootstrap_project(kedro_project_with_cache_config)

    with KedroSession.create(
        "fake_project", project_path=kedro_project_with_cache_config
    ) as session:
        # check that file already exists
        yaml_str = yaml.dump(dict(cachedb_dir="test.sqlite"))
        (
            kedro_project_with_cache_config
            / mock_settings_fake_project.CONF_SOURCE  # type: ignore
            / "local"
            / "cache.yml"
        ).write_text(yaml_str)
        result = cli_runner.invoke(cli_init)  # type: ignore

        # check an error message is raised
        assert "A 'cache.yml' already exists" in result.output

        context = session.load_context()
        cache_config: KedroCacheConfig = context.cache_config  # type: ignore
        assert cache_config.cachedb_dir == Path("test.sqlite"), "Config was overwritten"


@pytest.mark.parametrize(
    "env",
    ["base", "local"],
)
def test_cli_init_with_env(
    monkeypatch: MonkeyPatch, kedro_project: Path, env: str
) -> None:
    """Check that `cache init` creates an `cache.yml` file in correct environment."""
    # "kedro_project" is a pytest.fixture declared in conftest
    monkeypatch.chdir(kedro_project)
    cli_runner = CliRunner()
    result = cli_runner.invoke(cli_init, f"--env {env}")  # type: ignore

    # FIRST TEST:
    # the command should have executed propery
    assert result.exit_code == 0

    # check aim.yml file
    assert f"'conf/{env}/cache.yml' successfully updated." in result.output
    assert (kedro_project / "conf" / env / "cache.yml").is_file()


@pytest.mark.parametrize(
    "env",
    ["debug"],
)
def test_cli_init_with_wrong_env(
    monkeypatch: MonkeyPatch, kedro_project: Path, env: str
) -> None:
    """Check that the `cache init` fails when the environment is not valid."""
    # "kedro_project" is a pytest.fixture declared in conftest
    monkeypatch.chdir(kedro_project)
    cli_runner = CliRunner()
    result = cli_runner.invoke(cli_init, f"--env {env}")  # type: ignore

    # A warning message should appear
    assert f"No env '{env}' found" in result.output


def test_cache_is_found(
    monkeypatch: MonkeyPatch, kedro_project_with_cache_config: Path
) -> None:
    """Check that the `cache` command is found by `kedro`."""
    monkeypatch.chdir(kedro_project_with_cache_config)
    r = subprocess.run([sys.executable, "-m", "kedro", "--help"], capture_output=True)
    assert "cache" in r.stdout.decode("utf-8")


def test_rerun(
    monkeypatch: MonkeyPatch,
    mocker: MockerFixture,
    kedro_project_with_cache_config: Path,
) -> None:
    """Check that the `aim ui` command launches the UI."""
    monkeypatch.chdir(kedro_project_with_cache_config)
    cli_runner = CliRunner()

    # This does not test anything : the goal is to check whether it raises an error
    subprocess_mocker = mocker.patch(
        "subprocess.call"
    )  # make the test succeed, but no a real test

    result = cli_runner.invoke(cli_rerun, args=["--foo", "bar"])  # type: ignore
    assert result.exit_code == 0, "Command should have succeeded"
    subprocess_mocker.assert_called_once_with(
        [
            "kedro",
            "run",
            "--foo",
            "bar",
        ]
    )
