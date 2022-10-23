import subprocess
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional

import click
from click.core import Command, Context
from kedro.framework.project import settings
from kedro.framework.startup import _is_project, bootstrap_project

from kedro_cache.framework.cli.cli_utils import write_jinja_template
from kedro_cache.framework.cli.env_utils import rerun_env_context

LOGGER = getLogger(__name__)
TEMPLATE_FOLDER_PATH = Path(__file__).parent.parent.parent / "template" / "config"


class KedroClickGroup(click.Group):
    """The main entry point for the Kedro CLI."""

    def reset_commands(self) -> None:
        """Reset the commands to the default ones."""
        self.commands: Dict[str, Command] = {}

        # add commands on the fly based on conditions
        if _is_project(Path.cwd()):
            self.add_command(init)  # type: ignore

    def list_commands(self, ctx: Context) -> List[str]:
        """List the names of all commands.

        Args:
            ctx: The click context.

        Returns:
            A list of command names.
        """
        self.reset_commands()
        commands_list = sorted(self.commands)
        return commands_list

    def get_command(self, ctx: Context, cmd_name: str) -> Optional[click.Command]:
        """Get a click command by name.

        Args:
            ctx: The click context.
            cmd_name: The name of the command.

        Returns:
            The click command with the given name.
        """
        self.reset_commands()
        return self.commands.get(cmd_name)


@click.group(name="Cache")
def commands() -> None:
    """Kedro plugin for setting up result caching."""
    pass  # pragma: no cover


@commands.command(name="cache", cls=KedroClickGroup)
def cache_commands() -> None:
    """Use cache-specific commands inside kedro project."""
    pass  # pragma: no cover


@cache_commands.command()  # type: ignore
@click.option(
    "--env",
    "-e",
    default="local",
    help=(
        "The name of the kedro environment where the 'cache.yml' should be created. "
        "Default to 'local'"
    ),
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Update the template without any checks.",
)
@click.option(
    "--silent",
    "-s",
    is_flag=True,
    default=False,
    help="Should message be logged when files are modified?",
)
def init(env: str, force: bool, silent: bool) -> None:
    """Updates the template of a kedro project.

    Running this command is mandatory to use kedro-cache.
    This adds "conf/base/cache.yml": This is a configuration file
    used for run parametrization when calling "kedro run" command.
    """
    # get constants
    cache_yml = "cache.yml"
    project_path = Path().cwd()
    project_metadata = bootstrap_project(project_path)
    cache_yml_path: Path = (
        project_path / settings.CONF_SOURCE / env / cache_yml  # type: ignore
    )

    # cache.yml is just a static file,
    # but the name of the experiment is set to be the same as the project
    if cache_yml_path.is_file() and not force:
        click.secho(
            click.style(
                (
                    f"A 'cache.yml' already exists at '{cache_yml_path}'. "
                    "You can use the ``--force`` option to override it."
                ),
                fg="red",
            )
        )
    else:
        try:
            write_jinja_template(
                src=TEMPLATE_FOLDER_PATH / cache_yml,
                is_cookiecutter=False,
                dst=cache_yml_path,
                python_package=project_metadata.package_name,
            )
            if not silent:
                click.secho(
                    click.style(
                        (
                            f"'{settings.CONF_SOURCE}/{env}/{cache_yml}' "
                            "successfully updated."
                        ),
                        fg="green",
                    )
                )
        except FileNotFoundError:
            click.secho(
                click.style(
                    (
                        f"No env '{env}' found. "
                        "Please check this folder exists inside "
                        f"'{settings.CONF_SOURCE}' folder.",
                    ),
                    fg="red",
                )
            )


@cache_commands.command(  # type: ignore
    context_settings=dict(ignore_unknown_options=True)
)
@click.argument(
    "args",
    nargs=-1,
    type=click.UNPROCESSED,
    required=False,
)
def rerun(args: List[str]) -> None:
    """Rerun a kedro pipeline with `kedro run` without caching.

    Args:
        args: A list of arguments to pass to the kedro run command.
    """
    with rerun_env_context():
        subprocess.call(["kedro", "run", *args])
