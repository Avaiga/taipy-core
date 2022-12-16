# Copyright 2022 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.


import click

from ._version_manager_factory import _VersionManagerFactory


class bcolors:
    PURPLE = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--development",
    "--dev",
    "-d",
    "mode",
    flag_value="development",
    default=True,
    help="""
        When execute Taipy application in `development` mode, all entities from the previous development version will
        be deleted before running new Taipy application.
        This is the default behavior.
    """,
)
@click.option(
    "--experiment",
    "-e",
    "mode",
    flag_value="experiment",
    help="""
        When execute Taipy application in `experiment` mode, the current Taipy application is saved to a new version
        defined by "--version-number". If version already exists, check for compatibility with current Python Config
        and run the application.
    """,
)
@click.option(
    "--production",
    "-p",
    "mode",
    flag_value="production",
    help="""
        When execute in `production` mode, the current version or the version defined by "--version-number" is used in
        production. All production versions should have the same configuration and share all entities.
    """,
)
@click.option(
    "--version-number",
    type=str,
    default=None,
    help="The version number when execute in `experiment` mode. If not provided, a random version number is used.",
)
@click.option(
    "--override",
    "-o",
    is_flag=True,
    help='Override the version specified by "--version-number" if existed. Default to False.',
)
@click.option("--list_version", "-l", is_flag=True, help="List all existing versions of the Taipy application.")
def version_cli(mode, version_number, override, list_version):
    if list_version:
        list_version_message = f"\n{'Version number':<36}   {'Mode':<20}   {'Creation date':<20}\n"

        latest_version_number = _VersionManagerFactory._build_manager()._get_latest_version()
        development_version_number = _VersionManagerFactory._build_manager()._get_development_version()
        production_version_numbers = _VersionManagerFactory._build_manager()._get_production_version()
        versions = _VersionManagerFactory._build_manager()._get_all()
        versions.sort(key=lambda x: x.creation_date)
        for version in versions:
            if version.id == development_version_number:
                list_version_message += bcolors.GREEN
                mode = "Development"
            elif version.id in production_version_numbers:
                list_version_message += bcolors.PURPLE
                mode = "Production"
            else:
                list_version_message += bcolors.BLUE
                mode = "Experiment"

            if version.id == latest_version_number:
                list_version_message += bcolors.BOLD
                mode += " (latest)"

            list_version_message += (
                f"{(version.id):<36}   {mode:<20}   {version.creation_date.strftime('%Y-%m-%d %H:%M:%S'):<20}\n"
            )

        raise SystemExit(list_version_message)

    return mode, version_number, override
