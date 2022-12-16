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
def version_cli(mode, version_number, override):
    return mode, version_number, override
