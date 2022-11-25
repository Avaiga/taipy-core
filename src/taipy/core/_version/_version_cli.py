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

import uuid

import click

from taipy.config import Config

from ..taipy import clean_all_entities
from ._version_manager import _VersionManager


def skip_run(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    # TODO: Copy the current latest version to the new version
    print("Application skiped.")
    ctx.exit()


@click.command()
@click.option(
    "--development",
    "-d",
    is_flag=True,
    default=True,
    help="Execute Taipy application in DEVELOPMENT mode. This is the default behavior.",
)
@click.option("--experiment", "-e", is_flag=True, help="Execute Taipy application in EXPERIMENTAL mode.")
@click.option(
    "--verison-number",
    "-v",
    "_version_number",
    type=str,
    default=None,
    help="The version number when execute and save in experimental mode. If not provided, a random ID is used.",
)
@click.option(
    "--override",
    "-o",
    "_override",
    is_flag=True,
    help='Override the version specified by "--version-number" if existed. Default to False.',
)
@click.option(
    "--skip-run",
    "-s",
    is_flag=True,
    callback=skip_run,
    expose_value=False,
    is_eager=True,
    help='Save the "latest" version if existed to a new version specified by "--version-number" without running the application. Default to False.',
)
def version_cli(development, experiment, _version_number, _override):
    if experiment:
        if _version_number:
            version_number = _version_number
        else:
            version_number = str(uuid.uuid4())

        override = _override

    elif development:
        Config.configure_global_app(clean_entities_enabled=True)
        clean_all_entities()

        version_number = _VersionManager._DEFAULT_VERSION
        override = True

    _VersionManager.set_current_version(version_number, override)
