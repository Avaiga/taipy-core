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

from taipy.logger._taipy_logger import _TaipyLogger

from ..cycle._cycle_manager_factory import _CycleManagerFactory
from ..data._data_manager_factory import _DataManagerFactory
from ..job._job_manager_factory import _JobManagerFactory
from ..pipeline._pipeline_manager_factory import _PipelineManagerFactory
from ..scenario._scenario_manager_factory import _ScenarioManagerFactory
from ..task._task_manager_factory import _TaskManagerFactory
from ._version_manager import _VersionManager


def skip_run(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    # TODO: Copy the current latest version to the new version
    ctx.exit()


def clean_all_entities_by_version(version_number):
    """Delete all entities belongs to a version from the Taipy data folder."""
    _JobManagerFactory._build_manager()._delete_by_version(version_number)
    _ScenarioManagerFactory._build_manager()._delete_by_version(version_number)
    _PipelineManagerFactory._build_manager()._delete_by_version(version_number)
    _TaskManagerFactory._build_manager()._delete_by_version(version_number)
    _DataManagerFactory._build_manager()._delete_by_version(version_number)


@click.command()
@click.option(
    "--development",
    "-d",
    "mode",
    flag_value="development",
    default=True,
    help="Execute Taipy application in DEVELOPMENT mode. This is the default behavior.",
)
@click.option(
    "--experiment", "-e", "mode", flag_value="experiment", help="Execute Taipy application in EXPERIMENTAL mode."
)
@click.option(
    "--version-number",
    "-v",
    "_version_number",
    type=str,
    default=None,
    help="The version number when execute and save in experimental mode. If not provided, a random version name is used.",
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
def version_cli(mode, _version_number, _override):
    if mode == "experiment":
        if _version_number:
            print(_version_number)
            curren_version_number = _version_number
        else:
            curren_version_number = str(uuid.uuid4())

        override = _override

    elif mode == "development":
        curren_version_number = _VersionManager.get_development_version()
        print(curren_version_number)
        _VersionManager.set_development_version(curren_version_number)
        override = True

        clean_all_entities_by_version(curren_version_number)
        _TaipyLogger._get_logger().info(f"Development mode: Clean all entities with version {curren_version_number}")

    else:
        _TaipyLogger._get_logger().error("Undefined execution mode.")
        return

    _VersionManager.set_current_version(curren_version_number, override)
