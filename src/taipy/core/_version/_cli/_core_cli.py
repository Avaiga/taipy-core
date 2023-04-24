# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from argparse import ArgumentError, Namespace

from taipy.config._cli._cli import _CLI
from taipy.logger._taipy_logger import _TaipyLogger


class _CoreCLI:
    """Command-line interface for Taipy Core application."""

    _DEFAULT_ARGS = {
        "development": True,
        "experiment": "",
        "production": "",
        "force": False,
        "no_force": False,
        "clean_entities": False,
        "no_clean_entities": False,
    }

    @classmethod
    def create_parser(cls):
        core_parser = _CLI._add_subparser("taipy", help="Run application with Taipy arguments.")

        mode_group = core_parser.add_mutually_exclusive_group()
        mode_group.add_argument(
            "-dev",
            "--development",
            action="store_true",
            help="""
                When execute Taipy application in `development` mode, all entities from the previous development version
                will be deleted before running new Taipy application.
            """,
        )
        mode_group.add_argument(
            "--experiment",
            nargs="?",
            const=cls._DEFAULT_ARGS["experiment"],
            metavar="VERSION",
            help="""
                When execute Taipy application in `experiment` mode, the current Taipy application is saved to a new
                version. If version name already exists, check for compatibility with current Python Config and run the
                application. Without being specified, the version number will be a random string.
            """,
        )
        mode_group.add_argument(
            "--production",
            nargs="?",
            const=cls._DEFAULT_ARGS["production"],
            metavar="VERSION",
            help="""
                When execute in `production` mode, the current version is used in production. All production versions
                should have the same configuration and share all entities. Without being specified, the latest version
                is used.
            """,
        )

        force_group = core_parser.add_mutually_exclusive_group()
        force_group.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force override the configuration of the version if existed and run the application."
            " Default to False.",
        )
        force_group.add_argument(
            "--no-force",
            action="store_true",
            help="Stop the application if any Config conflict exists.",
        )

        clean_entities_group = core_parser.add_mutually_exclusive_group()
        clean_entities_group.add_argument(
            "--clean-entities",
            action="store_true",
            help="Clean all current version entities before running the application. Default to False.",
        )
        clean_entities_group.add_argument(
            "--no-clean-entities",
            action="store_true",
            help="Keep all entities of the current experiment version.",
        )

    @classmethod
    def parse_arguments(cls):
        try:
            args = _CLI._parse()
        except ArgumentError as err:
            if "invalid choice" in err.message:
                _TaipyLogger._get_logger().warn(f"{str(err)}. Ignoring command-line arguments")
                return cls.default()
            else:
                raise err

        if getattr(args, "which", None) != "taipy":
            return cls.default()

        return args

    @classmethod
    def default(cls):
        default_args = Namespace()
        for attr, value in cls._DEFAULT_ARGS.items():
            setattr(default_args, attr, value)

        return default_args
