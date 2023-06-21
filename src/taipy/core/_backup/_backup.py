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


import os

from taipy.config import Config

__BACKUP_FILE_PATH_ENVIRONMENT_VARIABLE_NAME = "TAIPY_BACKUP_FILE_PATH"


def _init_backup_file_with_storage_folder():
    if preserve_file_path := os.getenv(__BACKUP_FILE_PATH_ENVIRONMENT_VARIABLE_NAME):
        with open(preserve_file_path, "a") as f:
            f.write(f"{Config.global_config.storage_folder}\n")


def _append_to_backup_file(new_file_path: str):
    if preserve_file_path := os.getenv(__BACKUP_FILE_PATH_ENVIRONMENT_VARIABLE_NAME):
        storage_folder = os.path.abspath(Config.global_config.storage_folder) + os.sep
        if not os.path.abspath(new_file_path).startswith(storage_folder):
            with open(preserve_file_path, "a") as f:
                f.write(f"{new_file_path}\n")


def _remove_from_backup_file(to_remove_file_path: str):
    if preserve_file_path := os.getenv(__BACKUP_FILE_PATH_ENVIRONMENT_VARIABLE_NAME, None):
        storage_folder = os.path.abspath(Config.global_config.storage_folder) + os.sep
        if not os.path.abspath(to_remove_file_path).startswith(storage_folder):
            try:
                with open(preserve_file_path, "r+") as f:
                    old_backup = f.read()
                    to_remove_file_path = to_remove_file_path + "\n"

                    # To avoid removing the file path of different data nodes that are pointing
                    # to the same file. We will only replace the file path only once.
                    if old_backup.startswith(to_remove_file_path):
                        new_backup = old_backup.replace(to_remove_file_path, "", 1)
                    else:
                        new_backup = old_backup.replace("\n" + to_remove_file_path, "\n", 1)

                    if new_backup is not old_backup:
                        f.seek(0)
                        f.write(new_backup)
                        f.truncate()
            except Exception:
                pass


def _replace_in_backup_file(old_file_path: str, new_file_path: str):
    _remove_from_backup_file(old_file_path)
    _append_to_backup_file(new_file_path)
