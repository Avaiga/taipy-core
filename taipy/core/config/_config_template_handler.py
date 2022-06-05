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

import os
import re

from taipy.core.common._properties import _Properties
from taipy.core.common.frequency import Frequency
from taipy.core.common.scope import Scope
from taipy.core.exceptions.exceptions import InconsistentEnvVariableError, MissingEnvVariableError


class _ConfigTemplateHandler:
    """Factory to handle actions related to config value templating."""

    _PATTERN = r"^ENV\[([a-zA-Z_]\w*)\](:(\bbool\b|\bstr\b|\bfloat\b|\bint\b))?$"

    @classmethod
    def _replace_templates(cls, template, type=str, required=True, default=None):
        if isinstance(template, tuple):
            return tuple(cls._replace_template(item, type, required, default) for item in template)
        if isinstance(template, list):
            return [cls._replace_template(item, type, required, default) for item in template]
        if isinstance(template, dict):
            return {str(k): cls._replace_template(v, type, required, default) for k, v in template.items()}
        if isinstance(template, _Properties):
            return {str(k): cls._replace_template(v, type, required, default) for k, v in template.items()}
        return cls._replace_template(template, type, required, default)

    @classmethod
    def _replace_template(cls, template, type, required, default):
        if "ENV" not in str(template):
            return template
        match = re.fullmatch(cls._PATTERN, str(template))
        if match:
            var = match.group(1)
            dynamic_type = match.group(3)
            val = os.environ.get(var)
            if val is None:
                if required:
                    raise MissingEnvVariableError()
                return default
            if type == bool:
                return cls._to_bool(val)
            elif type == int:
                return cls._to_int(val)
            elif type == float:
                return cls._to_float(val)
            elif type == Scope:
                return cls._to_scope(val)
            elif type == Frequency:
                return cls._to_frequency(val)
            else:
                if dynamic_type == "bool":
                    return cls._to_bool(val)
                elif dynamic_type == "int":
                    return cls._to_int(val)
                elif dynamic_type == "float":
                    return cls._to_float(val)
                return val
        return template

    @staticmethod
    def _to_bool(val: str) -> bool:
        possible_values = ["true", "false"]
        if str.lower(val) not in possible_values:
            raise InconsistentEnvVariableError()
        return str.lower(val) == "true" or not (str.lower(val) == "false")

    @staticmethod
    def _to_int(val: str) -> int:
        try:
            return int(val)
        except ValueError:
            raise InconsistentEnvVariableError()

    @staticmethod
    def _to_float(val: str) -> float:
        try:
            return float(val)
        except ValueError:
            raise InconsistentEnvVariableError()

    @staticmethod
    def _to_scope(val: str) -> Scope:
        try:
            return Scope[str.upper(val)]
        except Exception:
            raise InconsistentEnvVariableError()

    @staticmethod
    def _to_frequency(val: str) -> Frequency:
        try:
            return Frequency[str.upper(val)]
        except Exception:
            raise InconsistentEnvVariableError()
