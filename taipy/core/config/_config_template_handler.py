import os
import re
from functools import singledispatch

from taipy.core.common.frequency import Frequency
from taipy.core.data.scope import Scope
from taipy.core.exceptions.exceptions import InconsistentEnvVariableError, MissingEnvVariableError


class _ConfigTemplateHandler:
    """Factory to handle actions related to config value templating."""

    _PATTERN = r"^ENV\[([a-zA-Z_]\w*)\]$"

    @classmethod
    def _replace_templates(cls, template, type=str, required=True, default=None):
        return _replace_templates_rec(template, type, required, default)

    @classmethod
    def _replace_string(cls, template, type, required, default):
        match = re.fullmatch(cls._PATTERN, str(template))
        if match:
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                if required:
                    raise MissingEnvVariableError()
                return default
            if type == bool:
                return cls._to_bool(val)
            elif type == int:
                return cls._to_int(val)
            elif type == Scope:
                return cls._to_scope(val)
            elif type == Frequency:
                return cls._to_frequency(val)
            else:
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


@singledispatch
def _replace_templates_rec(template, *args):
    return _ConfigTemplateHandler._replace_string(template, *args)


@_replace_templates_rec.register(tuple)
def _(template, *args):
    return tuple(_replace_templates_rec(list(template), *args))


@_replace_templates_rec.register(list)  # type: ignore[no-redef]
def _(template, *args):
    return [_replace_templates_rec(t, *args) for t in template]


@_replace_templates_rec.register(dict)  # type: ignore[no-redef]
def _(template, *args):
    return {k: _replace_templates_rec(v, *args) for k, v in template.items()}
