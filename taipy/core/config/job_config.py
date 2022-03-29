from importlib import util
from typing import Any, Dict, Optional, Union

from taipy.core.common._utils import _load_fct
from taipy.core.config._config_template_handler import _ConfigTemplateHandler as _tpl
from taipy.core.config.standalone_config import StandaloneConfig
from taipy.core.exceptions.exceptions import DependencyNotInstalled


class JobConfig:
    """
    Configuration fields related to the jobs' executions.

    Parameters:
        mode (str): The Taipy operating mode. By default, the "standalone" mode is set. On Taipy enterprise,
            the "airflow" mode is available.
        **properties: A dictionary of additional properties.
    """

    _MODE_KEY = "mode"
    _DEFAULT_MODE = "standalone"

    _TYPE_MAP_KEY = "_TYPE_MAP"

    def __init__(self, mode: str = None, **properties):
        self.mode = mode or self._DEFAULT_MODE
        self.config = self._create_config(self.mode, **properties)

    def __getattr__(self, key: str) -> Optional[Any]:
        return self.config.properties.get(key, None)

    @property
    def properties(self):
        return self.config.properties

    @classmethod
    def default_config(cls):
        return JobConfig(cls._DEFAULT_MODE)

    def _to_dict(self):
        as_dict = {}
        if self.mode is not None:
            as_dict[self._MODE_KEY] = self.mode
        as_dict.update(self.config.properties)
        return as_dict

    @classmethod
    def _from_dict(cls, config_as_dict: Dict[str, Any]):
        mode = config_as_dict.pop(cls._MODE_KEY, None)
        config = JobConfig(mode, **config_as_dict)
        return config

    def _update(self, config_as_dict: Dict[str, Any]):
        mode = _tpl._replace_templates(config_as_dict.pop(self._MODE_KEY, self.mode))
        if self.mode != mode:
            print("Taipy mode changed from {} to {}".format(self.mode, mode))
            self.mode = mode
            self.config = self._create_config(self.mode, **config_as_dict)
        if self.config:
            self._update_config(config_as_dict)

    def _update_config(self, config_as_dict: Dict[str, Any]):
        d = {}
        for k, v in config_as_dict.items():
            type_conversion_map = getattr(self.config, self._TYPE_MAP_KEY, {})
            type_to_convert = type_conversion_map.get(k, None)
            if type_to_convert:
                d[k] = _tpl._replace_templates(v, type_to_convert)
            else:
                d[k] = _tpl._replace_templates(v)

        d = {k: v for k, v in d.items() if v is not None}
        self.config.properties.update(d)

    @property
    def is_standalone(self) -> bool:
        """True if the config is set to standalone execution"""
        return self.mode == self._DEFAULT_MODE

    @property
    def is_multiprocess(self) -> bool:
        """True if the config is set to standalone execution and nb_of_workers is greater than 1"""
        return self.is_standalone and int(self.nb_of_workers) > 1  # type: ignore

    @classmethod
    def _create_config(cls, mode, **properties):
        if mode == cls._DEFAULT_MODE:
            return StandaloneConfig(**properties)
        return cls._external_config(mode, **properties)

    @staticmethod
    def _external_config(mode, **properties):
        dep = f"taipy.{mode}"
        if not util.find_spec(dep):
            raise DependencyNotInstalled(mode)
        return _load_fct(dep + ".config", "Config")(**properties)

    def _is_default_mode(self) -> bool:
        return self.mode == self._DEFAULT_MODE
