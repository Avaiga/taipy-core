from importlib import util
from typing import Any, Dict, Optional

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
    _DEFAULT_CONFIG_KEY = "_DEFAULT_CONFIG"

    def __init__(self, mode: str = None, **properties):
        self.mode = mode or self._DEFAULT_MODE
        self.config_cls = self._config_cls(self.mode)
        self.config = self._create_config(self.config_cls, **properties)

    def __getattr__(self, key: str) -> Optional[Any]:
        return self.config.get(key, None)

    @classmethod
    def default_config(cls):
        return JobConfig(cls._DEFAULT_MODE)

    def _to_dict(self):
        as_dict = {}
        if self.mode is not None:
            as_dict[self._MODE_KEY] = self.mode
        as_dict.update(self.config)
        return as_dict

    @classmethod
    def _from_dict(cls, config_as_dict: Dict[str, Any]):
        mode = config_as_dict.pop(cls._MODE_KEY, None)
        config = JobConfig(mode, **config_as_dict)
        return config

    def _update(self, config_as_dict: Dict[str, Any]):
        mode = _tpl._replace_templates(config_as_dict.pop(self._MODE_KEY, self.mode))
        if self.mode != mode:
            self.mode = mode
            self.config_cls = self._config_cls(self.mode)
            self.config = self._create_config(self.config_cls, **config_as_dict)
        if self.config:
            self._update_config(config_as_dict)

    def _update_config(self, config_as_dict: Dict[str, Any]):
        type_map = getattr(self.config_cls, self._TYPE_MAP_KEY, {})
        d = {}
        for k, v in config_as_dict.items():
            type_to_convert = type_map.get(k, str)
            value = _tpl._replace_templates(v, type_to_convert)
            if value is not None:
                d[k] = value
        self.config.update(d)

    @classmethod
    def _config_cls(cls, mode: str):
        if mode == cls._DEFAULT_MODE:
            return StandaloneConfig
        dep = f"taipy.{mode}"
        if not util.find_spec(dep):
            raise DependencyNotInstalled(mode)
        config_cls = _load_fct(dep + ".config", "Config")
        return config_cls

    @property
    def is_standalone(self) -> bool:
        """True if the config is set to standalone execution"""
        return self.mode == self._DEFAULT_MODE

    @property
    def is_multiprocess(self) -> bool:
        """True if the config is set to standalone execution and nb_of_workers is greater than 1"""
        return self.is_standalone and int(self.nb_of_workers) > 1  # type: ignore

    @classmethod
    def _create_config(cls, config_cls, **properties):
        default_config = getattr(config_cls, cls._DEFAULT_CONFIG_KEY, {})
        return {**default_config, **properties}

    def _is_default_mode(self) -> bool:
        return self.mode == self._DEFAULT_MODE
