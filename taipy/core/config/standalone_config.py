from typing import Any, Dict, Union


class StandaloneConfig:
    """
    Holds configuration fields related to the job executions.

    Parameters:
        nb_of_workers (int): The maximum number of running workers to execute jobs. It must be a positive integer.
            The default value is 1.
        other_properties (dict): A dictionary of additional properties.
    """

    _NB_OF_WORKERS_KEY = "nb_of_workers"
    _DEFAULT_NB_OF_WORKERS = 1

    _TYPE_MAP = {
        _NB_OF_WORKERS_KEY: int,
    }

    def __init__(self, nb_of_workers: Union[int, str] = None, *args, **other_properties):
        self.properties = {
            self._NB_OF_WORKERS_KEY: nb_of_workers or self._DEFAULT_NB_OF_WORKERS,
            **other_properties,
        }

    def __getattr__(self, key):
        return self.properties.get(key)

    def _to_dict(self):
        return {k: v for k, v in self.properties.items() if v is not None}

    @classmethod
    def _from_dict(cls, config_as_dict: Dict[str, Any]):
        config = StandaloneConfig(**config_as_dict)
        return config
