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

    _DEFAULT_CONFIG = {
        _NB_OF_WORKERS_KEY: _DEFAULT_NB_OF_WORKERS,
    }
