from typing import Any, Dict, Union

from taipy.core.config.job_mode_config import JobModeConfig


class StandaloneConfig(JobModeConfig):
    """
    Holds configuration fields related to the job executions.

    Parameters:
        nb_of_workers (int): The maximum number of running workers to execute jobs. It must be a positive integer.
            The default value is 1.
        other_properties (dict): A dictionary of additional properties.
    """

    _DEFAULT_CONFIG = {
        "nb_of_workers": 1,
    }
