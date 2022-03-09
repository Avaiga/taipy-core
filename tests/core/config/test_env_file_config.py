import os
from unittest import mock

import pytest

from taipy.core.config.config import Config
from taipy.core.config.global_app_config import GlobalAppConfig
from taipy.core.exceptions.configuration import InconsistentEnvVariableError
from tests.core.config.named_temporary_file import NamedTemporaryFile


def test_load_from_environment_overwrite_load_from_filename():
    config_from_filename = NamedTemporaryFile(
        """
[JOB]
parallel_execution = true
nb_of_workers = 10
    """
    )
    config_from_environment = NamedTemporaryFile(
        """
[JOB]
nb_of_workers = 21
    """
    )

    os.environ[Config._ENVIRONMENT_VARIABLE_NAME_WITH_CONFIG_PATH] = config_from_environment.filename
    Config._load(config_from_filename.filename)

    assert Config.job_config.parallel_execution is True
    assert Config.job_config.nb_of_workers == 21
    os.environ.pop(Config._ENVIRONMENT_VARIABLE_NAME_WITH_CONFIG_PATH)


def test_clean_entities_enabled_default():
    assert GlobalAppConfig.default_config().clean_entities_enabled is False
    with mock.patch.dict(os.environ, {f"{GlobalAppConfig._CLEAN_ENTITIES_ENABLED_ENV_VAR}": "true"}):
        assert GlobalAppConfig.default_config().clean_entities_enabled is True
    with mock.patch.dict(os.environ, {f"{GlobalAppConfig._CLEAN_ENTITIES_ENABLED_ENV_VAR}": "false"}):
        assert GlobalAppConfig.default_config().clean_entities_enabled is False
    with mock.patch.dict(os.environ, {f"{GlobalAppConfig._CLEAN_ENTITIES_ENABLED_ENV_VAR}": "foo"}):
        with pytest.raises(InconsistentEnvVariableError):
            GlobalAppConfig.default_config()
