from taipy.core.config.config import Config


def test_job_config():
    assert Config.job_config.mode == "standalone"
    assert Config.job_config.nb_of_workers == 1

    Config.configure_job_executions(nb_of_workers=2)
    assert Config.job_config.mode == "standalone"
    assert Config.job_config.nb_of_workers == 2

    Config._set_job_config(foo="bar")
    assert Config.job_config.foo == "bar"
