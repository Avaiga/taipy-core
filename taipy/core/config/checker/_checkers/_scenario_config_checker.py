from taipy.core.common.frequency import Frequency
from taipy.core.config._config import _Config
from taipy.core.config.checker._checkers._config_checker import _ConfigChecker
from taipy.core.config.checker.issue_collector import IssueCollector
from taipy.core.config.pipeline_config import PipelineConfig
from taipy.core.config.scenario_config import ScenarioConfig


class _ScenarioConfigChecker(_ConfigChecker):
    def __init__(self, config: _Config, collector: IssueCollector):
        super().__init__(config, collector)

    def _check(self) -> IssueCollector:
        scenario_configs = self._config.scenarios
        for scenario_config_id, scenario_config in scenario_configs.items():
            if scenario_config_id != _Config.DEFAULT_KEY:
                self._check_existing_config_id(scenario_config)
                self._check_frequency(scenario_config_id, scenario_config)
                self._check_pipelines(scenario_config_id, scenario_config)
                self._check_comparators(scenario_config_id, scenario_config)
        return self._collector

    def _check_pipelines(self, scenario_config_id: str, scenario_config: ScenarioConfig):
        self._check_children(
            ScenarioConfig,
            scenario_config_id,
            scenario_config.PIPELINE_KEY,
            scenario_config.pipelines,
            PipelineConfig,
        )

    def _check_frequency(self, scenario_config_id: str, scenario_config: ScenarioConfig):
        if scenario_config.frequency and not isinstance(scenario_config.frequency, Frequency):
            self._error(
                scenario_config.FREQUENCY_KEY,
                scenario_config.frequency,
                f"{scenario_config.FREQUENCY_KEY} field of Scenario {scenario_config_id} must be populated with a "
                f"Frequency value.",
            )

    def _check_comparators(self, scenario_config_id: str, scenario_config: ScenarioConfig):
        if not scenario_config.comparators:
            self._info(
                scenario_config.COMPARATOR_KEY,
                scenario_config.comparators,
                f"No scenario {scenario_config.COMPARATOR_KEY} defined for scenario {scenario_config_id}",
            )
