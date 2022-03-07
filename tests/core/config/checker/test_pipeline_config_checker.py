from copy import copy

from taipy.core.config._config import _Config
from taipy.core.config.checker._checkers._pipeline_config_checker import _PipelineConfigChecker
from taipy.core.config.checker.issue_collector import IssueCollector
from taipy.core.config.task_config import TaskConfig


class TestPipelineConfigChecker:
    def test_check_config_id(self):
        collector = IssueCollector()
        config = _Config.default_config()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0
        assert len(collector.warnings) == 0

        config.pipelines["new"] = copy(config.pipelines["default"])
        config.pipelines["new"].id = None
        collector = IssueCollector()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1
        assert len(collector.warnings) == 1

        config.pipelines["new"].id = "new"
        collector = IssueCollector()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0
        assert len(collector.warnings) == 1

    def test_check_task(self):
        collector = IssueCollector()
        config = _Config.default_config()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0
        assert len(collector.warnings) == 0

        config.pipelines["new"] = copy(config.pipelines["default"])
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0
        assert len(collector.warnings) == 1

        config.pipelines["new"].tasks = [TaskConfig("bar", None)]
        collector = IssueCollector()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0
        assert len(collector.warnings) == 0

        config.pipelines["new"].tasks = "bar"
        collector = IssueCollector()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1
        assert len(collector.warnings) == 0

        config.pipelines["new"].tasks = ["bar"]
        collector = IssueCollector()
        _PipelineConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1
        assert len(collector.warnings) == 0
