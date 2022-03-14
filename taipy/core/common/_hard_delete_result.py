from typing import Set, Union


class _TaskHardDeleteResult:
    def __init__(
        self, pipeline_data_node_ids: Union[None, Set[str]] = None, scenario_data_node_ids: Union[None, Set[str]] = None
    ):
        self.pipeline_data_node_ids = pipeline_data_node_ids if pipeline_data_node_ids is not None else set()
        self.scenario_data_node_ids = scenario_data_node_ids if scenario_data_node_ids is not None else set()


class _PipelineHardDeleteResult:
    def __init__(
        self, scenario_data_node_ids: Union[None, Set[str]] = None, scenario_task_ids: Union[None, Set[str]] = None
    ):
        self.scenario_data_node_ids = scenario_data_node_ids if scenario_data_node_ids is not None else set()
        self.scenario_task_ids = scenario_task_ids if scenario_task_ids is not None else set()
