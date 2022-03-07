import pytest

from taipy.core.common.alias import TaskId
from taipy.core.config.config import Config
from taipy.core.data.data_manager import DataManager
from taipy.core.data.in_memory import InMemoryDataNode
from taipy.core.data.scope import Scope
from taipy.core.exceptions.repository import ModelNotFound
from taipy.core.task.task import Task
from taipy.core.task.task_manager import TaskManager


def test_create_and_save():
    input_configs = [Config.add_data_node("my_input", "in_memory")]
    output_configs = Config.add_data_node("my_output", "in_memory")
    task_config = Config.add_task("foo", print, input_configs, output_configs)
    task = TaskManager.get_or_create(task_config)
    assert task.id is not None
    assert task.config_id == "foo"
    assert len(task.input) == 1
    assert len(DataManager._get_all()) == 2
    assert task.my_input.id is not None
    assert task.my_input.config_id == "my_input"
    assert task.my_output.id is not None
    assert task.my_output.config_id == "my_output"
    assert task.function == print

    task_retrieved_from_manager = TaskManager._get(task.id)
    assert task_retrieved_from_manager.id == task.id
    assert task_retrieved_from_manager.config_id == task.config_id
    assert len(task_retrieved_from_manager.input) == len(task.input)
    assert task_retrieved_from_manager.my_input.id is not None
    assert task_retrieved_from_manager.my_input.config_id == task.my_input.config_id
    assert task_retrieved_from_manager.my_output.id is not None
    assert task_retrieved_from_manager.my_output.config_id == task.my_output.config_id
    assert task_retrieved_from_manager.function == task.function


def test_do_not_recreate_existing_data_node():
    input_config = Config.add_data_node("my_input", "in_memory", scope=Scope.PIPELINE)
    output_config = Config.add_data_node("my_output", "in_memory", scope=Scope.PIPELINE)

    DataManager._create_and_set(input_config, "pipeline_id")
    assert len(DataManager._get_all()) == 1

    task_config = Config.add_task("foo", print, input_config, output_config)
    TaskManager.get_or_create(task_config, pipeline_id="pipeline_id")
    assert len(DataManager._get_all()) == 2


def test_do_not_recreate_existing_task():
    input_config_scope_pipeline = Config.add_data_node("my_input", "in_memory", scope=Scope.PIPELINE)
    output_config_scope_pipeline = Config.add_data_node("my_output", "in_memory", scope=Scope.PIPELINE)
    task_config = Config.add_task("foo", print, input_config_scope_pipeline, output_config_scope_pipeline)
    # task_config scope is Pipeline

    task_1 = TaskManager.get_or_create(task_config)
    assert len(TaskManager._get_all()) == 1
    TaskManager.get_or_create(task_config)  # Do not create. It already exists for None pipeline
    assert len(TaskManager._get_all()) == 1
    task_2 = TaskManager.get_or_create(
        task_config, "whatever_scenario"
    )  # Do not create. It already exists for None pipeline
    assert len(TaskManager._get_all()) == 1
    assert task_1.id == task_2.id
    task_3 = TaskManager.get_or_create(task_config, "whatever_scenario", "pipeline_1")
    assert len(TaskManager._get_all()) == 2
    assert task_1.id == task_2.id
    assert task_2.id != task_3.id
    task_4 = TaskManager.get_or_create(
        task_config, "other_sc", "pipeline_1"
    )  # Do not create. It already exists for pipeline_1
    assert len(TaskManager._get_all()) == 2
    assert task_1.id == task_2.id
    assert task_2.id != task_3.id
    assert task_3.id == task_4.id

    input_config_scope_scenario = Config.add_data_node("my_input_2", "in_memory", Scope.SCENARIO)
    output_config_scope_scenario = Config.add_data_node("my_output_2", "in_memory", Scope.SCENARIO)
    task_config_2 = Config.add_task("bar", print, input_config_scope_scenario, output_config_scope_scenario)
    # task_config_2 scope is Scenario

    task_5 = TaskManager.get_or_create(task_config_2)
    assert len(TaskManager._get_all()) == 3
    task_6 = TaskManager.get_or_create(task_config_2)  # Do not create. It already exists for None scenario
    assert len(TaskManager._get_all()) == 3
    assert task_5.id == task_6.id
    task_7 = TaskManager.get_or_create(
        task_config_2, None, "A_pipeline"
    )  # Do not create. It already exists for None scenario
    assert len(TaskManager._get_all()) == 3
    assert task_5.id == task_6.id
    assert task_6.id == task_7.id
    task_8 = TaskManager.get_or_create(
        task_config_2, "scenario_1", "A_pipeline"
    )  # Create even if pipeline is the same.
    assert len(TaskManager._get_all()) == 4
    assert task_5.id == task_6.id
    assert task_6.id == task_7.id
    assert task_7.id != task_8.id
    task_9 = TaskManager.get_or_create(
        task_config_2, "scenario_1", "bar"
    )  # Do not create. It already exists for scenario_1
    assert len(TaskManager._get_all()) == 4
    assert task_5.id == task_6.id
    assert task_6.id == task_7.id
    assert task_7.id != task_8.id
    assert task_8.id == task_9.id
    task_10 = TaskManager.get_or_create(task_config_2, "scenario_2", "baz")
    assert len(TaskManager._get_all()) == 5
    assert task_5.id == task_6.id
    assert task_6.id == task_7.id
    assert task_7.id != task_8.id
    assert task_8.id == task_9.id
    assert task_9.id != task_10.id
    assert task_7.id != task_10.id


def test_set_and_get_task():
    task_id_1 = TaskId("id1")
    first_task = Task("name_1", print, [], [], task_id_1)
    task_id_2 = TaskId("id2")
    second_task = Task("name_2", print, [], [], task_id_2)
    third_task_with_same_id_as_first_task = Task("name_is_not_1_anymore", print, [], [], task_id_1)

    # No task at initialization

    assert len(TaskManager._get_all()) == 0
    assert TaskManager._get(task_id_1) is None
    assert TaskManager._get(first_task) is None
    assert TaskManager._get(task_id_2) is None
    assert TaskManager._get(second_task) is None

    # Save one task. We expect to have only one task stored
    TaskManager._set(first_task)
    assert len(TaskManager._get_all()) == 1
    assert TaskManager._get(task_id_1).id == first_task.id
    assert TaskManager._get(first_task).id == first_task.id
    assert TaskManager._get(task_id_2) is None
    assert TaskManager._get(second_task) is None

    # Save a second task. Now, we expect to have a total of two tasks stored
    TaskManager._set(second_task)
    assert len(TaskManager._get_all()) == 2
    assert TaskManager._get(task_id_1).id == first_task.id
    assert TaskManager._get(first_task).id == first_task.id
    assert TaskManager._get(task_id_2).id == second_task.id
    assert TaskManager._get(second_task).id == second_task.id

    # We save the first task again. We expect nothing to change
    TaskManager._set(first_task)
    assert len(TaskManager._get_all()) == 2
    assert TaskManager._get(task_id_1).id == first_task.id
    assert TaskManager._get(first_task).id == first_task.id
    assert TaskManager._get(task_id_2).id == second_task.id
    assert TaskManager._get(second_task).id == second_task.id

    # We save a third task with same id as the first one.
    # We expect the first task to be updated
    TaskManager._set(third_task_with_same_id_as_first_task)
    assert len(TaskManager._get_all()) == 2
    assert TaskManager._get(task_id_1).id == third_task_with_same_id_as_first_task.id
    assert TaskManager._get(task_id_1).config_id != first_task.config_id
    assert TaskManager._get(first_task).id == third_task_with_same_id_as_first_task.id
    assert TaskManager._get(first_task).config_id != first_task.config_id
    assert TaskManager._get(task_id_2).id == second_task.id
    assert TaskManager._get(second_task).id == second_task.id


def test_ensure_conservation_of_order_of_data_nodes_on_task_creation():

    embedded_1 = Config.add_data_node("dn_1", "in_memory", scope=Scope.PIPELINE)
    embedded_2 = Config.add_data_node("dn_2", "in_memory", scope=Scope.PIPELINE)
    embedded_3 = Config.add_data_node("a_dn_3", "in_memory", scope=Scope.PIPELINE)
    embedded_4 = Config.add_data_node("dn_4", "in_memory", scope=Scope.PIPELINE)
    embedded_5 = Config.add_data_node("dn_5", "in_memory", scope=Scope.PIPELINE)

    input = [embedded_1, embedded_2, embedded_3]
    output = [embedded_4, embedded_5]
    task_config = Config.add_task("name_1", print, input, output)
    task = TaskManager.get_or_create(task_config)

    assert [i.config_id for i in task.input.values()] == [embedded_1.id, embedded_2.id, embedded_3.id]
    assert [o.config_id for o in task.output.values()] == [embedded_4.id, embedded_5.id]

    data_nodes = {
        embedded_1: InMemoryDataNode(embedded_1.id, Scope.PIPELINE),
        embedded_2: InMemoryDataNode(embedded_2.id, Scope.PIPELINE),
        embedded_3: InMemoryDataNode(embedded_3.id, Scope.PIPELINE),
        embedded_4: InMemoryDataNode(embedded_4.id, Scope.PIPELINE),
        embedded_5: InMemoryDataNode(embedded_5.id, Scope.PIPELINE),
    }

    task_config = Config.add_task("name_2", print, input, output)
    task = TaskManager.get_or_create(task_config, data_nodes)

    assert [i.config_id for i in task.input.values()] == [embedded_1.id, embedded_2.id, embedded_3.id]
    assert [o.config_id for o in task.output.values()] == [embedded_4.id, embedded_5.id]


def test_get_all_by_config_id():
    input_configs = [Config.add_data_node("my_input", "in_memory", scope=Scope.PIPELINE)]
    assert len(TaskManager._get_all_by_config_id("NOT_EXISTING_CONFIG_NAME")) == 0
    task_config_1 = Config.add_task("foo", print, input_configs, [])
    assert len(TaskManager._get_all_by_config_id("foo")) == 0

    TaskManager.get_or_create(task_config_1)
    assert len(TaskManager._get_all_by_config_id("foo")) == 1

    task_config_2 = Config.add_task("baz", print, input_configs, [])
    TaskManager.get_or_create(task_config_2)
    assert len(TaskManager._get_all_by_config_id("foo")) == 1
    assert len(TaskManager._get_all_by_config_id("baz")) == 1

    TaskManager.get_or_create(task_config_2, "other_scenario", "other_pipeline")
    assert len(TaskManager._get_all_by_config_id("foo")) == 1
    assert len(TaskManager._get_all_by_config_id("baz")) == 2


def test_delete_raise_exception():
    dn_input_config_1 = Config.add_data_node("my_input_1", "in_memory", scope=Scope.PIPELINE, default_data="testing")
    dn_output_config_1 = Config.add_data_node("my_output_1", "in_memory")
    task_config_1 = Config.add_task("task_config_1", print, dn_input_config_1, dn_output_config_1)
    task_1 = TaskManager.get_or_create(task_config_1)
    TaskManager._delete(task_1.id)

    with pytest.raises(ModelNotFound):
        TaskManager._delete(task_1.id)


def test_hard_delete():
    dn_input_config_1 = Config.add_data_node("my_input_1", "in_memory", scope=Scope.PIPELINE, default_data="testing")
    dn_output_config_1 = Config.add_data_node("my_output_1", "in_memory")
    task_config_1 = Config.add_task("task_config_1", print, dn_input_config_1, dn_output_config_1)
    task_1 = TaskManager.get_or_create(task_config_1)

    assert len(TaskManager._get_all()) == 1
    assert len(DataManager._get_all()) == 2
    TaskManager.hard_delete(task_1.id)
    assert len(TaskManager._get_all()) == 0
    assert len(DataManager._get_all()) == 2
