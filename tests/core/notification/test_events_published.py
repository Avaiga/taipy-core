# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from dataclasses import dataclass, field
from math import exp
from queue import SimpleQueue

from colorama import init

from src.taipy.core import taipy as tp
from src.taipy.core.config import scenario_config
from src.taipy.core.job.status import Status
from src.taipy.core.notification.core_event_consumer import CoreEventConsumerBase
from src.taipy.core.notification.event import Event, EventEntityType, EventOperation
from src.taipy.core.notification.notifier import Notifier
from taipy.config import Config, Frequency
from tests.core.utils import assert_true_after_time


class Snapshot:
    """
    A captured snapshot of the recording core events consumer.
    """

    def __init__(self):
        self.collected_events = []
        self.entity_type_collected = {}
        self.operation_collected = {}
        self.attr_name_collected = {}

    def capture_event(self, event):
        self.collected_events.append(event)
        self.entity_type_collected[event.entity_type] = self.entity_type_collected.get(event.entity_type, 0) + 1
        self.operation_collected[event.operation] = self.operation_collected.get(event.operation, 0) + 1
        if event.attribute_name:
            self.attr_name_collected[event.attribute_name] = self.attr_name_collected.get(event.attribute_name, 0) + 1


class RecordingConsumer(CoreEventConsumerBase):
    """
    A straightforward and no-thread core events consumer that allows to
    capture snapshots of received events.
    """

    def __init__(self, registration_id: str, queue: SimpleQueue):
        super().__init__(registration_id, queue)

    def capture(self) -> Snapshot:
        """
        Capture a snapshot of events received between the previous snapshot
        (or from the start of this consumer).
        """
        snapshot = Snapshot()
        while not self.queue.empty():
            event = self.queue.get()
            snapshot.capture_event(event)
        return snapshot

    def process_event(self, event: Event):
        # Nothing todo
        pass

    def start(self):
        # Nothing to do here
        pass

    def stop(self):
        # Nothing to do here either
        pass


def identity(x):
    return x


def test_event_published():
    register_id_0, register_queue_0 = Notifier.register()
    all_evts = RecordingConsumer(register_id_0, register_queue_0)
    all_evts.start()

    input_config = Config.configure_data_node("the_input")
    output_config = Config.configure_data_node("the_output")
    task_config = Config.configure_task("the_task", identity, input=input_config, output=output_config)
    sc_config = Config.configure_scenario(
        "the_scenario", task_configs=[task_config], frequency=Frequency.DAILY, sequences={"the_seq": [task_config]}
    )

    # Create a scenario only trigger 6 creation events (for cycle, data node(x2), task, sequence and scenario)
    scenario = tp.create_scenario(sc_config)
    snapshot = all_evts.capture()
    assert len(snapshot.collected_events) == 6
    assert snapshot.entity_type_collected.get(EventEntityType.CYCLE, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.DATA_NODE, 0) == 2
    assert snapshot.entity_type_collected.get(EventEntityType.TASK, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.SEQUENCE, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.SCENARIO, 0) == 1
    assert snapshot.operation_collected.get(EventOperation.CREATION, 0) == 6

    # Get all scenarios does not trigger any event
    tp.get_scenarios()
    snapshot = all_evts.capture()
    assert len(snapshot.collected_events) == 0

    # Get one scenario does not trigger any event
    sc = tp.get(scenario.id)
    snapshot = all_evts.capture()
    assert len(snapshot.collected_events) == 0

    # Write input manually trigger 4 data node update events (for last_edit_date, editor_id, editor_expiration_date
    # and edit_in_progress)
    sc.the_input.write("test")
    snapshot = all_evts.capture()
    assert len(snapshot.collected_events) == 4
    assert snapshot.entity_type_collected.get(EventEntityType.CYCLE, 0) == 0
    assert snapshot.entity_type_collected.get(EventEntityType.DATA_NODE, 0) == 4
    assert snapshot.entity_type_collected.get(EventEntityType.TASK, 0) == 0
    assert snapshot.entity_type_collected.get(EventEntityType.SEQUENCE, 0) == 0
    assert snapshot.entity_type_collected.get(EventEntityType.SCENARIO, 0) == 0
    assert snapshot.operation_collected.get(EventOperation.CREATION, 0) == 0
    assert snapshot.operation_collected.get(EventOperation.UPDATE, 0) == 4

    # Submit a scenario triggers 12 events:
    # 1 scenario submission event
    # 7 data node update events (for last_edit_date, editor_id(x2), editor_expiration_date(x2) and edit_in_progress(x2))
    # 1 job creation event
    # 3 job update events (for status: PENDING, RUNNING and COMPLETED)
    sc.submit()
    snapshot = all_evts.capture()
    assert len(snapshot.collected_events) == 12
    assert snapshot.entity_type_collected.get(EventEntityType.CYCLE, 0) == 0
    assert snapshot.entity_type_collected.get(EventEntityType.DATA_NODE, 0) == 7
    assert snapshot.entity_type_collected.get(EventEntityType.TASK, 0) == 0
    assert snapshot.entity_type_collected.get(EventEntityType.SEQUENCE, 0) == 0
    assert snapshot.entity_type_collected.get(EventEntityType.SCENARIO, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.JOB, 0) == 4
    assert snapshot.operation_collected.get(EventOperation.CREATION, 0) == 1
    assert snapshot.operation_collected.get(EventOperation.UPDATE, 0) == 10
    assert snapshot.operation_collected.get(EventOperation.SUBMISSION, 0) == 1

    # Delete a scenario trigger 7 update events
    tp.delete(scenario.id)
    snapshot = all_evts.capture()
    assert len(snapshot.collected_events) == 7
    assert snapshot.entity_type_collected.get(EventEntityType.CYCLE, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.DATA_NODE, 0) == 2
    assert snapshot.entity_type_collected.get(EventEntityType.TASK, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.SEQUENCE, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.SCENARIO, 0) == 1
    assert snapshot.entity_type_collected.get(EventEntityType.JOB, 0) == 1
    assert snapshot.operation_collected.get(EventOperation.UPDATE, 0) == 0
    assert snapshot.operation_collected.get(EventOperation.SUBMISSION, 0) == 0
    assert snapshot.operation_collected.get(EventOperation.DELETION, 0) == 7

    all_evts.stop()


def test_job_events():
    input_config = Config.configure_data_node("the_input")
    output_config = Config.configure_data_node("the_output")
    task_config = Config.configure_task("the_task", identity, input=input_config, output=output_config)
    sc_config = Config.configure_scenario(
        "the_scenario", task_configs=[task_config], frequency=Frequency.DAILY, sequences={"the_seq": [task_config]}
    )
    register_id, register_queue = Notifier.register(entity_type=EventEntityType.JOB)
    consumer = RecordingConsumer(register_id, register_queue)
    consumer.start()

    # Create scenario
    scenario = tp.create_scenario(sc_config)
    snapshot = consumer.capture()
    assert len(snapshot.collected_events) == 0

    # Submit scenario
    scenario.submit()
    snapshot = consumer.capture()

    # 2 events expected: one for creation, another for status update
    assert len(snapshot.collected_events) == 2
    assert snapshot.collected_events[0].operation == EventOperation.CREATION
    assert snapshot.collected_events[0].entity_type == EventEntityType.JOB
    assert snapshot.collected_events[0].metadata.get("task_config_id") == task_config.id

    assert snapshot.collected_events[1].operation == EventOperation.UPDATE
    assert snapshot.collected_events[1].entity_type == EventEntityType.JOB
    assert snapshot.collected_events[1].metadata.get("task_config_id") == task_config.id
    assert snapshot.collected_events[1].attribute_name == "status"
    assert snapshot.collected_events[1].attribute_value == Status.BLOCKED

    job = tp.get_jobs()[0]

    tp.cancel_job(job)
    snapshot = consumer.capture()
    assert len(snapshot.collected_events) == 1
    event = snapshot.collected_events[0]
    assert event.metadata.get("task_config_id") == task_config.id
    assert event.attribute_name == "status"
    assert event.attribute_value == Status.CANCELED

    consumer.stop()


def test_scenario_events():
    input_config = Config.configure_data_node("the_input")
    output_config = Config.configure_data_node("the_output")
    task_config = Config.configure_task("the_task", identity, input=input_config, output=output_config)
    sc_config = Config.configure_scenario(
        "the_scenario", task_configs=[task_config], frequency=Frequency.DAILY, sequences={"the_seq": [task_config]}
    )
    register_id, register_queue = Notifier.register(entity_type=EventEntityType.SCENARIO)
    consumer = RecordingConsumer(register_id, register_queue)
    consumer.start()
    scenario = tp.create_scenario(sc_config)

    snapshot = consumer.capture()
    assert len(snapshot.collected_events) == 1
    assert snapshot.collected_events[0].operation == EventOperation.CREATION
    assert snapshot.collected_events[0].entity_type == EventEntityType.SCENARIO
    assert snapshot.collected_events[0].metadata.get("config_id") == scenario.config_id

    scenario.submit()
    snapshot = consumer.capture()
    assert len(snapshot.collected_events) == 1
    assert snapshot.collected_events[0].operation == EventOperation.SUBMISSION
    assert snapshot.collected_events[0].entity_type == EventEntityType.SCENARIO
    assert snapshot.collected_events[0].metadata.get("config_id") == scenario.config_id

    # Delete scenario
    tp.delete(scenario.id)
    snapshot = consumer.capture()
    assert len(snapshot.collected_events) == 1

    assert snapshot.collected_events[0].operation == EventOperation.DELETION
    assert snapshot.collected_events[0].entity_type == EventEntityType.SCENARIO

    consumer.stop()


def test_data_node_events():
    input_config = Config.configure_data_node("the_input")
    output_config = Config.configure_data_node("the_output")
    task_config = Config.configure_task("the_task", identity, input=input_config, output=output_config)
    sc_config = Config.configure_scenario(
        "the_scenario", task_configs=[task_config], frequency=Frequency.DAILY, sequences={"the_seq": [task_config]}
    )
    register_id, register_queue = Notifier.register(entity_type=EventEntityType.DATA_NODE)
    consumer = RecordingConsumer(register_id, register_queue)
    consumer.start()

    scenario = tp.create_scenario(sc_config)

    snapshot = consumer.capture()
    # We expect two creation events since we have two data nodes:
    assert len(snapshot.collected_events) == 2

    assert snapshot.collected_events[0].operation == EventOperation.CREATION
    assert snapshot.collected_events[0].entity_type == EventEntityType.DATA_NODE
    assert snapshot.collected_events[0].metadata.get("config_id") in [output_config.id, input_config.id]

    assert snapshot.collected_events[1].operation == EventOperation.CREATION
    assert snapshot.collected_events[1].entity_type == EventEntityType.DATA_NODE
    assert snapshot.collected_events[1].metadata.get("config_id") in [output_config.id, input_config.id]

    # Delete scenario
    tp.delete(scenario.id)
    snapshot = consumer.capture()
    assert len(snapshot.collected_events) == 2

    assert snapshot.collected_events[0].operation == EventOperation.DELETION
    assert snapshot.collected_events[0].entity_type == EventEntityType.DATA_NODE

    assert snapshot.collected_events[1].operation == EventOperation.DELETION
    assert snapshot.collected_events[1].entity_type == EventEntityType.DATA_NODE

    consumer.stop()
