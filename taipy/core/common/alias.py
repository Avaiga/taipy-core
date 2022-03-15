from typing import NewType

PipelineId = NewType("PipelineId", str)
PipelineId.__doc__ = """ Type representing a pipeline identifier."""
ScenarioId = NewType("ScenarioId", str)
ScenarioId.__doc__ = """ Type representing a scenario identifier."""
TaskId = NewType("TaskId", str)
TaskId.__doc__ = """ Type representing a task identifier."""
JobId = NewType("JobId", str)
JobId.__doc__ = """ Type representing a job identifier."""
CycleId = NewType("CycleId", str)
CycleId.__doc__ = """ Type representing a cycle identifier."""
DataNodeId = NewType("DataNodeId", str)
DataNodeId.__doc__ = """ Type representing a data node identifier."""
