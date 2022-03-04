from taipy.core.common._repr_enum import _ReprEnum


class Frequency(_ReprEnum):
    """Enumeration representing the recurrence of a Scenario, and so the duration of its work cycle.
    The possible values are DAILY, WEEKLY, MONTHLY, QUARTERLY, YEARLY
    """

    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    QUARTERLY = 4
    YEARLY = 5
