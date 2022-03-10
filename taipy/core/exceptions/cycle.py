class CycleAlreadyExists(Exception):
    """
    Raised if it is trying to create a Cycle that has already exists
    """

    pass


class NonExistingCycle(Exception):
    """
    Raised if we request a cycle not known by the cycle manager.
    """
