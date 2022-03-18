from functools import reduce
from importlib import import_module
from typing import Callable, Optional


def _rgetattr(obj, attr):
    """Recursive getattr."""
    return reduce(getattr, attr.split("."), obj)


def _load_fct(module_name: str, fct_name: str) -> Callable:
    module = import_module(module_name)
    return _rgetattr(module, fct_name)


def _get_fct_name(f) -> Optional[str]:
    # Mock function does not have __qualname__ attribute
    # Partial or anonymous function does not have __name__ or __qualname__ attribute
    name = getattr(f, "__qualname__", getattr(f, "__name__", None))
    return name


def _fct_to_dict(obj):
    fct_name = _get_fct_name(obj)
    if not fct_name:
        return None
    return {"fct_name": fct_name, "fct_module": obj.__module__}


def _fcts_to_dict(objs):
    fcts = []
    for obj in objs:
        d = _fct_to_dict(obj)
        if d:
            fcts.append(d)
    return fcts
