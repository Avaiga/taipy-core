import keyword
import re

from taipy.core.exceptions.configuration import InvalidConfigurationId


def validate_id(name: str):
    if name.isidentifier() and not keyword.iskeyword(name):
        return name
    raise InvalidConfigurationId(f"{name} is not a valid identifier.")


def to_identifier(s: str):
    # Remove invalid characters
    s = re.sub("[^0-9a-zA-Z_]", "", s)
    # Remove leading characters until we find a letter or underscore
    s = re.sub("^[^a-zA-Z_]+", "", s)
    if keyword.iskeyword(s):
        s += "_"
    if not s:
        s = "_"
    return s
