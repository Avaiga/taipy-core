import pytest

from taipy.core.common.validate_id import validate_id
from taipy.core.exceptions.configuration import InvalidConfigurationId


class TestId:
    def test_validate_id(self):
        s = validate_id("foo")
        assert s == "foo"
        with pytest.raises(InvalidConfigurationId):
            validate_id("1foo")
        with pytest.raises(InvalidConfigurationId):
            validate_id("foo bar")
        with pytest.raises(InvalidConfigurationId):
            validate_id("foo/foo$")
        with pytest.raises(InvalidConfigurationId):
            validate_id("")
        with pytest.raises(InvalidConfigurationId):
            validate_id(" ")
        with pytest.raises(InvalidConfigurationId):
            validate_id("class")
        with pytest.raises(InvalidConfigurationId):
            validate_id("def")
        with pytest.raises(InvalidConfigurationId):
            validate_id("with")
