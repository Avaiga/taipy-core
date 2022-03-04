import pytest

from taipy.core.common.validate_id import to_identifier, validate_id
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

    def test_to_identifier(self):
        s = to_identifier("foo")
        assert s == "foo"
        s = to_identifier("1foo")
        assert s == "foo"
        s = to_identifier("foo bar")
        assert s == "foobar"
        s = to_identifier("foobÀr")
        assert s == "foobr"
        s = to_identifier("foo/foo$")
        assert s == "foofoo"
        s = to_identifier("")
        assert s == "_"
        s = to_identifier(" ")
        assert s == "_"
        s = to_identifier("class")
        assert s == "class_"
        s = to_identifier("def")
        assert s == "def_"
        s = to_identifier("with")
        assert s == "with_"
