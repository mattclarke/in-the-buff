import pytest
from streaming_data_types import SERIALISERS
from in_the_buff.deserialiser import Deserialiser


class TestDeserialiser:
    def test_if_schema_exists_then_does_not_throw(self):
        Deserialiser("hs00")
        assert True

    def test_if_schema_does_not_exist_then_throws(self):
        with pytest.raises(Exception):
            Deserialiser("not_a_schema")

    def test_can_deserialise(self):
        serialiser = SERIALISERS["ns10"]
        buffer = serialiser("my_key", "my_value", 123456)
        deserialiser = Deserialiser("ns10")

        result = deserialiser.deserialise(buffer)

        assert result.key == "my_key"
        assert result.value == "my_value"
        assert result.time_stamp == 123456
