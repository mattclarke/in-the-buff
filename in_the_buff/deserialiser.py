import json

from streaming_data_types import DESERIALISERS


class UnknownSchemaException(Exception):
    pass


class Deserialiser:
    @staticmethod
    def get_schema(buffer):
        return buffer[4:8].decode()

    @staticmethod
    def deserialise(buffer):
        schema = Deserialiser.get_schema(buffer)
        if schema not in DESERIALISERS:
            try:
                # Try JSON
                return "JSON", json.loads(buffer.decode())
            except:  # noqa
                raise UnknownSchemaException(
                    f"Could not find deserialiser for `{schema}`"
                )
        return schema, DESERIALISERS[schema](buffer)
