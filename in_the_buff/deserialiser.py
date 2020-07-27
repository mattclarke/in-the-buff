from streaming_data_types import DESERIALISERS


class UnknownSchemaException(Exception):
    pass


class Deserialiser:
    @staticmethod
    def deserialise(buffer):
        schema = buffer[4:8].decode()
        if schema not in DESERIALISERS:
            raise UnknownSchemaException(f"Could not find deserialiser for {schema}")
        return schema, DESERIALISERS[schema](buffer)
