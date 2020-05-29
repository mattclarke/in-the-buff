from streaming_data_types import DESERIALISERS


class UnknownSchemaException(Exception):
    pass


class Deserialiser:
    def __init__(self, schema):
        if schema not in DESERIALISERS:
            raise UnknownSchemaException(f"Could not find deserialiser for {schema}")
        self.deserialiser = DESERIALISERS[schema]

    def deserialise(self, buffer):
        return self.deserialiser(buffer)
