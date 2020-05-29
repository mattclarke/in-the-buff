# in-the-buff
A better name than buff-sniffer?
An simple application for decoding FlatBuffers messages stored in Apache Kafka.

## Usage
```
python ./bin/in-the-buff.py -b localhost:9092 -t data_topic -s hs00
```

Parameters:
 --broker (str) - The address of the Kafka broker
 --topic (str) - The name of the Kafka topic to consume from
 --schema (str) - The FlatBuffers schema to convert from
 --start-from-oldest (flag) - Whether to start from the oldest message

See https://github.com/ess-dmsc/python-streaming-data-types to see which schema
are supported.

## For developers

### Install the commit hooks (important)
There are commit hooks for Black and Flake8.

The commit hooks are handled using [pre-commit](https://pre-commit.com).

To install the hooks for this project run:
```
pre-commit install
```

To test the hooks run:
```
pre-commit run --all-files
```
This command can also be used to run the hooks manually.

