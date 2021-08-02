# ESS command-line message consumer
  - Kafka commandline message consumer specific to handle ESS flatbuffer messages
    of types

    |name|description|
    |----|-----------|
    |pl72|Run start|
    |6s4t|Run stop|
    |f142|Log data|
    |ev42|Event data|
    |x5f2|Status messages|
    |answ|File-writer command response|
    |wrdn|File-writer finished writing|
Installing
==========

`ess_message_consumer`

Create virtual environment with Python 3.6 or later:

    python3 -m venev {env_name}

Activate conda environment:

    source {env_name}/bin/activate
    pip install .

Usage:

    start_consumer -b {broker_address} -t {topics_to_consumer}
    broker_address: for eg. "localhost:9092"
    topics_to_consumer: "topic_1, topic_2, ..."