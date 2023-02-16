#!/bin/bash
sleep 60;
/usr/bin/curl -X PUT -H  "Content-Type:application/json" \
http://mock-kafka-connect:8083/connectors/File-In/config \
-d '{
    "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirBinaryFileSourceConnector",
    "empty.poll.wait.ms": "1000",
    "error.path": "/data/error",
    "file.minimum.age.ms": "1000",
    "finished.path": "/data/finished",
    "input.file.pattern": "^FRAME-DATA_\\d*+\\.BIN$",
    "input.path": "/data/input",
    "name": "File-In",
    "topic": "signal-in",
    "topic.creation.default.partitions": "3",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.groups": "signal-in"
}'