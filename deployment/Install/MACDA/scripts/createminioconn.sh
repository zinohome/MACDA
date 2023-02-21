#!/bin/bash
sleep 60;
/usr/bin/curl -X PUT -H  "Content-Type:application/json" \
http://kafka-connect:8083/connectors/MACDA-archive-to-minio/config \
-d '{
    "aws.access.key.id": "macdaminio",
    "aws.secret.access.key": "bgt56yhnPassw0rd",
    "connect.meta.data": "false",
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "enhanced.avro.schema.support": "false",
    "flush.size": "500",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "name": "MACDA-archive-to-minio",
    "s3.bucket.name": "macda-archive-bucket",
    "s3.region": "ap-east-1",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "store.url": "http://minio:9000",
    "topics.regex": "^MACDA-archive-signal-parsed-0\\d*0\\d*0\\d*$",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
}'