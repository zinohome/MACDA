#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA

import os
from pathlib import Path
from ssl import SSLContext

from core.appSettings import AppSettings

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(AppSettings):
    DEBUG: bool = True
    DEV_MODE: bool = True
    RUN_MODE: str = 'Parse'
    SOURCE_TOPIC_NAME: str = 'signal-in'
    PARSED_TOPIC_NAME: str = 'signal-parsed'
    KAFKA_BOOTSTRAP_SERVER: str = 'kafka://localhost:9092'
    SCHEMA_REGISTRY_URL: str = 'http://localhost:8081'
    STORE_URI: str = 'memory://'
    TOPIC_PARTITIONS: int = 3
    TOPIC_ALLOW_DECLARE: bool = True
    TOPIC_DISABLE_LEADER: bool = False
    SSL_ENABLED: bool = False
    SSL_CONTEXT: SSLContext = None
    # file in pem format containing the client certificate, as well as any ca certificates
    # needed to establish the certificate’s authenticity
    KAFKA_SSL_CERT: str = None
    # filename containing the client private key
    KAFKA_SSL_KEY: str = None
    # filename of ca file to use in certificate verification
    KAFKA_SSL_CABUNDLE: str = None
    # password for decrypting the client private key
    SSL_KEY_PASSWORD: str = None
    # SSL_CONTEXT = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=KAFKA_SSL_CABUNDLE)
    # SSL_CONTEXT.load_cert_chain(KAFKA_SSL_CERT, keyfile=KAFKA_SSL_KEY, password=SSL_KEY_PASSWORD)
    APP_EXCEPTIONN_DETAIL: bool = True
    APP_LOG_LEVEL: str = 'INFO'
    APP_LOG_FILENAME: str = 'macda.log'
    TSDB_URL: str = 'postgres://postgres:passw0rd@timescaledb:5432/postgres'
    TSDB_POOL_SIZE: int = 50
    TSDB_PARSE_BATCH_TIME: int = 10
    TSDB_BATCH_TIME: int = 10
    TSDB_PARSE_BATCH: int = 200
    TSDB_BATCH: int = 200
    PREDICT_SKIP_BATCH: int = 600
    #Rest Server URL
    SEND_FAULT_INTERVAL: int = 300
    SEND_STATS_INTERVAL: int = 14400
    SEND_FAULT_RECORD: bool = False
    SEND_STATS_RECORD: bool = False
    SEND_LIFE_RECORD: bool = False
    FAULT_RECORD_URL: str = 'http://192.168.32.160:8180/api/rest/InsertSrvAlert'
    STATS_RECORD_URL: str = 'http://192.168.32.160:8180/api/rest/InsertSrvLife'
    LIFE_RECORD_URL: str = 'http://192.168.32.160:8180/api/rest/InsertSrvLife'
    SYS_STATUS_URL: str = 'http://172.20.250.88:8080/gate/METRO-SELFCHECK/api/faultRecordsSubsystem/saveStatus'

settings = Settings(_env_file=os.path.join(BASE_DIR, '.env'))


if __name__ == '__main__':
    print(settings.KAFKA_BOOTSTRAP_SERVER)
    print(settings.RUN_MODE)