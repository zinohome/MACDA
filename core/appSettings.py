#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import ssl
from ssl import SSLContext

from pydantic import BaseSettings, Field, validator, root_validator


class AppSettings(BaseSettings):
    """项目配置"""
    APP_TITLE: str = 'MACDA'
    APP_VERSION: str = '1.0911'
    DEBUG: bool = Field(True, env='DEBUG')
    RUN_MODE: str = Field('Parse', env='RUN_MODE')
    DEV_MODE: bool = Field(True, env='DEV_MODE')
    SOURCE_TOPIC_NAME: str = Field('signal-in', env='SOURCE_TOPIC_NAME')
    PARSED_TOPIC_NAME: str = Field('signal-in', env='PARSED_TOPIC_NAME')
    KAFKA_BOOTSTRAP_SERVER: str = Field('kafka://localhost:9092', env='KAFKA_BOOTSTRAP_SERVER')
    SCHEMA_REGISTRY_URL: str = Field('http://localhost:8081', env='SCHEMA_REGISTRY_URL')
    STORE_URI: str = Field('memory://', env='STORE_URI')
    TOPIC_PARTITIONS: int = Field(3, env='TOPIC_PARTITIONS')
    TOPIC_ALLOW_DECLARE: bool = Field(True, env='TOPIC_ALLOW_DECLARE')
    TOPIC_DISABLE_LEADER: bool = Field(False, env='TOPIC_DISABLE_LEADER')
    SSL_ENABLED: bool = Field(False, env='SSL_ENABLED')
    SSL_CONTEXT: SSLContext = Field(None, env='SSL_CONTEXT')
    # file in pem format containing the client certificate, as well as any ca certificates
    # needed to establish the certificate’s authenticity
    KAFKA_SSL_CERT: str = Field(None, env='KAFKA_SSL_CERT')
    # filename containing the client private key
    KAFKA_SSL_KEY: str = Field(None, env='KAFKA_SSL_KEY')
    # filename of ca file to use in certificate verification
    KAFKA_SSL_CABUNDLE: str = Field(None, env='KAFKA_SSL_CABUNDLE')
    # password for decrypting the client private key
    SSL_KEY_PASSWORD: str = Field(None, env='SSL_KEY_PASSWORD')
    #SSL_CONTEXT = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=KAFKA_SSL_CABUNDLE)
    #SSL_CONTEXT.load_cert_chain(KAFKA_SSL_CERT, keyfile=KAFKA_SSL_KEY, password=SSL_KEY_PASSWORD)
    APP_EXCEPTIONN_DETAIL: bool = Field(True, env='APP_EXCEPTIONN_DETAIL')
    APP_LOG_LEVEL: str = Field('INFO', env='APP_LOG_LEVEL')
    APP_LOG_FILENAME: str = Field('macda.log', env='APP_LOG_FILENAME')
    TSDB_URL: str = Field('postgres://postgres:passw0rd@timescaledb:5432/postgres', env='TSDB_URL')
    TSDB_POOL_SIZE: int = Field(20, env='TSDB_POOL_SIZE')
    TSDB_PARSE_BATCH_TIME: int = Field(10, env='TSDB_PARSE_BATCH_TIME')
    TSDB_BATCH_TIME: int = Field(10, env='TSDB_BATCH_TIME')
    TSDB_PARSE_BATCH: int = Field(200, env='TSDB_PARSE_BATCH')
    TSDB_BATCH: int = Field(200, env='TSDB_BATCH')
    PREDICT_SKIP_BATCH: int = Field(200, env='PREDICT_SKIP_BATCH')
    #Rest Server URL
    SEND_FAULT_INTERVAL: int = Field(300, env='SEND_FAULT_INTERVAL')
    SEND_STATS_INTERVAL: int = Field(14400, env='SEND_STATS_INTERVAL')
    SEND_FAULT_RECORD: bool = Field(False, env='SEND_FAULT_RECORD')
    SEND_STATS_RECORD: bool = Field(False, env='SEND_STATS_RECORD')
    FAULT_RECORD_URL: str = Field('http://192.168.32.160:8180/api/rest/InsertSrvAlert', env='FAULT_RECORD_URL')
    STATS_RECORD_URL: str = Field('http://192.168.32.160:8180/api/rest/InsertSrvLife', env='STATS_RECORD_URL')

    @validator('KAFKA_BOOTSTRAP_SERVER', 'SCHEMA_REGISTRY_URL', pre = True)
    def valid_url(url: str):
        return url[:-1] if url.endswith('/') else url


