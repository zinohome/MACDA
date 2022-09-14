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
    SOURCE_TOPIC_NAME: str = Field('signal-in', env='SOURCE_TOPIC_NAME')
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
    APP_LOG_LEVEL: str = Field('', env='APP_LOG_LEVEL')
    APP_LOG_FILENAME: str = Field('', env='APP_LOG_FILENAME')

    @validator('KAFKA_BOOTSTRAP_SERVER', 'SCHEMA_REGISTRY_URL', pre = True)
    def valid_url(url: str):
        return url[:-1] if url.endswith('/') else url


