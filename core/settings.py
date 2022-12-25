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

settings = Settings(_env_file=os.path.join(BASE_DIR, '.env'))


if __name__ == '__main__':
    print(settings.KAFKA_BOOTSTRAP_SERVER)