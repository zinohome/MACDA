#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA

import faust
from core.settings import settings

app = faust.App(
    settings.APP_TITLE,
    version = float(settings.APP_VERSION),
    broker = settings.KAFKA_BOOTSTRAP_SERVER,
    store = settings.STORE_URI,
    topic_partitions = settings.TOPIC_PARTITIONS,
    topic_allow_declare=settings.TOPIC_ALLOW_DECLARE,
    topic_disable_leader=settings.TOPIC_DISABLE_LEADER
)
app.web.blueprints.add('/stats/', 'faust.web.apps.stats:blueprint')
app.discover('pipeline.parse','pipeline.batchstore','pipeline.predict')
#app.discover('pipeline.parse','pipeline.store')
