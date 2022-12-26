#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA

from app import app
from pipeline.store.models import input_topic
from utils.log import log as log

@app.agent(input_topic)
async def store_signal(stream):
    async for data in stream:
        log.debug(data)