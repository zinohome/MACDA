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
from core.settings import settings
from pipeline.predict.models import input_topic
from utils.log import log as log
from utils.sensorpolyfit import SensorPolyfit
from utils.tsutil import TSutil


@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    sp = SensorPolyfit()
    async for data in stream:
        dev_mode = settings.DEV_MODE
        mode = 'dev'
        if not dev_mode:
            mode = 'pro'
        refdata = tu.get_refdata(mode, data['msg_calc_dvc_no'])
        log.debug(refdata)