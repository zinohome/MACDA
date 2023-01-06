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
from utils.tsutil import TSutil


@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    async for data in stream:
        #log.debug(data['payload'])
        if 'payload' in data:
            tu.insert('pro_macda',data['payload'])
            tu.insert('dev_macda',data['payload'])
            tu.insertjson('pro_macda_json',data['payload'])
            tu.insertjson('dev_macda_json',data['payload'])
            log.info("Saved data with key : %s" % f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_dvc_time']}")
        #else:
        #    tu.insert('pro_macda', data)
        #    tu.insert('dev_macda', data)
        #    log.debug("Saved data with key : %s" % f"{data['msg_calc_dvc_no']}-{data['msg_calc_dvc_time']}")