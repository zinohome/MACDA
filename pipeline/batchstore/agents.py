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
from pipeline.batchstore.models import input_topic
from utils.log import log as log
from utils.tsutil import TSutil


@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    async for datas in stream.take(10000, within=settings.TSDB_BATCH_TIME):
        log.debug("==================== Get parsed data batch ====================")
        dev_mode = settings.DEV_MODE
        if dev_mode:
            tu.batchinsert('dev_macda', 'msg_calc_parse_time', datas)
            tu.batchinsertjson('dev_macda_json', 'msg_calc_parse_time', datas)
        else:
            tu.batchinsert('pro_macda', 'msg_calc_dvc_time', datas)
            tu.batchinsertjson('pro_macda_json', 'msg_calc_dvc_time', datas)
        log.debug("Saved data with batch length: %s" % len(datas))

    '''
    async for data in stream:
        #log.debug(data['payload'])
        datalist = []
        if len(datalist) < 50:
            if 'payload' in data:
                datalist.append(data['payload'])
                log.debug(
                    "Saved data with key : (%s: %s)" % (len(datalist),f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_dvc_time']}"))
        else:
            tu.batchinsert('pro_macda',datalist)
            tu.batchinsert('dev_macda',datalist)
        #else:
        #    tu.insert('pro_macda', data)
        #    tu.insert('dev_macda', data)
        #    log.debug("Saved data with key : %s" % f"{data['msg_calc_dvc_no']}-{data['msg_calc_dvc_time']}")
    '''