#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
from datetime import datetime

from app import app
from core.settings import settings
from pipeline.status.models import input_topic
from utils.alertutil import Alertutil
from utils.log import log as log
from utils.tsutil import TSutil
import time


@app.timer(interval=600)
async def on_started():
    status = {}
    log.debug("==========********** Send system health status ==========**********")
    tu = TSutil()
    au = Alertutil()
    dev_mode = settings.DEV_MODE
    status['message_type'] = '500'
    status['subsystem'] = '5'
    status['status'] = '1'
    status['remark'] = ''
    status['solution'] = ''
    current_time_milliseconds = int(datetime.now().timestamp() * 1000)
    status['time'] = str(current_time_milliseconds)
    statuslist = []
    statuslist.append(status)
    au.send_status(statuslist)
