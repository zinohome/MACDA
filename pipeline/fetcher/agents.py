#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import pytz
import requests

from app import app
from pipeline.fetcher.models import output_topic
from utils.log import log as log

#@app.crontab("*/1 * * * *", timezone=pytz.timezone("Asia/Shanghai"), on_leader=True)
@app.timer(interval=30, on_leader=True)
async def fetch():
    response = requests.get("http://192.168.32.195:3000/api/?results=300")
    if response.status_code == 200:
        log.success('PUBLISHING ON LEADER!')
        data = response.json()
        for result in data["results"]:
            key = result["id"]["value"]
            if not key:
                # randomuser.me has some some users with None or empty ID value,
                # we don't want to process these.
                continue
            log.success("Fetched user with ID : %s " %  key)
            await output_topic.send(key=key, value=result)