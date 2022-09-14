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
from pipeline.normaliser.models import input_topic, output_topic
from utils.log import log as log

def normalise_user(raw_user):
    return {
        "id": raw_user["id"]["value"],
        "name": f"{raw_user['name']['first']} {raw_user['name']['last']}",
        "cell": raw_user["cell"],
        "email": raw_user["email"],
    }

@app.agent(input_topic)
async def consume(stream):
    async for record in stream:
        raw_user = record.asdict()
        normalised_user = normalise_user(raw_user)
        key = normalised_user["id"]
        log.success("Normalised user with ID : %s" % key)
        await output_topic.send(key=key, value=normalised_user)