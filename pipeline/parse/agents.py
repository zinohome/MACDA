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
from codec.nb5 import Nb5
from pipeline.parse.models import input_topic, output_topic
from utils.log import log as log
from utils.tswriter import TSWriter


def parse_data(data):
    return Nb5.from_bytes_to_dict(data)

@app.agent(input_topic)
async def parse_signal(stream):
    tswriter = TSWriter("root.macda.dvc")
    async for data in stream:
        #log.debug(data)
        parsed_dict = parse_data(data)
        key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}"
        bintopicname = f"bin-archive-{parsed_dict['msg_calc_dvc_no']}"
        bintopic = app.topic(bintopicname, partitions=3, value_serializer='raw')
        log.success("Normalised data with key : %s" % f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}")
        await output_topic.send(key=key, value=parsed_dict)
        await bintopic.send(key=key,value=data)
        #tswriter = TSWriter(f"root.macda.dvc_{parsed_dict['msg_calc_dvc_no'].replace('-', '_')}")
        tswriter.create_aligned_record(parsed_dict)
        