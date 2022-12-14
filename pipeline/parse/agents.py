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
from core.settings import settings
from pipeline.fetcher.models import output_schema
from pipeline.parse.models import input_topic, output_topic, json_schema
from utils.log import log as log

@app.agent(input_topic)
async def parse_signal(stream):
    async for data in stream:
        #log.debug(data)
        # Parse data and send to parsed topic
        parsed_dict = Nb5.from_bytes_to_dict(data)
        out_record = {"schema":json_schema,"payload":parsed_dict}
        dev_mode = settings.DEV_MODE
        if dev_mode:
            key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_parse_time']}"
        else:
            key = f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}"
        log.debug("Parsed data with key : %s" % f"{parsed_dict['msg_calc_dvc_no']}-{parsed_dict['msg_calc_dvc_time']}")
        await output_topic.send(key=key, value=out_record, schema=output_schema)
        # Send json to Archive topics
        archivetopicname = f"MACDA-archive-{settings.PARSED_TOPIC_NAME}-{parsed_dict['msg_calc_dvc_no']}"
        archivetopic = app.topic(archivetopicname, partitions=settings.TOPIC_PARTITIONS, value_serializer='json')
        await archivetopic.send(key=key,value=out_record)
