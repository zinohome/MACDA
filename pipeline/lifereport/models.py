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
from app import app
from core.settings import settings

input_topic = app.topic(settings.PARSED_TOPIC_NAME, partitions=settings.TOPIC_PARTITIONS, value_serializer='json')