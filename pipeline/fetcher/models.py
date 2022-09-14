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

class RawUser(faust.Record):
    id: dict
    gender: str
    name: dict
    location: dict
    email: str
    login: dict
    dob: dict
    registered: dict
    phone: str
    cell: str
    picture: dict
    nat: str

output_schema = faust.Schema(
    key_type=str,
    value_type=RawUser,
    key_serializer="json",
    value_serializer="json",
)

output_topic = app.topic("fetcher-topic", schema=output_schema)
