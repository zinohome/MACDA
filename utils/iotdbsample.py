#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA

from iotdb.Session import Session
from utils.log import log as log

ts_srv_host = "192.168.32.195"
ts_srv_port = "6667"
ts_srv_username = "root"
ts_srv_password = "root"
session = Session(ts_srv_host, ts_srv_port, ts_srv_username, ts_srv_password)
session.open(False)
zone = session.get_time_zone()
log.debug(zone)
session.close()
