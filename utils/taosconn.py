#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: VACDA
import taosrest
from taosrest import connect, TaosRestConnection, TaosRestCursor

conn: TaosRestConnection = connect(url="http://192.168.32.195:6041",
                                   user="root",
                                   password="taosdata",
                                   timeout=30)
server_version = conn.server_info
print("server_version", server_version)

conn.close()