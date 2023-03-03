#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: VACDA

import os
import sys

from core.settings import settings
from loguru import logger as log

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_PATH = os.path.join(LOG_DIR, settings.APP_LOG_FILENAME)
'''
log.add(LOG_PATH,
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            rotation="100 MB",
            retention="14 days",
            level=settings.APP_LOG_LEVEL,
            enqueue=True)
'''

log.add(sys.stdout,
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            colorize=True,
            level=settings.APP_LOG_LEVEL,
            enqueue=True)

if __name__ == '__main__':
    a = "0500102"
    dvc_no_list = [i for i in a.split('0') if i != '']
    if len(dvc_no_list) == 3:
        log.debug(dvc_no_list)
    a = "0000002"
    dvc_no_list = [i for i in a.split('0') if i != '']
    if len(dvc_no_list) == 3:
        log.debug(dvc_no_list)

    '''
    log.success(settings.KAFKA_BOOTSTRAP_SERVER)
    log.success(settings.DEBUG)
    log.success('[测试log] hello, world')
    log.info('[测试log] hello, world')
    log.debug('[测试log] hello, world')
    log.warning('[测试log] hello, world')
    log.error('[测试log] hello, world')
    log.critical('[测试log] hello, world')
    #log.exception('[测试log] hello, world')
    '''
    hexstr = '2c 01 00 fffd 46 28 1b 5a 00 74 00 05 00 0a 00 00 00 62'
    log.debug(hexstr)
    b = bytes.fromhex(hexstr)
    log.debug(b)