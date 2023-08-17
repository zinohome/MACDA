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
from core.settings import settings
from pipeline.faultreport.models import input_topic
from utils.alertutil import Alertutil
from utils.log import log as log
from utils.tsutil import TSutil
import time


#@app.timer(interval=settings.SEND_FAULT_INTERVAL)
@app.crontab('0 20 * * *')
async def life_report():
    coachdict = {'1':'Tc1','2':'Mp1','3':'M1','4':'M2','5':'Mp2','6':'Tc2'}
    devicedict = {'EF_U1':'16001',
                  'CF_U1': '16002',
                  'Comp_U11': '16003',
                  'Comp_U12': '16004',
                  'FAD_U1': '16005',
                  'RAD_U1': '16006',
                  'EF_U2': '16007',
                  'CF_U2': '16008',
                  'Comp_U21': '16009',
                  'Comp_U22': '160010',
                  'FAD_U2': '160011',
                  'RAD_U2': '160012',
                  }
    log.debug("==========********** Get Life report data batch ==========**********")
    tu = TSutil()
    au = Alertutil()
    dev_mode = settings.DEV_MODE
    if dev_mode:
        life_data = tu.get_life_data('dev')
    else:
        life_data = tu.get_life_data('pro')
    log.debug("Device Life Data is : %s" % life_data)
    # Generata life data
    predict_data_list = []
    if predict_data['len'] > 0:
        #log.debug(au.predictfield)
        #log.debug(predict_data['data'])
        for item in predict_data['data']:
            dvc_no = item['msg_calc_dvc_no']
            dvc_no_list = [i for i in dvc_no.split('0') if i != '']
            if len(dvc_no_list) == 3:
                line_no = dvc_no_list[0]
                train_no = dvc_no_list[1]
                carbin_no = dvc_no_list[2]
                trainNo = f"0{line_no}0{str(train_no).zfill(2)}"
                #log.debug('line_no: %s, train_no: %s, carbin_no: %s' % (line_no, train_no, carbin_no))
                for field in au.predictfield:
                    if item[field] > 0:
                        pdata = {}
                        pdata['message_type'] = '1'
                        pdata['train_type'] = 'B2'
                        pdata['train_no'] = trainNo
                        pdata['line_name'] = str(line_no).replace(" ", "")
                        if "3" in pdata['line_name']:
                            pdata['line_name'] = '3S'
                        if "5" in pdata['line_name']:
                            pdata['line_name'] = '5'
                        pdata['coach'] = coachdict[carbin_no]
                        pdata['location'] = au.getvalue('alertcode',field,'location')
                        pdata['code'] = au.getvalue('alertcode',field,'code').replace('HVAC1',f"HVAC{carbin_no}")
                        pdata['station1'] = str(au.getvalue('alertcode',field,'station1'))
                        pdata['station2'] = str(au.getvalue('alertcode',field,'station2'))
                        pdata['subsystem'] = str(au.getvalue('alertcode',field,'subsystem'))
                        pdata['subsystem'] = str(au.getvalue('alertcode',field,'subsystem'))
                        #pdata['starttime'] = item['time'].strftime("%Y-%m-%d %H:%M:%S")
                        pdata['starttime'] = int(time.mktime(time.strptime(item['time'].strftime("%Y-%m-%d %H:%M:%S"))))
                        #pdata['endtime'] = '0'
                        pdata['endtime'] = 0
                        predict_data_list.append(pdata)
    #log.debug('predict_data_list is : %s' % predict_data_list)
    au.send_predict(predict_data_list)
