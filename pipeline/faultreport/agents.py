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


@app.timer(interval=settings.SEND_FAULT_INTERVAL)
async def on_started():
    coachdict = {'1':'Tc1','2':'Mp1','3':'M1','4':'M2','5':'Mp2','6':'Tc2',}
    log.debug("==========********** Get Fault report data batch ==========**********")
    tu = TSutil()
    au = Alertutil()
    dev_mode = settings.DEV_MODE
    if dev_mode:
        predict_data = tu.get_predict_data('dev')
        fault_data = tu.get_fault_data('dev')
    else:
        predict_data = tu.get_predict_data('pro')
        fault_data = tu.get_fault_data('pro')
    # Generata predict data
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
    # Generata fault data
    fault_data_list = []
    if fault_data['len'] > 0:
        # log.debug(au.alertfield)
        # log.debug(fault_data['data'])
        for item in fault_data['data']:
            dvc_no = item['msg_calc_dvc_no']
            dvc_no_list = [i for i in dvc_no.split('0') if i != '']
            if len(dvc_no_list) == 3:
                line_no = dvc_no_list[0]
                train_no = dvc_no_list[1]
                carbin_no = dvc_no_list[2]
                trainNo = f"0{line_no}0{str(train_no).zfill(2)}"
                # log.debug('line_no: %s, train_no: %s, carbin_no: %s' % (line_no, train_no, carbin_no))
                for field in au.alertfield:
                    if item[f"dvc_{field}"] > 0:
                        fdata = {}
                        fdata['message_type'] = '1'
                        fdata['train_type'] = 'B2'
                        fdata['train_no'] = trainNo
                        fdata['line_name'] = str(line_no).replace(" ", "")
                        if "3" in fdata['line_name']:
                            fdata['line_name'] = '3S'
                        if "5" in fdata['line_name']:
                            fdata['line_name'] = '5'
                        fdata['coach'] = coachdict[carbin_no]
                        fdata['location'] = au.getvalue('alertcode', field, 'location')
                        fdata['code'] = au.getvalue('alertcode', field, 'code').replace('HVAC1', f"HVAC{carbin_no}")
                        fdata['station1'] = str(au.getvalue('alertcode', field, 'station1'))
                        fdata['station2'] = str(au.getvalue('alertcode', field, 'station2'))
                        fdata['subsystem'] = str(au.getvalue('alertcode', field, 'subsystem'))
                        fdata['subsystem'] = str(au.getvalue('alertcode', field, 'subsystem'))
                        #fdata['starttime'] = item['time'].strftime("%Y-%m-%d %H:%M:%S")
                        fdata['starttime'] = int(time.mktime(time.strptime(item['time'].strftime("%Y-%m-%d %H:%M:%S"))))
                        #fdata['endtime'] = '0'
                        fdata['endtime'] = 0
                        fault_data_list.append(fdata)
    #log.debug('fault_data_list is : %s' % fault_data_list)
    au.send_predict(fault_data_list)