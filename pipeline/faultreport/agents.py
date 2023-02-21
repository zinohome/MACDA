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


@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    au = Alertutil()
    coachdict = {'1':'Tc1','2':'Mp1','3':'M1','4':'M2','5':'Mp2','6':'Tc2',}
    async for datas in stream.take(3600, within=settings.TSDB_BATCH_TIME):
        log.debug("==========********** Get report data batch ==========**********")
        dev_mode = settings.DEV_MODE
        if dev_mode:
            predict_data = tu.get_predict_data('dev')
            fault_data = tu.get_fault_data('dev')
            statis_data = tu.get_statis_data('dev')
        else:
            predict_data = tu.get_predict_data('pro')
            fault_data = tu.get_fault_data('pro')
            statis_data = tu.get_statis_data('pro')
        # Generata statis data
        statis_data_list = []
        if statis_data['len'] > 0:
            for item in statis_data['data']:
                dvc_no = item['msg_calc_dvc_no']
                dvc_no_list = [i for i in dvc_no.split('0') if i != '']
                line_no = dvc_no_list[0]
                train_no = dvc_no_list[1]
                carbin_no = dvc_no_list[2]
                trainNo = f"0{line_no}0{str(train_no).zfill(2)}"
                partCodepre = f"0{line_no}0{str(int(carbin_no) - 1).zfill(2)}"
                # log.debug('line_no: %s, train_no: %s, carbin_no: %s' % (line_no, train_no, carbin_no))
                for code in au.partcodefield:
                    sdata = {}
                    sdata['lineName'] = str(line_no)
                    sdata['trainType'] = 'B2'
                    sdata['trainNo'] = trainNo
                    sdata['partCode'] = str(au.getvalue('partcode', code, 'part_code')).replace('500', partCodepre)
                    if 'rad' in code or 'fad' in code:
                        sdata['serviceTime'] = 0
                        sdata['serviceValue'] = item[f"dvc_{code}"]
                    else:
                        sdata['serviceTime'] = item[f"dvc_{code}"]
                        sdata['serviceValue'] = 0
                    sdata['mileage'] = 0
                    statis_data_list.append(sdata)
        #log.debug('statis_data_list is : %s' % statis_data_list)
        au.send_statistics(statis_data_list)
        # Generata predict data
        predict_data_list = []
        if predict_data['len'] > 0:
            #log.debug(au.predictfield)
            #log.debug(predict_data['data'])
            for item in predict_data['data']:
                dvc_no = item['msg_calc_dvc_no']
                dvc_no_list = [i for i in dvc_no.split('0') if i != '']
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
                        pdata['coach'] = coachdict[carbin_no]
                        pdata['location'] = au.getvalue('alertcode',field,'location')
                        pdata['code'] = au.getvalue('alertcode',field,'code').replace('HVAC1',f"HVAC{carbin_no}")
                        pdata['station1'] = str(au.getvalue('alertcode',field,'station1'))
                        pdata['station2'] = str(au.getvalue('alertcode',field,'station2'))
                        pdata['subsystem'] = str(au.getvalue('alertcode',field,'subsystem'))
                        pdata['subsystem'] = str(au.getvalue('alertcode',field,'subsystem'))
                        pdata['starttime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        pdata['endtime'] = '0'
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
                        fdata['coach'] = coachdict[carbin_no]
                        fdata['location'] = au.getvalue('alertcode', field, 'location')
                        fdata['code'] = au.getvalue('alertcode', field, 'code').replace('HVAC1', f"HVAC{carbin_no}")
                        fdata['station1'] = str(au.getvalue('alertcode', field, 'station1'))
                        fdata['station2'] = str(au.getvalue('alertcode', field, 'station2'))
                        fdata['subsystem'] = str(au.getvalue('alertcode', field, 'subsystem'))
                        fdata['subsystem'] = str(au.getvalue('alertcode', field, 'subsystem'))
                        fdata['starttime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        fdata['endtime'] = '0'
                        fault_data_list.append(fdata)
        #log.debug('fault_data_list is : %s' % fault_data_list)
        au.send_predict(fault_data_list)
