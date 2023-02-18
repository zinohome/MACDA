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
from pipeline.predict.models import input_topic
from utils.log import log as log
from utils.sensorpolyfit import SensorPolyfit
from utils.tsutil import TSutil


@app.agent(input_topic)
async def store_signal(stream):
    tu = TSutil()
    sp = SensorPolyfit()
    async for data in stream:
        log.debug("---------------- Get predict data ----------------")
        dev_mode = settings.DEV_MODE
        mode = 'dev'
        if not dev_mode:
            mode = 'pro'
        predictdata = tu.get_predictdata(mode, data['payload']['msg_calc_dvc_no'])
        if predictdata['len'] > 0:
            # ref leak predict
            ref_leak_u11 = sp.polyfit(predictdata['data']['dvc_w_op_mode_u1'], predictdata['data']['dvc_i_fat_u1'], predictdata['data']['dvc_w_freq_u11'], predictdata['data']['dvc_i_suck_pres_u11'])
            ref_leak_u12 = sp.polyfit(predictdata['data']['dvc_w_op_mode_u1'], predictdata['data']['dvc_i_fat_u1'], predictdata['data']['dvc_w_freq_u12'], predictdata['data']['dvc_i_suck_pres_u12'])
            ref_leak_u21 = sp.polyfit(predictdata['data']['dvc_w_op_mode_u2'], predictdata['data']['dvc_i_fat_u2'], predictdata['data']['dvc_w_freq_u21'], predictdata['data']['dvc_i_suck_pres_u21'])
            ref_leak_u22 = sp.polyfit(predictdata['data']['dvc_w_op_mode_u2'], predictdata['data']['dvc_i_fat_u2'], predictdata['data']['dvc_w_freq_u22'], predictdata['data']['dvc_i_suck_pres_u22'])
            # ref pump predict
            ref_pump_u1 = 0
            ref_pump_u2 = 0
            if predictdata['data']['w_frequ1_sub'] == 0 and predictdata['data']['w_crntu1_sub'] > 2:
                ref_pump_u1 = 1
            if predictdata['data']['w_frequ2_sub'] == 0 and predictdata['data']['w_crntu2_sub'] > 2:
                ref_pump_u2 = 1
            # sensor predict
            fat_sensor = 0
            rat_sensor = 0
            if predictdata['data']['fat_sub'] > 4:
                fat_sensor =1
            if predictdata['data']['rat_sub'] > 4:
                rat_sensor =1
            if ref_leak_u11 + ref_leak_u12 + ref_leak_u21 + ref_leak_u22 + ref_pump_u1 + ref_pump_u2 + fat_sensor + rat_sensor > 0:
                # write to db
                if dev_mode:
                    key = f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_parse_time']}"
                else:
                    key = f"{data['payload']['msg_calc_dvc_no']}-{data['payload']['msg_calc_dvc_time']}"
                log.debug("Add predict data  with key : %s" % key)
                predictjson = {}
                predictjson['msg_calc_dvc_time'] = data['payload']['msg_calc_dvc_time']
                predictjson['msg_calc_parse_time'] = data['payload']['msg_calc_parse_time']
                predictjson['msg_calc_dvc_no'] = data['payload']['msg_calc_dvc_no']
                predictjson['msg_calc_train_no'] = data['payload']['msg_calc_train_no']
                predictjson['ref_leak_u11'] = ref_leak_u11
                predictjson['ref_leak_u12'] = ref_leak_u12
                predictjson['ref_leak_u21'] = ref_leak_u21
                predictjson['ref_leak_u22'] = ref_leak_u22
                predictjson['ref_pump_u1'] = ref_pump_u1
                predictjson['ref_pump_u2'] = ref_pump_u2
                predictjson['fat_sensor'] = fat_sensor
                predictjson['rat_sensor'] = rat_sensor
                if dev_mode:
                    tu.insert_predictdata('dev_predict', predictjson)
                else:
                    tu.insert_predictdata('pro_predict', predictjson)

