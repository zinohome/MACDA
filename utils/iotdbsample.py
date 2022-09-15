#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import time
import datetime
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

from utils.log import log as log

sampledict = {'msg_header_code01': 44, 'msg_header_code02': 1, 'msg_length': 242,
              'msg_src_dvc_no': 70, 'msg_host_dvc_no': 40, 'msg_type': 7002, 'msg_frame_no': 116,
              'msg_line_no': 5, 'msg_train_type': 10, 'msg_train_no': 99, 'msg_carriage_no': 2,
              'msg_protocal_version': 1, 'dvc_i_inner_temp': 12.3, 'dvc_i_outer_temp': -12,
              'dvc_i_set_temp': 0, 'dvc_i_seat_temp': -100, 'dvc_i_veh_temp': -100, 'dvc_w_passen_load': 0,
              'dvc_w_op_mode_u1': 0, 'dvc_i_fat_u1': 22, 'dvc_i_rat_u1': 22, 'dvc_i_sat_u11': 22,
              'dvc_i_sat_u12': 22, 'dvc_i_dft_u11': 22, 'dvc_i_dft_u12': 22, 'dvc_w_freq_u11': 2.34,
              'dvc_w_crnt_u11': 18.6, 'dvc_w_vol_u11': 345.6, 'dvc_w_freq_u12': 0, 'dvc_w_crnt_u12': 0,
              'dvc_w_vol_u12': 0, 'dvc_i_suck_temp_u11': 12.3, 'dvc_i_suck_pres_u11': -0.2,
              'dvc_i_sup_heat_u11': 34.5, 'dvc_i_eev_pos_u11': 45.6, 'dvc_i_suck_temp_u12': 0,
              'dvc_i_suck_pres_u12': 0, 'dvc_i_sup_heat_u12': 0, 'dvc_i_eev_pos_u12': 0,
              'dvc_w_pos_fad_u1': 0, 'dvc_w_op_mode_u2': 0, 'dvc_i_fat_u2': 22, 'dvc_i_rat_u2': 22,
              'dvc_i_sat_u21': 22, 'dvc_i_sat_u22': 22, 'dvc_i_dft_u21': 22, 'dvc_i_dft_u22': 22,
              'dvc_w_freq_u21': 0, 'dvc_w_crnt_u21': 0, 'dvc_w_vol_u21': 0, 'dvc_w_freq_u22': 0,
              'dvc_w_crnt_u22': 0, 'dvc_w_vol_u22': 0, 'dvc_i_suck_temp_u21': 0, 'dvc_i_suck_pres_u21': 0,
              'dvc_i_sup_heat_u21': 0, 'dvc_i_eev_pos_u21': 0, 'dvc_i_suck_temp_u22': 0, 'dvc_i_suck_pres_u22': 0,
              'dvc_i_sup_heat_u22': 0, 'dvc_i_eev_pos_u22': 0, 'dvc_w_pos_fad_u2': 0, 'dvc_cfbk_comp_u11': 1,
              'dvc_cfbk_comp_u12': 0, 'dvc_cfbk_comp_u21': 0, 'dvc_cfbk_comp_u22': 0, 'dvc_cfbk_ef_u1': 0,
              'dvc_cfbk_ef_u2': 0, 'dvc_cfbk_cf_u1': 0, 'dvc_cfbk_cf_u2': 0, 'dvc_cfbk_pwr': 0, 'dvc_bocflt_ef_u11': 0,
              'dvc_bocflt_ef_u12': 0, 'dvc_bocflt_cf_u11': 0, 'dvc_bocflt_cf_u12': 0, 'dvc_bflt_vfd_u11': 0,
              'dvc_blpflt_comp_u11': 0, 'dvc_bscflt_comp_u11': 1, 'dvc_bflt_vfd_u12': 0, 'dvc_blpflt_comp_u12': 0,
              'dvc_bscflt_comp_u12': 0, 'dvc_bflt_eev_u11': 1, 'dvc_bflt_eev_u12': 0, 'dvc_bflt_fad_u1': 0,
              'dvc_bflt_rad_u1': 0, 'dvc_bflt_airclean_u1': 0, 'dvc_bflt_frstemp_u1': 0, 'dvc_bflt_splytemp_u11': 0,
              'dvc_bflt_splytemp_u12': 0, 'dvc_bflt_rnttemp_u1': 0, 'dvc_bflt_dfstemp_u11': 0, 'dvc_bflt_dfstemp_u12': 0,
              'dvc_bocflt_ef_u21': 0, 'dvc_bocflt_ef_u22': 0, 'dvc_bocflt_cf_u21': 0, 'dvc_bocflt_cf_u22': 0,
              'dvc_bflt_vfd_u21': 0, 'dvc_blpflt_comp_u21': 0, 'dvc_bscflt_comp_u21': 0, 'dvc_bflt_vfd_u22': 0,
              'dvc_blpflt_comp_u22': 0, 'dvc_bscflt_comp_u22': 0, 'dvc_bflt_eev_u21': 0, 'dvc_bflt_eev_u22': 0,
              'dvc_bflt_fad_u2': 0, 'dvc_bflt_rad_u2': 0, 'dvc_bflt_airclean_u2': 0, 'dvc_bflt_frstemp_u2': 0,
              'dvc_bflt_splytemp_u21': 0, 'dvc_bflt_splytemp_u22': 0, 'dvc_bflt_rnttemp_u2': 0, 'dvc_bflt_dfstemp_u21': 0,
              'dvc_bflt_dfstemp_u22': 0, 'dvc_bflt_vehtemp': 0, 'dvc_bflt_seattemp': 0, 'dvc_bflt_emergivt': 0,
              'dvc_bflt_mvbbus': 0, 'dvc_bcomuflt_vfd_u11': 0, 'dvc_bcomuflt_vfd_u12': 1, 'dvc_bcomuflt_vfd_u21': 0,
              'dvc_bcomuflt_vfd_u22': 0, 'dvc_bcomuflt_eev_u11': 0, 'dvc_bcomuflt_eev_u12': 0, 'dvc_bcomuflt_eev_u21': 0,
              'dvc_bcomuflt_eev_u22': 0, 'dvc_bmcbflt_pwr_u1': 0, 'dvc_bmcbflt_pwr_u2': 0, 'dvc_blplockflt_u11': 0,
              'dvc_blplockflt_u12': 0, 'dvc_blplockflt_u21': 0, 'dvc_blplockflt_u22': 0, 'dvc_bsclockflt_u11': 0,
              'dvc_bsclockflt_u12': 0, 'dvc_bsclockflt_u21': 0, 'dvc_bsclockflt_u22': 0, 'dvc_bvfdlockflt_u11': 0,
              'dvc_bvfdlockflt_u12': 0, 'dvc_bvfdlockflt_u21': 0, 'dvc_bvfdlockflt_u22': 0, 'dvc_beevlockflt_u11': 0,
              'dvc_beevlockflt_u12': 0, 'dvc_beevlockflt_u21': 0, 'dvc_beevlockflt_u22': 0, 'dvc_cft_code_u1': 0,
              'dvc_cft_code_u2': 0, 'dvc_dwoptime_emergivt': 305419896, 'dvc_dwopcount_emergivt': 2,
              'dvc_dwoptime_ef_u1': 3622, 'dvc_dwoptime_cf_u1': 3124, 'dvc_dwoptime_comp_u11': 2553,
              'dvc_dwoptime_comp_u12': 886, 'dvc_dwopcount_ef_u1': 7, 'dvc_dwopcount_cf_u1': 6, 'dvc_dwopcount_cp_u11': 5,
              'dvc_dwopcount_cp_u12': 4, 'dvc_dwopcount_fad_u1': 22, 'dvc_dwopcount_rad_u1': 21, 'dvc_dwoptime_ef_u2': 3637,
              'dvc_dwoptime_cf_u2': 3108, 'dvc_dwoptime_comp_u21': 884, 'dvc_dwoptime_comp_u22': 2539,
              'dvc_dwopcount_ef_u2': 8, 'dvc_dwopcount_cf_u2': 6, 'dvc_dwopcount_cp_u21': 4, 'dvc_dwopcount_cp_u22': 5,
              'dvc_dwopcount_fad_u2': 24, 'dvc_dwopcount_rad_u2': 23, 'msg_crc': 54882, 'msg_calc_dvc_no': '5-99-2',
              'msg_calc_dvc_time': '2021-5-6 10:17:21', 'msg_calc_parse_time': '2022-09-15 12:21:47'}
ts_srv_host = "192.168.32.195"
ts_srv_port = "6667"
ts_srv_username = "root"
ts_srv_password = "root"
session = Session(ts_srv_host, ts_srv_port, ts_srv_username, ts_srv_password)
session.open(False)
zone = session.get_time_zone()
tsname = f"root.macda.dvc_{sampledict['msg_calc_dvc_no'].replace('-','_')}"
tsexist = session.check_time_series_exists(tsname)
recordvalue = []
if not tsexist:
    # measurements_lst
    measurements_lst = list(sampledict.keys())
    measurements_lst.remove('msg_calc_dvc_time')
    measurements_lst.remove('msg_crc')
    measurements_lst.remove('msg_calc_parse_time')
    # data_type_lst
    data_type_lst = []
    for i in range(len(measurements_lst)):
        data_type_lst.append(TSDataType.BOOLEAN)
        recordvalue.append('0')
    for i in range(len(measurements_lst)):
        recordvalue[i] = sampledict[measurements_lst[i]]
        if measurements_lst[i].startswith('msg_'):
            data_type_lst[i] = TSDataType.TEXT
        if measurements_lst[i].startswith('dvc_cft'):
            data_type_lst[i] = TSDataType.TEXT
        if measurements_lst[i].startswith('dvc_dw'):
            data_type_lst[i] = TSDataType.FLOAT
        if measurements_lst[i].startswith('dvc_i_'):
            data_type_lst[i] = TSDataType.FLOAT
        if measurements_lst[i].startswith('dvc_w_'):
            data_type_lst[i] = TSDataType.FLOAT
        if measurements_lst[i] == 'msg_length':
            data_type_lst[i] = TSDataType.INT32
        if measurements_lst[i] == 'dvc_w_passen_load':
            data_type_lst[i] = TSDataType.INT32
        if measurements_lst[i] == 'dvc_w_op_mode_u1':
            data_type_lst[i] = TSDataType.INT32
        if measurements_lst[i] == 'dvc_w_op_mode_u2':
            data_type_lst[i] = TSDataType.INT32
    encoding_lst = [TSEncoding.PLAIN for _ in range(len(data_type_lst))]
    compressor_lst = [Compressor.SNAPPY for _ in range(len(data_type_lst))]
    session.create_aligned_time_series(
        tsname, measurements_lst, data_type_lst, encoding_lst, compressor_lst
    )
    log.debug(recordvalue)
    timestamp = time.mktime(datetime.datetime.strptime(sampledict['msg_calc_parse_time'], "%Y-%m-%d %H:%M:%S").timetuple())
    log.debug(timestamp)
    session.insert_aligned_record(device_id=tsname,
                                  timestamp=time.mktime(datetime.datetime.strptime(sampledict['msg_calc_parse_time'], "%Y-%m-%d %H:%M:%S").timetuple()),
                                  measurements=measurements_lst,
                                  data_types=data_type_lst,
                                  values=recordvalue)
session.close()
