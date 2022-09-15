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
import traceback
import weakref

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

from utils.log import log as log

class Cached(type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__cache = weakref.WeakValueDictionary()

    def __call__(self, *args):
        if args in self.__cache:
            return self.__cache[args]
        else:
            obj = super().__call__(*args)
            self.__cache[args] = obj
            return obj

class TSWriter(metaclass=Cached):
    def __init__(self, ts_name):
        self.ts_name = ts_name
        self.session = None
        self.ts_srv_host = "192.168.32.195"
        self.ts_srv_port = "6667"
        self.ts_srv_username = "root"
        self.ts_srv_password = "root"
        self.connect()

    def connect(self):
        self.session = Session(self.ts_srv_host, self.ts_srv_port, self.ts_srv_username, self.ts_srv_password)

    def create_aligned_record(self,sampledict):
        try:
            self.session.open(False)
            tsexist = self.session.check_time_series_exists(self.ts_name)
            # measurements_lst
            measurements_lst = list(sampledict.keys())
            measurements_lst.remove('msg_calc_dvc_time')
            measurements_lst.remove('msg_crc')
            measurements_lst.remove('msg_calc_parse_time')
            # data_type_lst
            data_type_lst = []
            # recordvalue
            recordvalue = []
            for i in range(len(measurements_lst)):
                data_type_lst.append(TSDataType.BOOLEAN)
                recordvalue.append('0')
            for i in range(len(measurements_lst)):
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
                recordvalue[i] = sampledict[measurements_lst[i]]
                if data_type_lst[i] == TSDataType.INT32:
                    recordvalue[i] = int(recordvalue[i])
                if data_type_lst[i] == TSDataType.FLOAT:
                    recordvalue[i] = float(recordvalue[i])
                if data_type_lst[i] == TSDataType.TEXT:
                    recordvalue[i] = str(recordvalue[i])
                if data_type_lst[i] == TSDataType.BOOLEAN:
                    recordvalue[i] = bool(recordvalue[i])
            encoding_lst = [TSEncoding.PLAIN for _ in range(len(data_type_lst))]
            compressor_lst = [Compressor.SNAPPY for _ in range(len(data_type_lst))]
            if not tsexist:
                self.session.create_aligned_time_series(
                    self.ts_name, measurements_lst, data_type_lst, encoding_lst, compressor_lst
                )
            timestamp = int(time.mktime(time.strptime(sampledict['msg_calc_parse_time'], "%Y-%m-%d %H:%M:%S")))*1000
            self.session.insert_aligned_record(device_id=self.ts_name, timestamp=timestamp, measurements=measurements_lst,
                                          data_types=data_type_lst, values=recordvalue)
            self.session.close()
        except Exception as exp:
            log.error('Exception at DSConfig.readconfig() %s ' % exp)
            traceback.print_exc()
        finally:
            self.session.close()

if __name__ == '__main__':
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
                  'dvc_cfbk_ef_u2': 0, 'dvc_cfbk_cf_u1': 0, 'dvc_cfbk_cf_u2': 0, 'dvc_cfbk_pwr': 0,
                  'dvc_bocflt_ef_u11': 0,
                  'dvc_bocflt_ef_u12': 0, 'dvc_bocflt_cf_u11': 0, 'dvc_bocflt_cf_u12': 0, 'dvc_bflt_vfd_u11': 0,
                  'dvc_blpflt_comp_u11': 0, 'dvc_bscflt_comp_u11': 1, 'dvc_bflt_vfd_u12': 0, 'dvc_blpflt_comp_u12': 0,
                  'dvc_bscflt_comp_u12': 0, 'dvc_bflt_eev_u11': 1, 'dvc_bflt_eev_u12': 0, 'dvc_bflt_fad_u1': 0,
                  'dvc_bflt_rad_u1': 0, 'dvc_bflt_airclean_u1': 0, 'dvc_bflt_frstemp_u1': 0, 'dvc_bflt_splytemp_u11': 0,
                  'dvc_bflt_splytemp_u12': 0, 'dvc_bflt_rnttemp_u1': 0, 'dvc_bflt_dfstemp_u11': 0,
                  'dvc_bflt_dfstemp_u12': 0,
                  'dvc_bocflt_ef_u21': 0, 'dvc_bocflt_ef_u22': 0, 'dvc_bocflt_cf_u21': 0, 'dvc_bocflt_cf_u22': 0,
                  'dvc_bflt_vfd_u21': 0, 'dvc_blpflt_comp_u21': 0, 'dvc_bscflt_comp_u21': 0, 'dvc_bflt_vfd_u22': 0,
                  'dvc_blpflt_comp_u22': 0, 'dvc_bscflt_comp_u22': 0, 'dvc_bflt_eev_u21': 0, 'dvc_bflt_eev_u22': 0,
                  'dvc_bflt_fad_u2': 0, 'dvc_bflt_rad_u2': 0, 'dvc_bflt_airclean_u2': 0, 'dvc_bflt_frstemp_u2': 0,
                  'dvc_bflt_splytemp_u21': 0, 'dvc_bflt_splytemp_u22': 0, 'dvc_bflt_rnttemp_u2': 0,
                  'dvc_bflt_dfstemp_u21': 0,
                  'dvc_bflt_dfstemp_u22': 0, 'dvc_bflt_vehtemp': 0, 'dvc_bflt_seattemp': 0, 'dvc_bflt_emergivt': 0,
                  'dvc_bflt_mvbbus': 0, 'dvc_bcomuflt_vfd_u11': 0, 'dvc_bcomuflt_vfd_u12': 1, 'dvc_bcomuflt_vfd_u21': 0,
                  'dvc_bcomuflt_vfd_u22': 0, 'dvc_bcomuflt_eev_u11': 0, 'dvc_bcomuflt_eev_u12': 0,
                  'dvc_bcomuflt_eev_u21': 0,
                  'dvc_bcomuflt_eev_u22': 0, 'dvc_bmcbflt_pwr_u1': 0, 'dvc_bmcbflt_pwr_u2': 0, 'dvc_blplockflt_u11': 0,
                  'dvc_blplockflt_u12': 0, 'dvc_blplockflt_u21': 0, 'dvc_blplockflt_u22': 0, 'dvc_bsclockflt_u11': 0,
                  'dvc_bsclockflt_u12': 0, 'dvc_bsclockflt_u21': 0, 'dvc_bsclockflt_u22': 0, 'dvc_bvfdlockflt_u11': 0,
                  'dvc_bvfdlockflt_u12': 0, 'dvc_bvfdlockflt_u21': 0, 'dvc_bvfdlockflt_u22': 0,
                  'dvc_beevlockflt_u11': 0,
                  'dvc_beevlockflt_u12': 0, 'dvc_beevlockflt_u21': 0, 'dvc_beevlockflt_u22': 0, 'dvc_cft_code_u1': 0,
                  'dvc_cft_code_u2': 0, 'dvc_dwoptime_emergivt': 305419896, 'dvc_dwopcount_emergivt': 2,
                  'dvc_dwoptime_ef_u1': 3622, 'dvc_dwoptime_cf_u1': 3124, 'dvc_dwoptime_comp_u11': 2553,
                  'dvc_dwoptime_comp_u12': 886, 'dvc_dwopcount_ef_u1': 7, 'dvc_dwopcount_cf_u1': 6,
                  'dvc_dwopcount_cp_u11': 5,
                  'dvc_dwopcount_cp_u12': 4, 'dvc_dwopcount_fad_u1': 22, 'dvc_dwopcount_rad_u1': 21,
                  'dvc_dwoptime_ef_u2': 3637,
                  'dvc_dwoptime_cf_u2': 3108, 'dvc_dwoptime_comp_u21': 884, 'dvc_dwoptime_comp_u22': 2539,
                  'dvc_dwopcount_ef_u2': 8, 'dvc_dwopcount_cf_u2': 6, 'dvc_dwopcount_cp_u21': 4,
                  'dvc_dwopcount_cp_u22': 5,
                  'dvc_dwopcount_fad_u2': 24, 'dvc_dwopcount_rad_u2': 23, 'msg_crc': 54882, 'msg_calc_dvc_no': '5-99-2',
                  'msg_calc_dvc_time': '2021-5-6 10:17:21', 'msg_calc_parse_time': '2022-09-15 12:21:47'}
    tswriter = TSWriter(f"root.macda.dvc_{sampledict['msg_calc_dvc_no'].replace('-','_')}")
    tswriter.create_aligned_record(sampledict)