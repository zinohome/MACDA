#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import traceback
import weakref
import psycopg2
from datetime import datetime
from pgcopy import CopyManager
from psycopg2 import pool
from core.settings import settings
from utils.log import log as log
import simplejson as json


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
class TSutil(metaclass=Cached):
    def __init__(self):
        log.debug('Connect to timescaledb uri [ %s ]' % settings.TSDB_URL)
        self.conn_pool = psycopg2.pool.SimpleConnectionPool(1, settings.TSDB_POOL_SIZE, settings.TSDB_URL)
        if (self.conn_pool):
            log.debug("Connection pool created successfully")
        try:
            log.debug("Check tsdb table ...")
            conn = self.conn_pool.getconn()
            create_pro_table = "CREATE TABLE IF NOT EXISTS pro_macda (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, dvc_i_inner_temp DOUBLE PRECISION NULL, dvc_i_outer_temp DOUBLE PRECISION NULL, dvc_i_set_temp DOUBLE PRECISION NULL, dvc_i_seat_temp DOUBLE PRECISION NULL, dvc_i_veh_temp DOUBLE PRECISION NULL, dvc_w_passen_load DOUBLE PRECISION NULL, dvc_w_op_mode_u1 DOUBLE PRECISION NULL, dvc_i_fat_u1 DOUBLE PRECISION NULL, dvc_i_rat_u1 DOUBLE PRECISION NULL, dvc_i_sat_u11 DOUBLE PRECISION NULL, dvc_i_sat_u12 DOUBLE PRECISION NULL, dvc_i_dft_u11 DOUBLE PRECISION NULL, dvc_i_dft_u12 DOUBLE PRECISION NULL, dvc_w_freq_u11 DOUBLE PRECISION NULL, dvc_w_crnt_u11 DOUBLE PRECISION NULL, dvc_w_vol_u11 DOUBLE PRECISION NULL, dvc_w_freq_u12 DOUBLE PRECISION NULL, dvc_w_crnt_u12 DOUBLE PRECISION NULL, dvc_w_vol_u12 DOUBLE PRECISION NULL, dvc_i_suck_temp_u11 DOUBLE PRECISION NULL, dvc_i_suck_pres_u11 DOUBLE PRECISION NULL, dvc_i_sup_heat_u11 DOUBLE PRECISION NULL, dvc_i_eev_pos_u11 DOUBLE PRECISION NULL, dvc_i_suck_temp_u12 DOUBLE PRECISION NULL, dvc_i_suck_pres_u12 DOUBLE PRECISION NULL, dvc_i_sup_heat_u12 DOUBLE PRECISION NULL, dvc_i_eev_pos_u12 DOUBLE PRECISION NULL, dvc_w_pos_fad_u1 DOUBLE PRECISION NULL, dvc_w_op_mode_u2 DOUBLE PRECISION NULL, dvc_i_fat_u2 DOUBLE PRECISION NULL, dvc_i_rat_u2 DOUBLE PRECISION NULL, dvc_i_sat_u21 DOUBLE PRECISION NULL, dvc_i_sat_u22 DOUBLE PRECISION NULL, dvc_i_dft_u21 DOUBLE PRECISION NULL, dvc_i_dft_u22 DOUBLE PRECISION NULL, dvc_w_freq_u21 DOUBLE PRECISION NULL, dvc_w_crnt_u21 DOUBLE PRECISION NULL, dvc_w_vol_u21 DOUBLE PRECISION NULL, dvc_w_freq_u22 DOUBLE PRECISION NULL, dvc_w_crnt_u22 DOUBLE PRECISION NULL, dvc_w_vol_u22 DOUBLE PRECISION NULL, dvc_i_suck_temp_u21 DOUBLE PRECISION NULL, dvc_i_suck_pres_u21 DOUBLE PRECISION NULL, dvc_i_sup_heat_u21 DOUBLE PRECISION NULL, dvc_i_eev_pos_u21 DOUBLE PRECISION NULL, dvc_i_suck_temp_u22 DOUBLE PRECISION NULL, dvc_i_suck_pres_u22 DOUBLE PRECISION NULL, dvc_i_sup_heat_u22 DOUBLE PRECISION NULL, dvc_i_eev_pos_u22 DOUBLE PRECISION NULL, dvc_w_pos_fad_u2 DOUBLE PRECISION NULL, dvc_cfbk_comp_u11 DOUBLE PRECISION NULL, dvc_cfbk_comp_u12 DOUBLE PRECISION NULL, dvc_cfbk_comp_u21 DOUBLE PRECISION NULL, dvc_cfbk_comp_u22 DOUBLE PRECISION NULL, dvc_cfbk_ef_u1 DOUBLE PRECISION NULL, dvc_cfbk_ef_u2 DOUBLE PRECISION NULL, dvc_cfbk_cf_u1 DOUBLE PRECISION NULL, dvc_cfbk_cf_u2 DOUBLE PRECISION NULL, dvc_cfbk_pwr DOUBLE PRECISION NULL, dvc_bocflt_ef_u11 DOUBLE PRECISION NULL, dvc_bocflt_ef_u12 DOUBLE PRECISION NULL, dvc_bocflt_cf_u11 DOUBLE PRECISION NULL, dvc_bocflt_cf_u12 DOUBLE PRECISION NULL, dvc_bflt_vfd_u11 DOUBLE PRECISION NULL, dvc_blpflt_comp_u11 DOUBLE PRECISION NULL, dvc_bscflt_comp_u11 DOUBLE PRECISION NULL, dvc_bflt_vfd_u12 DOUBLE PRECISION NULL, dvc_blpflt_comp_u12 DOUBLE PRECISION NULL, dvc_bscflt_comp_u12 DOUBLE PRECISION NULL, dvc_bflt_eev_u11 DOUBLE PRECISION NULL, dvc_bflt_eev_u12 DOUBLE PRECISION NULL, dvc_bflt_fad_u1 DOUBLE PRECISION NULL, dvc_bflt_rad_u1 DOUBLE PRECISION NULL, dvc_bflt_airclean_u1 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u1 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u11 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u12 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u1 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u11 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u12 DOUBLE PRECISION NULL, dvc_bocflt_ef_u21 DOUBLE PRECISION NULL, dvc_bocflt_ef_u22 DOUBLE PRECISION NULL, dvc_bocflt_cf_u21 DOUBLE PRECISION NULL, dvc_bocflt_cf_u22 DOUBLE PRECISION NULL, dvc_bflt_vfd_u21 DOUBLE PRECISION NULL, dvc_blpflt_comp_u21 DOUBLE PRECISION NULL, dvc_bscflt_comp_u21 DOUBLE PRECISION NULL, dvc_bflt_vfd_u22 DOUBLE PRECISION NULL, dvc_blpflt_comp_u22 DOUBLE PRECISION NULL, dvc_bscflt_comp_u22 DOUBLE PRECISION NULL, dvc_bflt_eev_u21 DOUBLE PRECISION NULL, dvc_bflt_eev_u22 DOUBLE PRECISION NULL, dvc_bflt_fad_u2 DOUBLE PRECISION NULL, dvc_bflt_rad_u2 DOUBLE PRECISION NULL, dvc_bflt_airclean_u2 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u2 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u21 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u22 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u2 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u21 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u22 DOUBLE PRECISION NULL, dvc_bflt_vehtemp DOUBLE PRECISION NULL, dvc_bflt_seattemp DOUBLE PRECISION NULL, dvc_bflt_emergivt DOUBLE PRECISION NULL, dvc_bflt_mvbbus DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u22 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u22 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u1 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u2 DOUBLE PRECISION NULL, dvc_blplockflt_u11 DOUBLE PRECISION NULL, dvc_blplockflt_u12 DOUBLE PRECISION NULL, dvc_blplockflt_u21 DOUBLE PRECISION NULL, dvc_blplockflt_u22 DOUBLE PRECISION NULL, dvc_bsclockflt_u11 DOUBLE PRECISION NULL, dvc_bsclockflt_u12 DOUBLE PRECISION NULL, dvc_bsclockflt_u21 DOUBLE PRECISION NULL, dvc_bsclockflt_u22 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u11 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u12 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u21 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u22 DOUBLE PRECISION NULL, dvc_beevlockflt_u11 DOUBLE PRECISION NULL, dvc_beevlockflt_u12 DOUBLE PRECISION NULL, dvc_beevlockflt_u21 DOUBLE PRECISION NULL, dvc_beevlockflt_u22 DOUBLE PRECISION NULL, dvc_cft_code_u1 DOUBLE PRECISION NULL, dvc_cft_code_u2 DOUBLE PRECISION NULL, dvc_dwoptime_emergivt DOUBLE PRECISION NULL, dvc_dwopcount_emergivt DOUBLE PRECISION NULL, dvc_dwoptime_ef_u1 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u1 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u11 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u11 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u1 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u1 DOUBLE PRECISION NULL, dvc_dwoptime_ef_u2 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u2 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u21 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u21 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u2 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u2 DOUBLE PRECISION NULL);"
            create_dev_table = "CREATE TABLE IF NOT EXISTS dev_macda (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, dvc_i_inner_temp DOUBLE PRECISION NULL, dvc_i_outer_temp DOUBLE PRECISION NULL, dvc_i_set_temp DOUBLE PRECISION NULL, dvc_i_seat_temp DOUBLE PRECISION NULL, dvc_i_veh_temp DOUBLE PRECISION NULL, dvc_w_passen_load DOUBLE PRECISION NULL, dvc_w_op_mode_u1 DOUBLE PRECISION NULL, dvc_i_fat_u1 DOUBLE PRECISION NULL, dvc_i_rat_u1 DOUBLE PRECISION NULL, dvc_i_sat_u11 DOUBLE PRECISION NULL, dvc_i_sat_u12 DOUBLE PRECISION NULL, dvc_i_dft_u11 DOUBLE PRECISION NULL, dvc_i_dft_u12 DOUBLE PRECISION NULL, dvc_w_freq_u11 DOUBLE PRECISION NULL, dvc_w_crnt_u11 DOUBLE PRECISION NULL, dvc_w_vol_u11 DOUBLE PRECISION NULL, dvc_w_freq_u12 DOUBLE PRECISION NULL, dvc_w_crnt_u12 DOUBLE PRECISION NULL, dvc_w_vol_u12 DOUBLE PRECISION NULL, dvc_i_suck_temp_u11 DOUBLE PRECISION NULL, dvc_i_suck_pres_u11 DOUBLE PRECISION NULL, dvc_i_sup_heat_u11 DOUBLE PRECISION NULL, dvc_i_eev_pos_u11 DOUBLE PRECISION NULL, dvc_i_suck_temp_u12 DOUBLE PRECISION NULL, dvc_i_suck_pres_u12 DOUBLE PRECISION NULL, dvc_i_sup_heat_u12 DOUBLE PRECISION NULL, dvc_i_eev_pos_u12 DOUBLE PRECISION NULL, dvc_w_pos_fad_u1 DOUBLE PRECISION NULL, dvc_w_op_mode_u2 DOUBLE PRECISION NULL, dvc_i_fat_u2 DOUBLE PRECISION NULL, dvc_i_rat_u2 DOUBLE PRECISION NULL, dvc_i_sat_u21 DOUBLE PRECISION NULL, dvc_i_sat_u22 DOUBLE PRECISION NULL, dvc_i_dft_u21 DOUBLE PRECISION NULL, dvc_i_dft_u22 DOUBLE PRECISION NULL, dvc_w_freq_u21 DOUBLE PRECISION NULL, dvc_w_crnt_u21 DOUBLE PRECISION NULL, dvc_w_vol_u21 DOUBLE PRECISION NULL, dvc_w_freq_u22 DOUBLE PRECISION NULL, dvc_w_crnt_u22 DOUBLE PRECISION NULL, dvc_w_vol_u22 DOUBLE PRECISION NULL, dvc_i_suck_temp_u21 DOUBLE PRECISION NULL, dvc_i_suck_pres_u21 DOUBLE PRECISION NULL, dvc_i_sup_heat_u21 DOUBLE PRECISION NULL, dvc_i_eev_pos_u21 DOUBLE PRECISION NULL, dvc_i_suck_temp_u22 DOUBLE PRECISION NULL, dvc_i_suck_pres_u22 DOUBLE PRECISION NULL, dvc_i_sup_heat_u22 DOUBLE PRECISION NULL, dvc_i_eev_pos_u22 DOUBLE PRECISION NULL, dvc_w_pos_fad_u2 DOUBLE PRECISION NULL, dvc_cfbk_comp_u11 DOUBLE PRECISION NULL, dvc_cfbk_comp_u12 DOUBLE PRECISION NULL, dvc_cfbk_comp_u21 DOUBLE PRECISION NULL, dvc_cfbk_comp_u22 DOUBLE PRECISION NULL, dvc_cfbk_ef_u1 DOUBLE PRECISION NULL, dvc_cfbk_ef_u2 DOUBLE PRECISION NULL, dvc_cfbk_cf_u1 DOUBLE PRECISION NULL, dvc_cfbk_cf_u2 DOUBLE PRECISION NULL, dvc_cfbk_pwr DOUBLE PRECISION NULL, dvc_bocflt_ef_u11 DOUBLE PRECISION NULL, dvc_bocflt_ef_u12 DOUBLE PRECISION NULL, dvc_bocflt_cf_u11 DOUBLE PRECISION NULL, dvc_bocflt_cf_u12 DOUBLE PRECISION NULL, dvc_bflt_vfd_u11 DOUBLE PRECISION NULL, dvc_blpflt_comp_u11 DOUBLE PRECISION NULL, dvc_bscflt_comp_u11 DOUBLE PRECISION NULL, dvc_bflt_vfd_u12 DOUBLE PRECISION NULL, dvc_blpflt_comp_u12 DOUBLE PRECISION NULL, dvc_bscflt_comp_u12 DOUBLE PRECISION NULL, dvc_bflt_eev_u11 DOUBLE PRECISION NULL, dvc_bflt_eev_u12 DOUBLE PRECISION NULL, dvc_bflt_fad_u1 DOUBLE PRECISION NULL, dvc_bflt_rad_u1 DOUBLE PRECISION NULL, dvc_bflt_airclean_u1 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u1 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u11 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u12 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u1 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u11 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u12 DOUBLE PRECISION NULL, dvc_bocflt_ef_u21 DOUBLE PRECISION NULL, dvc_bocflt_ef_u22 DOUBLE PRECISION NULL, dvc_bocflt_cf_u21 DOUBLE PRECISION NULL, dvc_bocflt_cf_u22 DOUBLE PRECISION NULL, dvc_bflt_vfd_u21 DOUBLE PRECISION NULL, dvc_blpflt_comp_u21 DOUBLE PRECISION NULL, dvc_bscflt_comp_u21 DOUBLE PRECISION NULL, dvc_bflt_vfd_u22 DOUBLE PRECISION NULL, dvc_blpflt_comp_u22 DOUBLE PRECISION NULL, dvc_bscflt_comp_u22 DOUBLE PRECISION NULL, dvc_bflt_eev_u21 DOUBLE PRECISION NULL, dvc_bflt_eev_u22 DOUBLE PRECISION NULL, dvc_bflt_fad_u2 DOUBLE PRECISION NULL, dvc_bflt_rad_u2 DOUBLE PRECISION NULL, dvc_bflt_airclean_u2 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u2 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u21 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u22 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u2 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u21 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u22 DOUBLE PRECISION NULL, dvc_bflt_vehtemp DOUBLE PRECISION NULL, dvc_bflt_seattemp DOUBLE PRECISION NULL, dvc_bflt_emergivt DOUBLE PRECISION NULL, dvc_bflt_mvbbus DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u22 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u22 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u1 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u2 DOUBLE PRECISION NULL, dvc_blplockflt_u11 DOUBLE PRECISION NULL, dvc_blplockflt_u12 DOUBLE PRECISION NULL, dvc_blplockflt_u21 DOUBLE PRECISION NULL, dvc_blplockflt_u22 DOUBLE PRECISION NULL, dvc_bsclockflt_u11 DOUBLE PRECISION NULL, dvc_bsclockflt_u12 DOUBLE PRECISION NULL, dvc_bsclockflt_u21 DOUBLE PRECISION NULL, dvc_bsclockflt_u22 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u11 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u12 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u21 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u22 DOUBLE PRECISION NULL, dvc_beevlockflt_u11 DOUBLE PRECISION NULL, dvc_beevlockflt_u12 DOUBLE PRECISION NULL, dvc_beevlockflt_u21 DOUBLE PRECISION NULL, dvc_beevlockflt_u22 DOUBLE PRECISION NULL, dvc_cft_code_u1 DOUBLE PRECISION NULL, dvc_cft_code_u2 DOUBLE PRECISION NULL, dvc_dwoptime_emergivt DOUBLE PRECISION NULL, dvc_dwopcount_emergivt DOUBLE PRECISION NULL, dvc_dwoptime_ef_u1 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u1 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u11 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u11 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u1 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u1 DOUBLE PRECISION NULL, dvc_dwoptime_ef_u2 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u2 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u21 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u21 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u2 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u2 DOUBLE PRECISION NULL);"
            create_pro_json_table = "CREATE TABLE IF NOT EXISTS pro_macda_json (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, Indicators JSON);"
            create_dev_json_table = "CREATE TABLE IF NOT EXISTS dev_macda_json (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, Indicators JSON);"
            create_pro_predict_table = "CREATE TABLE IF NOT EXISTS pro_predict (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, ref_leak_u11 integer NOT NULL DEFAULT 0, ref_leak_u12 integer NOT NULL DEFAULT 0, ref_leak_u21 integer NOT NULL DEFAULT 0, ref_leak_u22 integer NOT NULL DEFAULT 0, ref_pump_u1 integer NOT NULL DEFAULT 0, ref_pump_u2 integer NOT NULL DEFAULT 0, fat_sensor integer NOT NULL DEFAULT 0, rat_sensor integer NOT NULL DEFAULT 0);"
            create_dev_predict_table = "CREATE TABLE IF NOT EXISTS dev_predict (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, msg_calc_train_no TEXT NOT NULL, ref_leak_u11 integer NOT NULL DEFAULT 0, ref_leak_u12 integer NOT NULL DEFAULT 0, ref_leak_u21 integer NOT NULL DEFAULT 0, ref_leak_u22 integer NOT NULL DEFAULT 0, ref_pump_u1 integer NOT NULL DEFAULT 0, ref_pump_u2 integer NOT NULL DEFAULT 0, fat_sensor integer NOT NULL DEFAULT 0, rat_sensor integer NOT NULL DEFAULT 0);"
            cur = conn.cursor()
            cur.execute(create_pro_table)
            cur.execute(create_dev_table)
            cur.execute(create_pro_json_table)
            cur.execute(create_dev_json_table)
            cur.execute(create_pro_predict_table)
            cur.execute(create_dev_predict_table)
            cur.execute("SELECT create_hypertable('pro_macda', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_macda_json', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda_json', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_predict', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_predict', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT remove_retention_policy('pro_macda', True);")
            cur.execute("SELECT add_retention_policy('pro_macda', INTERVAL '1 year');")
            cur.execute("SELECT remove_retention_policy('dev_macda', True);")
            cur.execute("SELECT add_retention_policy('dev_macda', INTERVAL '1 year');")
            cur.execute("SELECT remove_retention_policy('pro_macda_json', True);")
            cur.execute("SELECT add_retention_policy('pro_macda_json', INTERVAL '1 year');")
            cur.execute("SELECT remove_retention_policy('dev_macda_json', True);")
            cur.execute("SELECT add_retention_policy('dev_macda_json', INTERVAL '1 year');")
            cur.execute("SELECT remove_retention_policy('pro_predict', True);")
            cur.execute("SELECT add_retention_policy('pro_predict', INTERVAL '1 year');")
            cur.execute("SELECT remove_retention_policy('dev_predict', True);")
            cur.execute("SELECT add_retention_policy('dev_predict', INTERVAL '1 year');")
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
            log.debug("Check tsdb table ... Success !")
        except Exception as exp:
            log.error('Exception at tsutil.__init__() %s ' % exp)
            traceback.print_exc()

    def insert(self, tablename, jsonobj):
        keylst = []
        valuelst = []
        masklst = []
        ignorekeys = ['msg_header_code01', 'msg_header_code02', 'msg_length', 'msg_src_dvc_no', 'msg_host_dvc_no',
                      'msg_type', 'msg_frame_no', 'msg_line_no', 'msg_train_type', 'msg_train_no', 'msg_carriage_no',
                      'msg_protocal_version', 'msg_crc']
        for (key, value) in jsonobj.items():
            if not key in ignorekeys:
                keylst.append(key)
                valuelst.append(str(value))
                masklst.append('%s')
        keystr = ','.join(keylst)
        maskstr = ','.join(masklst)
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        # log.debug(insertsql)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            cur.execute(insertsql, valuelst)
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.insert() %s ' % exp)
            traceback.print_exc()

    def insertjson(self, tablename, jsonobj):
        valuelst = []
        keystr = 'msg_calc_dvc_time, msg_calc_parse_time, msg_calc_dvc_no, msg_calc_train_no, indicators'
        maskstr = '%s, %s, %s, %s'
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        valuelst.append(str(jsonobj['msg_calc_dvc_time']))
        valuelst.append(str(jsonobj['msg_calc_parse_time']))
        valuelst.append(str(jsonobj['msg_calc_dvc_no']))
        valuelst.append(str(jsonobj['msg_calc_train_no']))
        valuelst.append(json.dumps(jsonobj))
        # log.debug(insertsql)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            cur.execute(insertsql, valuelst)
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.insertjson() %s ' % exp)
            traceback.print_exc()

    def batchinsert(self, tablename, timefieldname, jsonobjlst):
        ignorekeys = ['msg_header_code01', 'msg_header_code02', 'msg_length', 'msg_src_dvc_no', 'msg_host_dvc_no',
                      'msg_type', 'msg_frame_no', 'msg_line_no', 'msg_train_type', 'msg_train_no', 'msg_carriage_no',
                      'msg_protocal_version', 'msg_crc']
        jsonobj = jsonobjlst[0]['payload']
        cols = []
        for (key, value) in jsonobj.items():
            if not key in ignorekeys:
                cols.append(key)
        #log.debug(cols)
        records = []
        for jsonobj in jsonobjlst:
            record = []
            for (key, value) in jsonobj['payload'].items():
                if not key in ignorekeys:
                    if key == timefieldname:
                        record.append(self.parse_time(value))
                    else:
                        record.append(value)
            records.append(record)
        #log.debug(records)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            mgr = CopyManager(conn, tablename, cols)
            mgr.copy(records)
            conn.commit()
            log.info("Batch Commited")
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.batchinsert() %s ' % exp)
            traceback.print_exc()

    def batchinsertjson(self, tablename, timefieldname, jsonobjlst):
        jsonobj = jsonobjlst[0]['payload']
        cols = ['msg_calc_dvc_no', 'msg_calc_train_no', 'msg_calc_dvc_time', 'msg_calc_parse_time', 'indicators']
        records = []
        for jsonobj in jsonobjlst:
            record = []
            record.append(jsonobj['payload']['msg_calc_dvc_no'])
            record.append(jsonobj['payload']['msg_calc_train_no'])
            if timefieldname == 'msg_calc_dvc_time':
                record.append(self.parse_time(jsonobj['payload']['msg_calc_dvc_time']))
                record.append(jsonobj['payload']['msg_calc_parse_time'])
            else:
                record.append(jsonobj['payload']['msg_calc_dvc_time'])
                record.append(self.parse_time(jsonobj['payload']['msg_calc_parse_time']))
            record.append(json.dumps(jsonobj['payload']))
            records.append(record)
        #log.debug(records)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            mgr = CopyManager(conn, tablename, cols)
            mgr.copy(records)
            conn.commit()
            log.info("Batch Commited")
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.batchinsertjson() %s ' % exp)
            traceback.print_exc()

    def get_refdata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(dvc_i_fat_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(dvc_w_freq_u11)) as dvc_w_freq_u11, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(dvc_w_freq_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u12)) as dvc_i_suck_pres_u12, approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(dvc_i_fat_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(dvc_w_freq_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u21)) as dvc_i_suck_pres_u21, approx_percentile(0.95, percentile_agg(dvc_w_freq_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u22)) as dvc_i_suck_pres_u22 " \
                   f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(dvc_i_fat_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(dvc_w_freq_u11)) as dvc_w_freq_u11, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(dvc_w_freq_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u12)) as dvc_i_suck_pres_u12, approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(dvc_i_fat_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(dvc_w_freq_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u21)) as dvc_i_suck_pres_u21, approx_percentile(0.95, percentile_agg(dvc_w_freq_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u22)) as dvc_i_suck_pres_u22 " \
                   f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_refdata() %s ' % exp)
            traceback.print_exc()

    def get_pumpdata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, avg(ABS(dvc_w_freq_u11 - dvc_w_freq_u12)) as w_frequ1_sub, avg(ABS(dvc_w_crnt_u11 - dvc_w_crnt_u12)) as w_crntu1_sub, avg(ABS(dvc_w_freq_u21 - dvc_w_freq_u22)) as w_frequ2_sub, avg(ABS(dvc_w_crnt_u21 - dvc_w_crnt_u22)) as w_crntu2_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, avg(ABS(dvc_w_freq_u11 - dvc_w_freq_u12)) as w_frequ1_sub, avg(ABS(dvc_w_crnt_u11 - dvc_w_crnt_u12)) as w_crntu1_sub, avg(ABS(dvc_w_freq_u21 - dvc_w_freq_u22)) as w_frequ2_sub, avg(ABS(dvc_w_crnt_u21 - dvc_w_crnt_u22)) as w_crntu2_sub " \
                       f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_pumpdata() %s ' % exp)
            traceback.print_exc()

    def get_sensordata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, avg(ABS(dvc_i_fat_u1 - dvc_i_fat_u2)) as fat_sub, avg(ABS(dvc_i_rat_u1 - dvc_i_rat_u2)) as rat_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, avg(ABS(dvc_i_fat_u1 - dvc_i_fat_u2)) as fat_sub, avg(ABS(dvc_i_rat_u1 - dvc_i_rat_u2)) as rat_sub " \
                       f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_sensordata() %s ' % exp)
            traceback.print_exc()

    def get_predictdata(self, mode, dvc_no):
        querysql = ''
        if mode == 'dev':
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(dvc_i_fat_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(dvc_w_freq_u11)) as dvc_w_freq_u11, " \
                       f"approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(dvc_w_freq_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u12)) as dvc_i_suck_pres_u12, " \
                       f"approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(dvc_i_fat_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(dvc_w_freq_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u21)) as dvc_i_suck_pres_u21, " \
                       f"approx_percentile(0.95, percentile_agg(dvc_w_freq_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u22)) as dvc_i_suck_pres_u22, avg(ABS(dvc_w_freq_u11 - dvc_w_freq_u12)) as w_frequ1_sub, avg(ABS(dvc_w_crnt_u11 - dvc_w_crnt_u12)) as w_crntu1_sub, " \
                       f"avg(ABS(dvc_w_freq_u21 - dvc_w_freq_u22)) as w_frequ2_sub, avg(ABS(dvc_w_crnt_u21 - dvc_w_crnt_u22)) as w_crntu2_sub, avg(ABS(dvc_i_fat_u1 - dvc_i_fat_u2)) as fat_sub, avg(ABS(dvc_i_rat_u1 - dvc_i_rat_u2)) as rat_sub " \
                       f"FROM public.dev_macda where msg_calc_parse_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        else:
            querysql = f"select msg_calc_dvc_no, approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u1)) as dvc_w_op_mode_u1, approx_percentile(0.95, percentile_agg(dvc_i_fat_u1)) as dvc_i_fat_u1, approx_percentile(0.95, percentile_agg(dvc_w_freq_u11)) as dvc_w_freq_u11, " \
                       f"approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u11)) as dvc_i_suck_pres_u11, approx_percentile(0.95, percentile_agg(dvc_w_freq_u12)) as dvc_w_freq_u12, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u12)) as dvc_i_suck_pres_u12, " \
                       f"approx_percentile(0.95, percentile_agg(dvc_w_op_mode_u2)) as dvc_w_op_mode_u2, approx_percentile(0.95, percentile_agg(dvc_i_fat_u2)) as dvc_i_fat_u2, approx_percentile(0.95, percentile_agg(dvc_w_freq_u21)) as dvc_w_freq_u21, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u21)) as dvc_i_suck_pres_u21, " \
                       f"approx_percentile(0.95, percentile_agg(dvc_w_freq_u22)) as dvc_w_freq_u22, approx_percentile(0.95, percentile_agg(dvc_i_suck_pres_u22)) as dvc_i_suck_pres_u22, avg(ABS(dvc_w_freq_u11 - dvc_w_freq_u12)) as w_frequ1_sub, avg(ABS(dvc_w_crnt_u11 - dvc_w_crnt_u12)) as w_crntu1_sub, " \
                       f"avg(ABS(dvc_w_freq_u21 - dvc_w_freq_u22)) as w_frequ2_sub, avg(ABS(dvc_w_crnt_u21 - dvc_w_crnt_u22)) as w_crntu2_sub, avg(ABS(dvc_i_fat_u1 - dvc_i_fat_u2)) as fat_sub, avg(ABS(dvc_i_rat_u1 - dvc_i_rat_u2)) as rat_sub " \
                       f"FROM public.pro_macda where msg_calc_dvc_time > now() - INTERVAL '2 minutes' and msg_calc_dvc_no = '{dvc_no}' group by msg_calc_dvc_no"
        try:
            returndata = {}
            conn = self.conn_pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(querysql)
            result = cur.fetchall()
            rlen = len(result)
            returndata['len'] = rlen
            if rlen >= 1:
                returndata['data'] = result[0]
            else:
                returndata['data'] = None
            cur.close()
            self.conn_pool.putconn(conn)
            return returndata
        except Exception as exp:
            log.error('Exception at tsutil.get_predictdata() %s ' % exp)
            traceback.print_exc()

    def insert_predictdata(self, tablename, jsonobj):
        keylst = []
        valuelst = []
        masklst = []
        for (key, value) in jsonobj.items():
            keylst.append(key)
            valuelst.append(str(value))
            masklst.append('%s')
        keystr = ','.join(keylst)
        maskstr = ','.join(masklst)
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        # log.debug(insertsql)
        try:
            conn = self.conn_pool.getconn()
            cur = conn.cursor()
            cur.execute(insertsql, valuelst)
            conn.commit()
            cur.close()
            self.conn_pool.putconn(conn)
        except Exception as exp:
            log.error('Exception at tsutil.insert_predictdata() %s ' % exp)
            traceback.print_exc()

    def __del__(self):
        if self.conn_pool:
            self.conn_pool.closeall
        log.debug("PostgreSQL connection pool is closed")

    def parse_time(self, txt):
        date_s,time_s = txt.split(' ')
        year_s, mon_s, day_s = date_s.split('-')
        hour_s, minute_s, second_s = time_s.split(':')
        return datetime(int(year_s), int(mon_s), int(day_s), int(hour_s), int(minute_s), int(second_s))

if __name__ == '__main__':
    tu = TSutil()
    jobj = {"schema":"s1","playload":"p1"}
    #tu.insert('dev_macda', jobj)
    '''
    result = tu.get_refdata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['msg_calc_dvc_no'])
    result = tu.get_pumpdata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['w_frequ1_sub'])
    result = tu.get_sensordata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['rat_sub'])
    '''
    result = tu.get_predictdata('dev', '5-98-2')
    if result['len'] > 0:
        log.debug(result['data']['rat_sub'])
        log.debug(result)
    #tu.get_refdata('pro', '5-98-1')
