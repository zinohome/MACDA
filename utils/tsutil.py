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
            create_pro_table = "CREATE TABLE IF NOT EXISTS pro_macda (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, dvc_i_inner_temp DOUBLE PRECISION NULL, dvc_i_outer_temp DOUBLE PRECISION NULL, dvc_i_set_temp DOUBLE PRECISION NULL, dvc_i_seat_temp DOUBLE PRECISION NULL, dvc_i_veh_temp DOUBLE PRECISION NULL, dvc_w_passen_load DOUBLE PRECISION NULL, dvc_w_op_mode_u1 DOUBLE PRECISION NULL, dvc_i_fat_u1 DOUBLE PRECISION NULL, dvc_i_rat_u1 DOUBLE PRECISION NULL, dvc_i_sat_u11 DOUBLE PRECISION NULL, dvc_i_sat_u12 DOUBLE PRECISION NULL, dvc_i_dft_u11 DOUBLE PRECISION NULL, dvc_i_dft_u12 DOUBLE PRECISION NULL, dvc_w_freq_u11 DOUBLE PRECISION NULL, dvc_w_crnt_u11 DOUBLE PRECISION NULL, dvc_w_vol_u11 DOUBLE PRECISION NULL, dvc_w_freq_u12 DOUBLE PRECISION NULL, dvc_w_crnt_u12 DOUBLE PRECISION NULL, dvc_w_vol_u12 DOUBLE PRECISION NULL, dvc_i_suck_temp_u11 DOUBLE PRECISION NULL, dvc_i_suck_pres_u11 DOUBLE PRECISION NULL, dvc_i_sup_heat_u11 DOUBLE PRECISION NULL, dvc_i_eev_pos_u11 DOUBLE PRECISION NULL, dvc_i_suck_temp_u12 DOUBLE PRECISION NULL, dvc_i_suck_pres_u12 DOUBLE PRECISION NULL, dvc_i_sup_heat_u12 DOUBLE PRECISION NULL, dvc_i_eev_pos_u12 DOUBLE PRECISION NULL, dvc_w_pos_fad_u1 DOUBLE PRECISION NULL, dvc_w_op_mode_u2 DOUBLE PRECISION NULL, dvc_i_fat_u2 DOUBLE PRECISION NULL, dvc_i_rat_u2 DOUBLE PRECISION NULL, dvc_i_sat_u21 DOUBLE PRECISION NULL, dvc_i_sat_u22 DOUBLE PRECISION NULL, dvc_i_dft_u21 DOUBLE PRECISION NULL, dvc_i_dft_u22 DOUBLE PRECISION NULL, dvc_w_freq_u21 DOUBLE PRECISION NULL, dvc_w_crnt_u21 DOUBLE PRECISION NULL, dvc_w_vol_u21 DOUBLE PRECISION NULL, dvc_w_freq_u22 DOUBLE PRECISION NULL, dvc_w_crnt_u22 DOUBLE PRECISION NULL, dvc_w_vol_u22 DOUBLE PRECISION NULL, dvc_i_suck_temp_u21 DOUBLE PRECISION NULL, dvc_i_suck_pres_u21 DOUBLE PRECISION NULL, dvc_i_sup_heat_u21 DOUBLE PRECISION NULL, dvc_i_eev_pos_u21 DOUBLE PRECISION NULL, dvc_i_suck_temp_u22 DOUBLE PRECISION NULL, dvc_i_suck_pres_u22 DOUBLE PRECISION NULL, dvc_i_sup_heat_u22 DOUBLE PRECISION NULL, dvc_i_eev_pos_u22 DOUBLE PRECISION NULL, dvc_w_pos_fad_u2 DOUBLE PRECISION NULL, dvc_cfbk_comp_u11 DOUBLE PRECISION NULL, dvc_cfbk_comp_u12 DOUBLE PRECISION NULL, dvc_cfbk_comp_u21 DOUBLE PRECISION NULL, dvc_cfbk_comp_u22 DOUBLE PRECISION NULL, dvc_cfbk_ef_u1 DOUBLE PRECISION NULL, dvc_cfbk_ef_u2 DOUBLE PRECISION NULL, dvc_cfbk_cf_u1 DOUBLE PRECISION NULL, dvc_cfbk_cf_u2 DOUBLE PRECISION NULL, dvc_cfbk_pwr DOUBLE PRECISION NULL, dvc_bocflt_ef_u11 DOUBLE PRECISION NULL, dvc_bocflt_ef_u12 DOUBLE PRECISION NULL, dvc_bocflt_cf_u11 DOUBLE PRECISION NULL, dvc_bocflt_cf_u12 DOUBLE PRECISION NULL, dvc_bflt_vfd_u11 DOUBLE PRECISION NULL, dvc_blpflt_comp_u11 DOUBLE PRECISION NULL, dvc_bscflt_comp_u11 DOUBLE PRECISION NULL, dvc_bflt_vfd_u12 DOUBLE PRECISION NULL, dvc_blpflt_comp_u12 DOUBLE PRECISION NULL, dvc_bscflt_comp_u12 DOUBLE PRECISION NULL, dvc_bflt_eev_u11 DOUBLE PRECISION NULL, dvc_bflt_eev_u12 DOUBLE PRECISION NULL, dvc_bflt_fad_u1 DOUBLE PRECISION NULL, dvc_bflt_rad_u1 DOUBLE PRECISION NULL, dvc_bflt_airclean_u1 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u1 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u11 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u12 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u1 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u11 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u12 DOUBLE PRECISION NULL, dvc_bocflt_ef_u21 DOUBLE PRECISION NULL, dvc_bocflt_ef_u22 DOUBLE PRECISION NULL, dvc_bocflt_cf_u21 DOUBLE PRECISION NULL, dvc_bocflt_cf_u22 DOUBLE PRECISION NULL, dvc_bflt_vfd_u21 DOUBLE PRECISION NULL, dvc_blpflt_comp_u21 DOUBLE PRECISION NULL, dvc_bscflt_comp_u21 DOUBLE PRECISION NULL, dvc_bflt_vfd_u22 DOUBLE PRECISION NULL, dvc_blpflt_comp_u22 DOUBLE PRECISION NULL, dvc_bscflt_comp_u22 DOUBLE PRECISION NULL, dvc_bflt_eev_u21 DOUBLE PRECISION NULL, dvc_bflt_eev_u22 DOUBLE PRECISION NULL, dvc_bflt_fad_u2 DOUBLE PRECISION NULL, dvc_bflt_rad_u2 DOUBLE PRECISION NULL, dvc_bflt_airclean_u2 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u2 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u21 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u22 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u2 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u21 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u22 DOUBLE PRECISION NULL, dvc_bflt_vehtemp DOUBLE PRECISION NULL, dvc_bflt_seattemp DOUBLE PRECISION NULL, dvc_bflt_emergivt DOUBLE PRECISION NULL, dvc_bflt_mvbbus DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u22 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u22 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u1 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u2 DOUBLE PRECISION NULL, dvc_blplockflt_u11 DOUBLE PRECISION NULL, dvc_blplockflt_u12 DOUBLE PRECISION NULL, dvc_blplockflt_u21 DOUBLE PRECISION NULL, dvc_blplockflt_u22 DOUBLE PRECISION NULL, dvc_bsclockflt_u11 DOUBLE PRECISION NULL, dvc_bsclockflt_u12 DOUBLE PRECISION NULL, dvc_bsclockflt_u21 DOUBLE PRECISION NULL, dvc_bsclockflt_u22 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u11 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u12 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u21 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u22 DOUBLE PRECISION NULL, dvc_beevlockflt_u11 DOUBLE PRECISION NULL, dvc_beevlockflt_u12 DOUBLE PRECISION NULL, dvc_beevlockflt_u21 DOUBLE PRECISION NULL, dvc_beevlockflt_u22 DOUBLE PRECISION NULL, dvc_cft_code_u1 DOUBLE PRECISION NULL, dvc_cft_code_u2 DOUBLE PRECISION NULL, dvc_dwoptime_emergivt DOUBLE PRECISION NULL, dvc_dwopcount_emergivt DOUBLE PRECISION NULL, dvc_dwoptime_ef_u1 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u1 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u11 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u11 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u1 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u1 DOUBLE PRECISION NULL, dvc_dwoptime_ef_u2 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u2 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u21 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u21 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u2 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u2 DOUBLE PRECISION NULL);"
            create_dev_table = "CREATE TABLE IF NOT EXISTS dev_macda (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, dvc_i_inner_temp DOUBLE PRECISION NULL, dvc_i_outer_temp DOUBLE PRECISION NULL, dvc_i_set_temp DOUBLE PRECISION NULL, dvc_i_seat_temp DOUBLE PRECISION NULL, dvc_i_veh_temp DOUBLE PRECISION NULL, dvc_w_passen_load DOUBLE PRECISION NULL, dvc_w_op_mode_u1 DOUBLE PRECISION NULL, dvc_i_fat_u1 DOUBLE PRECISION NULL, dvc_i_rat_u1 DOUBLE PRECISION NULL, dvc_i_sat_u11 DOUBLE PRECISION NULL, dvc_i_sat_u12 DOUBLE PRECISION NULL, dvc_i_dft_u11 DOUBLE PRECISION NULL, dvc_i_dft_u12 DOUBLE PRECISION NULL, dvc_w_freq_u11 DOUBLE PRECISION NULL, dvc_w_crnt_u11 DOUBLE PRECISION NULL, dvc_w_vol_u11 DOUBLE PRECISION NULL, dvc_w_freq_u12 DOUBLE PRECISION NULL, dvc_w_crnt_u12 DOUBLE PRECISION NULL, dvc_w_vol_u12 DOUBLE PRECISION NULL, dvc_i_suck_temp_u11 DOUBLE PRECISION NULL, dvc_i_suck_pres_u11 DOUBLE PRECISION NULL, dvc_i_sup_heat_u11 DOUBLE PRECISION NULL, dvc_i_eev_pos_u11 DOUBLE PRECISION NULL, dvc_i_suck_temp_u12 DOUBLE PRECISION NULL, dvc_i_suck_pres_u12 DOUBLE PRECISION NULL, dvc_i_sup_heat_u12 DOUBLE PRECISION NULL, dvc_i_eev_pos_u12 DOUBLE PRECISION NULL, dvc_w_pos_fad_u1 DOUBLE PRECISION NULL, dvc_w_op_mode_u2 DOUBLE PRECISION NULL, dvc_i_fat_u2 DOUBLE PRECISION NULL, dvc_i_rat_u2 DOUBLE PRECISION NULL, dvc_i_sat_u21 DOUBLE PRECISION NULL, dvc_i_sat_u22 DOUBLE PRECISION NULL, dvc_i_dft_u21 DOUBLE PRECISION NULL, dvc_i_dft_u22 DOUBLE PRECISION NULL, dvc_w_freq_u21 DOUBLE PRECISION NULL, dvc_w_crnt_u21 DOUBLE PRECISION NULL, dvc_w_vol_u21 DOUBLE PRECISION NULL, dvc_w_freq_u22 DOUBLE PRECISION NULL, dvc_w_crnt_u22 DOUBLE PRECISION NULL, dvc_w_vol_u22 DOUBLE PRECISION NULL, dvc_i_suck_temp_u21 DOUBLE PRECISION NULL, dvc_i_suck_pres_u21 DOUBLE PRECISION NULL, dvc_i_sup_heat_u21 DOUBLE PRECISION NULL, dvc_i_eev_pos_u21 DOUBLE PRECISION NULL, dvc_i_suck_temp_u22 DOUBLE PRECISION NULL, dvc_i_suck_pres_u22 DOUBLE PRECISION NULL, dvc_i_sup_heat_u22 DOUBLE PRECISION NULL, dvc_i_eev_pos_u22 DOUBLE PRECISION NULL, dvc_w_pos_fad_u2 DOUBLE PRECISION NULL, dvc_cfbk_comp_u11 DOUBLE PRECISION NULL, dvc_cfbk_comp_u12 DOUBLE PRECISION NULL, dvc_cfbk_comp_u21 DOUBLE PRECISION NULL, dvc_cfbk_comp_u22 DOUBLE PRECISION NULL, dvc_cfbk_ef_u1 DOUBLE PRECISION NULL, dvc_cfbk_ef_u2 DOUBLE PRECISION NULL, dvc_cfbk_cf_u1 DOUBLE PRECISION NULL, dvc_cfbk_cf_u2 DOUBLE PRECISION NULL, dvc_cfbk_pwr DOUBLE PRECISION NULL, dvc_bocflt_ef_u11 DOUBLE PRECISION NULL, dvc_bocflt_ef_u12 DOUBLE PRECISION NULL, dvc_bocflt_cf_u11 DOUBLE PRECISION NULL, dvc_bocflt_cf_u12 DOUBLE PRECISION NULL, dvc_bflt_vfd_u11 DOUBLE PRECISION NULL, dvc_blpflt_comp_u11 DOUBLE PRECISION NULL, dvc_bscflt_comp_u11 DOUBLE PRECISION NULL, dvc_bflt_vfd_u12 DOUBLE PRECISION NULL, dvc_blpflt_comp_u12 DOUBLE PRECISION NULL, dvc_bscflt_comp_u12 DOUBLE PRECISION NULL, dvc_bflt_eev_u11 DOUBLE PRECISION NULL, dvc_bflt_eev_u12 DOUBLE PRECISION NULL, dvc_bflt_fad_u1 DOUBLE PRECISION NULL, dvc_bflt_rad_u1 DOUBLE PRECISION NULL, dvc_bflt_airclean_u1 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u1 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u11 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u12 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u1 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u11 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u12 DOUBLE PRECISION NULL, dvc_bocflt_ef_u21 DOUBLE PRECISION NULL, dvc_bocflt_ef_u22 DOUBLE PRECISION NULL, dvc_bocflt_cf_u21 DOUBLE PRECISION NULL, dvc_bocflt_cf_u22 DOUBLE PRECISION NULL, dvc_bflt_vfd_u21 DOUBLE PRECISION NULL, dvc_blpflt_comp_u21 DOUBLE PRECISION NULL, dvc_bscflt_comp_u21 DOUBLE PRECISION NULL, dvc_bflt_vfd_u22 DOUBLE PRECISION NULL, dvc_blpflt_comp_u22 DOUBLE PRECISION NULL, dvc_bscflt_comp_u22 DOUBLE PRECISION NULL, dvc_bflt_eev_u21 DOUBLE PRECISION NULL, dvc_bflt_eev_u22 DOUBLE PRECISION NULL, dvc_bflt_fad_u2 DOUBLE PRECISION NULL, dvc_bflt_rad_u2 DOUBLE PRECISION NULL, dvc_bflt_airclean_u2 DOUBLE PRECISION NULL, dvc_bflt_frstemp_u2 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u21 DOUBLE PRECISION NULL, dvc_bflt_splytemp_u22 DOUBLE PRECISION NULL, dvc_bflt_rnttemp_u2 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u21 DOUBLE PRECISION NULL, dvc_bflt_dfstemp_u22 DOUBLE PRECISION NULL, dvc_bflt_vehtemp DOUBLE PRECISION NULL, dvc_bflt_seattemp DOUBLE PRECISION NULL, dvc_bflt_emergivt DOUBLE PRECISION NULL, dvc_bflt_mvbbus DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_vfd_u22 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u11 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u12 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u21 DOUBLE PRECISION NULL, dvc_bcomuflt_eev_u22 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u1 DOUBLE PRECISION NULL, dvc_bmcbflt_pwr_u2 DOUBLE PRECISION NULL, dvc_blplockflt_u11 DOUBLE PRECISION NULL, dvc_blplockflt_u12 DOUBLE PRECISION NULL, dvc_blplockflt_u21 DOUBLE PRECISION NULL, dvc_blplockflt_u22 DOUBLE PRECISION NULL, dvc_bsclockflt_u11 DOUBLE PRECISION NULL, dvc_bsclockflt_u12 DOUBLE PRECISION NULL, dvc_bsclockflt_u21 DOUBLE PRECISION NULL, dvc_bsclockflt_u22 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u11 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u12 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u21 DOUBLE PRECISION NULL, dvc_bvfdlockflt_u22 DOUBLE PRECISION NULL, dvc_beevlockflt_u11 DOUBLE PRECISION NULL, dvc_beevlockflt_u12 DOUBLE PRECISION NULL, dvc_beevlockflt_u21 DOUBLE PRECISION NULL, dvc_beevlockflt_u22 DOUBLE PRECISION NULL, dvc_cft_code_u1 DOUBLE PRECISION NULL, dvc_cft_code_u2 DOUBLE PRECISION NULL, dvc_dwoptime_emergivt DOUBLE PRECISION NULL, dvc_dwopcount_emergivt DOUBLE PRECISION NULL, dvc_dwoptime_ef_u1 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u1 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u11 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u1 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u11 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u12 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u1 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u1 DOUBLE PRECISION NULL, dvc_dwoptime_ef_u2 DOUBLE PRECISION NULL, dvc_dwoptime_cf_u2 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u21 DOUBLE PRECISION NULL, dvc_dwoptime_comp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_ef_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cf_u2 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u21 DOUBLE PRECISION NULL, dvc_dwopcount_cp_u22 DOUBLE PRECISION NULL, dvc_dwopcount_fad_u2 DOUBLE PRECISION NULL, dvc_dwopcount_rad_u2 DOUBLE PRECISION NULL);"
            create_pro_json_table = "CREATE TABLE IF NOT EXISTS pro_macda_json (msg_calc_dvc_time TIMESTAMPTZ NOT NULL, msg_calc_parse_time TEXT NOT NULL, msg_calc_dvc_no TEXT NOT NULL, Indicators JSON);"
            create_dev_json_table = "CREATE TABLE IF NOT EXISTS dev_macda_json (msg_calc_dvc_time TEXT NOT NULL, msg_calc_parse_time TIMESTAMPTZ NOT NULL, msg_calc_dvc_no TEXT NOT NULL, Indicators JSON);"
            cur = conn.cursor()
            cur.execute(create_pro_table)
            cur.execute(create_dev_table)
            cur.execute(create_pro_json_table)
            cur.execute(create_dev_json_table)
            cur.execute("SELECT create_hypertable('pro_macda', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('pro_macda_json', 'msg_calc_dvc_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
            cur.execute("SELECT create_hypertable('dev_macda_json', 'msg_calc_parse_time', chunk_time_interval => 86400000000, if_not_exists => TRUE);")
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
        keystr = 'msg_calc_dvc_time, msg_calc_parse_time, msg_calc_dvc_no, indicators'
        maskstr = '%s, %s, %s, %s'
        insertsql = f"INSERT INTO {tablename} ({keystr}) VALUES ({maskstr})"
        valuelst.append(str(jsonobj['msg_calc_dvc_time']))
        valuelst.append(str(jsonobj['msg_calc_parse_time']))
        valuelst.append(str(jsonobj['msg_calc_dvc_no']))
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
        cols = ['msg_calc_dvc_no', 'msg_calc_dvc_time', 'msg_calc_parse_time', 'indicators']
        records = []
        for jsonobj in jsonobjlst:
            record = []
            record.append(jsonobj['payload']['msg_calc_dvc_no'])
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
            log.error('Exception at tsutil.batchinsert() %s ' % exp)
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
