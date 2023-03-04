#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import faust
from app import app
from core.settings import settings

input_topic = app.topic(settings.SOURCE_TOPIC_NAME, value_serializer='raw')

class ACSignal(faust.Record):
    msg_header_code01: int
    msg_header_code02: int
    msg_length: int
    msg_src_dvc_no: int
    msg_host_dvc_no: int
    msg_type: int
    msg_frame_no: int
    msg_line_no: int
    msg_train_type: int
    msg_train_no: int
    msg_carriage_no: int
    msg_protocal_version: int
    dvc_i_inner_temp: int
    dvc_i_outer_temp: int
    dvc_i_set_temp: int
    dvc_i_seat_temp: int
    dvc_i_veh_temp: int
    dvc_w_passen_load: int
    dvc_w_op_mode_u1: int
    dvc_i_fat_u1: int
    dvc_i_rat_u1: int
    dvc_i_sat_u11: int
    dvc_i_sat_u12: int
    dvc_i_dft_u11: int
    dvc_i_dft_u12: int
    dvc_w_freq_u11: int
    dvc_w_crnt_u11: int
    dvc_w_vol_u11: int
    dvc_w_freq_u12: int
    dvc_w_crnt_u12: int
    dvc_w_vol_u12: int
    dvc_i_suck_temp_u11: int
    dvc_i_suck_pres_u11: int
    dvc_i_sup_heat_u11: int
    dvc_i_eev_pos_u11: int
    dvc_i_suck_temp_u12: int
    dvc_i_suck_pres_u12: int
    dvc_i_sup_heat_u12: int
    dvc_i_eev_pos_u12: int
    dvc_w_pos_fad_u1: int
    dvc_w_op_mode_u2: int
    dvc_i_fat_u2: int
    dvc_i_rat_u2: int
    dvc_i_sat_u21: int
    dvc_i_sat_u22: int
    dvc_i_dft_u21: int
    dvc_i_dft_u22: int
    dvc_w_freq_u21: int
    dvc_w_crnt_u21: int
    dvc_w_vol_u21: int
    dvc_w_freq_u22: int
    dvc_w_crnt_u22: int
    dvc_w_vol_u22: int
    dvc_i_suck_temp_u21: int
    dvc_i_suck_pres_u21: int
    dvc_i_sup_heat_u21: int
    dvc_i_eev_pos_u21: int
    dvc_i_suck_temp_u22: int
    dvc_i_suck_pres_u22: int
    dvc_i_sup_heat_u22: int
    dvc_i_eev_pos_u22: int
    dvc_w_pos_fad_u2: int
    dvc_cfbk_comp_u11: int
    dvc_cfbk_comp_u12: int
    dvc_cfbk_comp_u21: int
    dvc_cfbk_comp_u22: int
    dvc_cfbk_ef_u1: int
    dvc_cfbk_ef_u2: int
    dvc_cfbk_cf_u1: int
    dvc_cfbk_cf_u2: int
    dvc_cfbk_pwr: int
    dvc_bocflt_ef_u11: int
    dvc_bocflt_ef_u12: int
    dvc_bocflt_cf_u11: int
    dvc_bocflt_cf_u12: int
    dvc_bflt_vfd_u11: int
    dvc_blpflt_comp_u11: int
    dvc_bscflt_comp_u11: int
    dvc_bflt_vfd_u12: int
    dvc_blpflt_comp_u12: int
    dvc_bscflt_comp_u12: int
    dvc_bflt_eev_u11: int
    dvc_bflt_eev_u12: int
    dvc_bflt_fad_u1: int
    dvc_bflt_rad_u1: int
    dvc_bflt_airclean_u1: int
    dvc_bflt_expboard_u1: int
    dvc_bflt_frstemp_u1: int
    dvc_bflt_splytemp_u11: int
    dvc_bflt_splytemp_u12: int
    dvc_bflt_rnttemp_u1: int
    dvc_bflt_dfstemp_u11: int
    dvc_bflt_dfstemp_u12: int
    dvc_bocflt_ef_u21: int
    dvc_bocflt_ef_u22: int
    dvc_bocflt_cf_u21: int
    dvc_bocflt_cf_u22: int
    dvc_bflt_vfd_u21: int
    dvc_blpflt_comp_u21: int
    dvc_bscflt_comp_u21: int
    dvc_bflt_vfd_u22: int
    dvc_blpflt_comp_u22: int
    dvc_bscflt_comp_u22: int
    dvc_bflt_eev_u21: int
    dvc_bflt_eev_u22: int
    dvc_bflt_fad_u2: int
    dvc_bflt_rad_u2: int
    dvc_bflt_airclean_u2: int
    dvc_bflt_expboard_u2: int
    dvc_bflt_frstemp_u2: int
    dvc_bflt_splytemp_u21: int
    dvc_bflt_splytemp_u22: int
    dvc_bflt_rnttemp_u2: int
    dvc_bflt_dfstemp_u21: int
    dvc_bflt_dfstemp_u22: int
    dvc_bflt_vehtemp: int
    dvc_bflt_seattemp: int
    dvc_bflt_emergivt: int
    dvc_bflt_mvbbus: int
    dvc_bcomuflt_vfd_u11: int
    dvc_bcomuflt_vfd_u12: int
    dvc_bcomuflt_vfd_u21: int
    dvc_bcomuflt_vfd_u22: int
    dvc_bcomuflt_eev_u11: int
    dvc_bcomuflt_eev_u12: int
    dvc_bcomuflt_eev_u21: int
    dvc_bcomuflt_eev_u22: int
    dvc_bmcbflt_pwr_u1: int
    dvc_bmcbflt_pwr_u2: int
    dvc_blplockflt_u11: int
    dvc_blplockflt_u12: int
    dvc_blplockflt_u21: int
    dvc_blplockflt_u22: int
    dvc_bsclockflt_u11: int
    dvc_bsclockflt_u12: int
    dvc_bsclockflt_u21: int
    dvc_bsclockflt_u22: int
    dvc_bvfdlockflt_u11: int
    dvc_bvfdlockflt_u12: int
    dvc_bvfdlockflt_u21: int
    dvc_bvfdlockflt_u22: int
    dvc_beevlockflt_u11: int
    dvc_beevlockflt_u12: int
    dvc_beevlockflt_u21: int
    dvc_beevlockflt_u22: int
    dvc_cft_code_u1: int
    dvc_cft_code_u2: int
    dvc_dwoptime_emergivt: int
    dvc_dwopcount_emergivt: int
    dvc_dwoptime_ef_u1: int
    dvc_dwoptime_cf_u1: int
    dvc_dwoptime_comp_u11: int
    dvc_dwoptime_comp_u12: int
    dvc_dwopcount_ef_u1: int
    dvc_dwopcount_cf_u1: int
    dvc_dwopcount_cp_u11: int
    dvc_dwopcount_cp_u12: int
    dvc_dwopcount_fad_u1: int
    dvc_dwopcount_rad_u1: int
    dvc_dwoptime_ef_u2: int
    dvc_dwoptime_cf_u2: int
    dvc_dwoptime_comp_u21: int
    dvc_dwoptime_comp_u22: int
    dvc_dwopcount_ef_u2: int
    dvc_dwopcount_cf_u2: int
    dvc_dwopcount_cp_u21: int
    dvc_dwopcount_cp_u22: int
    dvc_dwopcount_fad_u2: int
    dvc_dwopcount_rad_u2: int
    msg_crc: int
    msg_src_dvc_time: str
    msg_src_parse_time: str

json_schema = {
        "type": "struct",
        "name": "ACSignal",
        "fields": [
            {"name": "msg_header_code01", "type": "int"},
            {"name": "msg_header_code02", "type": "int"},
            {"name": "msg_length", "type": "int"},
            {"name": "msg_src_dvc_no", "type": "int"},
            {"name": "msg_host_dvc_no", "type": "int"},
            {"name": "msg_type", "type": "int"},
            {"name": "msg_frame_no", "type": "int"},
            {"name": "msg_line_no", "type": "int"},
            {"name": "msg_train_type", "type": "int"},
            {"name": "msg_train_no", "type": "int"},
            {"name": "msg_carriage_no", "type": "int"},
            {"name": "msg_protocal_version", "type": "int"},
            {"name": "dvc_i_inner_temp", "type": "int"},
            {"name": "dvc_i_outer_temp", "type": "int"},
            {"name": "dvc_i_set_temp", "type": "int"},
            {"name": "dvc_i_seat_temp", "type": "int"},
            {"name": "dvc_i_veh_temp", "type": "int"},
            {"name": "dvc_w_passen_load", "type": "int"},
            {"name": "dvc_w_op_mode_u1", "type": "int"},
            {"name": "dvc_i_fat_u1", "type": "int"},
            {"name": "dvc_i_rat_u1", "type": "int"},
            {"name": "dvc_i_sat_u11", "type": "int"},
            {"name": "dvc_i_sat_u12", "type": "int"},
            {"name": "dvc_i_dft_u11", "type": "int"},
            {"name": "dvc_i_dft_u12", "type": "int"},
            {"name": "dvc_w_freq_u11", "type": "int"},
            {"name": "dvc_w_crnt_u11", "type": "int"},
            {"name": "dvc_w_vol_u11", "type": "int"},
            {"name": "dvc_w_freq_u12", "type": "int"},
            {"name": "dvc_w_crnt_u12", "type": "int"},
            {"name": "dvc_w_vol_u12", "type": "int"},
            {"name": "dvc_i_suck_temp_u11", "type": "int"},
            {"name": "dvc_i_suck_pres_u11", "type": "int"},
            {"name": "dvc_i_sup_heat_u11", "type": "int"},
            {"name": "dvc_i_eev_pos_u11", "type": "int"},
            {"name": "dvc_i_suck_temp_u12", "type": "int"},
            {"name": "dvc_i_suck_pres_u12", "type": "int"},
            {"name": "dvc_i_sup_heat_u12", "type": "int"},
            {"name": "dvc_i_eev_pos_u12", "type": "int"},
            {"name": "dvc_w_pos_fad_u1", "type": "int"},
            {"name": "dvc_w_op_mode_u2", "type": "int"},
            {"name": "dvc_i_fat_u2", "type": "int"},
            {"name": "dvc_i_rat_u2", "type": "int"},
            {"name": "dvc_i_sat_u21", "type": "int"},
            {"name": "dvc_i_sat_u22", "type": "int"},
            {"name": "dvc_i_dft_u21", "type": "int"},
            {"name": "dvc_i_dft_u22", "type": "int"},
            {"name": "dvc_w_freq_u21", "type": "int"},
            {"name": "dvc_w_crnt_u21", "type": "int"},
            {"name": "dvc_w_vol_u21", "type": "int"},
            {"name": "dvc_w_freq_u22", "type": "int"},
            {"name": "dvc_w_crnt_u22", "type": "int"},
            {"name": "dvc_w_vol_u22", "type": "int"},
            {"name": "dvc_i_suck_temp_u21", "type": "int"},
            {"name": "dvc_i_suck_pres_u21", "type": "int"},
            {"name": "dvc_i_sup_heat_u21", "type": "int"},
            {"name": "dvc_i_eev_pos_u21", "type": "int"},
            {"name": "dvc_i_suck_temp_u22", "type": "int"},
            {"name": "dvc_i_suck_pres_u22", "type": "int"},
            {"name": "dvc_i_sup_heat_u22", "type": "int"},
            {"name": "dvc_i_eev_pos_u22", "type": "int"},
            {"name": "dvc_w_pos_fad_u2", "type": "int"},
            {"name": "dvc_cfbk_comp_u11", "type": "int"},
            {"name": "dvc_cfbk_comp_u12", "type": "int"},
            {"name": "dvc_cfbk_comp_u21", "type": "int"},
            {"name": "dvc_cfbk_comp_u22", "type": "int"},
            {"name": "dvc_cfbk_ef_u1", "type": "int"},
            {"name": "dvc_cfbk_ef_u2", "type": "int"},
            {"name": "dvc_cfbk_cf_u1", "type": "int"},
            {"name": "dvc_cfbk_cf_u2", "type": "int"},
            {"name": "dvc_cfbk_pwr", "type": "int"},
            {"name": "dvc_bocflt_ef_u11", "type": "int"},
            {"name": "dvc_bocflt_ef_u12", "type": "int"},
            {"name": "dvc_bocflt_cf_u11", "type": "int"},
            {"name": "dvc_bocflt_cf_u12", "type": "int"},
            {"name": "dvc_bflt_vfd_u11", "type": "int"},
            {"name": "dvc_blpflt_comp_u11", "type": "int"},
            {"name": "dvc_bscflt_comp_u11", "type": "int"},
            {"name": "dvc_bflt_vfd_u12", "type": "int"},
            {"name": "dvc_blpflt_comp_u12", "type": "int"},
            {"name": "dvc_bscflt_comp_u12", "type": "int"},
            {"name": "dvc_bflt_eev_u11", "type": "int"},
            {"name": "dvc_bflt_eev_u12", "type": "int"},
            {"name": "dvc_bflt_fad_u1", "type": "int"},
            {"name": "dvc_bflt_rad_u1", "type": "int"},
            {"name": "dvc_bflt_airclean_u1", "type": "int"},
            {"name": "dvc_bflt_expboard_u1", "type": "int"},
            {"name": "dvc_bflt_frstemp_u1", "type": "int"},
            {"name": "dvc_bflt_splytemp_u11", "type": "int"},
            {"name": "dvc_bflt_splytemp_u12", "type": "int"},
            {"name": "dvc_bflt_rnttemp_u1", "type": "int"},
            {"name": "dvc_bflt_dfstemp_u11", "type": "int"},
            {"name": "dvc_bflt_dfstemp_u12", "type": "int"},
            {"name": "dvc_bocflt_ef_u21", "type": "int"},
            {"name": "dvc_bocflt_ef_u22", "type": "int"},
            {"name": "dvc_bocflt_cf_u21", "type": "int"},
            {"name": "dvc_bocflt_cf_u22", "type": "int"},
            {"name": "dvc_bflt_vfd_u21", "type": "int"},
            {"name": "dvc_blpflt_comp_u21", "type": "int"},
            {"name": "dvc_bscflt_comp_u21", "type": "int"},
            {"name": "dvc_bflt_vfd_u22", "type": "int"},
            {"name": "dvc_blpflt_comp_u22", "type": "int"},
            {"name": "dvc_bscflt_comp_u22", "type": "int"},
            {"name": "dvc_bflt_eev_u21", "type": "int"},
            {"name": "dvc_bflt_eev_u22", "type": "int"},
            {"name": "dvc_bflt_fad_u2", "type": "int"},
            {"name": "dvc_bflt_rad_u2", "type": "int"},
            {"name": "dvc_bflt_airclean_u2", "type": "int"},
            {"name": "dvc_bflt_expboard_u2", "type": "int"},
            {"name": "dvc_bflt_frstemp_u2", "type": "int"},
            {"name": "dvc_bflt_splytemp_u21", "type": "int"},
            {"name": "dvc_bflt_splytemp_u22", "type": "int"},
            {"name": "dvc_bflt_rnttemp_u2", "type": "int"},
            {"name": "dvc_bflt_dfstemp_u21", "type": "int"},
            {"name": "dvc_bflt_dfstemp_u22", "type": "int"},
            {"name": "dvc_bflt_vehtemp", "type": "int"},
            {"name": "dvc_bflt_seattemp", "type": "int"},
            {"name": "dvc_bflt_emergivt", "type": "int"},
            {"name": "dvc_bflt_mvbbus", "type": "int"},
            {"name": "dvc_bcomuflt_vfd_u11", "type": "int"},
            {"name": "dvc_bcomuflt_vfd_u12", "type": "int"},
            {"name": "dvc_bcomuflt_vfd_u21", "type": "int"},
            {"name": "dvc_bcomuflt_vfd_u22", "type": "int"},
            {"name": "dvc_bcomuflt_eev_u11", "type": "int"},
            {"name": "dvc_bcomuflt_eev_u12", "type": "int"},
            {"name": "dvc_bcomuflt_eev_u21", "type": "int"},
            {"name": "dvc_bcomuflt_eev_u22", "type": "int"},
            {"name": "dvc_bmcbflt_pwr_u1", "type": "int"},
            {"name": "dvc_bmcbflt_pwr_u2", "type": "int"},
            {"name": "dvc_blplockflt_u11", "type": "int"},
            {"name": "dvc_blplockflt_u12", "type": "int"},
            {"name": "dvc_blplockflt_u21", "type": "int"},
            {"name": "dvc_blplockflt_u22", "type": "int"},
            {"name": "dvc_bsclockflt_u11", "type": "int"},
            {"name": "dvc_bsclockflt_u12", "type": "int"},
            {"name": "dvc_bsclockflt_u21", "type": "int"},
            {"name": "dvc_bsclockflt_u22", "type": "int"},
            {"name": "dvc_bvfdlockflt_u11", "type": "int"},
            {"name": "dvc_bvfdlockflt_u12", "type": "int"},
            {"name": "dvc_bvfdlockflt_u21", "type": "int"},
            {"name": "dvc_bvfdlockflt_u22", "type": "int"},
            {"name": "dvc_beevlockflt_u11", "type": "int"},
            {"name": "dvc_beevlockflt_u12", "type": "int"},
            {"name": "dvc_beevlockflt_u21", "type": "int"},
            {"name": "dvc_beevlockflt_u22", "type": "int"},
            {"name": "dvc_cft_code_u1", "type": "int"},
            {"name": "dvc_cft_code_u2", "type": "int"},
            {"name": "dvc_dwoptime_emergivt", "type": "int"},
            {"name": "dvc_dwopcount_emergivt", "type": "int"},
            {"name": "dvc_dwoptime_ef_u1", "type": "int"},
            {"name": "dvc_dwoptime_cf_u1", "type": "int"},
            {"name": "dvc_dwoptime_comp_u11", "type": "int"},
            {"name": "dvc_dwoptime_comp_u12", "type": "int"},
            {"name": "dvc_dwopcount_ef_u1", "type": "int"},
            {"name": "dvc_dwopcount_cf_u1", "type": "int"},
            {"name": "dvc_dwopcount_cp_u11", "type": "int"},
            {"name": "dvc_dwopcount_cp_u12", "type": "int"},
            {"name": "dvc_dwopcount_fad_u1", "type": "int"},
            {"name": "dvc_dwopcount_rad_u1", "type": "int"},
            {"name": "dvc_dwoptime_ef_u2", "type": "int"},
            {"name": "dvc_dwoptime_cf_u2", "type": "int"},
            {"name": "dvc_dwoptime_comp_u21", "type": "int"},
            {"name": "dvc_dwoptime_comp_u22", "type": "int"},
            {"name": "dvc_dwopcount_ef_u2", "type": "int"},
            {"name": "dvc_dwopcount_cf_u2", "type": "int"},
            {"name": "dvc_dwopcount_cp_u21", "type": "int"},
            {"name": "dvc_dwopcount_cp_u22", "type": "int"},
            {"name": "dvc_dwopcount_fad_u2", "type": "int"},
            {"name": "dvc_dwopcount_rad_u2", "type": "int"},
            {"name": "msg_crc", "type": "int"},
            {"name": "msg_src_dvc_time", "type": "string"},
            {"name": "msg_src_parse_time", "type": "string"}
        ]
    }

output_schema = faust.Schema(
    key_type=str,
    value_type=ACSignal,
    key_serializer="raw",
    value_serializer="json",
)

output_topic = app.topic(settings.PARSED_TOPIC_NAME, partitions=settings.TOPIC_PARTITIONS, value_serializer='json')