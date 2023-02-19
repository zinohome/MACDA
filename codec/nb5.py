# This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild
import time

import kaitaistruct
from kaitaistruct import KaitaiStruct, KaitaiStream, BytesIO
from utils.log import log as log

if getattr(kaitaistruct, 'API_VERSION', (0, 9)) < (0, 9):
    raise Exception("Incompatible Kaitai Struct Python API: 0.9 or later is required, but you have %s" % (kaitaistruct.__version__))
'''
div10list = ['dvc_i_inner_temp','dvc_i_outer_temp','dvc_i_set_temp','dvc_i_seat_temp','dvc_i_veh_temp',
    'dvc_i_fat_u1','dvc_i_rat_u1','dvc_i_sat_u11','dvc_i_sat_u12','dvc_i_dft_u11','dvc_i_dft_u12',
    'dvc_w_crnt_u11','dvc_w_vol_u11','dvc_w_crnt_u12','dvc_w_vol_u12','dvc_i_suck_temp_u11','dvc_i_suck_pres_u11',
    'dvc_i_sup_heat_u11','dvc_i_eev_pos_u11','dvc_i_suck_temp_u12','dvc_i_suck_pres_u12','dvc_i_sup_heat_u12',
    'dvc_i_eev_pos_u12','dvc_w_pos_fad_u1','dvc_i_fat_u2','dvc_i_rat_u2','dvc_i_sat_u21','dvc_i_sat_u22',
    'dvc_i_dft_u21','dvc_i_dft_u22','dvc_w_crnt_u21','dvc_w_vol_u21','dvc_w_crnt_u22','dvc_w_vol_u22',
    'dvc_i_suck_temp_u21','dvc_i_suck_pres_u21','dvc_i_sup_heat_u21','dvc_i_eev_pos_u21','dvc_i_suck_temp_u22',
    'dvc_i_suck_pres_u22','dvc_i_sup_heat_u22','dvc_i_eev_pos_u22','dvc_w_pos_fad_u2']
div100list = ['dvc_w_freq_u11','dvc_w_freq_u12','dvc_w_freq_u21','dvc_w_freq_u22']
'''
div10list = ['dvc_i_inner_temp','dvc_i_outer_temp','dvc_i_set_temp','dvc_i_seat_temp','dvc_i_veh_temp',
    'dvc_i_fat_u1','dvc_i_rat_u1','dvc_i_sat_u11','dvc_i_sat_u12','dvc_i_dft_u11','dvc_i_dft_u12',
    'dvc_w_crnt_u11','dvc_w_vol_u11','dvc_w_crnt_u12','dvc_w_vol_u12','dvc_i_suck_temp_u11','dvc_i_suck_pres_u11',
    'dvc_i_sup_heat_u11','dvc_i_eev_pos_u11','dvc_i_suck_temp_u12','dvc_i_suck_pres_u12','dvc_i_sup_heat_u12',
    'dvc_i_eev_pos_u12','dvc_i_fat_u2','dvc_i_rat_u2','dvc_i_sat_u21','dvc_i_sat_u22',
    'dvc_i_dft_u21','dvc_i_dft_u22','dvc_w_crnt_u21','dvc_w_vol_u21','dvc_w_crnt_u22','dvc_w_vol_u22',
    'dvc_i_suck_temp_u21','dvc_i_suck_pres_u21','dvc_i_sup_heat_u21','dvc_i_eev_pos_u21','dvc_i_suck_temp_u22',
    'dvc_i_suck_pres_u22','dvc_i_sup_heat_u22','dvc_i_eev_pos_u22']
div100list = ['dvc_w_freq_u11','dvc_w_freq_u12','dvc_w_freq_u21','dvc_w_freq_u22']

class Nb5(KaitaiStruct):
    def __init__(self, _io, _parent=None, _root=None):
        self._io = _io
        self._parent = _parent
        self._root = _root if _root else self
        self._read()

    def _read(self):
        self.msg_header_code01 = self._io.read_u1()
        self.msg_header_code02 = self._io.read_u1()
        self.msg_length = self._io.read_u2be()
        self.msg_src_dvc_no = self._io.read_u1()
        self.msg_host_dvc_no = self._io.read_u1()
        self.msg_type = self._io.read_u2be()
        self.msg_frame_no = self._io.read_u2be()
        self.msg_line_no = self._io.read_u2be()
        self.msg_train_type = self._io.read_u2be()
        self.msg_train_no = self._io.read_u4be()
        self.msg_carriage_no = self._io.read_u1()
        self.msg_protocal_version = self._io.read_u1()
        self.msg_reversed1 = self._io.read_u2be()
        self.msg_reversed2 = self._io.read_u2be()
        self.msg_reversed3 = self._io.read_u2be()
        self.msg_reversed4 = self._io.read_u2be()
        self.msg_reversed5 = self._io.read_u2be()
        self.msg_src_dvc_year = self._io.read_u1()
        self.msg_src_dvc_month = self._io.read_u1()
        self.msg_src_dvc_day = self._io.read_u1()
        self.msg_src_dvc_hour = self._io.read_u1()
        self.msg_src_dvc_minute = self._io.read_u1()
        self.msg_src_dvc_second = self._io.read_u1()
        self.dvc_i_inner_temp = self._io.read_s2be()
        self.dvc_i_outer_temp = self._io.read_s2be()
        self.dvc_i_set_temp = self._io.read_s2be()
        self.dvc_i_seat_temp = self._io.read_s2be()
        self.dvc_i_veh_temp = self._io.read_s2be()
        self.dvc_w_passen_load = self._io.read_s2be()
        self.dvc_w_op_mode_u1 = self._io.read_s2be()
        self.dvc_i_fat_u1 = self._io.read_s2be()
        self.dvc_i_rat_u1 = self._io.read_s2be()
        self.dvc_i_sat_u11 = self._io.read_s2be()
        self.dvc_i_sat_u12 = self._io.read_s2be()
        self.dvc_i_dft_u11 = self._io.read_s2be()
        self.dvc_i_dft_u12 = self._io.read_s2be()
        self.dvc_w_freq_u11 = self._io.read_s2be()
        self.dvc_w_crnt_u11 = self._io.read_s2be()
        self.dvc_w_vol_u11 = self._io.read_s2be()
        self.dvc_w_freq_u12 = self._io.read_s2be()
        self.dvc_w_crnt_u12 = self._io.read_s2be()
        self.dvc_w_vol_u12 = self._io.read_s2be()
        self.dvc_i_suck_temp_u11 = self._io.read_s2be()
        self.dvc_i_suck_pres_u11 = self._io.read_s2be()
        self.dvc_i_sup_heat_u11 = self._io.read_s2be()
        self.dvc_i_eev_pos_u11 = self._io.read_s2be()
        self.dvc_i_suck_temp_u12 = self._io.read_s2be()
        self.dvc_i_suck_pres_u12 = self._io.read_s2be()
        self.dvc_i_sup_heat_u12 = self._io.read_s2be()
        self.dvc_i_eev_pos_u12 = self._io.read_s2be()
        self.dvc_w_pos_fad_u1 = self._io.read_s2be()
        self.dvc_w_op_mode_u2 = self._io.read_s2be()
        self.dvc_i_fat_u2 = self._io.read_s2be()
        self.dvc_i_rat_u2 = self._io.read_s2be()
        self.dvc_i_sat_u21 = self._io.read_s2be()
        self.dvc_i_sat_u22 = self._io.read_s2be()
        self.dvc_i_dft_u21 = self._io.read_s2be()
        self.dvc_i_dft_u22 = self._io.read_s2be()
        self.dvc_w_freq_u21 = self._io.read_s2be()
        self.dvc_w_crnt_u21 = self._io.read_s2be()
        self.dvc_w_vol_u21 = self._io.read_s2be()
        self.dvc_w_freq_u22 = self._io.read_s2be()
        self.dvc_w_crnt_u22 = self._io.read_s2be()
        self.dvc_w_vol_u22 = self._io.read_s2be()
        self.dvc_i_suck_temp_u21 = self._io.read_s2be()
        self.dvc_i_suck_pres_u21 = self._io.read_s2be()
        self.dvc_i_sup_heat_u21 = self._io.read_s2be()
        self.dvc_i_eev_pos_u21 = self._io.read_s2be()
        self.dvc_i_suck_temp_u22 = self._io.read_s2be()
        self.dvc_i_suck_pres_u22 = self._io.read_s2be()
        self.dvc_i_sup_heat_u22 = self._io.read_s2be()
        self.dvc_i_eev_pos_u22 = self._io.read_s2be()
        self.dvc_w_pos_fad_u2 = self._io.read_s2be()
        self.dvc_cfbk_comp_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_comp_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_comp_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_comp_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_ef_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_ef_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_cf_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_cf_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_pwr = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_revers0 = self._io.read_bits_int_be(7)
        self.dvc_bocflt_ef_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_ef_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_cf_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_cf_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_vfd_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_blpflt_comp_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bscflt_comp_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_vfd_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_blpflt_comp_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bscflt_comp_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_eev_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_eev_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_fad_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_rad_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_airclean_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_frstemp_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_splytemp_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_splytemp_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_rnttemp_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_dfstemp_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_dfstemp_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_ef_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_ef_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_cf_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bocflt_cf_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_vfd_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_blpflt_comp_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bscflt_comp_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_vfd_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_blpflt_comp_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bscflt_comp_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_eev_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_eev_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_fad_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_rad_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_airclean_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_frstemp_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_splytemp_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_splytemp_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_rnttemp_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_dfstemp_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_dfstemp_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_vehtemp = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_seattemp = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_emergivt = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_mvbbus = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_vfd_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_vfd_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_vfd_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_vfd_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_eev_u11 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_eev_u12 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_eev_u21 = self._io.read_bits_int_be(1) != 0
        self.dvc_bcomuflt_eev_u22 = self._io.read_bits_int_be(1) != 0
        self.dvc_bmcbflt_pwr_u1 = self._io.read_bits_int_be(1) != 0
        self.dvc_bmcbflt_pwr_u2 = self._io.read_bits_int_be(1) != 0
        self.dvc_cfbk_revers1 = self._io.read_bits_int_be(20)
        self.dvc_bflt_trainmove = self._io.read_bits_int_be(1) != 0
        self.dvc_bflt_cabinovertemp = self._io.read_bits_int_be(1) != 0
        self._io.align_to_byte()
        self.dvc_cft_code_u1 = self._io.read_u1()
        self.dvc_cft_code_u2 = self._io.read_u1()
        self.dvc_wposrad_u1 = self._io.read_u1()
        self.dvc_wposrad_u2 = self._io.read_u1()
        self.dvc_dwoptime_emergivt = self._io.read_u4be()
        self.dvc_dwopcount_emergivt = self._io.read_u4be()
        self.dvc_dwoptime_ef_u1 = self._io.read_u4be()
        self.dvc_dwoptime_cf_u1 = self._io.read_u4be()
        self.dvc_dwoptime_comp_u11 = self._io.read_u4be()
        self.dvc_dwoptime_comp_u12 = self._io.read_u4be()
        self.dvc_dwopcount_ef_u1 = self._io.read_u4be()
        self.dvc_dwopcount_cf_u1 = self._io.read_u4be()
        self.dvc_dwopcount_cp_u11 = self._io.read_u4be()
        self.dvc_dwopcount_cp_u12 = self._io.read_u4be()
        self.dvc_dwopcount_fad_u1 = self._io.read_u4be()
        self.dvc_dwopcount_rad_u1 = self._io.read_u4be()
        self.dvc_dwoptime_ef_u2 = self._io.read_u4be()
        self.dvc_dwoptime_cf_u2 = self._io.read_u4be()
        self.dvc_dwoptime_comp_u21 = self._io.read_u4be()
        self.dvc_dwoptime_comp_u22 = self._io.read_u4be()
        self.dvc_dwopcount_ef_u2 = self._io.read_u4be()
        self.dvc_dwopcount_cf_u2 = self._io.read_u4be()
        self.dvc_dwopcount_cp_u21 = self._io.read_u4be()
        self.dvc_dwopcount_cp_u22 = self._io.read_u4be()
        self.dvc_dwopcount_fad_u2 = self._io.read_u4be()
        self.dvc_dwopcount_rad_u2 = self._io.read_u4be()
        self.msg_crc = self._io.read_u2be()

    def from_file_to_dict(binfile):
        nb5dict = Nb5.from_file(binfile).__dict__.copy()
        nb5dict[
            'msg_calc_dvc_no'] = f"{nb5dict['msg_line_no']}-{nb5dict['msg_train_no']}-{nb5dict['msg_carriage_no']}"
        nb5dict[
            'msg_calc_train_no'] = f"{nb5dict['msg_line_no']}-{nb5dict['msg_train_no']}"
        nb5dict[
            'msg_calc_dvc_time'] = f"20{nb5dict['msg_src_dvc_year']}-{nb5dict['msg_src_dvc_month']}-{nb5dict['msg_src_dvc_day']} {nb5dict['msg_src_dvc_hour']}:{nb5dict['msg_src_dvc_minute']}:{nb5dict['msg_src_dvc_second']}"
        nb5dict['msg_calc_parse_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        for key in ('_io', '_parent', '_root',
                    'msg_reversed1', 'msg_reversed2', 'msg_reversed3', 'msg_reversed4', 'msg_reversed5',
                    'dvc_cfbk_revers0', 'dvc_cfbk_revers1', 'dvc_cfbk_revers2',
                    'msg_src_dvc_year', 'msg_src_dvc_month', 'msg_src_dvc_day',
                    'msg_src_dvc_hour', 'msg_src_dvc_minute', 'msg_src_dvc_second'):
            if key in nb5dict:
                del nb5dict[key]
        for key,value in nb5dict.items():
            if isinstance(value, bool):
                if value:
                    nb5dict[key] = 1
                else:
                    nb5dict[key] = 0
            if key in div100list:
                if value/100 == value//100:
                    nb5dict[key] = value//100
                else:
                    nb5dict[key] = round(value/100,2)
            if key in div10list:
                if value/10 == value//10:
                    nb5dict[key] = value//10
                else:
                    nb5dict[key] = round(value/10,1)
        return nb5dict

    def from_bytes_to_dict(bytesobj):
        nb5dict = Nb5.from_bytes(bytesobj).__dict__.copy()
        nb5dict[
            'msg_calc_dvc_no'] = f"0{nb5dict['msg_line_no']}0{str(nb5dict['msg_train_no']).zfill(2)}0{nb5dict['msg_carriage_no']}"
        nb5dict[
            'msg_calc_train_no'] = f"0{nb5dict['msg_line_no']}0{str(nb5dict['msg_train_no']).zfill(2)}"
        nb5dict[
            'msg_calc_dvc_time'] = f"20{nb5dict['msg_src_dvc_year']}-{nb5dict['msg_src_dvc_month']}-{nb5dict['msg_src_dvc_day']} {nb5dict['msg_src_dvc_hour']}:{nb5dict['msg_src_dvc_minute']}:{nb5dict['msg_src_dvc_second']}"
        nb5dict['msg_calc_parse_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        for key in ('_io', '_parent', '_root',
                    'msg_reversed1', 'msg_reversed2', 'msg_reversed3', 'msg_reversed4', 'msg_reversed5',
                    'dvc_cfbk_revers0', 'dvc_cfbk_revers1', 'dvc_cfbk_revers2',
                    'msg_src_dvc_year', 'msg_src_dvc_month', 'msg_src_dvc_day',
                    'msg_src_dvc_hour', 'msg_src_dvc_minute', 'msg_src_dvc_second'):
            if key in nb5dict:
                del nb5dict[key]
        for key,value in nb5dict.items():
            if isinstance(value, bool):
                if value:
                    nb5dict[key] = 1
                else:
                    nb5dict[key] = 0
            if key in div100list:
                if value / 100 == value // 100:
                    nb5dict[key] = value // 100
                else:
                    nb5dict[key] = round(value / 100, 2)
            if key in div10list:
                if value / 10 == value // 10:
                    nb5dict[key] = value // 10
                else:
                    nb5dict[key] = round(value / 10, 1)
        return nb5dict


