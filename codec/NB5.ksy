meta:
  id: nb5
  endian: be
seq:
  - id: msg_header_code01
    type: u1
  - id: msg_header_code02
    type: u1
  - id: msg_length
    type: u2
  - id: msg_src_dvc_no
    type: u1
  - id: msg_host_dvc_no
    type: u1
  - id: msg_type
    type: u2
  - id: msg_frame_no
    type: u2
  - id: msg_line_no
    type: u2
  - id: msg_train_type
    type: u2
  - id: msg_train_no
    type: u4
  - id: msg_carriage_no
    type: u1
  - id: msg_protocal_version
    type: u1
  - id: msg_reversed1
    type: u2
  - id: msg_reversed2
    type: u2
  - id: msg_reversed3
    type: u2
  - id: msg_reversed4
    type: u2
  - id: msg_reversed5
    type: u2
  - id: msg_src_dvc_year
    type: u1
  - id: msg_src_dvc_month
    type: u1
  - id: msg_src_dvc_day
    type: u1
  - id: msg_src_dvc_hour
    type: u1
  - id: msg_src_dvc_minute
    type: u1
  - id: msg_src_dvc_second
    type: u1
  - id: dvc_i_inner_temp
    type: s2
  - id: dvc_i_outer_temp
    type: s2
  - id: dvc_i_set_temp
    type: s2
  - id: dvc_i_seat_temp
    type: s2
  - id: dvc_i_veh_temp
    type: s2
  - id: dvc_w_passen_load
    type: s2
  - id: dvc_w_op_mode_u1
    type: s2
  - id: dvc_i_fat_u1
    type: s2
  - id: dvc_i_rat_u1
    type: s2
  - id: dvc_i_sat_u11
    type: s2
  - id: dvc_i_sat_u12
    type: s2
  - id: dvc_i_dft_u11
    type: s2
  - id: dvc_i_dft_u12
    type: s2
  - id: dvc_w_freq_u11
    type: s2
  - id: dvc_w_crnt_u11
    type: s2
  - id: dvc_w_vol_u11
    type: s2
  - id: dvc_w_freq_u12
    type: s2
  - id: dvc_w_crnt_u12
    type: s2
  - id: dvc_w_vol_u12
    type: s2
  - id: dvc_i_suck_temp_u11
    type: s2
  - id: dvc_i_suck_pres_u11
    type: s2
  - id: dvc_i_sup_heat_u11
    type: s2
  - id: dvc_i_eev_pos_u11
    type: s2
  - id: dvc_i_suck_temp_u12
    type: s2
  - id: dvc_i_suck_pres_u12
    type: s2
  - id: dvc_i_sup_heat_u12
    type: s2
  - id: dvc_i_eev_pos_u12
    type: s2
  - id: dvc_w_pos_fad_u1
    type: s2
  - id: dvc_w_op_mode_u2
    type: s2
  - id: dvc_i_fat_u2
    type: s2
  - id: dvc_i_rat_u2
    type: s2
  - id: dvc_i_sat_u21
    type: s2
  - id: dvc_i_sat_u22
    type: s2
  - id: dvc_i_dft_u21
    type: s2
  - id: dvc_i_dft_u22
    type: s2
  - id: dvc_w_freq_u21
    type: s2
  - id: dvc_w_crnt_u21
    type: s2
  - id: dvc_w_vol_u21
    type: s2
  - id: dvc_w_freq_u22
    type: s2
  - id: dvc_w_crnt_u22
    type: s2
  - id: dvc_w_vol_u22
    type: s2
  - id: dvc_i_suck_temp_u21
    type: s2
  - id: dvc_i_suck_pres_u21
    type: s2
  - id: dvc_i_sup_heat_u21
    type: s2
  - id: dvc_i_eev_pos_u21
    type: s2
  - id: dvc_i_suck_temp_u22
    type: s2
  - id: dvc_i_suck_pres_u22
    type: s2
  - id: dvc_i_sup_heat_u22
    type: s2
  - id: dvc_i_eev_pos_u22
    type: s2
  - id: dvc_w_pos_fad_u2
    type: s2
  - id: dvc_cfbk_comp_u11
    type: b1
  - id: dvc_cfbk_comp_u12
    type: b1
  - id: dvc_cfbk_comp_u21
    type: b1
  - id: dvc_cfbk_comp_u22
    type: b1
  - id: dvc_cfbk_ef_u1
    type: b1
  - id: dvc_cfbk_ef_u2
    type: b1
  - id: dvc_cfbk_cf_u1
    type: b1
  - id: dvc_cfbk_cf_u2
    type: b1
  - id: dvc_cfbk_pwr
    type: b1
  - id: dvc_cfbk_revers0
    type: b7
  - id: dvc_bocflt_ef_u11
    type: b1
  - id: dvc_bocflt_ef_u12
    type: b1
  - id: dvc_bocflt_cf_u11
    type: b1
  - id: dvc_bocflt_cf_u12
    type: b1
  - id: dvc_bflt_vfd_u11
    type: b1
  - id: dvc_blpflt_comp_u11
    type: b1
  - id: dvc_bscflt_comp_u11
    type: b1
  - id: dvc_bflt_vfd_u12
    type: b1
  - id: dvc_blpflt_comp_u12
    type: b1
  - id: dvc_bscflt_comp_u12
    type: b1
  - id: dvc_bflt_eev_u11
    type: b1
  - id: dvc_bflt_eev_u12
    type: b1
  - id: dvc_bflt_fad_u1
    type: b1
  - id: dvc_bflt_rad_u1
    type: b1
  - id: dvc_bflt_airclean_u1
    type: b1
  - id: dvc_bflt_frstemp_u1
    type: b1
  - id: dvc_bflt_splytemp_u11
    type: b1
  - id: dvc_bflt_splytemp_u12
    type: b1
  - id: dvc_bflt_rnttemp_u1
    type: b1
  - id: dvc_bflt_dfstemp_u11
    type: b1
  - id: dvc_bflt_dfstemp_u12
    type: b1
  - id: dvc_bocflt_ef_u21
    type: b1
  - id: dvc_bocflt_ef_u22
    type: b1
  - id: dvc_bocflt_cf_u21
    type: b1
  - id: dvc_bocflt_cf_u22
    type: b1
  - id: dvc_bflt_vfd_u21
    type: b1
  - id: dvc_blpflt_comp_u21
    type: b1
  - id: dvc_bscflt_comp_u21
    type: b1
  - id: dvc_bflt_vfd_u22
    type: b1
  - id: dvc_blpflt_comp_u22
    type: b1
  - id: dvc_bscflt_comp_u22
    type: b1
  - id: dvc_bflt_eev_u21
    type: b1
  - id: dvc_bflt_eev_u22
    type: b1
  - id: dvc_bflt_fad_u2
    type: b1
  - id: dvc_bflt_rad_u2
    type: b1
  - id: dvc_bflt_airclean_u2
    type: b1
  - id: dvc_bflt_frstemp_u2
    type: b1
  - id: dvc_bflt_splytemp_u21
    type: b1
  - id: dvc_bflt_splytemp_u22
    type: b1
  - id: dvc_bflt_rnttemp_u2
    type: b1
  - id: dvc_bflt_dfstemp_u21
    type: b1
  - id: dvc_bflt_dfstemp_u22
    type: b1
  - id: dvc_bflt_vehtemp
    type: b1
  - id: dvc_bflt_seattemp
    type: b1
  - id: dvc_bflt_emergivt
    type: b1
  - id: dvc_bflt_mvbbus
    type: b1
  - id: dvc_bcomuflt_vfd_u11
    type: b1
  - id: dvc_bcomuflt_vfd_u12
    type: b1
  - id: dvc_bcomuflt_vfd_u21
    type: b1
  - id: dvc_bcomuflt_vfd_u22
    type: b1
  - id: dvc_bcomuflt_eev_u11
    type: b1
  - id: dvc_bcomuflt_eev_u12
    type: b1
  - id: dvc_bcomuflt_eev_u21
    type: b1
  - id: dvc_bcomuflt_eev_u22
    type: b1
  - id: dvc_bmcbflt_pwr_u1
    type: b1
  - id: dvc_bmcbflt_pwr_u2
    type: b1
  - id: dvc_cfbk_revers1
    type: b20
  - id: dvc_bflt_trainmove
    type: b1
  - id: dvc_bflt_cabinovertemp
    type: b1
  - id: dvc_cft_code_u1
    type: u1
  - id: dvc_cft_code_u2
    type: u1
  - id: dvc_wposrad_u1
    type: u1
  - id: dvc_wposrad_u2
    type: u1
  - id: dvc_dwoptime_emergivt
    type: u4
  - id: dvc_dwopcount_emergivt
    type: u4
  - id: dvc_dwoptime_ef_u1
    type: u4
  - id: dvc_dwoptime_cf_u1
    type: u4
  - id: dvc_dwoptime_comp_u11
    type: u4
  - id: dvc_dwoptime_comp_u12
    type: u4
  - id: dvc_dwopcount_ef_u1
    type: u4
  - id: dvc_dwopcount_cf_u1
    type: u4
  - id: dvc_dwopcount_cp_u11
    type: u4
  - id: dvc_dwopcount_cp_u12
    type: u4
  - id: dvc_dwopcount_fad_u1
    type: u4
  - id: dvc_dwopcount_rad_u1
    type: u4
  - id: dvc_dwoptime_ef_u2
    type: u4
  - id: dvc_dwoptime_cf_u2
    type: u4
  - id: dvc_dwoptime_comp_u21
    type: u4
  - id: dvc_dwoptime_comp_u22
    type: u4
  - id: dvc_dwopcount_ef_u2
    type: u4
  - id: dvc_dwopcount_cf_u2
    type: u4
  - id: dvc_dwopcount_cp_u21
    type: u4
  - id: dvc_dwopcount_cp_u22
    type: u4
  - id: dvc_dwopcount_fad_u2
    type: u4
  - id: dvc_dwopcount_rad_u2
    type: u4
  - id: msg_crc
    type: u2
    
    
    
    
  
  
