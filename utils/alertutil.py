#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import os
import traceback
import requests

from core.settings import settings
import weakref
from utils.log import log as log
import simplejson as json
import numpy as np
import pandas as pd
import time

from utils.tsutil import TSutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'predictdata')

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
class Alertutil(metaclass=Cached):
    def __init__(self):
        log.debug('Code loading.')
        alertcodefile = os.path.join(DATA_DIR, 'alertcode.xlsx')
        partcodefile = os.path.join(DATA_DIR, 'partcode.xlsx')
        self.__alertcode__ = pd.read_excel(alertcodefile)
        self.__alertcode__['name'] = self.__alertcode__['name'].apply(str.lower)
        self.__partcode__ = pd.read_excel(partcodefile)
        self.__partcode__['name'] = self.__partcode__['name'].apply(str.lower)
        log.debug('Code loaded.')

    def getvalue(self, codetype, rowvalue, colname):
        rvalue = ''
        df = self.__alertcode__
        if codetype == 'partcode':
            df = self.__partcode__
        row = df.loc[df['name'] == rowvalue]
        if not row.empty:
            if colname in row.columns.values:
                rvalue = row[colname].values[0]
        return rvalue

    @property
    def predictfield(self):
        return ['ref_leak_u11','ref_leak_u12','ref_leak_u21','ref_leak_u22','ref_pump_u1','ref_pump_u2','fat_sensor','rat_sensor','cabin_overtemp']

    @property
    def alertfield(self):
        col = self.__alertcode__['name']
        return [c for c in col.tolist() if c not in self.predictfield]

    @property
    def partcodefield(self):
        col = self.__partcode__['name']
        return col.tolist()

    def send_statistics(self, statslist):
        srvurl = 'http://192.168.32.160:8180/api/rest/InsertSrvLife'
        #headers = {"content-type":"application/json","x-hasura-admin-secret":"passw0rd"}
        headers = {"content-type":"application/json"}
        data = json.dumps(statslist, encoding="utf-8", ensure_ascii=False)
        #log.debug(data)
        try:
            response = requests.post(srvurl, data.encode(), headers=headers)
            log.debug('Send Statistic data with response code: [%s] ' % response.status_code)
        except Exception as exp:
            log.error('Exception at alrtutil.send_predict() %s ' % exp)
            traceback.print_exc()

    def send_predict(self, predictlist):
        srvurl = 'http://192.168.32.160:8180/api/rest/InsertSrvAlert'
        #headers = {"content-type":"application/json","x-hasura-admin-secret":"passw0rd"}
        headers = {"content-type":"application/json"}
        data = json.dumps(predictlist, encoding="utf-8", ensure_ascii=False)
        #log.debug(data)
        try:
            response = requests.post(srvurl, data.encode(), headers=headers)
            log.debug('Send Alert data with response code: [%s] ' % response.status_code)
        except Exception as exp:
            log.error('Exception at alrtutil.send_predict() %s ' % exp)
            traceback.print_exc()



if __name__ == '__main__':
    au = Alertutil()
    tu = TSutil()
    coachdict = {'1':'Tc1','2':'Mp1','3':'M1','4':'M2','5':'Mp2','6':'Tc2',}
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
            partCodepre = f"0{line_no}0{str(int(carbin_no)-1).zfill(2)}"
            #log.debug('line_no: %s, train_no: %s, carbin_no: %s' % (line_no, train_no, carbin_no))
            for code in au.partcodefield:
                sdata = {}
                sdata['lineName'] = str(line_no)
                sdata['trainType'] = 'B2'
                sdata['trainNo'] = trainNo
                sdata['partCode'] = str(au.getvalue('partcode', code, 'part_code')).replace('500',partCodepre)
                if 'rad' in code or 'fad' in code:
                    sdata['serviceTime'] = 0
                    sdata['serviceValue'] = item[f"dvc_{code}"]
                else:
                    sdata['serviceTime'] = item[f"dvc_{code}"]
                    sdata['serviceValue'] = 0
                sdata['mileage'] = 0
                statis_data_list.append(sdata)
    log.debug('statis_data_list is : %s' % statis_data_list)
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
                    pdata['location'] = au.getvalue('alertcode',field,'location').encode('utf-8').decode('utf-8')
                    pdata['code'] = au.getvalue('alertcode',field,'code').replace('HVAC1',f"HVAC{carbin_no}")
                    pdata['station1'] = str(au.getvalue('alertcode',field,'station1'))
                    pdata['station2'] = str(au.getvalue('alertcode',field,'station2'))
                    pdata['subsystem'] = str(au.getvalue('alertcode',field,'subsystem'))
                    pdata['subsystem'] = str(au.getvalue('alertcode',field,'subsystem'))
                    pdata['starttime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    pdata['endtime'] = '0'
                    predict_data_list.append(pdata)
    log.debug('predict_data_list is : %s' % predict_data_list)
    au.send_predict(predict_data_list)

    # Generata fault data
    fault_data_list = []
    if fault_data['len'] > 0:
        #log.debug(au.alertfield)
        #log.debug(fault_data['data'])
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
                    fdata['location'] = au.getvalue('alertcode', field, 'location').encode('utf-8').decode('utf-8')
                    fdata['code'] = au.getvalue('alertcode', field, 'code').replace('HVAC1', f"HVAC{carbin_no}")
                    fdata['station1'] = str(au.getvalue('alertcode', field, 'station1'))
                    fdata['station2'] = str(au.getvalue('alertcode', field, 'station2'))
                    fdata['subsystem'] = str(au.getvalue('alertcode', field, 'subsystem'))
                    fdata['subsystem'] = str(au.getvalue('alertcode', field, 'subsystem'))
                    fdata['starttime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    fdata['endtime'] = '0'
                    fault_data_list.append(fdata)
    log.debug('fault_data_list is : %s' % fault_data_list)
    au.send_predict(fault_data_list)
    '''
    log.debug(f"SELECT msg_calc_dvc_no, max({') as dvc_, max('.join(au.predictfield)}) as dvc_ FROM dev_predict WHERE msg_calc_parse_time > now() - INTERVAL '2 minutes' group by msg_calc_dvc_no")
    log.debug(f"SELECT msg_calc_dvc_no, max(dvc_{') as dvc_, max(dvc_'.join(au.alertfield)}) as dvc_ FROM dev_macda WHERE msg_calc_parse_time > now() - INTERVAL '2 minutes' group by msg_calc_dvc_no")
    log.debug(f"SELECT msg_calc_dvc_no, last(dvc_{',msg_calc_parse_time) as dvc_, last(dvc_'.join(au.partcodefield)},msg_calc_parse_time) as dvc_ FROM dev_macda WHERE msg_calc_parse_time > now() - INTERVAL '2 minutes' group by msg_calc_dvc_no")
    log.debug(au.getvalue('alertcode','bcomuflt_eev_u22','location'))
    log.debug(au.predictfield)
    log.debug(au.alertfield)
    log.debug(au.partcodefield)

    log.debug(tu.get_predict_data('dev'))
    log.debug(tu.get_fault_data('dev'))
    log.debug(tu.get_statis_data('dev'))
    log.debug(au.__alertcode__)
    adf = au.__alertcode__
    log.debug(adf.columns.values)
    row1 = adf.loc[adf['name'] == 'bComuFlt_EEV_U32']
    log.debug(row1)
    log.debug(row1.empty)
    row1 = adf.loc[adf['name'] == 'bComuFlt_EEV_U12']
    log.debug(row1)
    log.debug('message_type' in row1.columns.values)
    
    log.debug(au.__partcode__)
    pdf = au.__partcode__
    log.debug(pdf.columns.values)
    row2 = pdf.loc[pdf['name'] == 'dwOPTime_Comp_U12']
    log.debug(row2['part_code'])
    log.debug(dir(row2['part_code']))
    log.debug(row2['part_code'].values[0])
    log.debug('this is the value : %s' % row2['part_code'].values[0])
    '''