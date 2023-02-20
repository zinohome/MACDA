#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import os
import traceback
from core.settings import settings
import weakref
from utils.log import log as log
import simplejson as json
import numpy as np
import pandas as pd

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

if __name__ == '__main__':
    au = Alertutil()
    log.debug(au.predictfield)
    log.debug(au.alertfield)
    log.debug(au.partcodefield)
    log.debug(au.getvalue('alertcode','bcomuflt_eev_u22','location'))
    log.debug(f"SELECT msg_calc_dvc_no, max({') as dvc_, max('.join(au.predictfield)}) as dvc_ FROM dev_predict WHERE msg_calc_parse_time > now() - INTERVAL '2 minutes' group by msg_calc_dvc_no")
    log.debug(f"SELECT msg_calc_dvc_no, max(dvc_{') as dvc_, max(dvc_'.join(au.alertfield)}) as dvc_ FROM dev_macda WHERE msg_calc_parse_time > now() - INTERVAL '2 minutes' group by msg_calc_dvc_no")
    log.debug(f"SELECT msg_calc_dvc_no, last(dvc_{',msg_calc_parse_time) as dvc_, last(dvc_'.join(au.partcodefield)},msg_calc_parse_time) as dvc_ FROM dev_macda WHERE msg_calc_parse_time > now() - INTERVAL '2 minutes' group by msg_calc_dvc_no")


    '''
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