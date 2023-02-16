#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  #
#  Copyright (C) 2021 ZinoHome, Inc. All Rights Reserved
#  #
#  @Time    : 2021
#  @Author  : Zhang Jun
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
class SensorPolyfit(metaclass=Cached):
    def __init__(self):
        colddatafile = os.path.join(DATA_DIR, 'colddata.xlsx')
        hotdatafile = os.path.join(DATA_DIR, 'hotdata.xlsx')
        self.__colddf__ = pd.read_excel(colddatafile, index_col='freq')
        self.__hotdf__ = pd.read_excel(hotdatafile, index_col='freq')
        log.debug('SensorPolyfit loaded.')
        self.__coldfuncdict__ = {}
        self.__hotfuncdict__ = {}
        for clidx in self.__colddf__.columns.values:
            keylist = self.__colddf__[clidx].keys().tolist()
            vallist = self.__colddf__[clidx].values.tolist()
            kvfunc = np.poly1d(np.polyfit(keylist, vallist, 3))
            self.__coldfuncdict__[clidx] = kvfunc
        for clidx in self.__hotdf__.columns.values:
            keylist = self.__hotdf__[clidx].keys().tolist()
            vallist = self.__hotdf__[clidx].values.tolist()
            kvfunc = np.poly1d(np.polyfit(keylist, vallist, 3))
            self.__hotfuncdict__[clidx] = kvfunc
        log.debug('SensorPolyfit Generated.')



    def polyfit(self, mode, temp, freq, pres):
        if mode == 6 or mode == 7:
            if freq >= 30 and freq <=70:
                if temp >=20 and temp <=45:
                    func = sp.__coldfuncdict__[round(temp)]
                    calc_pres = round(func(freq), 1)
                    if pres < round(0.8*calc_pres,1):
                        return 1
                    elif pres < round(1.1*calc_pres,1) and pres > round(0.9*calc_pres,1):
                        return 0
                    else:
                        return 0
                else:
                    return 0
            else:
                return 0
        elif mode == 8 or mode == 9:
            if freq >= 30 and freq <=45:
                if temp >= -5 and temp <= 15:
                    func = sp.__hotfuncdict__[round(temp)]
                    calc_pres = round(func(freq), 1)
                    if pres < round(0.8 * calc_pres, 1):
                        return 1
                    elif pres < round(1.1 * calc_pres, 1) and pres > round(0.9 * calc_pres, 1):
                        return 0
                    else:
                        return 0
                else:
                    return 0
            else:
                return 0
        else:
            return 0

if __name__ == '__main__':
    sp = SensorPolyfit()
    #log.debug(sp.__hotdf__.columns.values)
    #func = sp.__hotfuncdict__[-5]
    log.debug(sp.polyfit(6, 28, 30, 4.7))

    '''
    x = [10, 20, 30, 40, 50, 60, 70, 80]
    x = np.array(x)
    log.debug('x is :\n', x)
    num = [174, 236, 305, 334, 349, 351, 342, 323]
    y = np.array(num)
    log.debug('y is :\n', y)
    # f1 为各项的系数，3 表示想要拟合的最高次项是多少。
    f1 = np.polyfit(x, y, 3)
    # p1 为拟合的多项式表达式
    p1 = np.poly1d(f1)
    log.debug('p1 is :\n', p1)
    log.debug(p1(15))
    log.debug(DATA_DIR)
    colddatafile = os.path.join(DATA_DIR, 'colddata.xlsx')
    hotdatafile = os.path.join(DATA_DIR, 'hotdata.xlsx')
    #df = pd.read_excel(colddatafile)
    #log.debug(df)
    df = pd.read_excel(hotdatafile, index_col='freq')
    log.debug(df[-5])
    '''



