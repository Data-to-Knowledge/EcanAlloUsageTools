# -*- coding: utf-8 -*-
"""
Created on Mon Feb 18 09:13:34 2019

@author: michaelek
"""

from allotools import AlloUsage



#######################################
### Parameters

#server = 'sql2012test01'

from_date = '2014-07-01'
to_date = '2018-06-30'

datasets = ['allo', 'restr_allo', 'usage']

freq = 'A-JUN'


########################################

a1 = AlloUsage(from_date, to_date)

allo_ts1 = a1.get_allo_ts(freq, groupby=['crc', 'date'])

metered_allo_ts1 = a1.get_metered_allo_ts(freq, ['crc', 'date'], 150, False)

usage1 = a1.get_usage_ts(freq, ['crc', 'date'])

a1._lowflow_data(freq)

restr_allo = a1.get_restr_allo_ts(freq, ['crc', 'wap', 'date'])

combo_ts = a1.get_combo_ts(datasets, freq, ['crc', 'wap', 'date'])






































