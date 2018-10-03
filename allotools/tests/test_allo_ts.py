# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 11:48:42 2018

@author: michaelek
"""
#from allottools import allo_ts

#################################
### Parameters

server = 'sql2012test01'

from_date = '1999-07-01'
to_date = '2003-06-30'
freq = 'A-JUN'
restr_type = 'annual volume'
remove_months=False

####################################
### Run tests

sites, allo, allo_wap = allo_filter(server, from_date, to_date)

ts_allo = allo_ts(server, from_date, to_date, freq, restr_type)










































