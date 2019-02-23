# -*- coding: utf-8 -*-
"""
Created on Mon Feb 18 09:13:34 2019

@author: michaelek
"""

from allotools import AlloUsage
from pdsql import mssql


#######################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
sites_table = 'ExternalSite'

catch_group = ['Ashburton River']
summ_col = 'SwazName'

crc_filter = {'use_type': ['stockwater', 'irrigation']}

from_date = '2015-07-01'
to_date = '2018-06-30'

datasets = ['allo', 'restr_allo', 'usage']
datasets = ['allo', 'metered_allo', 'usage']

freq = 'A-JUN'
freq = 'M'

t1 = 'CRC012123'
t2 = 'Pendarves Area'

########################################
### Test 1
sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'CatchmentGroupName', summ_col], where_in={'CatchmentGroupName': catch_group})

site_filter = {'SwazName': sites1.SwazName.unique().tolist()}

a1 = AlloUsage(from_date, to_date, site_filter=site_filter, crc_filter=crc_filter)

allo_ts1 = a1._get_allo_ts(freq, groupby=['crc', 'date'])

metered_allo_ts1 = a1._get_metered_allo_ts(freq, ['crc', 'date'])

usage1 = a1._get_usage_ts(freq, ['crc', 'date'])

a1._lowflow_data(freq)

restr_allo = a1._get_restr_allo_ts(freq, ['crc', 'wap', 'date'])

combo_ts = a1.get_ts(datasets, freq, ['crc', 'wap', 'date'])

### Test 2
a1 = AlloUsage(from_date, to_date)

combo_ts = a1.get_combo_ts(datasets, freq, ['date'])

a1 = AlloUsage(from_date, to_date, crc_filter={'crc': ['CRC157373']})

a1 = AlloUsage(from_date, to_date, crc_filter={'crc': ['CRC182351']})
combo_ts = a1.get_combo_ts(datasets, freq, ['date'])

































