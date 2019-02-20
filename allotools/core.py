# -*- coding: utf-8 -*-
"""
Created on Sat Feb 16 09:50:42 2019

@author: michaelek
"""
import numpy as np
import pandas as pd
from pdsql import mssql
import filters
#from allotools.allocation_ts import allo_ts_apply
from allocation_ts import allo_ts_apply
#import allotools.parameters as param
import parameters as param
from datetime import datetime
import util

########################################
### Core class


class AlloUsage(object):

    dataset_types = param.dataset_types

    ### Initial import and assignment function
    def __init__(self, from_date=None, to_date=None, site_filter=None, crc_filter=None, crc_wap_filter=None, in_allo=True):
        sites, allo, allo_wap = filters.allo_filter(param.server, from_date, to_date, site_filter, crc_filter, crc_wap_filter, in_allo)
        sites.index.name = 'wap'
        setattr(self, 'sites', sites)
        setattr(self, 'allo', allo)

        allo_wap['tot_rate'] = allo_wap.groupby(['crc', 'take_type', 'allo_block'])['max_rate_wap'].transform('sum')
        allo_wap = allo_wap.reset_index()
        allo_wap['rate_ratio'] = allo_wap['max_rate_wap']/allo_wap['tot_rate']
        allo_wap.loc[(allo_wap.sd1_7.isnull()) & (allo_wap.take_type == 'Take Groundwater'), 'sd1_7'] = 0
        allo_wap.loc[(allo_wap.sd1_30.isnull()) & (allo_wap.take_type == 'Take Groundwater'), 'sd1_30'] = 0
        allo_wap.loc[(allo_wap.sd1_150.isnull()) & (allo_wap.take_type == 'Take Groundwater'), 'sd1_150'] = 0
        allo_wap.loc[(allo_wap.take_type == 'Take Surface Water'), ['sd1_7', 'sd1_30', 'sd1_150']] = 100
        allo_wap.set_index(['crc', 'take_type', 'allo_block', 'wap'], inplace=True)
        setattr(self, 'allo_wap', allo_wap.drop('tot_rate', axis=1))
        setattr(self, 'server', param.server)
        if from_date is None:
            from_date = '1900-01-01'
        if to_date is None:
            to_date = str(datetime.now().date())
        setattr(self, 'from_date', from_date)
        setattr(self, 'to_date', to_date)


    def _usage_summ(self):
        """

        """
        ### Get the ts summary tables
        ts_summ1 = mssql.rd_sql(self.server, param.database, param.ts_summ_table, ['ExtSiteID', 'DatasetTypeID', 'FromDate', 'ToDate'], {'DatasetTypeID': list(param.dataset_dict.keys())})
        ts_summ2 = ts_summ1[ts_summ1.ExtSiteID.isin(self.sites.index)].copy()
        ts_summ2['take_type'] = ts_summ2['DatasetTypeID']
        ts_summ2.replace({'take_type': param.dataset_dict}, inplace=True)
        ts_summ2.rename(columns={'ExtSiteID': 'wap', 'FromDate': 'from_date', 'ToDate': 'to_date'}, inplace=True)

        ts_summ2['from_date'] = pd.to_datetime(ts_summ2['from_date'])
        ts_summ2['to_date'] = pd.to_datetime(ts_summ2['to_date'])

        ts_summ3 = ts_summ2[(ts_summ2.from_date < self.to_date) & (ts_summ2.to_date > self.from_date)].copy()

        setattr(self, 'ts_usage_summ', ts_summ3)


    def _sw_gw_split_allo(self, sd_days=150):
        """
        Function to split the total allo into a SW and GW allocation.
        """
        allo5 = pd.merge(self.total_allo_ts.reset_index(), self.allo_wap.reset_index(), on=['crc', 'take_type', 'allo_block'], how='left')

        ## re-proportion the allocation
        allo5['total_allo'] = allo5['total_allo'] * allo5['rate_ratio']
        allo5['sw_allo'] = allo5['total_allo'] * allo5[param.sd_dict[sd_days]] * 0.01
        allo5['gw_allo'] = allo5['total_allo'] - allo5['sw_allo']

        ### Rearrange
        allo6 = allo5[['crc', 'take_type', 'allo_block', 'wap', 'date', 'sw_allo', 'gw_allo', 'total_allo']].copy()
        allo6.set_index(['crc', 'take_type', 'allo_block', 'wap', 'date'], inplace=True)

        setattr(self, 'allo_ts', allo6)

        return allo6


    def _est_allo_ts(self, freq):
        """

        """
        restr_col = param.allo_type_dict[freq]

        allo3 = self.allo.apply(allo_ts_apply, axis=1, from_date=self.from_date, to_date=self.to_date, freq=freq, restr_col=restr_col, remove_months=False)

        allo4 = allo3.stack()
        allo4.index.set_names(['crc', 'take_type', 'allo_block', 'date'], inplace=True)
        allo4.name = 'total_allo'

        setattr(self, 'total_allo_ts', allo4)
        setattr(self, 'freq', freq)


    def get_allo_ts(self, freq, groupby=['crc', 'date'], sd_days=150):
        """
        Function to create an allocation time series.

        Parameters
        ----------
        freq : str
            Pandas frequency str. Must be 'D', 'W', 'M', 'A-JUN', or 'A'.
        groupby : str or list of str
            A list of the combination of fields that the output should be aggregated to. Possible fields include: 'crc', 'take_type', 'allo_block', 'date', and 'wap'. Being a time series function, you should always add 'date' to the groupby.
        sd_days : int
            The stream depletion effect on groundwater takes. The value is the number of days of pumping. Accepted values are 7, 30, and 150.

        Returns
        -------
        Series
            indexed by crc, take_type, and allo_block
        """

        if isinstance(groupby, str):
            groupby = [groupby]
        if freq not in param.allo_type_dict:
            raise ValueError('freq must be one of ' + str(param.allo_type_dict))

        if hasattr(self, 'total_allo_ts'):
            if self.freq != freq:
                self._est_allo_ts(freq)
        else:
            self._est_allo_ts(freq)

        ### Convert to GW and SW allocation

        allo6 = self._sw_gw_split_allo(sd_days=sd_days)
        setattr(self, 'sd_days', sd_days)

        ### Return groupby

        allo7 = allo6.groupby(level=groupby).sum()

        return allo7


    def get_metered_allo_ts(self, freq, groupby=['crc', 'take_type'], sd_days=150, restr_allo=False, proportion_allo=True):
        """

        """
        setattr(self, 'proportion_allo', proportion_allo)

        ### Get the allocation ts either total or metered
        if restr_allo:
            if not hasattr(self, 'restr_allo_ts'):
                allo1 = self.get_allo_ts(freq, groupby=groupby, sd_days=sd_days)
            elif (self.freq != freq) or (self.sd_days != sd_days):
                allo1 = self.get_allo_ts(freq, groupby=groupby, sd_days=sd_days)
            allo1 = self.restr_allo_ts.drop(['site', 'band_num', 'restr_ratio'], axis=1).copy().reset_index()
            rename_dict = {'sw_restr_allo': 'sw_metered_restr_allo', 'gw_restr_allo': 'gw_metered_restr_allo', 'total_restr_allo': 'total_metered_restr_allo'}
        else:
            if not hasattr(self, 'allo_ts'):
                allo1 = self.get_allo_ts(freq, groupby=groupby, sd_days=sd_days)
            elif (self.freq != freq) or (self.sd_days != sd_days):
                allo1 = self.get_allo_ts(freq, groupby=groupby, sd_days=sd_days)
            allo1 = self.allo_ts.copy().reset_index()
            rename_dict = {'sw_allo': 'sw_metered_allo', 'gw_allo': 'gw_metered_allo', 'total_allo': 'total_metered_allo'}

        ### Get the ts summary tables
        if not hasattr(self, 'ts_usage_summ'):
            self._usage_summ()
        ts_usage_summ = self.ts_usage_summ.copy()

        ### Select the crcs with usage data
        allo_wap = self.allo_wap.copy().reset_index()
        allo_wap1 = allo_wap[allo_wap.wap.isin(ts_usage_summ.wap.unique())].copy()

        ### Select the allo_ts data
        allo2 = pd.merge(allo_wap1[['crc', 'take_type', 'allo_block', 'wap']], allo1, on=['crc', 'take_type', 'allo_block', 'wap'], how='right', indicator=True)
        allo2['_merge'] = allo2._merge.cat.rename_categories({'left_only': 2, 'right_only': 0, 'both': 1}).astype(int)

        if proportion_allo:
            allo3 = allo2[allo2._merge == 1].drop(['_merge'], axis=1).copy()
        else:
            allo2['usage_waps'] = allo2.groupby(['crc', 'take_type', 'allo_block', 'date'])['_merge'].transform('sum')

            allo3 = allo2[allo2.usage_waps > 0].drop(['_merge', 'usage_waps'], axis=1).copy()

        allo3.rename(columns=rename_dict, inplace=True)

        allo4 = allo3.groupby(groupby).sum()

#        setattr(self, 'metered_allo_ts', allo3)
        return allo4


    def _process_usage(self, freq):
        """

        """
        ### Get the ts summary tables
        if not hasattr(self, 'ts_usage_summ'):
            self._usage_summ()
        ts_usage_summ = self.ts_usage_summ.copy()

        ## Get the ts data and aggregate
        tsdata1 = mssql.rd_sql(self.server, param.database, param.ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_in={'ExtSiteID': ts_usage_summ.wap.unique().tolist(), 'DatasetTypeID': ts_usage_summ.DatasetTypeID.unique().tolist()}, from_date=self.from_date, to_date=self.to_date, date_col='DateTime')

        tsdata1['DateTime'] = pd.to_datetime(tsdata1['DateTime'])
        tsdata2 = util.grp_ts_agg(tsdata1, ['ExtSiteID'], 'DateTime', freq).sum().reset_index()

        tsdata2.rename(columns={'DateTime': 'date', 'ExtSiteID': 'wap', 'Value': 'total_usage'}, inplace=True)

        setattr(self, 'usage_ts', tsdata2)
        setattr(self, 'freq', freq)


    def get_usage_ts(self, freq, groupby=['wap', 'date'], sd_days=150):
        """

        """
        ### Get the usage data if it exists
        if hasattr(self, 'usage_ts'):
            if self.freq != freq:
                self._process_usage(freq)
        else:
            self._process_usage(freq)
        tsdata2 = self.usage_ts.copy()

        if ('crc' in groupby) or ('allo_block' in groupby):
            allo1 = self.get_allo_ts(freq, ['crc', 'take_type', 'allo_block', 'wap', 'date'], sd_days).reset_index()

            allo1['combo_allo'] = allo1.groupby(['wap', 'date'])['total_allo'].transform('sum')
            allo1['combo_ratio'] = allo1['total_allo']/allo1['combo_allo']

            ### combine with consents info
            usage1 = pd.merge(allo1, tsdata2, on=['wap', 'date'])
            usage1['total_usage'] = usage1['total_usage'] * usage1['combo_ratio']

            ### Split the GW and SW components
            usage1['sw_ratio'] = usage1['sw_allo']/usage1['total_allo']

            usage1['sw_usage'] = usage1['sw_ratio'] * usage1['total_usage']
            usage1['gw_usage'] = usage1['total_usage'] - usage1['sw_usage']

            usage1.drop(['sw_allo', 'gw_allo', 'total_allo', 'combo_allo', 'combo_ratio', 'sw_ratio'], axis=1, inplace=True)

        else:
            usage1 = tsdata2

        ### Groupby
        usage2 = usage1.groupby(groupby).sum()

        return usage2


    def _lowflow_data(self, freq):
        """

        """
        lf_band1 = mssql.rd_sql(self.server, param.database, param.lf_band_table, ['site', 'band_num', 'date', 'band_allo'], {'site_type': ['LowFlow']}, from_date=self.from_date, to_date=self.to_date, date_col='date')
        lf_band1['date'] = pd.to_datetime(lf_band1['date'])
        lf_band1.loc[lf_band1.band_allo > 100, 'band_allo'] = 100

        lf_crc1 = mssql.rd_sql(self.server, param.database, param.lf_band_crc_table, ['site', 'band_num', 'date', 'crc'],  from_date=self.from_date, to_date=self.to_date, date_col='date')
        lf_crc1['date'] = pd.to_datetime(lf_crc1['date'])

        lf_crc2 = pd.merge(lf_crc1, lf_band1, on=['site', 'band_num', 'date'])

        lf_crc3 = util.grp_ts_agg(lf_crc2, ['crc', 'site', 'band_num'], 'date', freq)['band_allo'].mean() * 0.01
        lf_crc3.name = 'restr_ratio'


        lf_crc4 = lf_crc3.sort_values().reset_index()
        lf_crc5 = lf_crc4.groupby(['crc', 'date']).first()

        setattr(self, 'lf_restr', lf_crc5)
        setattr(self, 'freq', freq)


    def get_restr_allo_ts(self, freq, groupby, sd_days=150):
        """

        """
        ### Get the allocation ts
        if not hasattr(self, 'allo_ts'):
            allo1 = self.get_allo_ts(freq, groupby=groupby, sd_days=sd_days)
        elif (self.freq != freq) or (self.sd_days != sd_days):
            allo1 = self.get_allo_ts(freq, groupby=groupby, sd_days=sd_days)
        if not hasattr(self, 'lf_restr'):
            self._lowflow_data(freq)
        elif (self.freq != freq):
            self._lowflow_data(freq)

        allo1 = self.allo_ts.copy().reset_index()
        lf_restr = self.lf_restr.copy().reset_index()

        ### Combine base data
        allo2 = pd.merge(allo1, lf_restr, on=['crc', 'date'], how='left')
        allo2.loc[allo2.restr_ratio.isnull(), 'restr_ratio'] = 1

        ### Update allo
        allo2.rename(columns={'sw_allo': 'sw_restr_allo', 'gw_allo': 'gw_restr_allo', 'total_allo': 'total_restr_allo'}, inplace=True)
        allo2['sw_restr_allo'] = allo2['sw_restr_allo'] * allo2['restr_ratio']
        allo2['gw_restr_allo'] = allo2['gw_restr_allo'] * allo2['restr_ratio']
        allo2['total_restr_allo'] = allo2['total_restr_allo'] * allo2['restr_ratio']

        allo2.set_index(['crc', 'take_type', 'allo_block', 'wap', 'date'], inplace=True)

        setattr(self, 'restr_allo_ts', allo2)

        allo3 = allo2.groupby(level=groupby)[['sw_restr_allo', 'gw_restr_allo', 'total_restr_allo']].sum()

        return allo3


    def get_combo_ts(self, datasets, freq, groupby, sd_days=150):
        """

        """
        ### Check the dataset types
        if not np.in1d(datasets, self.dataset_types).all():
            raise ValueError('datasets must be a list that includes one or more of ' + str(self.dataset_types))

        ### Get the results and combine
        all1 = []

        if 'allo' in datasets:
            allo1 = self.get_allo_ts(freq, groupby, sd_days)
            all1.append(allo1)
        if 'metered_allo' in datasets:
            metered_allo1 = self.get_metered_allo_ts(freq, groupby, sd_days)
            all1.append(metered_allo1)
        if 'restr_allo' in datasets:
            restr_allo1 = self.get_restr_allo_ts(freq, groupby, sd_days)
            all1.append(restr_allo1)
        if 'metered_restr_allo' in datasets:
            restr_allo2 = self.get_metered_allo_ts(freq, groupby, sd_days, True)
            all1.append(restr_allo2)
        if 'usage' in datasets:
            usage1 = self.get_usage_ts(freq, groupby, sd_days)
            all1.append(usage1)

        all2 = pd.concat(all1, axis=1)

        return all2
















AlloUsage.__doc__ = filters.allo_filter.__doc__

