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
import plot as allo_plot
#import allotools.parameters as param
import parameters as param
from datetime import datetime
import util

########################################
### Core class


class AlloUsage(object):

    dataset_types = param.dataset_types
    plot = allo_plot

    ### Initial import and assignment function
    def __init__(self, from_date=None, to_date=None, site_filter=None, crc_filter=None, crc_wap_filter=None, in_allo=True, include_hydroelectric=False):
        """

        Parameters
        ----------

        sd_days : int
            The stream depletion effect on groundwater takes. The value is the number of days of pumping. Accepted values are 7, 30, and 150.

        Returns
        -------


        """
        sites, allo, allo_wap = filters.allo_filter(param.server, from_date, to_date, site_filter, crc_filter, crc_wap_filter, in_allo, include_hydroelectric)
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


    def _sw_gw_split_allo(self):
        """
        Function to split the total allo into a SW and GW allocation.
        """
        allo5 = pd.merge(self.total_allo_ts.reset_index(), self.allo_wap.reset_index(), on=['crc', 'take_type', 'allo_block'], how='left')

        ## re-proportion the allocation
        allo5['total_allo'] = allo5['total_allo'] * allo5['rate_ratio']
        allo5['sw_allo'] = allo5['total_allo'] * allo5[param.sd_dict[self.sd_days]] * 0.01
        allo5['gw_allo'] = allo5['total_allo'] - allo5['sw_allo']
        allo5.loc[allo5['gw_allo'] < 0, 'gw_allo'] = 0

        ### Rearrange
        allo6 = allo5[['crc', 'take_type', 'allo_block', 'wap', 'date', 'sw_allo', 'gw_allo', 'total_allo']].copy()
        allo6.set_index(['crc', 'take_type', 'allo_block', 'wap', 'date'], inplace=True)

        setattr(self, 'allo_ts', allo6)

        return allo6


    def _est_allo_ts(self):
        """

        """
        restr_col = param.allo_type_dict[self.freq]

        allo3 = self.allo.apply(allo_ts_apply, axis=1, from_date=self.from_date, to_date=self.to_date, freq=self.freq, restr_col=restr_col, remove_months=True)

        allo4 = allo3.stack()
        allo4.index.set_names(['crc', 'take_type', 'allo_block', 'date'], inplace=True)
        allo4.name = 'total_allo'

        if self.irr_season:
            dates1 = self.total_allo_ts.index.levels[3]
            dates2 = dates1[dates1.month.isin([10, 11, 12, 1, 2, 3, 4])]
            allo4 = allo4.loc[(slice(None), slice(None), slice(None), dates2)]

        setattr(self, 'total_allo_ts', allo4)


    def _get_allo_ts(self):
        """
        Function to create an allocation time series.

        Parameters
        ----------
        freq : str
            Pandas frequency str. Must be 'D', 'W', 'M', 'A-JUN', or 'A'.
        groupby : str or list of str
            A list of the combination of fields that the output should be aggregated to. Possible fields include: 'crc', 'take_type', 'allo_block', 'date', and 'wap'. Being a time series function, you should always add 'date' to the groupby.

        Returns
        -------
        Series
            indexed by crc, take_type, and allo_block
        """

        if isinstance(self.groupby, str):
            self.groupby = [self.groupby]
        if self.freq not in param.allo_type_dict:
            raise ValueError('freq must be one of ' + str(param.allo_type_dict))

        if not hasattr(self, 'total_allo_ts'):
            self._est_allo_ts()

        ### Convert to GW and SW allocation

        allo6 = self._sw_gw_split_allo()

        ### Return groupby

        allo7 = allo6.groupby(level=self.groupby).sum()

        return allo7


    def _get_metered_allo_ts(self, restr_allo=False, proportion_allo=True):
        """

        """
        setattr(self, 'proportion_allo', proportion_allo)

        ### Get the allocation ts either total or metered
        if restr_allo:
            if not hasattr(self, 'restr_allo_ts'):
                allo1 = self._get_restr_allo_ts()
            allo1 = self.restr_allo_ts.drop(['site', 'band_num', 'restr_ratio'], axis=1).copy().reset_index()
            rename_dict = {'sw_restr_allo': 'sw_metered_restr_allo', 'gw_restr_allo': 'gw_metered_restr_allo', 'total_restr_allo': 'total_metered_restr_allo'}
        else:
            if not hasattr(self, 'allo_ts'):
                allo1 = self._get_allo_ts()
            allo1 = self.allo_ts.copy().reset_index()
            rename_dict = {'sw_allo': 'sw_metered_allo', 'gw_allo': 'gw_metered_allo', 'total_allo': 'total_metered_allo'}

        ### Combine the usage data to the allo data
        if hasattr(self, 'usage_crc_ts'):
            allo2 = pd.merge(self.usage_crc_ts.reset_index()[['crc', 'take_type', 'allo_block', 'wap', 'date']], allo1, on=['crc', 'take_type', 'allo_block', 'wap', 'date'], how='right', indicator=True)
        else:
            if not hasattr(self, 'ts_usage_summ'):
                self._usage_summ()
            usage_waps = self.ts_usage_summ.wap.unique()

            allo_wap = self.allo_wap.copy().reset_index()
            allo_wap1 = allo_wap[allo_wap.wap.isin(usage_waps)].copy()

            allo2 = pd.merge(allo_wap1[['crc', 'take_type', 'allo_block', 'wap']], allo1, on=['crc', 'take_type', 'allo_block', 'wap'], how='right', indicator=True)

        ## Re-categorise
        allo2['_merge'] = allo2._merge.cat.rename_categories({'left_only': 2, 'right_only': 0, 'both': 1}).astype(int)

        if proportion_allo:
            allo2.loc[allo2._merge != 1, list(rename_dict.keys())] = 0
            allo3 = allo2.drop('_merge', axis=1).copy()
        else:
            allo2['usage_waps'] = allo2.groupby(['crc', 'take_type', 'allo_block', 'date'])['_merge'].transform('sum')

            allo2.loc[allo2.usage_waps == 0, list(rename_dict.keys())] = 0
            allo3 = allo2.drop(['_merge', 'usage_waps'], axis=1).copy()

        allo3.rename(columns=rename_dict, inplace=True)

        if 'total_metered_allo' in allo3:
            setattr(self, 'metered_allo_ts', allo3)
        else:
            setattr(self, 'metered_restr_allo_ts', allo3)

        allo4 = allo3.groupby(self.groupby).sum()

#        setattr(self, 'metered_allo_ts', allo3)
        return allo4


    def _process_usage(self):
        """

        """
        ### Get the ts summary tables
        if not hasattr(self, 'ts_usage_summ'):
            self._usage_summ()
        ts_usage_summ = self.ts_usage_summ.copy()

        ## Get the ts data and aggregate
        if hasattr(self, 'usage_ts_daily'):
            tsdata1 = self.usage_ts_daily
        else:
            tsdata1 = mssql.rd_sql(self.server, param.database, param.ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_in={'ExtSiteID': ts_usage_summ.wap.unique().tolist(), 'DatasetTypeID': ts_usage_summ.DatasetTypeID.unique().tolist()}, from_date=self.from_date, to_date=self.to_date, date_col='DateTime')

            tsdata1['DateTime'] = pd.to_datetime(tsdata1['DateTime'])
            tsdata1.rename(columns={'DateTime': 'date', 'ExtSiteID': 'wap', 'Value': 'total_usage'}, inplace=True)

            ### filter - remove individual spikes
            tsdata1[tsdata1['total_usage'] < 0 ] = 0

            def remove_spikes(x):
                val1 = bool(x[1] > (x[0] + x[2] + 2))
                if val1:
                    return (x[0] + x[2])/2
                else:
                    return x[1]

            tsdata1.iloc[1:-1, 2] = tsdata1['total_usage'].rolling(3, center=True).apply(remove_spikes, raw=True).iloc[1:-1]

            setattr(self, 'usage_ts_daily', tsdata1)

        ### Aggregate
        tsdata2 = util.grp_ts_agg(tsdata1, ['wap'], 'date', self.freq).sum()

        setattr(self, 'usage_ts', tsdata2)


    def _get_usage_ts(self):
        """

        """
        ### Get the usage data if it exists
        if not hasattr(self, 'usage_ts'):
            self._process_usage()
        tsdata2 = self.usage_ts.copy()

        if not hasattr(self, 'allo_ts'):
            allo1 = self._get_allo_ts()
        allo1 = self.allo_ts.copy().reset_index()

        allo1['combo_allo'] = allo1.groupby(['wap', 'date'])['total_allo'].transform('sum')
        allo1['combo_ratio'] = allo1['total_allo']/allo1['combo_allo']

        ### combine with consents info
        usage1 = pd.merge(allo1, tsdata2, on=['wap', 'date'])
        usage1['total_usage'] = usage1['total_usage'] * usage1['combo_ratio']

        ### Remove high outliers
        usage1.loc[usage1['total_usage'] > (usage1['total_allo'] * 1.8), 'total_usage'] = np.nan

        ### Split the GW and SW components
        usage1['sw_ratio'] = usage1['sw_allo']/usage1['total_allo']

        usage1['sw_usage'] = usage1['sw_ratio'] * usage1['total_usage']
        usage1['gw_usage'] = usage1['total_usage'] - usage1['sw_usage']
        usage1.loc[usage1['gw_usage'] < 0, 'gw_usage'] = 0

        usage1.drop(['sw_allo', 'gw_allo', 'total_allo', 'combo_allo', 'combo_ratio', 'sw_ratio'], axis=1, inplace=True)

        usage2 = usage1.dropna().set_index(['crc', 'take_type', 'allo_block', 'wap', 'date'])

        setattr(self, 'usage_crc_ts', usage2)

        ### Groupby
        usage3 = usage2.groupby(level=self.groupby).sum()

        return usage3


    def _lowflow_data(self):
        """

        """
        if hasattr(self, 'lf_restr_daily'):
            lf_crc2 = self.lf_restr_daily
        else:
            lf_band1 = mssql.rd_sql(self.server, param.database, param.lf_band_table, ['site', 'band_num', 'date', 'band_allo'], {'site_type': ['LowFlow']}, from_date=self.from_date, to_date=self.to_date, date_col='date')
            lf_band1['date'] = pd.to_datetime(lf_band1['date'])
            lf_band1.loc[lf_band1.band_allo > 100, 'band_allo'] = 100

            lf_crc1 = mssql.rd_sql(self.server, param.database, param.lf_band_crc_table, ['site', 'band_num', 'date', 'crc'],  from_date=self.from_date, to_date=self.to_date, date_col='date')
            lf_crc1['date'] = pd.to_datetime(lf_crc1['date'])

            lf_crc2 = pd.merge(lf_crc1, lf_band1, on=['site', 'band_num', 'date'])
            setattr(self, 'lf_restr_daily', lf_crc2)

        lf_crc3 = util.grp_ts_agg(lf_crc2, ['crc', 'site', 'band_num'], 'date', self.freq)['band_allo'].mean() * 0.01
        lf_crc3.name = 'restr_ratio'

        lf_crc4 = lf_crc3.sort_values().reset_index()
        lf_crc5 = lf_crc4.groupby(['crc', 'date']).first()

        setattr(self, 'lf_restr', lf_crc5)


    def _get_restr_allo_ts(self):
        """

        """
        ### Get the allocation ts
        if not hasattr(self, 'allo_ts'):
            allo1 = self._get_allo_ts()
        if not hasattr(self, 'lf_restr'):
            self._lowflow_data()

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

        allo3 = allo2.groupby(level=self.groupby)[['sw_restr_allo', 'gw_restr_allo', 'total_restr_allo']].sum()

        return allo3


    def get_ts(self, datasets, freq, groupby, sd_days=150, irr_season=False):
        """

        """
        ### Check the dataset types
        if not np.in1d(datasets, self.dataset_types).all():
            raise ValueError('datasets must be a list that includes one or more of ' + str(self.dataset_types))

        ### Check new to old parameters and remove attributes if necessary
        if hasattr(self, 'freq'):
            if (self.freq != freq) or (self.groupby != groupby) or (self.sd_days != sd_days) or (self.irr_season != irr_season):
                delattr(self, 'allo_ts')
                delattr(self, 'total_allo_ts')
                if hasattr(self, 'restr_allo_ts'):
                    delattr(self, 'restr_allo_ts')
                if hasattr(self, 'lf_restr'):
                    delattr(self, 'lf_restr')
                if hasattr(self, 'usage_crc_ts'):
                    delattr(self, 'usage_crc_ts')
                if hasattr(self, 'usage_ts'):
                    delattr(self, 'usage_ts')
                if hasattr(self, 'usage_ts'):
                    delattr(self, 'usage_ts')

        ### Assign pararameters
        setattr(self, 'freq', freq)
        setattr(self, 'groupby', groupby)
        setattr(self, 'sd_days', sd_days)
        setattr(self, 'irr_season', irr_season)

        ### Get the results and combine
        all1 = []

        if 'allo' in datasets:
            allo1 = self._get_allo_ts()
            all1.append(allo1)
        if 'metered_allo' in datasets:
            metered_allo1 = self._get_metered_allo_ts()
            all1.append(metered_allo1)
        if 'restr_allo' in datasets:
            restr_allo1 = self._get_restr_allo_ts()
            all1.append(restr_allo1)
        if 'metered_restr_allo' in datasets:
            restr_allo2 = self._get_metered_allo_ts(True)
            all1.append(restr_allo2)
        if 'usage' in datasets:
            usage1 = self._get_usage_ts()
            all1.append(usage1)


        all2 = pd.concat(all1, axis=1)

        return all2
















AlloUsage.__doc__ = filters.allo_filter.__doc__

