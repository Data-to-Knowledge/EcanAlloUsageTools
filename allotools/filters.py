# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 16:40:35 2018

@author: michaelek
"""
import pandas as pd
from pdsql import mssql
import parameters as param


#########################################
### Functions


def ts_filter(allo, wap_allo, from_date='1900-07-01', to_date='2020-06-30', in_allo=True):
    """
    Function to take an allo DataFrame and filter out the consents that cannot be converted to a time series due to missing data.
    """
    allo.loc[:, 'to_date'] = pd.to_datetime(allo.loc[:, 'to_date'], errors='coerce')
    allo.loc[:, 'from_date'] = pd.to_datetime(allo.loc[:, 'from_date'], errors='coerce')
    allo1 = allo[allo.take_type.isin(['Take Surface Water', 'Take Groundwater'])]

    ### Remove consents without daily volumes (and consequently yearly volumes)
    allo2 = allo1[allo1.daily_vol.notnull()]

    ### Remove consents without to/from dates or date ranges of less than a month
    allo3 = allo2[allo2['from_date'].notnull() & allo2['to_date'].notnull()]

    ### Restrict dates
    start_time = pd.Timestamp(from_date)
    end_time = pd.Timestamp(to_date)

    allo4 = allo3[(allo3['to_date'] - start_time).dt.days > 31]
    allo5 = allo4[(end_time - allo4['from_date']).dt.days > 31]

    allo5 = allo5[(allo5['to_date'] - allo5['from_date']).dt.days > 31]

    ### Restrict by status_details
    allo6 = allo5[allo5.crc_status.isin(param.status_codes)]

    ### In allocation columns
    if in_allo:
        wap_allo = wap_allo[(wap_allo.take_type == 'Take Surface Water') & (wap_allo.in_sw_allo) | (wap_allo.take_type == 'Take Groundwater')]
        allo6 = allo6[(allo6.take_type == 'Take Surface Water') | ((allo6.take_type == 'Take Groundwater') & (allo6.in_gw_allo))]
        allo6 = allo6[(allo6.take_type == 'Take Groundwater') | allo6.crc.isin(wap_allo.crc.unique())]

    ### Select the crc_waps
    wap_allo2 = pd.merge(wap_allo, allo6[['crc', 'take_type', 'allo_block']], on=['crc', 'take_type', 'allo_block'], how='inner')
    allo6 = pd.merge(allo6, wap_allo2[['crc', 'take_type', 'allo_block']].drop_duplicates(), on=['crc', 'take_type', 'allo_block'], how='inner')

    ### Return
    return allo6, wap_allo2


def allo_filter(server, from_date=None, to_date=None, site_filter=None, crc_filter=None, crc_wap_filter=None, in_allo=True, include_hydroelectric=False):
    """
    Function to filter consents and WAPs in various ways.

    Parameters
    ----------
    server : str
        The server of the Hydro db.
    from_date : str
        The start date for the time series.
    to_date: str
        The end date for the time series.
    site_filter : list or dict
        If site_filter is a list, then it should represent the columns from the ExternalSite table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    crc_filter : list or dict
        If crc_filter is a list, then it should represent the columns from the CrcAllo table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    crc_wap_filter : list or dict
        If crc_wap_filter is a list, then it should represent the columns from the CrcWapAllo table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    in_allo : bool
        Should only the consumptive takes be returned?

    Returns
    -------
    Three DataFrames
        Representing the filters on the ExternalSites, CrcAllo, and CrcWapAllo
    """
    ### ExternalSite
    site_cols = ['ExtSiteID', 'NZTMX', 'NZTMY']
    if isinstance(site_filter, dict):
        extra_site_cols = list(site_filter.keys())
        site_cols.extend(extra_site_cols)
    elif isinstance(site_filter, list):
        site_cols.extend(site_filter)
        site_filter = None
    sites = mssql.rd_sql(server, param.database, param.site_table, site_cols, site_filter)
    sites1 = sites[sites.ExtSiteID.str.contains('[A-Z]+\d\d/\d+')].copy()

    ### CrcWapAllo
    crc_wap_cols = set(['crc', 'take_type', 'allo_block', 'wap', 'max_rate_wap', 'in_sw_allo', 'sd1_7', 'sd1_30', 'sd1_150'])
    if isinstance(crc_wap_filter, dict):
        extra_crc_wap_cols = set(crc_wap_filter.keys())
        crc_wap_cols.update(extra_crc_wap_cols)
    elif isinstance(crc_wap_filter, list):
        crc_wap_cols.update(set(crc_wap_filter))
        crc_wap_filter = None
    crc_wap = mssql.rd_sql(server, param.database, param.wap_allo_table, list(crc_wap_cols), crc_wap_filter)
    crc_wap1 = crc_wap[crc_wap.wap.isin(sites1.ExtSiteID)]

    ### CrcAllo
    crc_cols = set(['crc', 'take_type', 'allo_block', 'max_rate_crc', 'daily_vol', 'feav', 'crc_status', 'from_date', 'to_date', 'from_month', 'to_month', 'in_gw_allo', 'use_type'])
    if isinstance(crc_filter, dict):
        extra_crc_cols = set(crc_filter.keys())
        crc_cols.update(extra_crc_cols)
        if 'use_type' in crc_filter:
            reverse_dict = {}
            for key, val in param.use_type_dict.items():
                try:
                    reverse_dict[val].append(key)
                except:
                    reverse_dict[val] = [key]
            use_types = []
            [use_types.extend(reverse_dict[val]) for val in crc_filter['use_type']]
            crc_filter = crc_filter.copy()
            crc_filter.update({'use_type': use_types})
    elif isinstance(crc_wap_filter, list):
        crc_cols.update(set(crc_filter))
        crc_filter = None
    crc_allo = mssql.rd_sql(server, param.database, param.allo_table, list(crc_cols), crc_filter)
    crc_allo.replace({'use_type': param.use_type_dict}, inplace=True)
    if not include_hydroelectric:
        crc_allo = crc_allo[crc_allo.use_type != 'hydroelectric']
    crc_allo1 = pd.merge(crc_allo, crc_wap1[['crc', 'take_type', 'allo_block']].drop_duplicates(), on=['crc', 'take_type', 'allo_block'])

    ## Update the CrcAllo table
    crc_wap1 = pd.merge(crc_wap1, crc_allo1[['crc', 'take_type', 'allo_block']].drop_duplicates(), on=['crc', 'take_type', 'allo_block'])

    ### Time series filtering
    if (from_date is None) and (to_date is None):
        crc_allo2 = crc_allo1.copy()
        crc_wap2 = crc_wap1.copy()
    elif isinstance(from_date, str) and isinstance(to_date, str):
        crc_allo2, crc_wap2 = ts_filter(crc_allo1, crc_wap1, from_date, to_date, in_allo)
    else:
        raise ValueError('from_date and to_date must both be either None or strings')
    sites2 = sites1[sites1.ExtSiteID.isin(crc_wap2.wap.unique())].copy()

    ### Index the DataFrames
    crc_allo2.set_index(['crc', 'take_type', 'allo_block'], inplace=True)
    crc_wap2.set_index(['crc', 'take_type', 'allo_block', 'wap'], inplace=True)
    sites2.set_index('ExtSiteID', inplace=True)

    return sites2, crc_allo2, crc_wap2
