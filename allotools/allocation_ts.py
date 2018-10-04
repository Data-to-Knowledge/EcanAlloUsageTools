# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 09:55:07 2018

@author: michaelek
"""
import numpy as np
import pandas as pd
from allotools.filters import allo_filter
import allotools.parameters as param

###################################
### Functions


def allo_ts_apply(row, from_date, to_date, freq, restr_col, remove_months=False):
    """
    Pandas apply function that converts the allocation data to a monthly time series.
    """

    crc_from_date = pd.Timestamp(row['from_date'])
    crc_to_date = pd.Timestamp(row['to_date'])
    start = pd.Timestamp(from_date)
    end = pd.Timestamp(to_date)

    if crc_from_date > start:
        start = crc_from_date
    if crc_to_date < end:
        end = crc_to_date

    end_date = end - pd.DateOffset(hours=1) + pd.tseries.frequencies.to_offset(freq)
    dates1 = pd.date_range(start, end_date, freq=freq)
    if remove_months and 'A' not in freq:
        mon1 = np.arange(row['from_month'], 13)
        mon2 = np.arange(1, row['to_month'] + 1)
        in_mons = np.concatenate((mon1, mon2))
        dates1 = dates1[dates1.month.isin(in_mons)]
    dates2 = dates1 - pd.tseries.frequencies.to_offset(freq)
    diff_days1 = (dates1 - dates2).days.values
    diff_days2 = diff_days1.copy()

    if freq in ['A-JUN', 'D', 'W']:
        vol1 = row[restr_col]
    elif 'M' in freq:
        vol1 = dates1.daysinmonth.values / 365.0 * row[restr_col]
    else:
        raise ValueError("freq must be either 'A-JUN', 'M', or 'D'")

    if len(diff_days1) == 1:
        diff_days2[0] = diff_days1[0] - (dates1[-1] - end).days - (diff_days1[0] - (dates1[0] - start).days)
    else:
        diff_days2[0] = (dates1[0] - start).days + 1
        diff_days2[-1] = diff_days1[-1] - (dates1[-1] - end).days
    ratio_days = diff_days2/diff_days1

    vols = pd.Series((ratio_days * vol1).round(), index=dates1)

    return vols


def allo_ts(server, from_date, to_date, freq, restr_type, site_filter=None, crc_filter=None, crc_wap_filter=None, remove_months=False, in_allo=True):
    """
    Combo function to completely create a time series from the allocation DataFrame. Source data must be from an instance of the Hydro db.

    Parameters
    ----------
    server : str
        The server where the Hydro db lays.
    from_date : str
        The start date for the time series.
    to_date: str
        The end date for the time series.
    freq : str
        Pandas frequency str. Must be 'D', 'W', 'M', or 'A-JUN'.
    restr_type : str
        The allocation rate/volume used as the values in the time series. Must be 'max rate', 'daily volume', or 'annual volume'.
    remove_months : bool
        Should the months that are defined in the consent only be returned?
    in_allo : bool
        Should the consumptive consents be returned?

    Returns
    -------
    Series
        indexed by crc, take_type, and allo_block
    """

    if restr_type not in param.restr_type_dict:
        raise ValueError('restr_type must be one of ' + str(list(param.restr_type_dict.keys())))
    if freq not in param.freq_codes:
        raise ValueError('freq must be one of ' + str(param.freq_codes))

    sites, allo2, wap_allo = allo_filter(server, from_date=from_date, to_date=to_date, site_filter=site_filter, crc_filter=crc_filter, crc_wap_filter=crc_wap_filter, in_allo=in_allo)

    restr_col = param.restr_type_dict[restr_type]

    allo3 = allo2.apply(allo_ts_apply, axis=1, from_date=from_date, to_date=to_date, freq=freq, restr_col=restr_col, remove_months=remove_months)

    allo4 = allo3.stack()
    allo4.index.set_names(['crc', 'take_type', 'allo_block', 'date'], inplace=True)
    allo4.name = 'allo'

    return allo4
