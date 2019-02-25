# -*- coding: utf-8 -*-
"""
Created on Wed Feb 20 15:25:06 2019

@author: michaelek
"""
import os
import numpy as np
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import OrderedDict
from datetime import datetime


#####################################
### Global parameters

date1 = pd.Timestamp.now()
date2 = date1.strftime('%Y%m%d%H%M')

sns.set_style("whitegrid")
sns.set_context('poster')
base_names = {'{}_allo': '{}_restr_allo', '{}_metered_allo': '{}_metered_restr_allo', '{}_usage': '{}_usage'}
base_label_names = {'{}_allo': '{} Allocation', '{}_restr_allo': '{} Allocation with Restrictions', '{}_metered_allo': '{} Metered Allocation', '{}_metered_restr_allo': '{} Metered Allocation with Restrictions', '{}_usage': '{} Usage'}
dict_type = {'water_supply': 'Water Supply', 'irrigation': 'Irrigation', 'stockwater': 'Stockwater', 'other': 'Other', 'industrial': 'Industrial', 'municipal': 'Municipal'}

#####################################
### Functions


def plot_group(self, freq, val='total', group='SwazName', with_restr=True, sd_days=150, irr_season=False, yaxis_mag=1000000, yaxis_lab='Million', col_pal='pastel', export_path=''):
    """
    Function to plot the summarized groups as stacked bars of the total.
    """
    plt.ioff()

    ### prepare inputs
    col_pal1 = sns.color_palette(col_pal)

    vol_names = {key.format(val): value.format(val) for key, value in base_names.items()}
    label_names = {key.format(val): value.format(val.capitalize()) for key, value in base_label_names.items()}

    groupby = ['date']
    if isinstance(group, str):
        groupby.insert(0, group)

    datasets = ['allo', 'metered_allo', 'usage']
    if with_restr:
        datasets.extend(['restr_allo', 'metered_restr_allo'])

    ### Get ts data
    ts1 = self.get_ts(datasets, freq, groupby, sd_days=sd_days, irr_season=irr_season)

    ts2 = ts1[[c for c in ts1 if val in c]] / yaxis_mag

    ### Prepare data
    top_grp = ts2.groupby(level=group)

    for i, grp1 in top_grp:

        if grp1.size > 1:

            set1 = grp1.loc[i].reset_index()

            allo_all = pd.melt(set1, id_vars='date', value_vars=list(vol_names.keys()), var_name='tot_allo')

            index1 = allo_all.date.astype('str')
#            index2 = [pd.Period(d) for d in index1.tolist()]

            ## Plot total allo
            fig, ax = plt.subplots(figsize=(15, 10))
            sns.barplot(x=index1, y='value', hue='tot_allo', data=allo_all, palette=col_pal1, edgecolor='0')

            if with_restr:
                allo_up_all = pd.melt(set1, id_vars='date', value_vars=list(vol_names.values()), var_name='up_allo')
                allo_up_all.loc[allo_up_all.up_allo.str.contains('usage'), 'value'] = 0
                sns.barplot(x=index1, y='value', hue='up_allo', data=allo_up_all, palette=col_pal1, edgecolor='0', hatch='/')
    #        plt.ylabel('Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
            plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
            plt.xlabel('Water Year')

            # Legend
            handles, lbs = ax.get_legend_handles_labels()
            order1 = [lbs.index(j) for j in label_names if j in lbs]
            labels = [label_names[i] for i in lbs]
            plt.legend([handles[i] for i in order1], labels, loc='upper left')
    #        leg1.legendPatch.set_path_effects(pathe.withStroke(linewidth=5, foreground="w"))

            # Other plotting adjustments
            xticks = ax.get_xticks()
            if len(xticks) > 15:
                for label in ax.get_xticklabels()[::2]:
                    label.set_visible(False)
                ax.xaxis_date()
                fig.autofmt_xdate(ha='center')
                plt.tight_layout()
            plt.tight_layout()
#          sns.despine(offset=10, trim=True)

            # Save figure
            plot2 = ax.get_figure()
            export_name = '_'.join([i, date1.strftime('%Y%m%d%H%M')]) + '.png'
            export_name = export_name.replace('/', '-').replace(' ', '-')
            plot2.savefig(os.path.join(export_path, export_name))
            plt.close()

    plt.ion()


def plot_stacked(self, freq, val='total', stack='use_type', group='SwazName', sd_days=150, irr_season=False, yaxis_mag=1000000, yaxis_lab='Million', col_pal='pastel', export_path=''):
    """
    Function to plot the summarized groups as stacked bars of the total.
    """
    plt.ioff()

    ### Prepare inputs
    col_pal1 = sns.color_palette(col_pal)

    vol_name = '{}_allo'.format(val)
#    label_name = {'{}_allo'.format(val): '{} Allocation'.format(val.capitalize())}

    groupby = [stack, 'date']
    if isinstance(group, str):
        groupby.insert(0, group)

    datasets = ['allo']
#    if with_restr:
#        datasets.extend(['restr_allo'])

    ### Get ts data
    ts1 = self.get_ts(datasets, freq, groupby, sd_days=sd_days, irr_season=irr_season)

    ts2 = ts1[vol_name] / yaxis_mag

    ### Reorganize data

    ts3 = ts2.unstack([0,2]).cumsum().stack([0,1]).reorder_levels([1,0,2])
    ts3.name = 'vol'
    if stack == 'use_type':
        ts3.rename(dict_type, level='use_type', inplace=True)

    top_grp = ts3.groupby(level=group)

    stack_levels = ts3.index.levels[1]
    col_lab = {stack_levels[i]: col_pal1[i] for i in np.arange(stack_levels.size)}

    for i, grp1 in top_grp:
        i
        if grp1.size > 1:

            grp2 = grp1.groupby('use_type').sum().sort_values(ascending=False).index

            fig, ax = plt.subplots(figsize=(15, 10))

            for u in grp2:
                grp3 = grp1.loc[(i, u, slice(None))]
                allo_all = pd.melt(grp3.reset_index(), id_vars='date', value_vars='vol', var_name=u)

                index1 = allo_all.date.astype('str')
                sns.barplot(x=index1, y='value', data=allo_all, edgecolor='0', color=col_lab[u], label=u)

    #        plt.ylabel('Allocated Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
            plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
            plt.xlabel('Water Year')

            # Legend
            handles, lbs = ax.get_legend_handles_labels()
            plt.legend(handles, lbs, loc='upper left')

            xticks = ax.get_xticks()
            if len(xticks) > 15:
                for label in ax.get_xticklabels()[::2]:
                    label.set_visible(False)
                ax.xaxis_date()
                fig.autofmt_xdate(ha='center')
                plt.tight_layout()
            plt.tight_layout()
    #      sns.despine(offset=10, trim=True)

            # Save figure
            plot2 = ax.get_figure()
            export_name = '_'.join([i, vol_name, stack, date1.strftime('%Y%m%d%H%M')]) + '.png'
            export_name = export_name.replace('/', '-').replace(' ', '-')
            plot2.savefig(os.path.join(export_path, export_name))
            plt.close()

    plt.ion()


#def plot_stacked_use_type(df, yaxis_mag=1000000, yaxis_lab='Million', start='1990', end='2019', agg_level=[0, 1], cat='total_allo', cat_type='use_type', col_pal='pastel', export_path='', export_name='tot_allo_type.png'):
#    """
#    Function to plot the summarized groups as stacked bars of the total.
#    """
##    plt.ioff()
#
#    ### Reorganize data
#    ## Set up dictionaries and parameters
#    dict_type = {'public_supply': 'Public Water Supply', 'irrigation': 'Irrigation', 'stockwater': 'Stockwater', 'other': 'Other', 'industry': 'Industry'}
#    order1 = np.array(['public_supply', 'irrigation', 'stockwater', 'other', 'industry'])
#    cols1 = sns.color_palette(col_pal)[3:]
#    col_dict = {'public_supply': cols1[0], 'irrigation': cols1[1], 'stockwater': cols1[2], 'other': cols1[4], 'industry': cols1[3]}
#
#    df2 = df[[cat]].copy()
#    df2a = df2.sum(axis=0, level=agg_level)
#    df3 = df2a.stack()
#    if cat_type is 'use_type':
#        cols1 = [i for i in order1 if i in df3.index.levels[0]]
#        df4 = df3.unstack(0)
#        df4 = df4[cols1]
#        dict0 = dict_type
#    else:
#        df4 = df3.unstack(1)
#    df5 = df4.cumsum(axis=1)
#    allo1 = df5.unstack()
#
#    grp1 = df4.columns.tolist()
#    lab_names = [dict0[i] for i in grp1]
#    col_lab = [col_dict[i] for i in grp1]
#
#    allo1.index = pd.to_datetime(allo1.index)
#    allo1.index.name = 'dates'
#    allo2 = allo1[start:end] * 1 / yaxis_mag
#
##    colors = sns.color_palette(col_pal)
#
#    if allo2.size > 1:
#        ### Set plotting parameters
#        sns.set_style("whitegrid")
#        sns.set_context('poster')
##        pw = len(str(yaxis_mag)) - 1
#
#        fig, ax = plt.subplots(figsize=(15, 10))
#
#        for i in grp1[::-1]:
#            seq1 = int(np.where(np.in1d(grp1, i))[0])
#            allo_all = pd.melt(allo2[i].reset_index(), id_vars='dates', value_vars=cat, var_name=i)
#
#            index1 = allo_all.dates.astype('str').str[0:4].astype('int')
#            index2 = [pd.Period(d) for d in index1.tolist()]
#            sns.barplot(x=index2, y='value', data=allo_all, edgecolor='0', color=col_lab[seq1], label=i)
#
##        plt.ylabel('Allocated Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
#        plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
#        plt.xlabel('Water Year')
#
#        # Legend
#        handles, lbs = ax.get_legend_handles_labels()
#        plt.legend(handles, lab_names[::-1], loc='upper left')
#
#        xticks = ax.get_xticks()
#        if len(xticks) > 15:
#            for label in ax.get_xticklabels()[::2]:
#                label.set_visible(False)
#            ax.xaxis_date()
#            fig.autofmt_xdate(ha='center')
#            plt.tight_layout()
#        plt.tight_layout()
##      sns.despine(offset=10, trim=True)
#        plot2 = ax.get_figure()
#        export_name = export_name.replace('/', '-')
#        plot2.savefig(os.path.join(export_path, export_name))
#        plt.close()
#
#
#def plot_group1(df, yaxis_mag=1000000, yaxis_lab='Million', start='2010', end='2018', cat=['total_allo', 'total_metered_allo', 'total_usage'], col_pal='pastel', export_path='', export_name='tot_allo_use_restr.png'):
#    """
#    Function to plot either total allocation with restrictions or total allo, metered allo, and metered usage with restrictions over a period of years.
#    """
##    plt.ioff()
#
#    col_pal1 = sns.color_palette(col_pal)
#    color_dict = {'total_allo': col_pal1[0], 'total_metered_allo': col_pal1[1], 'total_usage': col_pal1[2]}
#
#    ### Reorganize data
#    allo2 = df[start:end] * 1 / yaxis_mag
#    dict2 = {'total_allo': 'total_restr_allo', 'total_metered_allo': 'total_metered_restr_allo', 'total_usage': 'total_usage'}
#    lst1 = [d for d in cat if d in color_dict.keys()]
#    lst2 = [dict2[d] for d in cat]
#
#    if allo2.size > 1:
#
#        allo_all = pd.melt(allo2.reset_index(), id_vars='date', value_vars=list(color_dict.keys()), var_name='tot_allo')
#        allo_up_all = pd.melt(allo2.reset_index(), id_vars='date', value_vars=list(dict2.values()), var_name='up_allo')
#        allo_up_all.loc[allo_up_all.up_allo == 'total_usage', 'value'] = 0
#
#        allo_all = allo_all[np.in1d(allo_all.tot_allo, lst1)]
#        allo_up_all = allo_up_all[np.in1d(allo_up_all.up_allo, lst2)]
#
#        index1 = allo_all.date.astype('str').str[0:4].astype('int')
#        index2 = [pd.Period(d) for d in index1.tolist()]
#
#        ### Total Allo and restricted allo and usage
#        ## Set basic plot settings
#        sns.set_style("whitegrid")
#        sns.set_context('poster')
#        col_pal2 = [color_dict[i] for i in lst1]
#
#        ## Plot total allo
#        fig, ax = plt.subplots(figsize=(15, 10))
#        sns.barplot(x=index2, y='value', hue='tot_allo', data=allo_all, palette=col_pal2, edgecolor='0')
#        sns.barplot(x=index2, y='value', hue='up_allo', data=allo_up_all, palette=col_pal2, edgecolor='0', hatch='/')
##        plt.ylabel('Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
#        plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
#        plt.xlabel('Water Year')
#
#        # Legend
#        handles, lbs = ax.get_legend_handles_labels()
##        hand_len = len(handles)
#        order1 = [lbs.index(j) for j in ['total_allo', 'total_restr_allo', 'total_metered_allo', 'total_metered_restr_allo', 'total_usage'] if j in lbs]
#        label_dict1 = OrderedDict([('total_allo', ['Total allocation', 'Total allocation with restrictions']), ('total_metered_allo', ['Metered allocation', 'Metered allocation with restrictions']), ('total_usage', ['Metered usage'])])
#        labels = []
#        [labels.extend(label_dict1[i]) for i in cat]
#        plt.legend([handles[i] for i in order1], labels, loc='upper left')
##        leg1.legendPatch.set_path_effects(pathe.withStroke(linewidth=5, foreground="w"))
#
#        xticks = ax.get_xticks()
#        if len(xticks) > 15:
#            for label in ax.get_xticklabels()[::2]:
#                label.set_visible(False)
#            ax.xaxis_date()
#            fig.autofmt_xdate(ha='center')
#            plt.tight_layout()
#        plt.tight_layout()
##      sns.despine(offset=10, trim=True)
#        plot2 = ax.get_figure()
#        export_name = export_name.replace('/', '-')
#        plot2.savefig(os.path.join(export_path, export_name))
#        plt.close()

#        grp2 = grp1.loc[i].unstack(1).cumsum(axis=0)
#        grp3 = grp2.stack([0,1]).unstack([0, 1])
#
#        fig, ax = plt.subplots(figsize=(15, 10))
#
#        for vert in grp3.columns.levels[0]:
#            set1 = grp3[vert]
#
#            allo_all = pd.melt(set1.reset_index(), id_vars='date', value_vars=list(color_dict.keys()), var_name='tot_allo')
#            allo_up_all = pd.melt(set1.reset_index(), id_vars='date', value_vars=list(dict2.values()), var_name='up_allo')
#            allo_up_all.loc[allo_up_all.up_allo == 'total_usage', 'value'] = 0
#
#            index1 = allo_all.date.astype('str').str[0:4].astype('int')
#            index2 = [pd.Period(d) for d in index1.tolist()]
#
#            sns.barplot(x=index2, y='value', hue='tot_allo', data=allo_all, palette=col_pal1, edgecolor='0')
#            sns.barplot(x=index2, y='value', hue='up_allo', data=allo_up_all, palette=col_pal1, edgecolor='0', hatch='/')
#
#
#
#    df2a = df2.sum(axis=0, level=agg_level)
#    df3 = df2a.stack()
#    if cat_type is 'use_type':
#        cols1 = [i for i in order1 if i in df3.index.levels[0]]
#        df4 = df3.unstack(0)
#        df4 = df4[cols1]
#        dict0 = dict_type
#    else:
#        df4 = df3.unstack(1)
#    df5 = df4.cumsum(axis=1)
#    allo1 = df5.unstack()
#
#    grp1 = df4.columns.tolist()
#    lab_names = [dict0[i] for i in grp1]
#    col_lab = [col_dict[i] for i in grp1]
#
#    allo1.index = pd.to_datetime(allo1.index)
#    allo1.index.name = 'dates'
#    allo2 = allo1[start:end] * 1 / yaxis_mag
#
##    colors = sns.color_palette(col_pal)
#
#    if allo2.size > 1:
#        ### Set plotting parameters
#        sns.set_style("whitegrid")
#        sns.set_context('poster')
##        pw = len(str(yaxis_mag)) - 1
#
#        fig, ax = plt.subplots(figsize=(15, 10))
#
#        for i in grp1[::-1]:
#            seq1 = int(np.where(np.in1d(grp1, i))[0])
#            allo_all = pd.melt(allo2[i].reset_index(), id_vars='dates', value_vars=cat, var_name=i)
#
#            index1 = allo_all.dates.astype('str').str[0:4].astype('int')
#            index2 = [pd.Period(d) for d in index1.tolist()]
#            sns.barplot(x=index2, y='value', data=allo_all, edgecolor='0', color=col_lab[seq1], label=i)
#
##        plt.ylabel('Allocated Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
#        plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
#        plt.xlabel('Water Year')
#
#        # Legend
#        handles, lbs = ax.get_legend_handles_labels()
#        plt.legend(handles, lab_names[::-1], loc='upper left')
#
#        xticks = ax.get_xticks()
#        if len(xticks) > 15:
#            for label in ax.get_xticklabels()[::2]:
#                label.set_visible(False)
#            ax.xaxis_date()
#            fig.autofmt_xdate(ha='center')
#            plt.tight_layout()
#        plt.tight_layout()
##      sns.despine(offset=10, trim=True)
#        plot2 = ax.get_figure()
#        export_name = export_name.replace('/', '-')
#        plot2.savefig(os.path.join(export_path, export_name))
#        plt.close()


