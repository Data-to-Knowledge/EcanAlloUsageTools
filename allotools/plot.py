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


date1 = pd.Timestamp.now()
date2 = date1.strftime('%Y%m%d%H%M')


def plot_stacked_use_type(df, yaxis_mag=1000000, yaxis_lab='Million', start='1990', end='2019', agg_level=[0, 1], cat='total_allo', cat_type='use_type', col_pal='pastel', export_path='', export_name='tot_allo_type.png'):
    """
    Function to plot the summarized groups as stacked bars of the total.
    """
#    plt.ioff()

    ### Reorganize data
    ## Set up dictionaries and parameters
    dict_type = {'public_supply': 'Public Water Supply', 'irrigation': 'Irrigation', 'stockwater': 'Stockwater', 'other': 'Other', 'industry': 'Industry'}
    order1 = np.array(['public_supply', 'irrigation', 'stockwater', 'other', 'industry'])
    cols1 = sns.color_palette(col_pal)[3:]
    col_dict = {'public_supply': cols1[0], 'irrigation': cols1[1], 'stockwater': cols1[2], 'other': cols1[4], 'industry': cols1[3]}

    df2 = df[[cat]].copy()
    df2a = df2.sum(axis=0, level=agg_level)
    df3 = df2a.stack()
    if cat_type is 'use_type':
        cols1 = [i for i in order1 if i in df3.index.levels[0]]
        df4 = df3.unstack(0)
        df4 = df4[cols1]
        dict0 = dict_type
    else:
        df4 = df3.unstack(1)
    df5 = df4.cumsum(axis=1)
    allo1 = df5.unstack()

    grp1 = df4.columns.tolist()
    lab_names = [dict0[i] for i in grp1]
    col_lab = [col_dict[i] for i in grp1]

    allo1.index = pd.to_datetime(allo1.index)
    allo1.index.name = 'dates'
    allo2 = allo1[start:end] * 1 / yaxis_mag

#    colors = sns.color_palette(col_pal)

    if allo2.size > 1:
        ### Set plotting parameters
        sns.set_style("whitegrid")
        sns.set_context('poster')
#        pw = len(str(yaxis_mag)) - 1

        fig, ax = plt.subplots(figsize=(15, 10))

        for i in grp1[::-1]:
            seq1 = int(np.where(np.in1d(grp1, i))[0])
            allo_all = pd.melt(allo2[i].reset_index(), id_vars='dates', value_vars=cat, var_name=i)

            index1 = allo_all.dates.astype('str').str[0:4].astype('int')
            index2 = [pd.Period(d) for d in index1.tolist()]
            sns.barplot(x=index2, y='value', data=allo_all, edgecolor='0', color=col_lab[seq1], label=i)

#        plt.ylabel('Allocated Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
        plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
        plt.xlabel('Water Year')

        # Legend
        handles, lbs = ax.get_legend_handles_labels()
        plt.legend(handles, lab_names[::-1], loc='upper left')

        xticks = ax.get_xticks()
        if len(xticks) > 15:
            for label in ax.get_xticklabels()[::2]:
                label.set_visible(False)
            ax.xaxis_date()
            fig.autofmt_xdate(ha='center')
            plt.tight_layout()
        plt.tight_layout()
#      sns.despine(offset=10, trim=True)
        plot2 = ax.get_figure()
        export_name = export_name.replace('/', '-')
        plot2.savefig(os.path.join(export_path, export_name))
        plt.close()


def plot_group(df, yaxis_mag=1000000, yaxis_lab='Million', start='2010', end='2018', cat=['total_allo', 'total_metered_allo', 'total_usage'], col_pal='pastel', export_path='', export_name='tot_allo_use_restr.png'):
    """
    Function to plot either total allocation with restrictions or total allo, metered allo, and metered usage with restrictions over a period of years.
    """
#    plt.ioff()

    col_pal1 = sns.color_palette(col_pal)
    color_dict = {'total_allo': col_pal1[0], 'total_metered_allo': col_pal1[1], 'total_usage': col_pal1[2]}

    ### Reorganize data
    allo2 = df[start:end] * 1 / yaxis_mag
    dict2 = {'total_allo': 'total_restr_allo', 'total_metered_allo': 'total_metered_restr_allo', 'total_usage': 'total_usage'}
    lst1 = [d for d in cat if d in color_dict.keys()]
    lst2 = [dict2[d] for d in cat]

    if allo2.size > 1:

        allo_all = pd.melt(allo2.reset_index(), id_vars='date', value_vars=list(color_dict.keys()), var_name='tot_allo')
        allo_up_all = pd.melt(allo2.reset_index(), id_vars='date', value_vars=list(dict2.values()), var_name='up_allo')
        allo_up_all.loc[allo_up_all.up_allo == 'total_usage', 'value'] = 0

        allo_all = allo_all[np.in1d(allo_all.tot_allo, lst1)]
        allo_up_all = allo_up_all[np.in1d(allo_up_all.up_allo, lst2)]

        index1 = allo_all.date.astype('str').str[0:4].astype('int')
        index2 = [pd.Period(d) for d in index1.tolist()]

        ### Total Allo and restricted allo and usage
        ## Set basic plot settings
        sns.set_style("whitegrid")
        sns.set_context('poster')
        col_pal2 = [color_dict[i] for i in lst1]

        ## Plot total allo
        fig, ax = plt.subplots(figsize=(15, 10))
        sns.barplot(x=index2, y='value', hue='tot_allo', data=allo_all, palette=col_pal2, edgecolor='0')
        sns.barplot(x=index2, y='value', hue='up_allo', data=allo_up_all, palette=col_pal2, edgecolor='0', hatch='/')
#        plt.ylabel('Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
        plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
        plt.xlabel('Water Year')

        # Legend
        handles, lbs = ax.get_legend_handles_labels()
#        hand_len = len(handles)
        order1 = [lbs.index(j) for j in ['total_allo', 'total_restr_allo', 'total_metered_allo', 'total_metered_restr_allo', 'total_usage'] if j in lbs]
        label_dict1 = OrderedDict([('total_allo', ['Total allocation', 'Total allocation with restrictions']), ('total_metered_allo', ['Metered allocation', 'Metered allocation with restrictions']), ('total_usage', ['Metered usage'])])
        labels = []
        [labels.extend(label_dict1[i]) for i in cat]
        plt.legend([handles[i] for i in order1], labels, loc='upper left')
#        leg1.legendPatch.set_path_effects(pathe.withStroke(linewidth=5, foreground="w"))

        xticks = ax.get_xticks()
        if len(xticks) > 15:
            for label in ax.get_xticklabels()[::2]:
                label.set_visible(False)
            ax.xaxis_date()
            fig.autofmt_xdate(ha='center')
            plt.tight_layout()
        plt.tight_layout()
#      sns.despine(offset=10, trim=True)
        plot2 = ax.get_figure()
        export_name = export_name.replace('/', '-')
        plot2.savefig(os.path.join(export_path, export_name))
        plt.close()


def plot_group_yr(self, horizontal='total', multi='SwazName', with_restr=True, sd_days=150, irr_season=False, yaxis_mag=1000000, yaxis_lab='Million', col_pal='pastel', export_path=''):
    """
    Function to plot the summarized groups as stacked bars of the total.
    """
    plt.ioff()

    ### prepare inputs
    sns.set_style("whitegrid")
    sns.set_context('poster')
    col_pal1 = sns.color_palette(col_pal)
    color_dict = {'total_allo': col_pal1[0], 'total_metered_allo': col_pal1[1], 'total_usage': col_pal1[2]}
    dict2 = {'total_allo': 'total_restr_allo', 'total_metered_allo': 'total_metered_restr_allo', 'total_usage': 'total_usage'}

    groupby = ['date']
    if isinstance(multi, str):
        groupby.insert(0, multi)

    datasets = ['allo', 'metered_allo', 'usage']
    if with_restr:
        datasets.extend(['restr_allo', 'metered_restr_allo'])

    ### Get ts data
    ts1 = self.get_ts(datasets, 'A-JUN', groupby, sd_days=sd_days, irr_season=irr_season)

    ts2 = ts1[[c for c in ts1 if horizontal in c]] / yaxis_mag

    ### Prepare data
    top_grp = ts2.groupby(level=multi)

    for i, grp1 in top_grp:

        if grp1.size > 1:

            set1 = grp1.loc[i].reset_index()

            allo_all = pd.melt(set1, id_vars='date', value_vars=list(color_dict.keys()), var_name='tot_allo')
            allo_up_all = pd.melt(set1, id_vars='date', value_vars=list(dict2.values()), var_name='up_allo')
            allo_up_all.loc[allo_up_all.up_allo.str.contains('usage'), 'value'] = 0

            index1 = allo_all.date.astype('str').str[0:4].astype('int')
            index2 = [pd.Period(d) for d in index1.tolist()]

            ### Total Allo and restricted allo and usage
            ## Set basic plot settings
            sns.set_style("whitegrid")
            sns.set_context('poster')

            ## Plot total allo
            fig, ax = plt.subplots(figsize=(15, 10))
            sns.barplot(x=index2, y='value', hue='tot_allo', data=allo_all, palette=col_pal1, edgecolor='0')
            sns.barplot(x=index2, y='value', hue='up_allo', data=allo_up_all, palette=col_pal1, edgecolor='0', hatch='/')
    #        plt.ylabel('Water Volume $(10^{' + str(pw) + '} m^{3}/year$)')
            plt.ylabel('Water Volume $(' + yaxis_lab + '\; m^{3}/year$)')
            plt.xlabel('Water Year')

            # Legend
            handles, lbs = ax.get_legend_handles_labels()
    #        hand_len = len(handles)
            order1 = [lbs.index(j) for j in ['total_allo', 'total_restr_allo', 'total_metered_allo', 'total_metered_restr_allo', 'total_usage'] if j in lbs]
            label_dict1 = OrderedDict([('total_allo', ['Total allocation', 'Total allocation with restrictions']), ('total_metered_allo', ['Metered allocation', 'Metered allocation with restrictions']), ('total_usage', ['Metered usage'])])
            labels = []
            [labels.extend(label_dict1[i]) for i in dict2]
            plt.legend([handles[i] for i in order1], labels, loc='upper left')
    #        leg1.legendPatch.set_path_effects(pathe.withStroke(linewidth=5, foreground="w"))

            xticks = ax.get_xticks()
            if len(xticks) > 15:
                for label in ax.get_xticklabels()[::2]:
                    label.set_visible(False)
                ax.xaxis_date()
                fig.autofmt_xdate(ha='center')
                plt.tight_layout()
            plt.tight_layout()
    #      sns.despine(offset=10, trim=True)
            plot2 = ax.get_figure()
            export_name = '_'.join([i, date1.strftime('%Y%m%d%H%M')]) + '.png'
            export_name = export_name.replace('/', '-').replace(' ', '-')
            plot2.savefig(os.path.join(export_path, export_name))
            plt.close()

    plt.ion()





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


