# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 13:17:03 2018

@author: michaelek
"""

#####################################
### Misc parameters for the various functions

database = 'hydro'
allo_table = 'CrcAllo'
wap_allo_table = 'CrcWapAllo'
site_table = 'ExternalSite'

status_codes = ['Terminated - Replaced', 'Issued - Active', 'Terminated - Surrendered', 'Terminated - Cancelled', 'Terminated - Expired', 'Terminated - Lapsed', 'Issued - s124 Continuance']

use_type_dict = {'Aquaculture': 'agriculture', 'Dairy Shed (Washdown/Cooling)': 'agriculture', 'Intensive Farming - Dairy': 'agriculture', 'Intensive Farming - Other (Washdown/Stockwater/Cooling)': 'agriculture', 'Intensive Farming - Poultry': 'agriculture', 'Irrigation - Arable (Cropping)': 'agriculture', 'Irrigation - Industrial': 'agriculture', 'Irrigation - Mixed': 'agriculture', 'Irrigation - Pasture': 'agriculture', 'Irrigation Scheme': 'agriculture' , 'Viticulture': 'agricutlure', 'Community Water Supply': 'water_supply', 'Domestic Use': 'water_supply', 'Construction': 'industrial', 'Construction - Dewatering': 'industrial', 'Cooling Water (non HVAC)': 'industrial', 'Dewatering': 'industrial', 'Gravel Extraction/Processing': 'industrial', 'HVAC': 'industrial', 'Industrial Use - Concrete Plant': 'industrial', 'Industrial Use - Food Products': 'industrial', 'Industrial Use - Other': 'industrial', 'Industrial Use - Water Bottling': 'industrial', 'Mining': 'industrial', 'Firefighting ': 'municipal', 'Firefighting': 'municipal', 'Flood Control': 'municipal', 'Landfills': 'municipal', 'Stormwater': 'municipal', 'Waste Water': 'municipal', 'Stockwater': 'agriculture', 'Snow Making': 'industrial', 'Augment Flow/Wetland': 'other', 'Fisheries/Wildlife Management': 'other', 'Other': 'other', 'Recreation/Sport': 'other', 'Research (incl testing)': 'other', 'Power Generation': 'hydroelectric'}

restr_type_dict = {'max rate': 'max_rate_crc', 'daily volume': 'daily_vol', 'annual volume': 'feav'}

freq_codes = ['D', 'W', 'M', 'A-JUN']























