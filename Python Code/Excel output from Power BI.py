# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 11:46:13 2022

@author: alex.messina
"""

maindir = 'C:/Users/alex.messina/Documents/GitHub/2022_County_LowFlow/'
import os
os.chdir('C:/Users/alex.messina/Documents/GitHub/2022_County_LowFlow/Python Code/')

import pandas as pd
import datetime as dt
import string
import textwrap
import numpy as np
import calendar
from scipy import signal
## Plotting modules
import matplotlib as mpl
from matplotlib import pyplot as plt
## Path to Custom modules
import sys
sys.path.append('..')
# Import Custom Modules
#from ZentraAPI import *
#from Get_GoogleSheets import *
#from OneRain_data import *
from Excel_Plots_2022 import *
from OvertoppingFlows import *

def xl_columnrow(col,row=''):
    """ Convert given row and column number to an Excel-style cell name. """
    LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []
    while col:
        col, rem = divmod(col-1, 26)
        result[:0] = LETTERS[rem]
    return ''.join(result)+str(row)


#%%
start, end =  dt.datetime(2022,5,4,0,0), dt.datetime(2022,9,15,23,55)

data_df = pd.read_csv(maindir + 'Data/PBI flow data output - County sites only.csv',index_col=1, parse_dates=True)
data_df = data_df.loc[ (data_df.index >= start) & (data_df.index <= end) ]
site_list = data_df['SiteName'].unique()

#%% Database import

db_flow = pd.DataFrame(data_df[['Flow_gpm_nostorms','Temp_F', 'Cond_uScm']]


#%% Excel output

from Excel_Plots_2022 import *
for site_name in site_list:
    print (site_name)
    df = data_df.loc[ data_df['SiteName'] ==site_name]
    #df['Flow_gpm'].plot()
    


    ## Rain data
    Rain1D = df['Rain_hour_in'].resample('D').sum()
    Rain1H = pd.DataFrame(df['Rain_hour_in'].resample('H').sum())

    ### FINALIZED FLOW OUTPUT
    Corr_flow = df[['Flow_gpm', 'Flow_gpm_nostorms']].round(3)
    
    
    ## Add base/quickflow
#    Corr_flow[['Baseflow (gpm)','Quickflow (gpm)']] = WLout[['Baseflow (gpm)','Quickflow (gpm)']]
    
    ## Add temp and conductivity to deliverable
    Corr_flow[u'uS/cm EC'] = df['Cond_uScm']
    Corr_flow[u'\xb0F Water Temperature'] = df['Temp_F'].round(1)
    
    Corr_flow.rename(columns={'Flow_gpm':'Flow compound weir (gpm)', 'Flow_gpm_nostorms':'Flow compound weir stormflow clipped (gpm)'},inplace=True)
    
    ## PIVOT TABLE STUFF
    Corr_flow.loc[:,('Year')] = Corr_flow.index.year
    Corr_flow.loc[:,('Month')] = Corr_flow.index.month
    Corr_flow.loc[:,('Day')] = Corr_flow.index.day
    Corr_flow.loc[:,('Hour')] = Corr_flow.index.hour
    Corr_flow.loc[:,('Minute')] = Corr_flow.index.minute
    Corr_flow.loc[:,('Weekday')] = Corr_flow.index.map(lambda x: calendar.day_name[x.weekday()])
    ## Kick out to Excel
    final_flow_ExcelFile = pd.ExcelWriter(maindir+'Flow_Output_Excel_files/'+site_name+'-working draft.xlsx')
#    start_time_loc, data_deliverable_end_time_loc = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
    start_time_loc, data_deliverable_end_time_loc = start.date(), end.date()
    max_row, rain_max_row = Excel_Plots(site_name, Corr_flow, Rain1D, final_flow_ExcelFile, start_time_loc, data_deliverable_end_time_loc)
    ### Pivot TABLES
    ## Old style-SUM but ADDING the multiplication by 5min (gpm->gp5M)
    #PivotTable_Sum = pd.pivot_table(Corr_flow,values='Flow compound weir stormflow clipped (gpm)', columns=['Month','Day','Weekday'], index=['Hour'], aggfunc=np.sum).round(1) * 5. # *5 for 5Min interval data
    #PivotTable_Sum.to_excel(final_flow_ExcelFile,site_name+'PivotTable-Sum')
    ## Freeze Panes
    #final_flow_ExcelFile.sheets[site_name+'PivotTable-Sum'].freeze_panes(4, 1)
    ## Conditional Formatting
    def rgb_hex(red,green,blue):
        return '#%02x%02x%02x' % (red, green, blue)
    green, yellow, red = rgb_hex(99,190,123),rgb_hex(255,235,132),rgb_hex(248,105,107)
    #max_col_row = xl_columnrow(len(PivotTable_Sum.columns)+1,28) #24th hour is on row 28
    #final_flow_ExcelFile.sheets[site_name+'PivotTable-Sum'].conditional_format('B5:'+max_col_row, {'type': '3_color_scale','min_color': green,'mid_color':yellow,'max_color':red})
    ## Old style-AVG
    PivotTable_Avg = pd.pivot_table(Corr_flow,values='Flow compound weir stormflow clipped (gpm)', columns=['Month','Day','Weekday'], index=['Hour'], aggfunc=np.mean).round(3)
    PivotTable_Avg.to_excel(final_flow_ExcelFile,site_name+'PivotTable-Avg')
    ## Freeze Panes
    final_flow_ExcelFile.sheets[site_name+'PivotTable-Avg'].freeze_panes(4, 1)
    ## Conditional Formatting
    max_col_row = xl_columnrow(len(PivotTable_Avg.columns)+1,28)  #24th hour is on row 28
    final_flow_ExcelFile.sheets[site_name+'PivotTable-Avg'].conditional_format('B5:'+max_col_row, {'type': '3_color_scale','min_color': green,'mid_color': yellow,'max_color': red})
    ## Seven day Average style
    PivotTable = pd.pivot_table(Corr_flow,values='Flow compound weir stormflow clipped (gpm)',columns=['Weekday'],index=['Hour'],aggfunc=np.mean)
    col_order=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    PivotTable = PivotTable.reindex_axis(col_order,axis=1)
    PivotTable.to_excel(final_flow_ExcelFile,site_name+'PivotTable-Avg7day')
    ## Format Pivot Table 
    pivot = final_flow_ExcelFile.sheets[site_name+'PivotTable-Avg7day']
    ## Conditional formatting
    # Add a format. Yellow fill with RED text.
    redtxt = final_flow_ExcelFile.book.add_format({'bg_color': '#FFFF00',
                               'font_color': '#FF0000'})
    # Add a format. Yellow fill with black text.
    blacktxt = final_flow_ExcelFile.book.add_format({'bg_color': '#FFFF00',
                               'font_color': '#000000'})
    day_cols={'Monday':'B','Tuesday':'C','Wednesday':'D','Thursday':'E','Friday':'F','Saturday':'G','Sunday':'H'}
    col_order=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    for index, letter in enumerate(string.ascii_uppercase[1:9]):
        ## Count cells over 25th percentile
        pivot.write_formula(25,index, '=SUMPRODUCT(--('+letter+'2:'+letter+'25>PERCENTILE($B$2:$H$25,0.85)))')
    ## Annotate
    pivot.write(25,0, 'Count>15% by day')
    pivot.write(26,3, 'Count>15% by day')
    for i, day in zip(np.arange(27,34,1),col_order):
        col = day_cols[day]
        print (i, day, col)
        pivot.write(i,0,day)
        pivot.write_formula(i,1,'=AVERAGE('+col+'2:'+col+'25)')
        pivot.write(i,2,'>Avg')
        pivot.write_formula(i,3,'=SUM('+col+'26)')
        ## Conditionally format each day
        pivot.conditional_format(col+'2:'+col+'25', {'type': 'cell','criteria': '>=','value':'$B$35','format': redtxt})
        pivot.conditional_format(col+'2:'+col+'25', {'type': 'cell','criteria': '>=','value':'$B$'+str(i+1),'format': blacktxt})
    pivot.write(34,0,'Top 15th%ile (excluding zeros)')
    pivot.write_formula(34,1,'=PERCENTILE(IF(B2:H25>0, B2:H25), 0.85)')
    pivot.write(34,2,'>15th%ile excl 0s')
    pivot.write(34,3,'(need to hit F2, then Ctrl+Shift+Enter to execute equation if you edit it)')
    pivot.write(35,0,'Top 15th%ile (including zeros)')
    pivot.write_formula(35,1,'=PERCENTILE(B2:H25,0.85)')
    pivot.write(35,2,'>15th%ile incl 0s')
    ### SAVE FINAL FILE
    final_flow_ExcelFile.save()
    
    
#    # Final Hydrograph    
#    fig, ax1 = plt.subplots(1,1,figsize = (14,8))
#    ## FLOW
#    ax1.plot_date(Corr_flow.index, Corr_flow['Flow compound weir (gpm)'], marker='None', ls='-', c='grey',alpha=0.2,label='Stormflow, clipped, compound weir')
#    ax1.plot_date(Corr_flow.index, Corr_flow['Flow compound weir stormflow clipped (gpm)'], marker='None', ls='-', c='b',label='Flow, compound weir')
#    ## BASEFLOW
##    ax1.plot_date(Corr_flow.index,Corr_flow['Baseflow (gpm)'], marker='None', ls='-', c='grey',label='Baseflow')
#    ## RAIN
#    ax2 = ax1.twinx()
#    ax2.plot_date(Rain1H.index, Rain1H['Rain_hour_in'], marker='None',ls='steps-mid',color='skyblue',label='Precip')
#    ## FORMAT
#    ax1.set_ylim(-Corr_flow['Flow compound weir stormflow clipped (gpm)'].max() * 0.25, Corr_flow['Flow compound weir stormflow clipped (gpm)'].max() * 2.)
#    ax2.set_ylim(0, Rain1H['Rain_hour_in'].max() * 3.)
#    ax2.invert_yaxis()
#    ## LEGEND
#    ax1.legend(fontsize=12,loc='lower left'), ax2.legend(fontsize=12,loc='lower right')
#    ax1.set_ylabel('Flow (gpm)'), ax2.set_ylabel('Precip (inches)')
#    ax1.xaxis.set_major_formatter(mpl.dates.DateFormatter('%A \n %m/%d/%y %H:%M'))
#    plt.xticks(rotation=90)
#    ## set x-axis to monitoring period
#    ax1.set_xlim(start_time_loc, data_deliverable_end_time_loc)
#    fig.suptitle('Working Draft Hydrograph for site: '+site_name,fontsize=16,fontweight='bold')
#    plt.tight_layout()
#    plt.subplots_adjust(top=0.95)
