'''This sets the basics for the program'''
import os
import getpass

#fname_3a4 = '/users/wangken/downloads/backlog3a4-detail - 2019-09-17T082650.347.csv'

# Quarter end cutoff - currently not used
qend_list=['2020-7-26','2020-10-24','2021-1-23','2021-5-1','2021-7-31',]

super_user='kwang2'

top_customers_bookings_threshold=3 # M$
top_customers_bookings_history_days=7

org_name_global = {'APJC': {'APJC':['FOC', 'FDO', 'JPE', 'SHK','NCB'],
                            'FOC': ['FOC'],
                            'FDO': ['FDO'],
                            'JPE': ['JPE'],
                            'SHK': ['SHK'],
                            'NCB': ['NCB']
                            },
                   'EMEA':{'EMEA':['FCZ','FVE'],
                           'FCZ': ['FCZ'],
                           'FVE': ['FVE'],
                           },
                   'Americas':{'Americas':['FTX','TAU','SJZ','JMX','FJZ','FGU','TSP'],
                               'FTX': ['FTX'],
                               'TAU': ['TAU'],
                               'SJZ': ['SJZ'],
                               'JMX': ['JMX'],
                               'FJZ': ['FJZ'],
                               'FGU': ['FGU'],
                               'TSP': ['TSP']
                               }
                  }


# Date labels that will be in revenue summary and chart
backlog_distribution_date_labels = ['CUSTOMER_REQUEST_DATE', 'LT_TARGET_FCD', 'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE']


# define the path
base_dir_output = os.path.join(os.getcwd(),'output_file')
base_dir_uploaded = os.path.join(os.getcwd(), 'upload_file')
base_dir_chart = os.path.join(os.getcwd(),'chart')
if getpass.getuser()=='ubuntu': # if it's on crate server
    base_dir_tracker = '/home/ubuntu/tracker'
    base_dir_logs = '/home/ubuntu/logs'
    base_dir_db = '/home/ubuntu/db'
else:
    base_dir_tracker = os.path.join(os.getcwd(),'tracker')
    base_dir_logs = os.path.join(os.getcwd(), 'logs')
    base_dir_db = os.path.join(os.getcwd(), 'database')

# addressable summary record file name
addr_history_fname_global= {'APJC': os.path.join(base_dir_tracker,'apjc_history_addressable.xlsx'),
                     'EMEA':os.path.join(base_dir_tracker,'emea_history_addressable.xlsx'),
                     'Americas':os.path.join(base_dir_tracker,'americas_history_addressable.xlsx')
                     }

# Outlier columns: threshold mapping - for making chart and df summary
# !!!!!! - keep below sequence unchanged
# !!!!!! - must be in same sequence as below outlier charts definition
outlier_elements = {
                    'entered_not_booked':[20,os.path.join(base_dir_tracker,'history_entered_not_booked.xlsx')],
                    'booked_not_scheduled':[3,os.path.join(base_dir_tracker,'history_booked_not_scheduled.xlsx')],
                    'missed_ossd': [60,os.path.join(base_dir_tracker,'history_scheduled_not_packed.xlsx')],
                    'cancel_aging_days':[30,os.path.join(base_dir_tracker,'history_aging_cancel.xlsx')],
                    'ss_partial_staged_days':[5,os.path.join(base_dir_tracker,'history_partial_staged.xlsx')],
                    'missed_recommit':[1,os.path.join(base_dir_tracker,'historical_missed_recommit.xlsx')],
                    }
# top number of orders to show in email when reported
outlier_detail_top_x=10

# outlier chart filenames,chart_id and address: besides apjc there are just foc/fdo since this is for CM_3a4 and now only cover foc/fdo
# this is a dict with chart name: {chart_id:chart address}
# !!!!!! - must be in same sequence as above outlier elements definitio
outlier_chart_apjc={
                'outlier_book_apjc': os.path.join(base_dir_chart,'outlier_book_apjc.png'),
                'outlier_schedule_apjc':os.path.join(base_dir_chart,'outlier_schedule_apjc.png'),
                'outlier_pack_apjc':os.path.join(base_dir_chart,'outlier_pack_apjc.png'),
                'outlier_cancel_apjc':os.path.join(base_dir_chart,'outlier_cancel_aging_apjc.png'),
                'outlier_partial_apjc':os.path.join(base_dir_chart,'outlier_partial_staged_apjc.png'),
                }

outlier_chart_foc={
                'outlier_book_foc':os.path.join(base_dir_chart,'outlier_book_foc.png'),
                'outlier_schedule_foc':os.path.join(base_dir_chart,'outlier_schedule_foc.png'),
                'outlier_pack_foc':os.path.join(base_dir_chart,'outlier_pack_foc.png'),
                'outlier_cancel_foc':os.path.join(base_dir_chart,'outlier_cancel_aging_foc.png'),
                'outlier_partial_foc':os.path.join(base_dir_chart,'outlier_partial_staged_foc.png'),
                 'outlier_recommit_foc':os.path.join(base_dir_chart,'outlier_recommit_foc.png'),
                 }

outlier_chart_fdo={
                'outlier_book_fdo':os.path.join(base_dir_chart,'outlier_book_fdo.png'),
                'outlier_schedule_fdo':os.path.join(base_dir_chart,'outlier_schedule_fdo.png'),
                'outlier_pack_fdo':os.path.join(base_dir_chart,'outlier_pack_fdo.png'),
                'outlier_cancel_fdo':os.path.join(base_dir_chart,'outlier_cancel_aging_fdo.png'),
                'outlier_partial_fdo':os.path.join(base_dir_chart,'outlier_partial_staged_fdo.png'),
                'outlier_recommit_fdo':os.path.join(base_dir_chart,'outlier_recommit_fdo.png'),
                }


# Used to define embeded charts to be send in backlog summary email: {chart_id:chart address};
# this is also used to create the charts.
backlog_chart_global={'APJC':{'banner_addr':os.path.join(os.getcwd(),'static/banner_addr.png'),
                            'apjc_add_summary':os.path.join(base_dir_chart,'addressable_summary_apjc.png'),
                            'foc_add_summary':os.path.join(base_dir_chart,'addressable_summary_foc.png'),
                            'fdo_add_summary':os.path.join(base_dir_chart,'addressable_summary_fdo.png'),
                            'jpe_add_summary':os.path.join(base_dir_chart,'addressable_summary_jpe.png'),
                            'shk_add_summary':os.path.join(base_dir_chart,'addressable_summary_shk.png'),
                            'ncb_add_summary': os.path.join(base_dir_chart, 'addressable_summary_ncb.png'),
                            'apjc_add_trending':os.path.join(base_dir_chart,'addressable_trending_apjc.png'),
                            'foc_add_trending':os.path.join(base_dir_chart,'addressable_trending_foc.png'),
                            'fdo_add_trending':os.path.join(base_dir_chart,'addressable_trending_fdo.png'),
                            'jpe_add_trending':os.path.join(base_dir_chart,'addressable_trending_jpe.png'),
                            'shk_add_trending':os.path.join(base_dir_chart,'addressable_trending_shk.png'),
                            'ncb_add_trending':os.path.join(base_dir_chart,'addressable_trending_ncb.png'),
                            },
                    'EMEA':{'banner_addr':os.path.join(os.getcwd(),'static/banner_addr.png'),
                            'emea_add_summary':os.path.join(base_dir_chart,'addressable_summary_emea.png'),
                            'fcz_add_summary':os.path.join(base_dir_chart,'addressable_summary_fcz.png'),
                            'fve_add_summary':os.path.join(base_dir_chart,'addressable_summary_fve.png'),
                            'emea_add_trending':os.path.join(base_dir_chart,'addressable_trending_emea.png'),
                            'fcz_add_trending':os.path.join(base_dir_chart,'addressable_trending_fcz.png'),
                            'fve_add_trending':os.path.join(base_dir_chart,'addressable_trending_fve.png'),
                            },
                    'Americas':{'banner_addr':os.path.join(os.getcwd(),'static/banner_addr.png'),
                            'americas_add_summary':os.path.join(base_dir_chart,'addressable_summary_americas.png'),
                            'ftx_add_summary':os.path.join(base_dir_chart,'addressable_summary_ftx.png'),
                            'tau_add_summary':os.path.join(base_dir_chart,'addressable_summary_tau.png'),
                            'sjz_add_summary':os.path.join(base_dir_chart,'addressable_summary_sjz.png'),
                            'fgu_add_summary':os.path.join(base_dir_chart,'addressable_summary_fgu.png'),
                            'fjz_add_summary':os.path.join(base_dir_chart,'addressable_summary_fjz.png'),
                            'jmx_add_summary':os.path.join(base_dir_chart,'addressable_summary_jmx.png'),
                            'tsp_add_summary':os.path.join(base_dir_chart,'addressable_summary_tsp.png'),
                            'americas_add_trending':os.path.join(base_dir_chart,'addressable_trending_americas.png'),
                            'ftx_add_trending':os.path.join(base_dir_chart,'addressable_trending_ftx.png'),
                            'tau_add_trending':os.path.join(base_dir_chart,'addressable_trending_tau.png'),
                            'sjz_add_trending':os.path.join(base_dir_chart,'addressable_trending_sjz.png'),
                            'fgu_add_trending':os.path.join(base_dir_chart,'addressable_trending_fgu.png'),
                            'fjz_add_trending':os.path.join(base_dir_chart,'addressable_trending_fjz.png'),
                            'jmx_add_trending':os.path.join(base_dir_chart,'addressable_trending_jmx.png'),
                            'tsp_add_trending':os.path.join(base_dir_chart,'addressable_trending_tsp.png'),
                            }
                    }

# hold types
mfg_holds=['Booking Validation Hold','Cancellation','CFOP Product Hold','CMFS-Credit Check Pending','CMFS-Scheduled, Booked',
 'Compliance Hold','CONDITIONAL HOLD','Config Problem Hold','Configuration Hold','Conversion Dispatch Hold',
 'CSC-Credit Check Pending','CSC-Not Scheduled, Booked','Export','Localization Change Hold','New Product',
 'Non-FCC Compliant Hold','Order Aging Hold','Order Change','Order Transfer Changes (OTC) Hold','Order Validation Hold',
 'Pending Trade Collaborator Response','Quantity Validation','SCORE Chg Parameter','Scheduling COO','TCH Order Validation',
           'Country Certification Hold','CMFS-Fulfillment Hold','Awaiting EA Fulfillment','CSC-Shipment Hold','Customer Acceptance-Full Stack',
           'ELC-WWL Resolution','HW Fulfillment','Logistics Pick Release Hold','Pending Change Transaction','Pending Source Subscription Cancellation',
           'Pre-Launch','Smart License Registration (SLR)  Pre-Install Hold','WWL Custom Delivery Hold','Pre-Order',
           'SLR Pre-Install Hold','SON_HCL']

# Col required for each task under glo_app
col_3a4_must_have_global_backlog_summary=['ORGANIZATION_CODE','BUSINESS_UNIT', 'PRODUCT_FAMILY',
                   'PO_NUMBER', 'MFG_HOLD', 'ORDER_HOLDS','ADDRESSABLE_FLAG','PACKOUT_QUANTITY','C_UNSTAGED_DOLLARS',
                                          'REVENUE_NON_REVENUE']
col_3a4_must_have_global_wnbu_compliance=['ORGANIZATION_CODE','BUSINESS_UNIT','PRODUCT_FAMILY','PO_NUMBER',
                                          'ORDER_HOLDS','LINE_CREATION_DATE','SHIP_TO_COUNTRY','END_TO_COUNTRY']
col_3a4_must_have_global_config_check=['ORGANIZATION_CODE','BUSINESS_UNIT','PRODUCT_FAMILY', 'PO_NUMBER',
                                       'OPTION_NUMBER','PRODUCT_ID','ORDERED_QUANTITY']
# Col required for dfpm_app
col_3a4_must_have_dfpm=['ORGANIZATION_CODE','BUSINESS_UNIT', 'PRODUCT_FAMILY','OPTION_NUMBER', 'SO_SS','FINAL_ACTION_SUMMARY','SECONDARY_PRIORITY',
                   'BUP_RANK','PO_NUMBER', 'PRODUCT_ID', 'MFG_HOLD', 'ORDER_HOLDS','ADDRESSABLE_FLAG',
                       'PACKOUT_QUANTITY','ORDERED_QUANTITY', 'C_UNSTAGED_QTY','C_UNSTAGED_DOLLARS',
                       'BOOKED_DATE', 'LINE_CREATION_DATE','LT_TARGET_FCD', 'TARGET_SSD','CURRENT_FCD_NBD_DATE',
                        'ORIGINAL_FCD_NBD_DATE','CUSTOMER_REQUEST_DATE','ASN_CREATION_DATE','OTM_SHIPPING_ROUTE_CODE',
                        'SHIP_TO_COUNTRY','END_TO_COUNTRY','COMMENTS','MCD','SALES_ORDER_OPERATING_UNIT']

col_3a4_must_have_config_check=['ORGANIZATION_CODE','BUSINESS_UNIT', 'PO_NUMBER','OPTION_NUMBER','PRODUCT_ID','ORDERED_QUANTITY']

col_3a4_must_have_backlog_ranking=['ORGANIZATION_CODE','BUSINESS_UNIT', 'PRODUCT_FAMILY','SO_SS','PO_NUMBER','PRODUCT_ID','MFG_HOLD', 'ORDER_HOLDS',
                                   'PACKOUT_QUANTITY','C_UNSTAGED_QTY','REVENUE_NON_REVENUE','BUP_RANK','OTM_SHIPPING_ROUTE_CODE',
                                   'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE','SECONDARY_PRIORITY','FINAL_ACTION_SUMMARY',]

# output col
col_3a4_backlog_ranking_output_col=['ORGANIZATION_CODE', 'SO_SS','PO_NUMBER','BUSINESS_UNIT','PRODUCT_FAMILY','PRODUCT_ID','ADDRESSABLE_FLAG','priority_cat','priority_rank','ss_overall_rank','riso_ranking','MFG_HOLD', 'ORDER_HOLDS',
                                   'PACKOUT_QUANTITY','C_UNSTAGED_QTY','REVENUE_NON_REVENUE','BUP_RANK','OTM_SHIPPING_ROUTE_CODE',
                                   'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE']

# Condensed col for site level data (& without $ col): for site lead, and CM
col_3a4_cm=['ORGANIZATION_CODE','SO_SS','PO_NUMBER','BUSINESS_UNIT', 'PRODUCT_FAMILY','PRODUCT_ID',
        'ADDRESSABLE_FLAG','PACKOUT_QUANTITY','BUILD_COMPLETE_DATE','MFG_HOLD','ORDER_HOLDS','priority_cat','priority_rank','ss_overall_rank','riso_ranking',
        'exception_highlight','missed_recommit','entered_not_booked','booked_not_scheduled','missed_ossd',
        'need_rtv','cancel_aging_days','ss_partial_staged_days','unstaged_under_ss','CURRENT_QUARTER_REVENUE_ELIGIBILITY',
            'TARGET_SSD','CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE','CUSTOMER_REQUEST_DATE','COMMENTS','CM_CTB','CTB_STATUS','CTB_COMMENT']

col_3a4_dfpm=['ORGANIZATION_CODE','SO_SS','PO_NUMBER','OPTION_NUMBER','BUSINESS_UNIT', 'main_bu','main_pf','PRODUCT_FAMILY','PRODUCT_ID','TAN',
        'ADDRESSABLE_FLAG','priority_cat','priority_rank','ss_overall_rank','riso_ranking','MFG_HOLD','ORDER_HOLDS',
              'ORDERED_QUANTITY','C_UNSTAGED_QTY','C_UNSTAGED_DOLLARS','po_rev_unstg','ss_unstg_rev','ss_rev_rank','BUP_RANK','PROGRAM','PACKOUT_QUANTITY','BUILD_COMPLETE_DATE',
              'FINAL_ACTION_SUMMARY','TIED_SHIP_SET','ASN_CREATION_DATE','ORDERED_DATE','BOOKED_DATE','LINE_CREATION_DATE',
              'LT_TARGET_FCD','TARGET_SSD','CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE','CUSTOMER_REQUEST_DATE',
              'CUSTOMER_REQUESTED_SHIP_DATE','ORIGINAL_PROMISE_DATE','CURRENT_PROMISE_DATE',
              'STD_VS_TWO_TIER','CARTON_LOCATION','END_CUSTOMER_COUNTRY','END_CUSTOMER_NAME','SHIP_TO_COUNTRY','SHIP_TO_COUNTRY',
              'SHIP_TO_CUSTOMER_NAME','EXPEDITE_STATUS','MCD','OTM_SHIPPING_ROUTE_CODE','MFG_LINE_ID',
              'SALES_ORDER_OPERATING_UNIT','DPAS_RATING','CURRENT_QUARTER_REVENUE_ELIGIBILITY','exception_highlight',
              'COMMENTS','category_comments','CM_CTB','CTB_STATUS','CTB_COMMENT','GLOBAL_RANK','CONSOLIDATED_FLAG']

col_3a4_regional=['ORGANIZATION_CODE','SO_SS','PO_NUMBER','BUSINESS_UNIT', 'PRODUCT_FAMILY','PRODUCT_ID','TAN',
        'ADDRESSABLE_FLAG','ORDER_HOLDS','priority_cat','priority_rank','ss_overall_rank','riso_ranking',
              'ORDERED_QUANTITY','C_UNSTAGED_QTY','C_UNSTAGED_DOLLARS','po_rev_unstg','ss_unstg_rev','ss_rev_rank','BUP_RANK','PROGRAM','PACKOUT_QUANTITY','BUILD_COMPLETE_DATE',
              'FINAL_ACTION_SUMMARY','TIED_SHIP_SET','ASN_CREATION_DATE','ORDERED_DATE','BOOKED_DATE','LINE_CREATION_DATE',
              'LT_TARGET_FCD','TARGET_SSD','CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE','CUSTOMER_REQUEST_DATE',
              'CUSTOMER_REQUESTED_SHIP_DATE','ORIGINAL_PROMISE_DATE','CURRENT_PROMISE_DATE',
              'STD_VS_TWO_TIER','CARTON_LOCATION','END_CUSTOMER_COUNTRY','END_CUSTOMER_NAME','SHIP_TO_COUNTRY','SHIP_TO_COUNTRY',
              'SHIP_TO_CUSTOMER_NAME','EXPEDITE_STATUS','MCD','OTM_SHIPPING_ROUTE_CODE','MFG_LINE_ID',
              'SALES_ORDER_OPERATING_UNIT','DPAS_RATING','CURRENT_QUARTER_REVENUE_ELIGIBILITY','exception_highlight',
              'COMMENTS','category_comments','CM_CTB','CTB_STATUS','CTB_COMMENT','GLOBAL_RANK','CONSOLIDATED_FLAG']


# Col about revenue
rev_col = ['ss_rev', 'po_rev_unstg','po_rev_unit', 'C_UNSTAGED_DOLLARS','C_STAGED_DOLLARS']

# other col to remove while generating 3a4
#other_removal_col=['GLOBAL_RANK','BUP_RANK','CONSOLIDATED_ALLOCATION_PRIORITY','REVENUE_NON_REVENUE','ROW_NUM','category_comments','priority_rank']

# outlier columns to place in front
col_outlier={
        'df_not_booked':['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER', 'entered_not_booked', 'ORDER_HOLDS', 'COMMENTS',
                        'exception_highlight','po_rev_unstg'],
        'df_not_scheduled':['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER', 'booked_not_scheduled', 'ORDER_HOLDS','COMMENTS',
                        'exception_highlight','po_rev_unstg'],
        'df_not_packed':['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER', 'missed_ossd', 'ORDER_HOLDS', 'COMMENTS',
                        'exception_highlight','po_rev_unstg','C_UNSTAGED_DOLLARS'],
        'df_aging_cancel':['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER', 'C_STAGED_QTY','cancel_aging_days',
                        'category_comments','COMMENTS','exception_highlight','po_rev_unstg','C_UNSTAGED_DOLLARS'],
        'df_partial_staged':['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'SO_SS', 'ss_partial_staged_days', 'ORDER_HOLDS',
                        'partial_comments','po_rev_unstg','C_UNSTAGED_DOLLARS'],
        'df_missed_recommit':['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER', 'missed_recommit', 'ORDER_HOLDS',
                        'COMMENTS']
        }

# Date columns that needs to parse while loading
date_col_to_parse_apjc = [ 'BOOKED_DATE','CUSTOMER_REQUEST_DATE',
            'LINE_CREATION_DATE', 'LT_TARGET_FCD',
             'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE','CUSTOMER_REQUESTED_SHIP_DATE','TARGET_SSD',
             'ASN_CREATION_DATE']


# col for priority PO and rank sequence (excel spreadsheet columns)
priority_po_col=['ORGANIZATION_CODE','BUSINESS_UNIT', 'PRODUCT_FAMILY', 'PO_NUMBER','priority_cat',
                 'CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE','PACKOUT_QUANTITY','ORDER_HOLDS',
                 'ADDRESSABLE_FLAG','COMMENTS']
priority_cat_to_report=['PR1','PR2','PR3','L4','TOP 100']

# rank sequences
ranking_options = {'ss_overall_rank': ['priority_rank_top', 'CURRENT_FCD_NBD_DATE', 'priority_rank_mid',
                                     'ORIGINAL_FCD_NBD_DATE','PROGRAM','C_UNSTAGED_QTY', 'rev_non_rev_rank',
                                     'SO_SS', 'PO_NUMBER'],
                    'riso_ranking' : ['priority_rank', 'ORIGINAL_FCD_NBD_DATE','PROGRAM','C_UNSTAGED_QTY',
                                      'rev_non_rev_rank', 'SO_SS', 'PO_NUMBER']
                    }

# lowest priority categories
lowest_priority_cat={'ADDRESSABLE_FLAG':['MFG_HOLD'],
                     'OTM_SHIPPING_ROUTE_CODE':['US Server']}
