# Ken, 2019

from blg_functions import *

from sending_email import send_attachment_and_embded_image
from send_sms import send_me_sms

test = input('Is this a test? (YES/NO)').strip().upper()

if test == 'YES':
    # ~~~~~~~~~~~~~~~~~~~~ Enable below when doing testing~~~~~~~~~
    dfpm_mapping = {'kwang2': {'FOC': (['WNBU'], ['M2M800', 'CGR2000'], ['CGR1000']),
                               'FDO': ([], [], [])
                               }
                    }
    backlog_dashboard_emails = ['kwang2@cisco.com']
    cm_emails = {'FOC': ['kwang2@cisco.com'], 'FDO': ['kwang2@cisco.com']}
    outlier_emails = ['kwang2@cisco.com']
    wnbu_compliance_hold_emails = ['kwang2@cisco.com']

    report_frequency = {
        'apjc_backlog_summary': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
        'dfpm_3a4_summary': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
        'cm_3a4_outlier_summary': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
        'apjc_outlier_summary': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
        'wnbu_compliance_hold':['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
    }

    fname_3a4 = '/users/wangken/Downloads/backlog3a4-detail - 2019-11-06T084107.268.csv'

elif test == 'NO':
    input_3a4 = input('\nPls specifiy the 3a4 file name (under Downloads folder): ').strip()
    fname_3a4 = '/users/wangken/Downloads/' + input_3a4

alert_msg = []
error_msg = []
program_log = []

start_time = pd.Timestamp.now()

# send sms to notify start
# send_me_sms('+8618665932236','[Ken] 3a4 auto program start @' + start_time.strftime('%H:%M'))

# read 3a4 data from file
df_3a4 = read_3a4(fname_3a4, program_log)

# execute the basic data processing in 3a4 file - extra columns added
df_3a4, df_compliance_release, df_compliance_hold = basic_data_processing_for_dfpm_cm(df_3a4, col_and_sequence_full, data_exclusion_rules, addressable_window)

# Read CTB from smartsheet, add to 3a4, and make different summaries
df_3a4, addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material, program_log, ctb_error_msg = create_ctb_summaries(
    df_3a4,program_log)

# Make addr_df_summary for addressable snapshop and addr_df_dict for trending chart; also collect addressable data to tracker.
addr_df_summary, addr_df_dict = create_addressable_summary_and_collect_addressable_history(df_3a4, org_name, addr_fname)


if pd.Timestamp.now().day_name() in report_frequency['apjc_backlog_summary']:
    create_and_send_addressable_summaries(addr_df_summary, addr_df_dict, org_name, backlog_dashboard_emails,
                                          program_log)

# APJC outlier summary
if pd.Timestamp.now().day_name() in report_frequency['apjc_outlier_summary']:
    create_and_send_outlier_summaries(df_3a4, outlier_elements, outlier_chart_apjc, outlier_emails, program_log)


if pd.Timestamp.now().day_name() in report_frequency['cm_3a4_outlier_summary']:
    email_to_only=[]
    create_and_send_cm_3a4(df_3a4, email_to_only, cm_emails, outlier_elements, program_log)

if pd.Timestamp.now().day_name() in report_frequency['dfpm_3a4_summary']:
    create_and_send_dfpm_3a4(df_3a4,dfpm_mapping,backlog_distribution_date_labels,
                             addr_ctb_by_org_bu,addr_ctb_by_org_bu_pf,ctb_summary_for_material,addr_df_dict,program_log)

finish_time = pd.Timestamp.now()
processing_time = round((finish_time - start_time).total_seconds() / 60, 1)

# send sms to notify finish
send_me_sms('+8618665932236', '[Ken] 3a4 program finish @' + str(finish_time) + '. Time used: ' + str(processing_time))

# Send program report to Ken wang
send_attachment_and_embded_image(['kwang2@cisco.com'], '3A4 program running summary', 'program_summary.html',
                                 ctb_error_msg=ctb_error_msg, program_log=program_log,
                                 processing_time=processing_time,
                                 start_time=start_time.strftime('%H:%M'), finish_time=finish_time.strftime('%H:%M'),
                                 att_filenames=None, embeded_filenames=None)
