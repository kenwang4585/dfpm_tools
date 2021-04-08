# import cryptography

import pandas as pd
from flask_setting import *


def add_user_log(user='', location='', user_action='',summary=''):
    '''
    Add the user log to db
    '''

    log = UserLog(USER_NAME=user,
                    DATE=pd.Timestamp.now().date(),
                    TIME=pd.Timestamp.now().strftime('%H:%M:%S'),
                    LOCATION=location,
                    USER_ACTION=user_action,
                    SUMMARY=summary)


    db.session.add(log)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')


def add_dfpm_mapping_data(dfpm, org, bu, extra_pf, exclusion_pf,login_user):
    '''
    Add the user log to db
    '''

    log = DfpmMapping(DFPM=dfpm,
                  Org=org,
                  BU=bu,
                  Extra_PF=extra_pf,
                  Exclusion_PF=exclusion_pf,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(log)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_subscription(email,task,login_user):
    '''
    Add the user log to db
    '''

    record = Subscription(Email=email,
                  Subscription=task,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(record)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_slot_and_rsp_keyword(pf,rsp_keyword,slot_keyword,login_user):
    '''
    '''

    record = RspSlot(PF=pf,
                    RSP_KEYWORD=rsp_keyword,
                    SLOT_KEYWORD=slot_keyword,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(record)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_incl_excl_rule_pid(org,bu,pf,pid_a,pid_b,pid_c,remark,login_user):
    '''
    Add the general rule data to db
    '''

    record = GeneralConfigRulePid(
                  ORG=org,
                  BU=bu,
                  PF=pf,
                  PID_A=pid_a,
                  PID_B=pid_b,
                  PID_C=pid_c,
                  #PID_A_EXCEPTION=pid_a_exception,
                  #PID_B_EXCEPTION=pid_b_exception,
                  #PID_C_EXCEPTION=pid_c_exception,
                  REMARK=remark,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(record)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_incl_excl_rule(org, bu, pf, exception_main_pid, pid_a, pid_b,pid_b_operator,pid_b_qty, effective_date,remark, login_user):
    '''
    Add the general rule data to db
    '''

    record = GeneralConfigRule( ORG=org,
                                    BU=bu,
                                    PF=pf,
                                    EXCEPTION_MAIN_PID=exception_main_pid,
                                    PID_A=pid_a,
                                    PID_B=pid_b,
                                    PID_B_OPERATOR=pid_b_operator,
                                    PID_B_QTY=pid_b_qty,
                                    EFFECTIVE_DATE=effective_date,
                                    REMARK=remark,
                                    Added_by=login_user,
                                    Added_on=pd.Timestamp.now().date(),)

    db.session.add(record)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_error_config_data(df_upload,login_user):
    '''
    Add the error config data to db
    '''

    df_data = df_upload.values

    db.session.bulk_insert_mappings(HistoryNewErrorConfigRecord,
                                    [dict(
                                        ORGANIZATION_CODE=row[0],
                                        BUSINESS_UNIT=row[1],
                                        PO_NUMBER=row[2],
                                        OPTION_NUMBER=row[3],
                                        PRODUCT_ID=row[4],
                                        ORDERED_QUANTITY=row[5],
                                        REMARK=row[6],
                                        Added_by=login_user,
                                        Added_on=pd.Timestamp.now().date()
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()


def add_data_from_file_initial():
    '''
    Add data from file: initial update
    '''
    fname='/users/wangken/downloads/program_lo (2020-01-17).xlsx'


    df = pd.read_excel(fname,parse_dates=['Date'])

    print(df.columns)
    print(df.shape)

    #print(df_summary.LINE_CREATION_DATE)
    df.loc[:, 'Date'] = df.Date.map(lambda x: x.date())
    df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    UserLog,
                                    [dict(
                                        USER_NAME=row[0],
                                        DATE=row[1],
                                        START_TIME=row[2],
                                        FINISH_TIME=row[3],
                                        PROCESSING_TIME=row[4],
                                        SELECTED_PROGRAMS=row[5],
                                        EMAIL_TO_ONLY=row[6],
                                        PROGRAM_LOG=row[7]
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')


def roll_back():
    try:
        db.session.commit()
    except:
        db.session.rollback()


if __name__ == '__main__':
    add_user_log(user='kwang2', location='Admin', user_action='Visit',
                 summary='Warning')