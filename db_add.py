# import cryptography

import pandas as pd
from flask_settings import *


def add_user_log_summary(user='', location='', user_action='',summary=''):
    '''
    Add the user log to db
    '''

    log = DfpmToolUserLog(USER_NAME=user,
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

    log = DfpmToolDfpmMapping(DFPM=dfpm,
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

    record = DfpmToolSubscription(Email=email,
                  Subscription=task,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(record)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_slot_and_rsp_keyword(pf,rsp_keyword,slot_keyword,login_user):
    '''
    '''

    record = DfpmToolRspSlot(PF=pf,
                    RSP_KEYWORD=rsp_keyword,
                    SLOT_KEYWORD=slot_keyword,
                  Added_by=login_user,
                  Added_on=pd.Timestamp.now().date(),)

    db.session.add(record)  # can also use add_all() for multiple adding at one time
    db.session.commit()
    #print('User log added')

def add_incl_excl_rule(org, bu, pf, exception_main_pid, pid_a, pid_b,pid_b_operator,pid_b_qty, effective_date,remark, login_user):
    '''
    Add the general rule data to db
    '''

    record = DfpmToolGeneralConfigRule( ORG=org,
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

def add_uploaded_error_config_data(df_upload,login_user):
    '''
    Add the error config data to db
    '''

    df_data = df_upload.values

    db.session.bulk_insert_mappings(DfpmToolUserUploadedErrorConfig,
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

def add_identified_error_config_data(df):
    '''
    Add the identified error config data to db
    '''

    df.fillna('', inplace=True)
    df_data = df.values

    db.session.bulk_insert_mappings(
                                    DfpmToolIdentifiedErrorConfig,
                                    [dict(
                                        ORGANIZATION_CODE=row[0],
                                        BUSINESS_UNIT=row[1],
                                        PRODUCT_FAMILY=row[2],
                                        PO_NUMBER=row[3],
                                        OPTION_NUMBER=row[4],
                                        PRODUCT_ID=row[5],
                                        ORDERED_QUANTITY=row[6],
                                        LINE_CREATION_DATE=row[7],
                                        ORDER_HOLDS=row[8],
                                        Config_error=row[9],
                                        Report_date=pd.Timestamp.now().date()
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')


def add_backlog_data(df_backlog):
    '''
    Add backlog data
    '''
    df_data = df_backlog.values

    db.session.bulk_insert_mappings(DfpmToolBacklog,
                                    [dict(
                                        DATE=pd.Timestamp.now().date(),
                                        REGION=row[0],
                                        ORG=row[1],
                                        BU=row[2],
                                        ADDRESSABLE=row[3],
                                        MFG_HOLD=row[4],
                                        UNSCHEDULED=row[5],
                                        PO_CANCELLED=row[6],
                                        NOT_ADDRESSABLE=row[7],
                                        TOTAL_BACKLOG=row[8],
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

def from_file_add_backlog_data_from_template(df):
    '''
    Add tan grouping data
    '''
    #df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    DfpmToolBacklog,
                                    [dict(
                                        DATE=row[0],
                                        REGION=row[1].replace('\xa0',''),
                                        ORG=row[2].replace('\xa0',''),
                                        BU=row[3].replace('\xa0',''),
                                        ADDRESSABLE=row[4],
                                        MFG_HOLD=row[5],
                                        UNSCHEDULED=row[6],
                                        PO_CANCELLED=row[7],
                                        NOT_ADDRESSABLE=row[8],
                                        TOTAL_BACKLOG=row[9],
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

def from_file_add_identified_error_config_data_from_template(df):
    '''
    Add tan grouping data
    '''
    df.ORDER_HOLDS.fillna('',inplace=True)
    df.PRODUCT_FAMILY.fillna('', inplace=True)
    df.loc[:,'LINE_CREATION_DATE']=pd.to_datetime(df.LINE_CREATION_DATE)
    df.loc[:, 'Report_date'] = pd.to_datetime(df.Report_date)
    df.loc[:,'LINE_CREATION_DATE']=df.LINE_CREATION_DATE.map(lambda x: x.date())
    df.loc[:, 'Report_date'] = df.Report_date.map(lambda x: x.date())

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    DfpmToolIdentifiedErrorConfig,
                                    [dict(
                                        ORGANIZATION_CODE=row[0].replace('\xa0',''),
                                        BUSINESS_UNIT=row[1].replace('\xa0',''),
                                        PRODUCT_FAMILY=row[2].replace('\xa0',''),
                                        PO_NUMBER=row[3].replace('\xa0',''),
                                        OPTION_NUMBER=row[4],
                                        PRODUCT_ID=row[5].replace('\xa0',''),
                                        ORDERED_QUANTITY=row[6],
                                        LINE_CREATION_DATE=row[7],
                                        ORDER_HOLDS=row[8].replace('\xa0',''),
                                        Config_error=row[9].replace('\xa0',''),
                                        Report_date=row[10]
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')


def from_file_add_config_rule_data_from_template(df):
    '''
    Add tan grouping data
    '''
    #df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
        DfpmToolGeneralConfigRule,
        [dict(
            ORG=row[0].replace('\xa0', ''),
            BU=row[1].replace('\xa0', ''),
            PF=row[2].replace('\xa0', ''),
            EXCEPTION_MAIN_PID=row[3].replace('\xa0', ''),
            PID_A=row[3].replace('\xa0', ''),
            PID_B=row[4].replace('\xa0', ''),
            PID_B_OPERATOR=row[5].replace('\xa0', ''),
            PID_B_QTY=int(row[6].replace('\xa0', '')),
            EFFECTIVE_DATE=row[7].replace('\xa0', ''),
            REMARK=row[8].replace('\xa0', ''),
            Added_by=row[9].replace('\xa0', ''),
            Added_on=row[10].replace('\xa0', ''),
        )
            for row in df_data]
    )

    db.session.commit()
    print('data added')

def from_file_add_dfpm_mapping_data_from_template(df):
    '''
    Add tan grouping data
    '''
    #df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    DfpmToolDfpmMapping,
                                    [dict(
                                        DFPM=row[0].replace('\xa0',''),
                                        Org=row[1].replace('\xa0',''),
                                        BU=row[2].replace('\xa0',''),
                                        Extra_PF=row[3].replace('\xa0',''),
                                        Exclusion_PF=row[4].replace('\xa0',''),
                                        Added_by=row[5].replace('\xa0',''),
                                        Added_on=row[6].replace('\xa0',''),
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')


def from_file_add_subscribe_data_from_template(df):
    '''
    Add tan grouping data
    '''
    #df.fillna('',inplace=True)

    #df=df[['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PO_NUMBER','LINE_CREATION_DATE', 'OPTION_NUMBER',
    #   'PRODUCT_ID', 'ORDERED_QUANTITY','LABEL','COMMENTS','REPORT_DATE', 'UPLOAD_BY','ML_COLLECTED']]

    df_data = df.values

    db.session.bulk_insert_mappings(
                                    DfpmToolSubscription,
                                    [dict(
                                        Email=row[0].replace('\xa0',''),
                                        Subscription=row[1].replace('\xa0',''),
                                        Added_by=row[2].replace('\xa0',''),
                                        Added_on=row[3].replace('\xa0',''),
                                        )
                                     for row in df_data]
                                    )
    db.session.commit()
    print('data added')

if __name__ == '__main__':
    #add_user_log(user='kwang2', location='Admin', user_action='Visit',
    #             summary='Warning')

    #df_backlog=pd.read_excel('/Users/wangken/py/dfpm_tools/backlog history.xlsx')
    #df_config_rule=pd.read_excel('/Users/wangken/downloads/dfpm tool data.xlsx',sheet_name='config_rule')
    #df_dfpm_mapping=pd.read_excel('/Users/wangken/downloads/dfpm tool data.xlsx',sheet_name='dfpm')
    #df_subscribe   =pd.read_excel('/Users/wangken/downloads/dfpm tool data.xlsx',sheet_name='subscribe')

    #from_file_add_tan_grouping_data_from_template(df_grouping)
    #from_file_add_exceptional_sourcing_split_data_from_template(df_split)
    #from_file_add_exceptional_priority_data_from_template(df_priority)
    #from_file_add_backlog_data_from_template(df_backlog)
    #from_file_add_config_rule_data_from_template(df_config_rule)
    #from_file_add_dfpm_mapping_data_from_template(df_dfpm_mapping)
    #from_file_add_subscribe_data_from_template(df_subscribe)

    #df_config = pd.read_excel('/Users/wangken/downloads/Copy of config_error_tracker.xlsx')
    #from_file_add_identified_error_config_data_from_template(df_config)
    pass
