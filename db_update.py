from flask_setting import *
import pandas as pd

def update_dfpm_mapping_data(dfpm, org, bu, extra_pf, exclusion_pf,login_user):
    '''
    Update table based on user input
    '''
    # multiple criteria use filter (different from filter_by)
    records = DfpmMapping.query.filter(DfpmMapping.DFPM==dfpm,DfpmMapping.Org==org).all()

    for record in records:
        record.DFPM =dfpm
        record.Org=org
        record.BU=bu
        record.Extra_PF=extra_pf
        record.Exclusion_PF=exclusion_pf
        record.Added_by=login_user
        record.Added_on=pd.Timestamp.now().date()

        db.session.commit()

def update_subscription(email,task,login_user):
    '''
    Update table based on user input
    '''
    # multiple criteria use filter (different from filter_by)
    records = Subscription.query.filter(Subscription.Email==email).all()

    for record in records:
        record.Email = email
        record.Subscription=task
        record.Added_by=login_user
        record.Added_on=pd.Timestamp.now().date()

        db.session.commit()

if __name__=='__main__':

    #update_ml_collected_label('packed_order_with_new_pid')
    start_time=pd.Timestamp.now()
    from db_read import read_table
    df=read_table('packed_order_with_new_pid','ML_COLLECTED IS NULL')
    print(df.shape)
    update_ml_collected_label_batch('packed_order_with_new_pid', df)
    finish_time=pd.Timestamp.now()
    processing_time = round((finish_time - start_time).total_seconds() / 60, 1)
    print(processing_time)
