import pandas as pd
import numpy as np
from sending_email import send_attachment_and_embded_image
from blg_settings import *
from blg_functions import commonize_and_create_main_item
from db_read import read_table
from db_add import add_error_config_data
import time
from datetime import datetime

def config_func_mapping():
    """
    Define a config rule mapping based on PF and corresponding function name to do the config check.
    :return:
    """
    #[[exclusion org],[PF],rule_function]
    # the restrction criteria here [org][pf] are disabled!! followed in the template - C9400 need further update
    config_func = ['find_config_error_per_c9400_rules_pwr_sup_lc_alternative_solution(dfx,wrong_po_dict,checking_time)',
                    'find_config_error_per_isr43xx_vg450_rules_sm_nim(dfx,wrong_po_dict,checking_time)',
                    'find_pabu_wrong_slot_combination_alternative_solution(dfx,wrong_po_dict,checking_time)',
                    'find_error_by_config_comparison_with_history_error(dfx,wrong_po_dict,checking_time)',
                   'find_config_error_per_generic_rule(dfx,wrong_po_dict,checking_time)',
                   #'find_config_error_per_generic_rule_alternative_way(dfx, wrong_po_dict,checking_time)'
                    ]

    return config_func


def combine_pid_and_slot(df):
    """
    Identify the slot PIDs and combine it with the module PID beneath it
    """
    df_rsp_slot=read_table('dfpm_tool_rsp_slot')
    for row in df_rsp_slot.itertuples():
        pf_list=row.PF.split(';')
        rsp=row.RSP_KEYWORD
        slot=row.SLOT_KEYWORD

        df.loc[:,'slot']=np.where((df.main_pf.isin(pf_list)) & (df.PRODUCT_ID.str.contains(slot)),
                                  df.PRODUCT_ID,
                                  np.nan)

        df.loc[:, 'pid_slot'] = np.nan
        slot = ''
        for row in df.itertuples():
            if not pd.isnull(row.slot):
                slot = row.slot
            else:
                if slot != '':
                    if not rsp in row.PRODUCT_ID:
                        df.loc[row.Index, 'pid_slot'] = row.PRODUCT_ID + '_' + slot
                    slot = ''

    df.loc[:, 'PRODUCT_ID'] = np.where(df.pid_slot.notnull(),
                                            df.pid_slot,
                                            df.PRODUCT_ID)

    return df


def find_pabu_wrong_slot_combination(dfx,wrong_po_dict):
    """
    Check related PABU product if the cards are using right slot or if necessary PIDs are included.
    Three types are checked:
    1) Exclusion:
    2) Inclusion:
    3) No support:
    """
    fname_config=os.path.join(base_dir_tracker,'PABU slot config rules.xlsx')
    df_rule_exclusion=pd.read_excel(fname_config, sheet_name='EXCLUSION')
    df_rule_exclusion.loc[:,'PID_SLOT_A']=df_rule_exclusion.PID_A.str.strip() + '_' + df_rule_exclusion.SLOT_A.str.strip()
    df_rule_exclusion.loc[:, 'PID_SLOT_B'] = df_rule_exclusion.PID_B.str.strip() + '_' + df_rule_exclusion.SLOT_B.str.strip()
    df_rule_exclusion.drop_duplicates(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True) # in case duplication
    df_rule_exclusion.sort_values(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True)

    df_rule_inclusion=pd.read_excel(fname_config, sheet_name='INCLUSION')
    df_rule_inclusion.loc[:,'PID_SLOT_A']=df_rule_inclusion.PID_A.str.strip() + '_' + df_rule_inclusion.SLOT_A.str.strip()
    df_rule_inclusion.loc[:, 'PID_SLOT_B'] = df_rule_inclusion.PID_B.str.strip()  # no need consider slot
    df_rule_inclusion.drop_duplicates(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True) # in case duplication
    df_rule_inclusion.sort_values(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True)

    df_rule_no_support = pd.read_excel(fname_config, sheet_name='NO_SUPPORT')
    df_rule_no_support.loc[:, 'PID_SLOT_A'] = df_rule_no_support.PID_A.str.strip() + '_' + df_rule_no_support.SLOT_A.str.strip()
    df_rule_no_support.drop_duplicates(['PID_RSP','PID_SLOT_A'], inplace=True)  # in case duplication

    df_org=pd.read_excel(fname_config, sheet_name='APPLICABLE_ORG')

    df_rule_exclusion.fillna('',inplace=True)
    df_rule_inclusion.fillna('',inplace=True)
    df_rule_no_support.fillna('',inplace=True)

    # limit org from 3a4
    applicable_org=df_org.iloc[0,0].strip().split(';')
    dfx=dfx[dfx.ORGANIZATION_CODE.isin(applicable_org)].copy()

    # for exclusion rules
    for row in df_rule_exclusion.itertuples():
        bu = row.BU.strip().upper()
        pf = row.PF.strip().upper()
        pid_rsp=row.PID_RSP.strip().upper()
        pid_slot_a = row.PID_SLOT_A.strip().upper()
        pid_slot_b = row.PID_SLOT_B.strip().upper()
        remark = row.REMARK

        # limit the df based on rsp/org/bu/pf
        dfy = dfx.copy()
        dfy.loc[:, 'eligible'] = np.where(dfy.PRODUCT_ID.str.contains(pid_rsp),
                                          'YES',
                                          'NO')

        dfy_eligible = dfy[dfy.eligible == 'YES']
        po_list = dfy_eligible.PO_NUMBER.unique()
        dfy = dfx[dfx.PO_NUMBER.isin(po_list)].copy()

        if bu != '':
            dfy = dfy[dfy.main_bu==bu].copy()
        if pf != '':
            dfy = dfy[dfy.main_pf==pf].copy()

        for po in po_list:
            pid_list = dfy[dfy.PO_NUMBER == po].PRODUCT_ID.values
            # check if including wong pid_slot
            if pid_slot_a in pid_list:
                if pid_slot_b in pid_list:
                    wrong_pid_slot = True
                else:
                    wrong_pid_slot = False

                if wrong_pid_slot==True:
                    wrong_po_dict[po]='(cagong)' + remark

    # inclusion rules
    for row in df_rule_inclusion.itertuples():
        bu = row.BU.strip().upper()
        pf = row.PF.strip().upper()
        pid_rsp=row.PID_RSP.strip().upper()
        pid_slot_a = row.PID_SLOT_A.strip().upper()
        pid_b = row.PID_B.strip().upper()
        remark = row.REMARK

        # limit the df based on rsp/org/bu/pf
        dfy = dfx.copy()
        dfy.loc[:, 'eligible'] = np.where(dfy.PRODUCT_ID.str.contains(pid_rsp),
                                          'YES',
                                          'NO')

        dfy_eligible = dfy[dfy.eligible == 'YES']
        po_list = dfy_eligible.PO_NUMBER.unique()
        dfy = dfx[dfx.PO_NUMBER.isin(po_list)].copy()

        if bu != '':
            dfy = dfy[dfy.main_bu==bu].copy()
        if pf != '':
            dfy = dfy[dfy.main_pf==pf].copy()

        for po in po_list:
            pid_list = dfy[dfy.PO_NUMBER == po].PRODUCT_ID.values

            # check if including wong pid_slot
            if pid_slot_a in pid_list:
                if pid_b in pid_list:
                    missing_pid = False
                else:
                    missing_pid = True

                if missing_pid==True:
                    wrong_po_dict[po]='(cagong)' + remark

    # No support rules
    for row in df_rule_no_support.itertuples():
        bu = row.BU.strip().upper()
        pf = row.PF.strip().upper()
        pid_rsp=row.PID_RSP.upper()
        pid_slot_a = row.PID_SLOT_A.strip().upper()
        remark = row.REMARK

        # limit the df based on rsp/org/bu/pf
        dfy = dfx.copy()
        dfy.loc[:, 'eligible'] = np.where(dfy.PRODUCT_ID.str.contains(pid_rsp),
                                          'YES',
                                          'NO')

        dfy_eligible = dfy[dfy.eligible == 'YES']
        po_list = dfy_eligible.PO_NUMBER.unique()
        dfy = dfx[dfx.PO_NUMBER.isin(po_list)].copy()

        if bu !='':
            dfy = dfy[dfy.main_bu==bu].copy()
        if pf != '':
            dfy = dfy[dfy.main_pf==pf].copy()

        for po in po_list:
            pid_list = dfy[dfy.PO_NUMBER == po].PRODUCT_ID.unique()

            # check if including wong pid_slot
            if pid_slot_a in pid_list:
                extra_wrong_pid = True
            else:
                extra_wrong_pid = False

            if extra_wrong_pid==True:
                wrong_po_dict[po]='(cagong)' + remark

    return wrong_po_dict


def find_pabu_wrong_slot_combination_alternative_solution(dfx,wrong_po_dict,checking_time):
    """
    Check related PABU product if the cards are using right slot or if necessary PIDs are included.
    Three types are checked:
    1) Exclusion:
    2) Inclusion:
    3) No support:
    """
    time_start=time.time()

    fname_config=os.path.join(base_dir_tracker,'PABU slot config rules.xlsx')
    df_rule_exclusion=pd.read_excel(fname_config, sheet_name='EXCLUSION')
    df_rule_exclusion.loc[:,'PID_SLOT_A']=df_rule_exclusion.PID_A.str.strip() + '_' + df_rule_exclusion.SLOT_A.str.strip()
    df_rule_exclusion.loc[:, 'PID_SLOT_B'] = df_rule_exclusion.PID_B.str.strip() + '_' + df_rule_exclusion.SLOT_B.str.strip()
    df_rule_exclusion.drop_duplicates(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True) # in case duplication
    df_rule_exclusion.sort_values(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True)

    df_rule_inclusion=pd.read_excel(fname_config, sheet_name='INCLUSION')
    df_rule_inclusion.loc[:,'PID_SLOT_A']=df_rule_inclusion.PID_A.str.strip() + '_' + df_rule_inclusion.SLOT_A.str.strip()
    df_rule_inclusion.loc[:, 'PID_SLOT_B'] = df_rule_inclusion.PID_B.str.strip()  # no need consider slot
    df_rule_inclusion.drop_duplicates(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True) # in case duplication
    df_rule_inclusion.sort_values(['PID_RSP','PID_SLOT_A','PID_SLOT_B'],inplace=True)

    df_rule_no_support = pd.read_excel(fname_config, sheet_name='NO_SUPPORT')
    df_rule_no_support.loc[:, 'PID_SLOT_A'] = df_rule_no_support.PID_A.str.strip() + '_' + df_rule_no_support.SLOT_A.str.strip()
    df_rule_no_support.drop_duplicates(['PID_RSP','PID_SLOT_A'], inplace=True)  # in case duplication

    df_scope=pd.read_excel(fname_config, sheet_name='APPLICABLE_SCOPE')

    df_rule_exclusion.fillna('',inplace=True)
    df_rule_inclusion.fillna('',inplace=True)
    df_rule_no_support.fillna('',inplace=True)
    df_scope.fillna('',inplace=True)

    # Limit dfx based on applicable:org/bu/pf
    org_scope=df_scope.iloc[0,0].strip().split(';')
    bu_scope=df_scope.iloc[0,1].strip().split(';')
    pf_scope=df_scope.iloc[0,2].strip().split(';')

    #if org_scope!=['']:
    dfx=dfx[dfx.ORGANIZATION_CODE.isin(org_scope)].copy()

    if bu_scope!=['']:
        dfx=dfx[dfx.main_bu.isin(bu_scope)].copy()
    if pf_scope!=['']:
        dfx=dfx[dfx.main_pf.isin(pf_scope)].copy()

    # Further limit dfx based on RSP PID
    pid_rsp_list=list(df_rule_exclusion.PID_RSP.unique())+list(df_rule_inclusion.PID_RSP.unique()) + list(df_rule_no_support.PID_RSP.unique())
    for pid_rsp in pid_rsp_list:
        dfx.loc[:, 'eligible'] = np.where(dfx.PRODUCT_ID.str.contains(pid_rsp), # do this because PID is already combined PID and SLOT
                                          'YES',
                                          'NO')

    dfx_eligible = dfx[dfx.eligible == 'YES']
    po_list = dfx_eligible.PO_NUMBER.unique()
    dfx = dfx[dfx.PO_NUMBER.isin(po_list)].copy()

    # check po
    for po in po_list:
        pid_list = dfx[dfx.PO_NUMBER == po].PRODUCT_ID.values

        for row in df_rule_exclusion.itertuples():
            pid_rsp = row.PID_RSP.strip().upper()
            pid_slot_a = row.PID_SLOT_A.strip().upper()
            pid_slot_b = row.PID_SLOT_B.strip().upper()
            remark = row.REMARK

            # judge if pid_rsp is in pid_list and hence apply this rule
            if pid_rsp in pid_list:
                if pid_slot_a in pid_list:
                    if pid_slot_b in pid_list:
                        wrong_pid_slot = True
                    else:
                        wrong_pid_slot = False

                    if wrong_pid_slot==True:
                        wrong_po_dict[po]='(cagong)' + remark
                        break # if already fine one error, skip with other rule for this order

        for row in df_rule_inclusion.itertuples():
            pid_rsp = row.PID_RSP.strip().upper()
            pid_slot_a = row.PID_SLOT_A.strip().upper()
            pid_b = row.PID_B.strip().upper()
            remark = row.REMARK

            # judge if pid_rsp is in pid_list and hence apply this rule
            if pid_rsp in pid_list:
                if pid_slot_a in pid_list:
                    if pid_b in pid_list:
                        missing_pid = False
                    else:
                        missing_pid = True

                    if missing_pid == True:
                        wrong_po_dict[po] = '(cagong)' + remark
                        break  # if already fine one error, skip with other rule for this order

        for row in df_rule_no_support.itertuples():
            pid_rsp = row.PID_RSP.upper()
            pid_slot_a = row.PID_SLOT_A.strip().upper()
            remark = row.REMARK

            # judge if pid_rsp is in pid_list and hence apply this rule
            if pid_rsp in pid_list:
                if pid_slot_a in pid_list:
                    extra_wrong_pid = True
                else:
                    extra_wrong_pid = False

                if extra_wrong_pid == True:
                    wrong_po_dict[po] = '(cagong)' + remark
                    break  # if already fine one error, skip with other rule for this order

    time_finish = time.time()
    total_time = int(time_finish - time_start)
    checking_time['ASR903 Slot'] = total_time

    return wrong_po_dict, checking_time



def isr43xx_vg450_rules_sm_nim(pid_qty, nim_pid_slots_dict, sm_pid_slots_dict, adapter_pid_slot_dict,wrong_po_dict, po, sm_criteria, nim_criteria):
    """
    """
    sm_qty = 0
    nim_qty = 0
    adapter_qty = 0

    for pid, qty in pid_qty:
        if pid in sm_pid_slots_dict.keys():
            sm_qty =sm_qty + sm_pid_slots_dict[pid]*qty
        elif pid in nim_pid_slots_dict.keys():
            nim_qty = nim_qty + nim_pid_slots_dict[pid] * qty
        elif pid in adapter_pid_slot_dict.keys():
            adapter_qty=adapter_qty + adapter_pid_slot_dict[pid] * qty

    # check the qty combinations: if SM correct then check NIM; if SM wrong, then no need to check NIM
    if sm_qty +  adapter_qty> sm_criteria:
        #wrong_po_dict.append(po)
        wrong_po_dict[po]='(rachzhan)SM slot over used'
    elif sm_qty +  adapter_qty< sm_criteria:
        wrong_po_dict[po]='(rachzhan)SM slot under used'
    elif nim_qty < nim_criteria:  #(means adapter may or may not carry a NIM card)
        wrong_po_dict[po]='(rachzhan)NIM slot under used'
    elif nim_qty > nim_criteria + adapter_qty:
        wrong_po_dict[po] = '(rachzhan)NIM/ADPTR slot over used'

    return wrong_po_dict


def find_config_error_per_isr43xx_vg450_rules_sm_nim(dfx,wrong_po_dict,checking_time):
    '''
    Check if any PO in SRG ISR43xx having wrong config based on SM-NIM rules
    :param dfx: df filtered by PF that need to check with
    :return error order dict
    '''
    time_start=time.time()

    fname_rule=os.path.join(base_dir_tracker,'SRGBU SM_NIM config rules.xlsx')
    df_pid_slots=pd.read_excel(fname_rule,sheet_name='SM_NIM')
    df_scope=pd.read_excel(fname_rule,sheet_name='APPLICABLE_SCOPE')
    df_scope.fillna('',inplace=True)

    org_scope=df_scope.iloc[0,0].strip().split(';')
    bu_scope=df_scope.iloc[0,1].strip().split(';')
    pf_scope=df_scope.iloc[0,2].strip().split(';')
    nim_pid_slots_dict={}
    sm_pid_slots_dict = {}
    adapter_pid_slot_dict={}
    for row in df_pid_slots.itertuples():
        if row.TYPE=='NIM':
            nim_pid_slots_dict[row.PID.strip().upper()]=int(row.SLOTS)
        elif row.TYPE=='SM':
            sm_pid_slots_dict[row.PID.strip().upper()] = int(row.SLOTS)
        elif row.TYPE=='ADAPTER':
            adapter_pid_slot_dict[row.PID.strip().upper()] = int(row.SLOTS)

    # find main and also limit dfx based on applicable:org/bu/pf
    #if org_scope!=['']:
    dfx=dfx[dfx.ORGANIZATION_CODE.isin(org_scope)].copy()

    if bu_scope!=['']:
        dfx=dfx[dfx.main_bu.isin(bu_scope)].copy()
    if pf_scope!=['']:
        dfx=dfx[dfx.main_pf.isin(pf_scope)].copy()

    # Identify in scope po_list to check with
    dfx_main = dfx[dfx.OPTION_NUMBER == 0]
    #main_po_pid=zip(dfx_main.PO_NUMBER,dfx_main.PRODUCT_ID)
    po_list=dfx_main.PO_NUMBER.values

    for po in po_list:
        main_pid=dfx[(dfx.PO_NUMBER==po)&(dfx.OPTION_NUMBER==0)].PRODUCT_ID.values[0]
        dfy=dfx[dfx.PO_NUMBER == po][['PRODUCT_ID','ORDERED_QUANTITY']].groupby('PRODUCT_ID').sum()
        #print(po,main_pid)

        pid_list=dfy.index.values
        qty_list=dfy.values.reshape(1,-1)[0]
        pid_qty_list=zip(pid_list,qty_list)

        if 'ISR4321' in main_pid:
            sm_criteria=0
            nim_criteria=2

            wrong_po_dict=isr43xx_vg450_rules_sm_nim(pid_qty_list, nim_pid_slots_dict,sm_pid_slots_dict, adapter_pid_slot_dict,wrong_po_dict, po,
                                         sm_criteria, nim_criteria)
        elif 'ISR4331' in main_pid:
            sm_criteria = 1
            nim_criteria = 2

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, nim_pid_slots_dict,sm_pid_slots_dict, adapter_pid_slot_dict,wrong_po_dict, po,
                                         sm_criteria, nim_criteria)
        elif 'ISR4351' in main_pid:
            sm_criteria = 2
            nim_criteria = 3

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, nim_pid_slots_dict,sm_pid_slots_dict, adapter_pid_slot_dict,wrong_po_dict, po,
                                         sm_criteria, nim_criteria)
        elif 'ISR4461' in main_pid:
            sm_criteria = 4
            nim_criteria = 3

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, nim_pid_slots_dict,sm_pid_slots_dict, adapter_pid_slot_dict,wrong_po_dict, po,
                                         sm_criteria, nim_criteria)
        elif 'VG450' in main_pid:
            sm_criteria = 4
            nim_criteria = 3

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, nim_pid_slots_dict,sm_pid_slots_dict, adapter_pid_slot_dict,wrong_po_dict, po,
                                         sm_criteria, nim_criteria)

    time_finish = time.time()
    total_time = int(time_finish - time_start)
    checking_time['SRG NIM_SM'] = total_time

    return wrong_po_dict, checking_time



def c9400_rules_pwr_sup_lc(pid_qty_list, wrong_po_dict, po,main_pid,psu_pid):
    """
    Config rules for C9400 based on main PID, PWR, SUP and LC
    """
    pwr_qty = 0
    sup_qty = 0
    lc_qty = 0

    for pid, qty in pid_qty_list:
        if pid in ['C9400-PWR-2100AC','C9400-PWR-3200AC','C9400-PWR-3200DC']:
            pwr_qty = pwr_qty + qty
        elif 'C9400-SUP' in pid:
            sup_qty = sup_qty + qty
        elif 'C9400-LC' in pid:
            lc_qty = lc_qty + qty
    # check if PWR is missing
    if pwr_qty==0:
        wrong_po_dict[po] = 'PWR missing'
    else:
        # check the qty combinations based on
        if main_pid=='C9410R*':
            if psu_pid=='C9400-PWR-3200AC':
                if pwr_qty==1:
                    if sup_qty>2:
                        wrong_po_dict[po]='SUP qty over'
                    elif lc_qty>4:
                        wrong_po_dict[po] = 'LC qty over'
                else:
                    if sup_qty == 0 :
                        wrong_po_dict[po] = 'SUP missing'
            elif psu_pid=='C9400-PWR-2100AC':
                if pwr_qty==1:
                    if sup_qty + lc_qty >3:
                        wrong_po_dict[po]='SUP+LC qty over'
                else:
                    if sup_qty + lc_qty == 0 :
                        wrong_po_dict[po] = 'SUP/LC missing'
            elif psu_pid == 'C9400-PWR-3200DC':
                if pwr_qty == 1:
                    if sup_qty > 2:
                        wrong_po_dict[po] = 'SUP qty over'
                    elif lc_qty > 1:
                        wrong_po_dict[po] = 'LC qty over'
                else:
                    if sup_qty <2 :
                        wrong_po_dict[po] = 'SUP qty short'
                    elif lc_qty ==0:
                        wrong_po_dict[po] = 'LC missing'
        elif main_pid=='C9407R*':
            if psu_pid == 'C9400-PWR-3200AC':
                if sup_qty==0:
                    wrong_po_dict[po] = 'SUP missing'
            elif psu_pid == 'C9400-PWR-2100AC':
                if pwr_qty==1:
                    if sup_qty>2:
                        wrong_po_dict[po] = 'SUP qty over'
                    elif lc_qty>2:
                        wrong_po_dict[po] = 'LC qty over'
                else:
                    if sup_qty==0:
                        wrong_po_dict[po] = 'SUP missing'
            elif psu_pid == 'C9400-PWR-3200DC':
                if sup_qty==0:
                    wrong_po_dict[po] = 'SUP missing'

    return wrong_po_dict

def find_config_error_per_c9400_rules_pwr_sup_lc(dfx,wrong_po_dict):
    '''
    Check if any PO in UAG C9400 having wrong config based on Chassis-PWR-PSU-LC rules
    :param dfx: df filtered by PF that need to check with
    :return error order dict
    '''
    fname_rule = os.path.join(base_dir_tracker, 'UABU C9400 PWR_LC_SUP combination rule.xlsx')
    df_rule = pd.read_excel(fname_rule, sheet_name='RULE')
    df_org = pd.read_excel(fname_rule, sheet_name='APPLICABLE_ORG')

    # Limit data by org
    org_applicable = df_org.iloc[0, 0].strip().split(';')
    dfx=dfx[dfx.ORGANIZATION_CODE.isin(org_applicable)].copy()

    # for exclusion rules
    for row in df_rule.itertuples():
        bu = row.BU.strip().upper()
        pf = row.PF.strip().upper()
        main_pid=row.MAIN_PID.strip().upper()
        pid_a = row.PID_A.strip().upper()
        a_criteria = row.A_CRITERIA.strip().upper()
        a_qty=int(row.A_QTY)
        pid_b = row.PID_B.strip().upper()
        b_criteria = row.B_CRITERIA.strip().upper()
        b_qty = int(row.B_QTY)
        remark = row.REMARK

        # make the criteria
        if a_criteria=='=':
            a_criteria='=='
        if b_criteria == '=':
            b_criteria = '=='
        a_criteria_qty=a_criteria+str(a_qty)
        b_criteria_qty=b_criteria+str(b_qty)

        # limit the df based on rsp/org/bu/pf
        dfy = dfx.copy()
        dfy.loc[:, 'eligible'] = np.where(dfy.OPTION_NUMBER==0,
                                          np.where(dfy.PRODUCT_ID.str.contains(main_pid),
                                                    'YES',
                                                    'NO'),
                                          None)

        dfy_eligible = dfy[dfy.eligible == 'YES']
        po_list = dfy_eligible.PO_NUMBER.unique()
        dfy = dfx[dfx.PO_NUMBER.isin(po_list)].copy()

        if bu != '':
            dfy = dfy[dfy.main_bu==bu].copy()
        if pf != '':
            dfy = dfy[dfy.main_pf==pf].copy()

        for po in po_list:
            pid_list = dfy[dfy.PO_NUMBER == po].PRODUCT_ID.values

            # check if including wong pid_slot
            if pid_a in pid_list:
                pid_a_qty=dfy[(dfy.PO_NUMBER == po)&(dfy.PRODUCT_ID==pid_a)].ORDERED_QUANTITY.sum()

                pid_b_qty=dfy[(dfy.PO_NUMBER == po)&(dfy.PRODUCT_ID.str.contains(pid_b))].ORDERED_QUANTITY.sum()

                if eval('pid_a_qty'+a_criteria_qty):
                    if not eval('pid_b_qty'+b_criteria_qty):
                        wrong_po_dict[po] = remark

    return wrong_po_dict

def find_config_error_per_c9400_rules_pwr_sup_lc_alternative_solution(dfx,wrong_po_dict,checking_time):
    '''
    Check if any PO in UAG C9400 having wrong config based on Chassis-PWR-PSU-LC rules
    :param dfx: df filtered by PF that need to check with
    :return error order dict
    '''
    time_start=time.time()
    fname_rule = os.path.join(base_dir_tracker, 'UABU C9400 PWR_LC_SUP combination rule.xlsx')
    df_rule = pd.read_excel(fname_rule, sheet_name='RULE')
    df_scope = pd.read_excel(fname_rule, sheet_name='APPLICABLE_SCOPE')

    # Limit dfx based on applicable:org/bu/pf
    df_scope.fillna('', inplace=True)
    org_scope = df_scope.iloc[0, 0].strip().split(';')
    bu_scope = df_scope.iloc[0, 1].strip().split(';')
    pf_scope = df_scope.iloc[0, 2].strip().split(';')

    #if org_scope != ['']:
    dfx = dfx[dfx.ORGANIZATION_CODE.isin(org_scope)].copy()

    if bu_scope != ['']:
        dfx = dfx[dfx.main_bu.isin(bu_scope)].copy()
    if pf_scope != ['']:
        dfx = dfx[dfx.main_pf.isin(pf_scope)].copy()

    # Further limit dfx based on RSP PID
    main_pid_list = df_rule.MAIN_PID.unique()
    for main_pid in main_pid_list:
        dfx.loc[:, 'eligible'] = np.where(dfx.PRODUCT_ID.str.contains(main_pid),
                                          # do this because PID might be already combined PID and SLOT
                                          'YES',
                                          'NO')

    dfx_eligible = dfx[dfx.eligible == 'YES']
    po_list = dfx_eligible.PO_NUMBER.unique()
    dfx = dfx[dfx.PO_NUMBER.isin(po_list)].copy()

    # check po
    for po in po_list:
        pid_list = dfx[dfx.PO_NUMBER == po].PRODUCT_ID.values

        for row in df_rule.itertuples():
            main_pid = row.MAIN_PID.strip().upper()
            pid_a = row.PID_A.strip().upper()
            a_criteria = row.A_CRITERIA.strip().upper()
            a_qty = int(row.A_QTY)
            pid_b = row.PID_B.strip().upper()
            b_criteria = row.B_CRITERIA.strip().upper()
            b_qty = int(row.B_QTY)
            remark = row.REMARK

            # make the criteria
            if a_criteria == '=':
                a_criteria = '=='
            if b_criteria == '=':
                b_criteria = '=='
            a_criteria_qty = a_criteria + str(a_qty)
            b_criteria_qty = b_criteria + str(b_qty)

            # judge if main_pid is in pid_list and hence apply this rule
            if main_pid in pid_list:
                # check if including wong pid_slot
                if pid_a in pid_list:
                    pid_a_qty = dfx[(dfx.PO_NUMBER == po) & (dfx.PRODUCT_ID == pid_a)].ORDERED_QUANTITY.sum()
                    pid_b_qty = dfx[(dfx.PO_NUMBER == po) & (dfx.PRODUCT_ID.str.contains(pid_b))].ORDERED_QUANTITY.sum()

                    if eval('pid_a_qty' + a_criteria_qty):
                        if not eval('pid_b_qty' + b_criteria_qty):
                            wrong_po_dict[po] = '(gsolisgo)' + remark

    time_finish=time.time()
    total_time=int(time_finish-time_start)
    checking_time['C9400']=total_time

    return wrong_po_dict,checking_time



def find_config_error_per_generic_rule(dfx,wrong_po_dict,checking_time):
    '''
    Using generic rules to identify errors
    :return error order dict
    '''
    df_rule = read_table('dfpm_tool_general_config_rule')

    #df_rule=pd.read_excel(os.path.join(base_dir_tracker,'General rule.xlsx'))
    #print(df_rule)
    for row in df_rule.itertuples():
        rule_start=time.time()
        id=row.id
        org=row.ORG.split(';')
        bu=row.BU.split(';')
        pf=row.PF.split(';')
        exception_main_pid=row.EXCEPTION_MAIN_PID.split(';')
        pid_a=row.PID_A.split(';')
        pid_b=row.PID_B.split(';')
        pid_b_operator=row.PID_B_OPERATOR
        pid_b_qty=row.PID_B_QTY
        effective_date=row.EFFECTIVE_DATE
        remark=row.REMARK
        added_by=row.Added_by

        dfy=dfx[(dfx.ORGANIZATION_CODE.isin(org))&(dfx.main_bu.isin(bu))].copy()

        if effective_date != '':
            effective_date = datetime.strptime(effective_date,'%Y-%m-%d')
            dfy=dfy[dfy.LINE_CREATION_DATE>=effective_date].copy()

        if pf!=['']:
            dfy = dfy[dfy.main_pf.isin(pf)].copy()
        if exception_main_pid!=['']:
            dfy.loc[:,'exception']=np.where(dfy.PRODUCT_ID.isin(exception_main_pid), # Note: assuming it's main PID so not specifically check if it's main PID
                                            'YES',
                                            'NO')
            exception_po=dfy[dfy.exception=='YES'].PO_NUMBER
            dfy=dfy[~dfy.PO_NUMBER.isin(exception_po)].copy()

        if pid_a!=['']:
            dfy.loc[:, 'eligible_pid'] = np.where(dfy.PRODUCT_ID.isin(pid_a),
                                               'YES',
                                               'NO')
            po_list = dfy[dfy.eligible_pid == 'YES'].PO_NUMBER.unique()
        else:
            po_list = dfy.PO_NUMBER.unique()

        # check po
        for po in po_list:
            pid_list = dfy[dfy.PO_NUMBER == po].PRODUCT_ID.values

            # make the criteria
            if pid_b_operator == '=':
                pid_b_operator = '=='

            criteria_qty = pid_b_operator + str(pid_b_qty)

            dfz=dfy[(dfy.PO_NUMBER == po) & (dfy.PRODUCT_ID.isin(pid_b))]
            if dfz.shape[0]>0: # include PID_B
                pid_b_actual_qty = dfz.ORDERED_QUANTITY.sum()
            else:
                pid_b_actual_qty=0

            if not eval('pid_b_actual_qty' + criteria_qty):
                wrong_po_dict[po] = 'Rule#'+ str(id) + '('+added_by+'):' +remark
        rule_finish = time.time()
        rule_time=int(rule_finish-rule_start)
        checking_time['#'+str(id)]=rule_time

    return wrong_po_dict,checking_time



def find_config_error_per_generic_rule_alternative_way(dfx,wrong_po_dict):
    '''
    Using generic rules to identify errors;
    Get a full order list, then iter each order, and each rule under each order
    (performance is lower thus not used)
    :return error order dict
    '''
    df_rule = read_table('dfpm_tool_general_config_rule')
    orgs=df_rule.ORG.unique()
    bus=df_rule.BU.unique()
    pfs = df_rule.PF.unique()
    #exception_main_pid=df_rule.EXCEPTION_MAIN_PID.unique()

    org_list=[]
    for org in orgs:
        sub_org_list=org.split(';')
        org_list=org_list+sub_org_list
    org_list=set(org_list)

    bu_list = []
    for bu in bus:
        sub_bu_list = bu.split(';')
        bu_list = bu_list + sub_bu_list
    bu_list = set(bu_list)

    pf_list = []
    for pf in pfs:
        sub_pf_list = pf.split(';')
        pf_list = pf_list + sub_pf_list
    pf_list = set(pf_list)

    dfx = dfx[(dfx.ORGANIZATION_CODE.isin(org_list)) & (dfx.main_bu.isin(bu_list)) & (dfx.main_pf.isin(pf_list))].copy()

    po_list=[]
    dfy=dfx[dfx.OPTION_NUMBER==0]
    for org,bu,pf,po in zip(dfy.ORGANIZATION_CODE,dfy.main_bu,dfy.main_pf,dfy.PO_NUMBER):
        po_list.append((org,bu,pf,po))

    for item in po_list:
        org=item[0]
        bu=item[1]
        pf=item[2]
        po=item[3]
        pid_list = dfx[dfx.PO_NUMBER == po].PRODUCT_ID.values

        for row in df_rule.itertuples():
            id=row.id
            rule_org=row.ORG.split(';')
            rule_bu=row.BU.split(';')
            rule_pf=row.PF.split(';')
            rule_exception_pid=row.EXCEPTION_MAIN_PID.split(';')
            pid_a=row.PID_A.split(';')
            pid_b=row.PID_B.split(';')
            pid_b_operator=row.PID_B_OPERATOR
            pid_b_qty=row.PID_B_QTY
            remark=row.REMARK

            if org in rule_org and bu in rule_bu and pf in rule_pf:
                if np.in1d(rule_exception_pid,pid_list).sum()==0:
                    # make the criteria
                    if pid_b_operator == '=':
                        pid_b_operator = '=='

                    criteria_qty = pid_b_operator + str(pid_b_qty)

                    if pid_a != ['']:
                        for pid in pid_a:
                            if pid in pid_list:
                                dfz = dfy[(dfy.PO_NUMBER == po) & (dfy.PRODUCT_ID.isin(pid_b))]
                                break
                            else:
                                dfz = pd.DataFrame()
                    else:
                        dfz = dfy[(dfy.PO_NUMBER == po) & (dfy.PRODUCT_ID.isin(pid_b))]

                    if dfz.shape[0] > 0:  # include PID_B
                        pid_b_actual_qty = dfz.ORDERED_QUANTITY.sum()

                        if not eval('pid_b_actual_qty' + criteria_qty):
                            wrong_po_dict[po] = 'Rule#' + str(id) + ':' + remark

    return wrong_po_dict



def make_error_config_df_output_and_save_tracker(df_3a4,region, login_user, wrong_po_dict,save_to_tracker):
    """
    Create the df output for error config based on check result. and save tracker.
    """
    col = ['ORGANIZATION_CODE','BUSINESS_UNIT', 'PRODUCT_FAMILY', 'PO_NUMBER', 'OPTION_NUMBER',  'PRODUCT_ID',
           'ORDERED_QUANTITY', 'LINE_CREATION_DATE', 'ORDER_HOLDS', 'Config_error', 'Report date']

    # config error tracker
    df_error_tracker_old_full = pd.read_excel(os.path.join(base_dir_tracker, 'config_error_tracker.xlsx'))
    df_error_tracker_old_full.loc[:, 'Report date'] = df_error_tracker_old_full['Report date'].map(lambda x: x.date())

    # create the new order error df and old open error df
    df_error_new = df_3a4[
        (df_3a4.PO_NUMBER.isin(wrong_po_dict.keys())) & (~df_3a4.PO_NUMBER.isin(df_error_tracker_old_full.PO_NUMBER))].copy()
    df_error_old = df_3a4[
        (df_3a4.PO_NUMBER.isin(wrong_po_dict.keys())) & (df_3a4.PO_NUMBER.isin(df_error_tracker_old_full.PO_NUMBER)) & (df_3a4.OPTION_NUMBER==0)].copy()

    df_error_new.loc[:, 'Config_error'] = np.where(df_error_new.OPTION_NUMBER == 0,
                                                   df_error_new.PO_NUMBER.map(lambda x: wrong_po_dict.get(x)),
                                                   None)
    df_error_new.loc[:, 'Report date'] = np.where(df_error_new.Config_error.notnull(),
                                                  pd.Timestamp.now().date(),
                                                  None)

    report_date_dic={}
    for row in df_error_tracker_old_full.itertuples():
        report_date_dic[row.PO_NUMBER]=[row.Config_error,row._11]

    df_error_old.loc[:, 'Report date'] = df_error_old.PO_NUMBER.map(lambda x:report_date_dic.get(x)[1])
    df_error_old.loc[:, 'Config_error'] = df_error_old.PO_NUMBER.map(lambda x: report_date_dic.get(x)[0])

    df_error_new = df_error_new[col].copy()
    df_error_old = df_error_old[col].copy()

    df_error_new.set_index('ORGANIZATION_CODE', inplace=True)
    df_error_old.set_index('ORGANIZATION_CODE', inplace=True)

    qty_new_error = df_error_new[df_error_new.OPTION_NUMBER == 0].shape[0]

    if qty_new_error>0:
        if region!='':
            name_prefix=region + ' new error config '
        else:
            name_prefix='New error config '

        fname_new_error = name_prefix + login_user + ' ' + pd.Timestamp.now().strftime('%m-%d %H:%M') + '.xlsx'
        df_error_new.to_excel(os.path.join(base_dir_output, fname_new_error))

        # save new tracker file
        if save_to_tracker:
            df_error_tracker_old_full.set_index('ORGANIZATION_CODE', inplace=True)
            df_error_tracker = pd.concat([df_error_tracker_old_full, df_error_new[df_error_new.OPTION_NUMBER == 0]], sort=False)
            df_error_tracker.to_excel(os.path.join(base_dir_tracker, 'config_error_tracker.xlsx'))
    else:
        fname_new_error = ''

    df_error_new.reset_index(inplace=True)
    df_error_old.reset_index(inplace=True)
    df_error_new = df_error_new[df_error_new.OPTION_NUMBER == 0].copy()
    col.remove('OPTION_NUMBER')
    df_error_new=df_error_new[col].copy()
    df_error_new.sort_values(['ORGANIZATION_CODE', 'BUSINESS_UNIT', 'PRODUCT_FAMILY','PRODUCT_ID'],inplace=True)
    df_error_old=df_error_old[col].copy()
    df_error_old.ORDER_HOLDS.fillna('', inplace=True)
    df_error_new.ORDER_HOLDS.fillna('', inplace=True)

    return qty_new_error, df_error_new, df_error_old, fname_new_error

def send_config_error_data_by_email(org, df_error_new, df_error_old,fname_new_error,login_user,to_address,sender):
    """
    Send the result with attachment
    """
    if df_error_new.shape[0]>0:
        att_file = [(base_dir_output, fname_new_error)]  # List of tuples (path, file_name)
        new_error_summary=df_error_new.pivot_table(index='ORGANIZATION_CODE',values='PO_NUMBER',aggfunc=len).to_dict()
        new_error_summary=list(new_error_summary.values())[0]
    else:
        att_file = None
        new_error_summary=0

    if df_error_old.shape[0]>0:
        old_error_summary = df_error_old.pivot_table(index='ORGANIZATION_CODE', values='PO_NUMBER', aggfunc=len).to_dict()
        old_error_summary = list(old_error_summary.values())[0]
    else:
        old_error_summary=0

    subject = org + ' config error check summary (sent by: '+login_user +')'
    html = 'config_check_result_email.html'

    send_attachment_and_embded_image(to_address, subject, html,
                                     sender=sender,
                                     att_filenames=att_file,
                                     bcc=[super_user + '@cisco.com'],
                                     new_error_summary=new_error_summary,
                                     old_error_summary=old_error_summary,
                                     new_error_header=df_error_new.columns,
                                     new_error_data=df_error_new.values,
                                     old_error_header=df_error_old.columns,
                                     old_error_data=df_error_old.values)



def scale_down_po_to_one_set(df):
    '''
    Scale down each PO to one set - without pivot
    '''

    dfx=df[df.OPTION_NUMBER==0]
    dfx=dfx[dfx.ORDERED_QUANTITY>1]
    po_qty=zip(dfx.PO_NUMBER,dfx.ORDERED_QUANTITY)
    po_qty_dic={}

    for po,qty in po_qty:
        po_qty_dic[po]=qty

    df.loc[:,'max_qty']=df.PO_NUMBER.map(lambda x: po_qty_dic[x] if x in po_qty_dic.keys() else 1)
    df.loc[:,'ORDERED_QUANTITY']=df.ORDERED_QUANTITY / df.max_qty

    return df

def identify_config_error_po(df_3a4,config_func,checking_time):

    wrong_po_dict = {}

    # pick out ATO PO
    df_pivot=df_3a4.pivot_table(index='PO_NUMBER',values='OPTION_NUMBER',aggfunc=sum)
    ato_po=df_pivot[df_pivot.OPTION_NUMBER>0].index
    df_ato=df_3a4[df_3a4.PO_NUMBER.isin(ato_po)].copy()
    df_ato=df_ato[df_ato.ADDRESSABLE_FLAG!='PO_CANCELLED']

    for org_pf_func in config_func:
        dfx=df_ato.copy() # make a copy of original each time
        """
        if org_pf_func[0]!=[]:
            dfx = dfx[(~df_ato.ORGANIZATION_CODE.isin(org_pf_func[0]))].copy()
        if org_pf_func[1]!=[]:
            dfx=dfx[dfx.main_pf.isin(org_pf_func[1])].copy()  # used in below config_func
        """

        if dfx.shape[0] > 0:
            wrong_po_dict,checking_time = eval(org_pf_func)

    return wrong_po_dict,checking_time








### config comparison

def get_same_config_data_to_remove_from_db(df_error_db, df_remove):
    """
    Remove same config in error db to remove based on uploaded config data
    """
    # remove the duplicated configs from the uploading df
    df_remove_p = df_remove.pivot_table(index=['PO_NUMBER'], columns='PRODUCT_ID', values='ORDERED_QUANTITY',
                                aggfunc=sum)

    df_remove_p = df_remove_p.apply(lambda x: x / x.min(), axis=1)
    df_remove_p.drop_duplicates(inplace=True)
    df_remove_p.reset_index(inplace=True)
    unique_config_po=df_remove_p.PO_NUMBER.values
    df_remove=df_remove[df_remove.PO_NUMBER.isin(unique_config_po)].copy()
    df_remove.loc[:,'Added_by']=''
    # Fill up remark
    df_remove = fill_up_remark(df_remove)

    # find out the new configs not yet in database
    fsc = FindSameConfig()
    base_config_dict = fsc.create_base_config_dict(df_remove) # use df_remove as the base
    new_config_dict = fsc.create_new_config_dict(df_error_db,df_remove)
    compare_result_dict = fsc.compare_new_and_base_dict(new_config_dict, base_config_dict)
    # exclude those same configs identified
    df_error_db_remove=df_error_db[df_error_db.PO_NUMBER.isin(compare_result_dict.keys())].copy()

    return df_error_db_remove



def add_reported_po_to_tracker_and_upload_unique_new_config_to_db(df_upload,df_error_db,login_user):
    """
    For user to report/upload new error config to database. When uploading remove the duplicated error configs in uploading data,
    and also only save new config errors.
    """
    # Fill up remark based on option 0 remark
    df_upload = fill_up_remark(df_upload)

    # remove the duplicated configs from the uploading df
    df_upload_p = df_upload.pivot_table(index=['PO_NUMBER'], columns='PRODUCT_ID', values='ORDERED_QUANTITY',
                                aggfunc=sum)

    df_upload_p = df_upload_p.apply(lambda x: x / x.min(), axis=1)
    df_upload_p.drop_duplicates(inplace=True)
    #df_upload_p.reset_index(inplace=True)
    #unique_config_po=df_upload_p.PO_NUMBER.values
    df_upload_unique=df_upload[df_upload.PO_NUMBER.isin(df_upload_p.index)].copy()

    # find out the new configs not yet in database
    fsc = FindSameConfig()
    base_config_dict = fsc.create_base_config_dict(df_error_db)
    new_config_dict = fsc.create_new_config_dict(df_upload_unique,df_error_db)
    compare_result_dict = fsc.compare_new_and_base_dict(new_config_dict, base_config_dict)
    # exclude those same configs
    df_upload_unique=df_upload_unique[~df_upload_unique.PO_NUMBER.isin(compare_result_dict.keys())].copy()

    # add to database
    new_config_po_qty = len(df_upload.PO_NUMBER.unique())
    if new_config_po_qty > 0:
        add_error_config_data(df_upload_unique, login_user)

    # TODO: add to tracker file as well (issue, deleting from tracker might be an issue if want to)
    df_error_tracker = pd.read_excel(os.path.join(base_dir_tracker, 'config_error_tracker.xlsx'))
    df_upload_main_new = df_upload[(~df_upload.PO_NUMBER.isin(df_error_tracker.PO_NUMBER))&(df_upload.OPTION_NUMBER==0)]
    df_upload_main_new.loc[:,'Config_error']=df_upload_main_new.REMARK.map(lambda x: '(user report) '+ x)
    df_upload_main_new.loc[:, 'Report date'] = pd.Timestamp.now()
    df_upload_main_new.drop(['REMARK'],axis=1,inplace=True)
    df_error_tracker=pd.concat([df_error_tracker,df_upload_main_new],sort=False)
    df_error_tracker.set_index('ORGANIZATION_CODE',inplace=True)
    df_error_tracker.to_excel(os.path.join(base_dir_tracker, 'config_error_tracker.xlsx'))

    return new_config_po_qty

def fill_up_remark(df):
    """
    Fill up/replace the remark with the error remark put in the option 0 line
    """
    error_remark={}
    df_main=df[df.OPTION_NUMBER==0]
    for row in df_main.itertuples():
        if pd.isnull(row.REMARK):
            error_remark[row.PO_NUMBER] = 'No reason provided'
        else:
            error_remark[row.PO_NUMBER]=row.REMARK
    df.loc[:,'REMARK']=df.PO_NUMBER.map(lambda x: error_remark[x])

    return df

def find_error_by_config_comparison_with_history_error(dfx,wrong_po_dict,checking_time):
    '''
    做config对比，找出相同的error config订单。
    :param dfx: new order df to find error with
    :param wrong_po_dict: {PO:error_message}
    '''
    time_start=time.time()

    # read history error data fill up/replace the REMARK for options based on OPTION 0 comments
    df_history_error=read_table('dfpm_tool_history_new_error_config_record')

    # 生成模型对象并使用方法
    fsc = FindSameConfig()
    base_config_dict=fsc.create_base_config_dict(df_history_error)
    new_config_dict=fsc.create_new_config_dict(dfx,df_history_error) # dfx is the new order df
    compare_result_dict=fsc.compare_new_and_base_dict(new_config_dict, base_config_dict)

    for po,info in compare_result_dict.items():
        if po not in wrong_po_dict.keys():
            wrong_po_dict[po]='Same error as {}:({}){}'.format(info[0],info[1],info[2])

    time_finish = time.time()
    total_time = int(time_finish - time_start)
    checking_time['Error comparison'] = total_time

    return wrong_po_dict, checking_time


class FindSameConfig():
    '''
    FindSameConfig，生成以下方法：
    create_base_config_dict: from base df
    create_new_config_dict: from new df
    compare_new_and_base_dict: get comparison result for same config
    '''

    def __init__(self):
        self.base_config_dict = {}
        self.new_config_dict = {}
        self.compare_result_dict = {}
        self.df_new_order_error = pd.DataFrame()

    def create_base_config_dict(self, df_base):
        # create the base_config_dict based on df_base - different from new config: here also include "REMARK"
        # TODO: create new pid to combine module&slot

        df_base_p = df_base.pivot_table(index=['PO_NUMBER','Added_by','REMARK'], columns='PRODUCT_ID', values='ORDERED_QUANTITY',
                                      aggfunc=sum)

        df_base_p = df_base_p.apply(lambda x: x / x.min(), axis=1)

        base_config_dict = {}

        for i in range(df_base_p.shape[0]):
            df_line = df_base_p.iloc[i].dropna()

            sub_dic = {}
            for pid, qty in zip(df_line.index, df_line.values):
                sub_dic[pid] = qty

            base_config_dict[df_line.name] = sub_dic

        return base_config_dict

    def create_new_config_dict(self,df_new,df_base):
        # create the new_config_dict based on df_new; as df_new may be big, so limit df_new based on df_base BU, and
        # create the dict by batch then combine. This shall avoid memory overflow when doing pivot table.
        #TODO: create new pid to combine module&slot

        # limit df_new by df_base BU
        if 'main_bu' not in df_new.columns:
            df_new = commonize_and_create_main_item(df_new, 'BUSINESS_UNIT', 'main_bu')
        if 'main_bu' not in df_base.columns:
            df_base = commonize_and_create_main_item(df_base, 'BUSINESS_UNIT', 'main_bu')

        df_new=df_new[df_new.main_bu.isin(df_base.main_bu)].copy()
        bu_list=df_new.main_bu.unique()
        new_config_dict = {}
        for bu in bu_list:
            bu_new_config_dict = {}
            dfx=df_new[df_new.main_bu==bu].copy()

            dfx_p = dfx.pivot_table(index=['PO_NUMBER'], columns='PRODUCT_ID', values='ORDERED_QUANTITY',
                                              aggfunc=sum)

            dfx_p = dfx_p.apply(lambda x: x / x.min(), axis=1)

            for i in range(dfx_p.shape[0]):
                df_line = dfx_p.iloc[i].dropna()

                sub_dic = {}
                for pid, qty in zip(df_line.index, df_line.values):
                    sub_dic[pid] = qty

                bu_new_config_dict[df_line.name] = sub_dic

            # combine to the main dict
            new_config_dict.update(bu_new_config_dict)

        return new_config_dict

    def compare_new_and_base_dict(self,new_config_dict, base_config_dict):
        # compare the new config_dict against the base_config_dict for same config
        compare_result_dict = {}
        for key, value in new_config_dict.items():
            matches = filter(lambda x: x[1] == value, base_config_dict.items())

            for order_label, _ in matches:
                # compare_result_dict[key]=(order_label[0],order_label[1])
                compare_result_dict[key] = order_label  # 如果字典中有重复的值，一直循环并取最后一个。

        return compare_result_dict
