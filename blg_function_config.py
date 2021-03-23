import pandas as pd
import numpy as np
from sending_email import send_attachment_and_embded_image
from blg_settings import *
from blg_functions import commonize_and_create_main_item
from db_read import read_table
from db_add import add_error_config_data

def config_rule_mapping():
    """
    Define a config rule mapping based on PF and corresponding function name to do the config check.
    :return:
    """
    #[[exclusion org],[PF],rule_function]
    config_rules = (
                    [[],['C9400'],'find_config_error_per_c9400_rules_pwr_sup_lc(dfx,wrong_po_dict)'],
                    [['FVE'],['4300ISR','4400ISR','ICV'],'find_config_error_per_isr43xx_vg450_rules_sm_nim(dfx,wrong_po_dict)'],
                    [[],['ASR903'],'find_pabu_wrong_slot_combination(dfx,wrong_po_dict)'],
                    [[],[],'find_missing_or_extra_pid_base_on_incl_excl_config_rule_bupf(dfx, df_bupf_rule,wrong_po_dict)'],
                    [[],[],'find_missing_or_extra_pid_base_on_incl_excl_config_rule_pid(dfx,df_pid_rule,wrong_po_dict)'],
                    [[],[],'find_error_by_config_comparison_with_history_error(dfx,wrong_po_dict)'],
                    )

    notes = [
             '- UABU C9400: PSU/LC/SUP combinations (Alex Solis Gonzalez)',
             '- SRGBU 4300ISR/4400ISR/ICV (FVE excluded): SM/NIM combinations (Rachel Zhang)',
             '- SRGBU 4xxxISR/800BB/900ISR/CAT8200/CAT8300/ENCS/ISR1K/ISR900 (FVE excluded; 3 config spares excluded): missing PSU (Rachel Zhang)',
             '- PABU ASR903: Slot related check (Calina, Joe,.. )',
             '- Inclusion/excelusion rules',
             ]

    return config_rules,notes


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
    df_rule_exclusion.drop_duplicates(['PRODUCT_ID_CHASIS','PID_SLOT_A','PID_SLOT_B'],inplace=True) # in case duplication
    df_rule_exclusion.sort_values(['PRODUCT_FAMILY','PRODUCT_ID_CHASIS','PID_A','SLOT_A'],inplace=True)

    df_rule_inclusion=pd.read_excel(fname_config, sheet_name='INCLUSION')
    df_rule_inclusion.loc[:,'PID_SLOT_A']=df_rule_inclusion.PID_A.str.strip() + '_' + df_rule_inclusion.SLOT_A.str.strip()
    df_rule_inclusion.loc[:, 'PID_SLOT_B'] = df_rule_inclusion.PID_B.str.strip()  # no need consider slot
    df_rule_inclusion.drop_duplicates(['PRODUCT_ID_CHASIS','PID_SLOT_A','PID_SLOT_B'],inplace=True) # in case duplication
    df_rule_inclusion.sort_values(['PRODUCT_FAMILY','PRODUCT_ID_CHASIS','PID_A','SLOT_A'],inplace=True)

    df_rule_no_support = pd.read_excel(fname_config, sheet_name='NO_SUPPORT')
    df_rule_no_support.loc[:, 'PID_SLOT_A'] = df_rule_no_support.PID_A.str.strip() + '_' + df_rule_no_support.SLOT_A.str.strip()
    df_rule_no_support.drop_duplicates(['PRODUCT_ID_CHASIS','PID_SLOT_A'], inplace=True)  # in case duplication

    # create exclusion rules
    config_rules_exclusion={}
    excl_rules={}
    pid_chassis_base = df_rule_exclusion.iloc[0,:].PRODUCT_ID_CHASIS.strip()
    pid_slot_a_base = df_rule_exclusion.iloc[0,:].PID_SLOT_A
    pid_slot_list = []
    pid_slot_list.append(df_rule_exclusion.iloc[0,:].PID_SLOT_B)
    excl_rules[pid_slot_a_base]=pid_slot_list
    config_rules_exclusion[pid_chassis_base]=excl_rules
    for row in df_rule_exclusion.iloc[1:,:].itertuples():
        if row.PRODUCT_ID_CHASIS == pid_chassis_base:
            if row.PID_SLOT_A.strip() == pid_slot_a_base:
                pid_slot_list.append(row.PID_SLOT_B)
                excl_rules[pid_slot_a_base] = pid_slot_list
            else:
                pid_slot_a_base=row.PID_SLOT_A
                pid_slot_list = []
                pid_slot_list.append(row.PID_SLOT_B)
                excl_rules[pid_slot_a_base] = pid_slot_list
            config_rules_exclusion[pid_chassis_base]=excl_rules
        else:
            pid_chassis_base = row.PRODUCT_ID_CHASIS.strip()
            pid_slot_a_base = row.PID_SLOT_A
            excl_rules={}
            pid_slot_list=[]
            pid_slot_list.append(row.PID_SLOT_B)
            excl_rules[pid_slot_a_base] = pid_slot_list
            config_rules_exclusion[pid_chassis_base]=excl_rules

    # create inclusion rules
    config_rules_inclusion = {}
    incl_rules = {}
    pid_chassis_base = df_rule_inclusion.iloc[0, :].PRODUCT_ID_CHASIS.strip()
    pid_slot_a_base = df_rule_inclusion.iloc[0, :].PID_SLOT_A
    pid_slot_list = []
    pid_slot_list.append(df_rule_inclusion.iloc[0, :].PID_SLOT_B)
    incl_rules[pid_slot_a_base] = pid_slot_list
    config_rules_inclusion[pid_chassis_base] = incl_rules
    for row in df_rule_inclusion.iloc[1:, :].itertuples():
        if row.PRODUCT_ID_CHASIS == pid_chassis_base:
            if row.PID_SLOT_A.strip() == pid_slot_a_base:
                pid_slot_list.append(row.PID_SLOT_B)
                incl_rules[pid_slot_a_base] = pid_slot_list
            else:
                pid_slot_a_base = row.PID_SLOT_A
                pid_slot_list = []
                pid_slot_list.append(row.PID_SLOT_B)
                incl_rules[pid_slot_a_base] = pid_slot_list
            config_rules_inclusion[pid_chassis_base] = incl_rules
        else:
            pid_chassis_base = row.PRODUCT_ID_CHASIS.strip()
            pid_slot_a_base = row.PID_SLOT_A
            incl_rules = {}
            pid_slot_list = []
            pid_slot_list.append(row.PID_SLOT_B)
            incl_rules[pid_slot_a_base] = pid_slot_list
            config_rules_inclusion[pid_chassis_base] = incl_rules

    # create no support rules
    config_rules_no_support = {}
    pid_chassis_base = df_rule_no_support.iloc[0, :].PRODUCT_ID_CHASIS.strip()
    #pid_slot_a_base = df_rule_no_support.iloc[0, :].PID_SLOT_A
    pid_slot_list = []
    pid_slot_list.append(df_rule_no_support.iloc[0, :].PID_SLOT_A)
    config_rules_no_support[pid_chassis_base] = pid_slot_list
    for row in df_rule_no_support.iloc[1:, :].itertuples():
        if row.PRODUCT_ID_CHASIS == pid_chassis_base:
            pid_slot_list.append(row.PID_SLOT_A)
            config_rules_no_support[pid_chassis_base] = pid_slot_list
        else:
            pid_chassis_base = row.PRODUCT_ID_CHASIS.strip()
            pid_slot_list = []
            pid_slot_list.append(row.PID_SLOT_A)
            config_rules_no_support[pid_chassis_base] = pid_slot_list

    # Update the PRODUCT_ID to PID_SLOT when eligiable slot PID is found
    slots = df_rule_exclusion.SLOT_A.unique().tolist() + df_rule_exclusion.SLOT_B.unique().tolist() + df_rule_inclusion.SLOT_A.unique().tolist()
    slot=''
    dfx.loc[:, 'pid_slot'] = np.nan
    for row in dfx.itertuples():
        if row.PRODUCT_ID in slots:
            slot = row.PRODUCT_ID
        else:
            if slot!='':
                dfx.loc[row.Index,'pid_slot']=row.PRODUCT_ID + '_' + slot
                slot=''

    dfx.loc[:,'PRODUCT_ID']=np.where(dfx.pid_slot.notnull(),
                                     dfx.pid_slot,
                                     dfx.PRODUCT_ID)

    # get the ATO PO list
    target_main_pid=list(config_rules_exclusion.keys())+list(config_rules_inclusion.keys())+list(config_rules_no_support.keys())
    dfx_main = dfx[(dfx.OPTION_NUMBER == 0)]
    main_po_pid = zip(dfx_main.PO_NUMBER, dfx_main.PRODUCT_ID)
    po_pid_dict = {}
    for po,main_pid in main_po_pid:
        if main_pid in target_main_pid:
            po_pid_dict[po]=main_pid

    for po,main_pid in po_pid_dict.items():
        pid_list = dfx[dfx.PO_NUMBER == po].PRODUCT_ID.unique()
        # check no support pid_slot
        no_support=False
        if main_pid in pid_list:
            for pid in pid_list:
                if pid in config_rules_no_support[main_pid]:
                    no_support = True
                    no_support_pid_slot = pid
                    break
        if no_support==True:
            wrong_po_dict[po] = 'No support pid/slot: {}'.format(no_support_pid_slot)
            break

        #  check exclusion
        pid_slot_a_in = False
        pid_slot_b_in = False
        for pid in pid_list:
            if pid in config_rules_exclusion[main_pid].keys():
                pid_slot_a_in=True
                pid_slot_a=pid
            elif pid_slot_a_in==True:
                if pid in config_rules_exclusion[main_pid][pid_slot_a]:
                    pid_slot_b_in=True
                    wrong_pid_slot=pid
                    pid_slot_a_in=False
                    break

        if pid_slot_a_in==True and pid_slot_b_in==True:
            wrong_po_dict[po] = 'Wrong slot: {}'.format(wrong_pid_slot)

        # check inclusion
        pid_slot_a_in = False
        missing_part = True
        for pid in pid_list:
            if pid in config_rules_inclusion[main_pid].keys():
                pid_slot_a_in = True
                pid_slot_a = pid
                break
        if pid_slot_a_in == True:
            for pid in pid_list:
                if pid in config_rules_inclusion[main_pid][pid_slot_a]:
                    missing_part = False
                    break
        if missing_part==True and pid_slot_a_in == True:
            wrong_po_dict[po] = 'Missing part: {}'.format(config_rules_inclusion[main_pid][pid_slot_a])

    return wrong_po_dict

def find_missing_or_extra_pid_base_on_incl_excl_config_rule_bupf(dfx,df_bupf_rule,wrong_po_dict):
    '''
    Check if any PO if missing pid_a or wrongly include pid_b based on BU/PF general rules.
    '''
    for row in df_bupf_rule.itertuples():
        id=row.id
        org=row.ORG.split(';')
        bu = row.BU.split(';')
        pf=row.PF.split(';')
        exception_main_pid=row.EXCEPTION_MAIN_PID.split(';')
        pid_a=row.PID_A.split(';')
        pid_b=row.PID_B.split(';')
        remark=row.REMARK

        # limit the df based on org/bu/pf
        dfy=dfx.copy()
        if org!=['']:
            dfy=dfy[dfy.ORGANIZATION_CODE.isin(org)].copy()
        if bu!=['']:
            dfy = dfy[dfy.main_bu.isin(bu)].copy()
        if pf!=['']:
            dfy = dfy[dfy.main_pf.isin(pf)].copy()

        dfy_main= dfy[(dfy.OPTION_NUMBER == 0)]
        main_po_pid=zip(dfy_main.PO_NUMBER,dfy_main.PRODUCT_ID)
        po_list=[]
        for po,pid in main_po_pid:
            if '=' not in pid and 'MISC' not in pid:
                if pid not in exception_main_pid:
                    po_list.append(po)

        for po in po_list:
            pid_list=dfy[dfy.PO_NUMBER==po].PRODUCT_ID.unique()

            missing_pid_a = True
            if pid_a!=['']:
                for including_pid_keyword in pid_a:
                    for pid in pid_list:
                        if including_pid_keyword in pid:
                            missing_pid_a = False
                            break
                    if missing_pid_a==False:
                        break
                if missing_pid_a==True:
                    wrong_po_dict[po] = 'BU/PF rule #{}:{}'.format(id,remark)

            extra_pid_b = False
            if missing_pid_a==False and pid_b!=['']:
                for pid in pid_list:
                    for extra_pid in pid_b:
                        if extra_pid in pid:
                            extra_pid_b = True
                            break
                if extra_pid_b:
                    wrong_po_dict[po] = 'BU/PF rule #{}:{}'.format(id,remark)

    return wrong_po_dict


def find_missing_or_extra_pid_base_on_incl_excl_config_rule_pid(dfx,df_pid_rule,wrong_po_dict):
    '''
    Check if any PO if missing pid_a or wrongly include pid_b based on BU/PF general rules.
    '''
    for row in df_pid_rule.itertuples():
        id=row.id
        org=row.ORG.split(';')
        bu = row.BU.split(';')
        pf=row.PF.split(';')
        pid_a = row.PID_A.split(';')
        #pid_a_exception=row.PID_A_EXCEPTION.split(';')
        pid_b = row.PID_B.split(';')
        #pid_b_exception = row.PID_B_EXCEPTION.split(';')
        pid_c = row.PID_C.split(';')
        #pid_c_exception = row.PID_C_EXCEPTION.split(';')
        remark=row.REMARK

        # limit the df based on org/bu/pf
        dfy=dfx.copy()
        if org!=['']:
            dfy=dfy[dfy.ORGANIZATION_CODE.isin(org)].copy()
        if bu!=['']:
            dfy = dfy[dfy.main_bu.isin(bu)].copy()
        if pf!=['']:
            dfy = dfy[dfy.main_pf.isin(pf)].copy()

        #Identify the elible order that includes pid_a (pid_a is list of full pid names)
        dfy.loc[:,'eligible']=np.where(dfy.PRODUCT_ID.isin(pid_a),
                                       'YES',
                                       'NO')
        dfy_eligible_pid=dfy[dfy.eligible=='YES']
        po_list=dfy_eligible_pid.PO_NUMBER.values
        dfy_eligible=dfy[dfy.PO_NUMBER.isin(po_list)]

        for po in po_list:
            pid_list=dfy_eligible[dfy_eligible.PO_NUMBER==po].PRODUCT_ID.unique()

            missing_pid_b = True
            extra_pid_c = False
            if pid_b!=['']:
                for including_pid in pid_b:
                    if including_pid in pid_list:
                        missing_pid_b = False
                        break

                if missing_pid_b==True:
                    wrong_po_dict[po] = 'PID rule #{}:{}(missing)'.format(id,remark)

            if pid_c!=[''] and missing_pid_b==False:
                for extra_pid in pid_c:
                    if extra_pid in pid_list:
                        extra_pid_c = True
                        break

                if extra_pid_c==True:
                    wrong_po_dict[po] = 'PID rule #{}:{}(extra)'.format(id,remark)

    return wrong_po_dict


def isr43xx_vg450_rules_sm_nim(pid_qty, extra_slot, wrong_po_dict, po, sm_criteria, nim_criteria):
    """
    
    :return: wrong po dict
    """
    sm_qty = 0
    sm_blank_qty = 0
    ucs_qty = 0
    adapter_qty = 0
    nim_blank_qty = 0
    nim_qty = 0

    for pid, qty in pid_qty:
        if 'SM-X-NIM-ADPTR' in pid:
            if pid in extra_slot.keys():
                adapter_qty = adapter_qty + extra_slot[pid]*qty
            else:
                adapter_qty = adapter_qty + qty
        elif 'SM-' in pid:
            if pid in extra_slot.keys():
                sm_qty =sm_qty + extra_slot[pid]*qty
            else:
                sm_qty = sm_qty + qty
        elif 'UCS-EN140N-M2/K9' in pid:
            if pid in extra_slot.keys():
                nim_qty = nim_qty + extra_slot[pid] * qty
            else:
                nim_qty = nim_qty + qty
        elif 'UCS-' in pid:
            if pid in extra_slot.keys():
                ucs_qty = ucs_qty + extra_slot[pid] * qty
            else:
                ucs_qty = ucs_qty + qty
        elif 'NIM-' in pid:
            if pid in extra_slot.keys():
                nim_qty = nim_qty + extra_slot[pid] * qty
            else:
                nim_qty = nim_qty + qty

    # check the qty combinations: if SM correct then check NIM; if SM wrong, then no need to check NIM
    if sm_qty +  ucs_qty + adapter_qty> sm_criteria:
        #wrong_po_dict.append(po)
        wrong_po_dict[po]='SM slot over used'
    elif sm_qty +  ucs_qty + adapter_qty< sm_criteria:
        wrong_po_dict[po]='SM slot under used'
    elif nim_qty < nim_criteria:  #(means adapter may or may not carry a NIM card)
        wrong_po_dict[po]='NIM slot under used'
    elif nim_qty > nim_criteria + adapter_qty:
        wrong_po_dict[po] = 'NIM/ADPTR slot over used'

    return wrong_po_dict


def find_config_error_per_isr43xx_vg450_rules_sm_nim(dfx,wrong_po_dict):
    '''
    Check if any PO in SRG ISR43xx having wrong config based on SM-NIM rules
    :param dfx: df filtered by PF that need to check with
    :return error order dict
    '''

    # below dic for exceptional qty (>1)
    extra_slot = {'UCS-E160D-M2/K9': 2,
                  'UCS-E180D-M2/K9': 2,
                  'UCS-E180D-M3/K9': 2,
                  'UCS-E1120D-M3/K9': 2,
                  'SM-X-ES3D-48-P': 2,
                  'SM-X-72FXS': 2,
                  'SM-X-40G8M2X': 2, }

    dfx_main= dfx[(dfx.OPTION_NUMBER == 0)]
    main_po_pid=zip(dfx_main.PO_NUMBER,dfx_main.PRODUCT_ID)
    po_list=[]
    for po,pid in main_po_pid:
        if '=' not in pid:
            if 'ISR4321' in pid or 'ISR4331' in pid or 'ISR4351' in pid or 'ISR4461' in pid or 'VG450' in pid:
                po_list.append(po)

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

            wrong_po_dict=isr43xx_vg450_rules_sm_nim(pid_qty_list, extra_slot, wrong_po_dict, po,
                                         sm_criteria, nim_criteria)
        elif 'ISR4331' in main_pid:
            sm_criteria = 1
            nim_criteria = 2

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, extra_slot, wrong_po_dict, po,
                                           sm_criteria, nim_criteria)
        elif 'ISR4351' in main_pid:
            sm_criteria = 2
            nim_criteria = 3

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, extra_slot, wrong_po_dict, po,
                                           sm_criteria, nim_criteria)
        elif 'ISR4461' in main_pid:
            sm_criteria = 4
            nim_criteria = 3

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, extra_slot, wrong_po_dict, po,
                                           sm_criteria, nim_criteria)
        elif 'VG450' in main_pid:
            sm_criteria = 4
            nim_criteria = 3

            wrong_po_dict = isr43xx_vg450_rules_sm_nim(pid_qty_list, extra_slot, wrong_po_dict, po,
                                           sm_criteria, nim_criteria)

    return wrong_po_dict



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

    dfx_main= dfx[(dfx.OPTION_NUMBER == 0)]
    main_po_pid=zip(dfx_main.PO_NUMBER,dfx_main.PRODUCT_ID)
    po_list=[]
    for po,pid in main_po_pid:
        if '=' not in pid:
            if 'C9410R' in pid or 'C9407R' in pid:
                po_list.append(po)

    for po in po_list:
        main_pid=dfx[(dfx.PO_NUMBER==po)&(dfx.OPTION_NUMBER==0)].PRODUCT_ID.values[0]
        dfy=dfx[dfx.PO_NUMBER == po][['PRODUCT_ID','ORDERED_QUANTITY']].groupby('PRODUCT_ID').sum()
        #print(po,main_pid)

        pid_list=dfy.index.values
        qty_list=dfy.values.reshape(1,-1)[0]
        pid_qty_list=zip(pid_list,qty_list)

        if 'C9410R' in main_pid:
            main_pid = 'C9410R*'

            if 'C9400-PWR-3200AC' in pid_list:
                psu_pid='C9400-PWR-3200AC'
            elif 'C9400-PWR-2100AC' in pid_list:
                psu_pid = 'C9400-PWR-2100AC'
            elif 'C9400-PWR-3200DC' in pid_list:
                psu_pid = 'C9400-PWR-3200DC'

            wrong_po_dict=c9400_rules_pwr_sup_lc(pid_qty_list, wrong_po_dict, po,main_pid,psu_pid)
        elif 'C9407R' in main_pid:
            main_pid = 'C9407R*'
            if 'C9400-PWR-3200AC' in pid_list:
                psu_pid='C9400-PWR-3200AC'
            elif 'C9400-PWR-2100AC' in pid_list:
                psu_pid = 'C9400-PWR-2100AC'
            elif 'C9400-PWR-3200DC' in pid_list:
                psu_pid = 'C9400-PWR-3200DC'

            wrong_po_dict = c9400_rules_pwr_sup_lc(pid_qty_list, wrong_po_dict, po, main_pid, psu_pid)

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

def send_config_error_data_by_email(org, df_error_new, df_error_old,fname_new_error,login_user,to_address,sender,notes):
    """
    Send the result with attachment
    """
    if df_error_new.shape[0]>0:
        att_file = [(base_dir_output, fname_new_error)]  # List of tuples (path, file_name)
    else:
        att_file = None

    subject = org + ' config error check summary (sent by: '+login_user +')'
    html = 'config_check_result_email.html'

    send_attachment_and_embded_image(to_address, subject, html,
                                     sender=sender,
                                     att_filenames=att_file,
                                     bcc=[super_user + '@cisco.com'],
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

def identify_config_error_po(df_3a4,df_bupf_rule,df_pid_rule,config_rules):
    wrong_po_dict = {}

    # pick out ATO PO
    df_pivot=df_3a4.pivot_table(index='PO_NUMBER',values='OPTION_NUMBER',aggfunc=sum)
    ato_po=df_pivot[df_pivot.OPTION_NUMBER>0].index
    df_ato=df_3a4[df_3a4.PO_NUMBER.isin(ato_po)].copy()

    for org_pf_func in config_rules:
        dfx=df_ato.copy() # make a copy of original each time
        if org_pf_func[0]!=[]:
            dfx = dfx[(~df_ato.ORGANIZATION_CODE.isin(org_pf_func[0]))].copy()
        if org_pf_func[1]!=[]:
            dfx=dfx[dfx.main_pf.isin(org_pf_func[1])].copy()  # used in below config_func

        if dfx.shape[0] > 0:
            wrong_po_dict = eval(org_pf_func[2])

    return wrong_po_dict








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

def find_error_by_config_comparison_with_history_error(dfx,wrong_po_dict):
    '''
    做config对比，找出相同的error config订单。
    :param dfx: new order df to find error with
    :param wrong_po_dict: {PO:error_message}
    '''

    # read history error data fill up/replace the REMARK for options based on OPTION 0 comments
    df_history_error=read_table('history_new_error_config_record')

    # 生成模型对象并使用方法
    fsc = FindSameConfig()
    base_config_dict=fsc.create_base_config_dict(df_history_error)
    new_config_dict=fsc.create_new_config_dict(dfx,df_history_error) # dfx is the new order df
    compare_result_dict=fsc.compare_new_and_base_dict(new_config_dict, base_config_dict)

    for po,info in compare_result_dict.items():
        if po not in wrong_po_dict.keys():
            wrong_po_dict[po]='Same error as {}:({}){}'.format(info[0],info[1],info[2])

    return wrong_po_dict


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
