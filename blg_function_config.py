import pandas as pd
import numpy as np
from sending_email import send_attachment_and_embded_image
from blg_settings import *
import time

def config_rule_mapping():
    """
    Define a config rule mapping based on PF and corresponding function name to do the config check.
    :return:
    """
    #[[exclusion org],[PF],rule_function]
    config_rules = (
                    [[],['C9400'],'find_config_error_per_c9400_rules_pwr_sup_lc(dfx,wrong_po_dict)'],
                    [['FVE'],['4300ISR','4400ISR','ICV'],'find_config_error_per_isr43xx_vg450_rules_sm_nim(dfx,wrong_po_dict)'],
                    [['FVE'],['4200ISR','4300ISR','4400ISR','800BB','900ISR','CAT8200','CAT8300','ENCS','ISR1K','ISR900'],'find_srg_psu_missing(dfx,wrong_po_dict)'],
                    [[],['ASR903'],'find_pabu_wrong_slot_combination(dfx,wrong_po_dict)'],
                    )

    notes = [
             '- UABU C9400: PSU/LC/SUP combinations (Alex Solis Gonzalez)',
             '- SRGBU 4300ISR/4400ISR/ICV (FVE excluded): SM/NIM combinations (Rachel Zhang)',
             '- SRGBU 4xxxISR/800BB/900ISR/CAT8200/CAT8300/ENCS/ISR1K/ISR900 (FVE excluded; 3 config spares excluded): missing PSU (Rachel Zhang)',
             '- PABU ASR903: Slot related check (Calina, Joe,.. )'
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

    print(config_rules_exclusion)
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
        if po=='111431523-2':
            print(pid_list)
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



def find_srg_psu_missing(dfx,wrong_po_dict):
    '''
    Check if any PO in SRG missing PSU
    '''
    exclusion_pids= ['ENCS5406P/K9','ENCS5412P/K9','ENCS5408P/K9'] #config_spare
    dfx_main= dfx[(dfx.OPTION_NUMBER == 0)]
    main_po_pid=zip(dfx_main.PO_NUMBER,dfx_main.PRODUCT_ID)
    po_list=[]
    for po,pid in main_po_pid:
        if '=' not in pid and 'MISC' not in pid:
            if pid not in exclusion_pids:
                po_list.append(po)

    for po in po_list:
        pid_list=dfx[dfx.PO_NUMBER==po].PRODUCT_ID.unique()

        missing_psu = True
        for pid in pid_list:
            if 'AC' in pid or 'DC' in pid:
                missing_psu = False
                break
        if missing_psu:
            wrong_po_dict[po] = 'Missing PSU'

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
                                     bcc=['kwang2@cisco.com'],
                                     new_error_header=df_error_new.columns,
                                     new_error_data=df_error_new.values,
                                     old_error_header=df_error_old.columns,
                                     old_error_data=df_error_old.values,config_notes=notes)



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

def identify_config_error_po(df_3a4,config_rules):
    wrong_po_dict = {}
    pf_checked = []

    # pick out ATO PO
    df_pivot=df_3a4.pivot_table(index='PO_NUMBER',values='OPTION_NUMBER',aggfunc=sum)
    ato_po=df_pivot[df_pivot.OPTION_NUMBER>0].index
    df_ato=df_3a4[df_3a4.PO_NUMBER.isin(ato_po)].copy()

    for org_pf_func in config_rules:
        start = time.time()
        dfx = df_ato[(~df_ato.ORGANIZATION_CODE.isin(org_pf_func[0]))&(df_ato.main_pf.isin(org_pf_func[1]))].copy()  # used in below config_func
        if dfx.shape[0] > 0:
            pf_checked += org_pf_func[1]
            print('Checking config on PF: {}'.format(org_pf_func[1]))
            wrong_po_dict = eval(org_pf_func[2])
        print('Time spent to check PF {}: {}'.format(org_pf_func[1], time.time() - start))

    return wrong_po_dict,pf_checked
