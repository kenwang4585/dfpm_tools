'''
Ken: 2019
Flask web service app for 3a4 automated reports
'''

# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

from werkzeug.utils import secure_filename
from flask import flash,send_from_directory,render_template,request, redirect, url_for
from flask_setting import *
from blg_functions import *
from blg_function_config import *
from blg_settings import *
from db_add import add_user_log,add_dfpm_mapping_data, add_subscription, add_incl_excl_rule_pid,add_incl_excl_rule_bupf,add_slot  # remove db and use above instead
from db_read import read_table
from db_update import update_dfpm_mapping_data,update_subscription
from db_delete import delete_record
import traceback
import pprint
#from flask_bootstrap import Bootstrap


@app.route('/dfpm_automation', methods=['GET', 'POST'])
def global_app():
    form = GlobalAppForm()
    # as these email valiable are redefined below in email_to_only check, thus have to use global to define here in advance
    # otherwise can't be used. (as we create new vaiables with _ suffix thus no need to set global variable)
    # global backlog_dashboard_emails
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_title = 'unknown'

    if '[C]' in login_title: # for c-workers
        return 'Sorry, you are not authorized to access this.'

    user_selection = []

    if form.validate_on_submit():
        start_time_=pd.Timestamp.now()
        print(start_time_)
        # 通过条件判断及邮件赋值，开始执行任务
        f = form.file.data
        region=form.region.data
        backlog_summary = form.backlog_summary.data
        wnbu_compliance = form.wnbu_compliance.data
        config_check = form.config_check.data
        running_option=form.running_option.data

        # 汇总task list, 判断是否有选择至少一个summary需要制作
        user_selection.append('Region: {}'.format(region))

        if backlog_summary:
            user_selection.append(form.backlog_summary.label.text)
        if wnbu_compliance:
            user_selection.append(form.wnbu_compliance.label.text)
        if config_check:
            user_selection.append(form.config_check.label.text)

        if len(user_selection) > 1:
            print('Start to create below selected reports: {}'.format('/'.join(user_selection)))
        else:
            flash('You should at least select one task to do!', 'warning')
            return render_template('global_app.html', form=form,user=login_name,subtitle='')

        user_selection.append('Running option: {}'.format(running_option))

        if f==None:
            flash('Pls upload 3a4 file!', 'warning')
            return render_template('global_app.html', form=form,user=login_name,subtitle='')

        # 上传的文件名称并判断文件类型
        filename_3a4 = secure_filename(f.filename)
        ext_3a4 = os.path.splitext(filename_3a4)[1]

        if ext_3a4 == '.csv':
            file_path_3a4 = os.path.join(base_dir_uploaded, '3a4 for GLO app - ' + login_user + '.csv')
        elif ext_3a4 == '.xlsx':
            file_path_3a4 = os.path.join(base_dir_uploaded, '3a4 for GLO app - ' + login_user + '.xlsx')
        else:
            flash('3A4 file type error: Only csv or xlsx file accepted! File you were trying to upload: {}'.format(
                    f.filename),'warning')
            return render_template('global_app.html', form=form,user=login_name,subtitle='')

        # 存储文件
        f.save(file_path_3a4)

        # 根据region选项定义default值 （暂时只启用backlog_summary）
        org_name_region = org_name_global[region]
        #backlog_chart = backlog_chart_global[region]
        addr_history_fname= addr_history_fname_global[region]
        sender = region + ' DF'
        if running_option=='formal':
            # read email from the subscription DB
            backlog_dashboard_emails_global,wnbu_compliance_check_emails_global,config_check_emails_global= read_subscription_by_region()
            backlog_dashboard_emails=backlog_dashboard_emails_global[region]
            wnbu_compliance_emails=wnbu_compliance_check_emails_global[region]
            config_check_emails = config_check_emails_global[region]
            if len(backlog_dashboard_emails)==0:
                backlog_dashboard_emails = [login_user + '@cisco.com']
            if len(wnbu_compliance_emails)==0:
                wnbu_compliance_emails = [login_user + '@cisco.com']
            if len(config_check_emails)==0:
                config_check_emails = [login_user + '@cisco.com']
        else:
            backlog_dashboard_emails = [login_user + '@cisco.com']
            wnbu_compliance_emails = [login_user + '@cisco.com']
            config_check_emails = [login_user + '@cisco.com']


        # 正式开始程序; start processing the data and create summaries
        try:
            # read whole 3a4 without parsing dates
            df_3a4 = read_3a4(file_path_3a4)

            # check the format: col and org based on tasks

            if backlog_summary:
                # org check
                if not np.all(np.in1d(org_name_global[region][region], df_3a4.ORGANIZATION_CODE.unique())):
                    flash('The 3a4 you uploaded does not contain all orgs from {}!'.format(region), 'warning')
                    return render_template('global_app.html', form=form,user=login_name,subtitle='')

                # col check
                if not np.all(np.in1d(col_3a4_must_have_global_backlog_summary, df_3a4.columns)):
                    flash('File format error! Following required \
                                                columns for regional backlog summary not found in 3a4 data: {}'.format(
                        str(np.setdiff1d(col_3a4_must_have_global_backlog_summary, df_3a4.columns))),'warning')
                    return render_template('global_app.html', form=form,user=login_name,subtitle='')

            if wnbu_compliance:
                # col check
                if not np.all(np.in1d(col_3a4_must_have_global_wnbu_compliance, df_3a4.columns)):
                    flash('File format error! Following required \
                                                            columns for WNBU compliance check not found in 3a4 data: {}'.format(
                        str(np.setdiff1d(col_3a4_must_have_global_wnbu_compliance, df_3a4.columns))), 'warning')
                    return render_template('global_app.html', form=form,user=login_name,subtitle='')

            if config_check:
                # col check
                if not np.all(np.in1d(col_3a4_must_have_global_config_check, df_3a4.columns)):
                    flash('File format error! Following required \
                                                                columns for config check not found in 3a4 data: {}'.format(
                            str(np.setdiff1d(col_3a4_must_have_global_config_check, df_3a4.columns))), 'warning')
                    return render_template('global_app.html', form=form,user=login_name,subtitle='')

            if running_option=='test':
                if backlog_summary and config_check:
                    msg = 'Addressable data and new config error will NOT be saved to record with the test run!'
                    flash(msg, 'info')
                elif backlog_summary:
                    msg = 'Addressable data will NOT be saved to record with the test run!'
                    flash(msg, 'info')
                elif config_check:
                    msg = 'New config error data will NOT be saved to record with the test run!'
                    flash(msg,'info')

            # initial basic data processing
            df_3a4=basic_data_processing_global(df_3a4,region,org_name_global)

            if df_3a4.shape[0]==0:
                msg = 'Empty data to process based on the file you uploaded and region you selected!'
                flash(msg, 'warning')
                return render_template('global_app.html', form=form, user=login_name, subtitle='')

            # below execute each task
            if wnbu_compliance:
                df_compliance_table, no_ship = read_compliance_from_smartsheet(df_3a4)
                df_compliance_release, df_compliance_hold, df_country_missing = check_compliance_for_wnbu(df_3a4,no_ship)
                create_and_send_wnbu_compliance(wnbu_compliance_emails,
                                                df_compliance_release,
                                                df_compliance_hold,
                                                df_country_missing,
                                                df_compliance_table,
                                                login_user,
                                                sender)
                msg = 'WNBU PO compliance check done and result sent for {}; Total no. of PO ready for release: {}'.format(region,df_compliance_release.shape[0])
                flash(msg, 'success')

            if config_check:
                config_func = config_func_mapping()
                # combine pid and slot when applicable and use that to replace PID
                df_3a4=combine_pid_and_slot(df_3a4)
                df_3a4 = scale_down_po_to_one_set(df_3a4)
                if running_option == 'formal':
                    save_to_tracker = True
                else:
                    save_to_tracker = False

                df_bupf_rule=read_table('general_config_rule_bu_pf')
                df_pid_rule=read_table('general_config_rule_pid')
                wrong_po_dict = identify_config_error_po(df_3a4,df_bupf_rule,df_pid_rule,config_func)
                qty_new_error, df_error_new, df_error_old, fname_new_error = make_error_config_df_output_and_save_tracker(
                    df_3a4, region, login_user, wrong_po_dict, save_to_tracker)

                msg = 'Config check completed and sent for {}; Total no. of new errors found: {}'.format(
                    region,qty_new_error)
                flash(msg, 'success')

                # send the error summary to users
                if qty_new_error > 0 or df_error_old.shape[0]>0:
                    send_config_error_data_by_email(region, df_error_new, df_error_old, fname_new_error,
                                                    login_user,config_check_emails, sender)

            if backlog_summary:
                # Redefine the addressable flag, add in MFG_HOLD, and split out wk+1, wk+2 for outside_window portion
                df_3a4 = redefine_addressable_flag_new(df_3a4, mfg_holds)

                # addressable data to tracker.
                addr_df_summary, addr_df_dict = create_addressable_summary_and_comb_addressable_history(df_3a4,
                                                                                                           org_name_region,
                                                                                                           region,
                                                                                                           addr_history_fname)
                create_and_send_addressable_summaries(addr_df_summary, addr_df_dict, org_name_region,
                                                      backlog_dashboard_emails,region, sender,login_user)
                msg = 'Backlog summary created and sent for {}.'.format(region)
                flash(msg, 'success')

                if running_option == 'formal':
                    # save new addressable data to tracker
                    save_addr_tracker(df_3a4, addr_df_dict, region, org_name_region, addr_history_fname)

                    # send trackers to ken as backup
                    if region == 'APJC':
                        backup_day = 'Monday'
                    elif region == 'EMA':
                        backup_day = 'Wednesday'
                    else:
                        backup_day = 'Friday'
                    download_and_send_tracker_as_backup(backup_day,login_user)

            # Release the memories
            del df_3a4
            gc.collect()

            # summarize time
            time_stamp = pd.Timestamp.now()
            processing_time = round((time_stamp - start_time_).total_seconds() / 60, 1)

            # write program log to log file
            add_user_log(user=login_user, location='Home', user_action='Run', summary='Processing time: ' + str(processing_time) + 'min; parameters: ' + '/'.join(user_selection))

            return render_template('global_app.html', form=form,user=login_name,subtitle='')

        except Exception as e:
            try:
                del df_3a4
                gc.collect()
            except:
                pass

            traceback.print_exc()
            flash(str(e),'warning')
            add_user_log(user=login_user, location='Home', user_action='Run',
                         summary='[Error] ' + '/'.join(user_selection) + ' | ' + str(e))
            error_msg='\n['+login_user + '] Home: ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            error_msg=error_msg + '\n' + '/'.join(user_selection) + '\n'
            with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                file_object.write(error_msg)
            traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

            return render_template('global_app.html', form=form,user=login_name,subtitle='')

    return render_template('global_app.html', form=form,user=login_name,subtitle='')


@app.route('/ranking', methods=['GET', 'POST'])
def backlog_ranking():
    form = BacklogRankingForm()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    if form.validate_on_submit():
        start_time_=pd.Timestamp.now()
        # 通过条件判断及邮件赋值，开始执行任务
        f = form.file.data
        org = form.org.data.strip().upper()
        email_option = form.email_option.data

        filename_3a4 = secure_filename(f.filename)
        ext_3a4 = os.path.splitext(filename_3a4)[1]

        if ext_3a4 == '.csv':
            file_path_3a4 = os.path.join(base_dir_uploaded, org + ' 3a4 for backlog ranking - ' + login_user + '.csv')
        elif ext_3a4 == '.xlsx':
            file_path_3a4 = os.path.join(base_dir_uploaded, org + ' 3a4 for backlog ranking - ' + login_user + '.xlsx')
        else:

            flash('3A4 file type error: Only csv or xlsx file accepted! File you were trying to upload: {}'.format(
                    f.filename),'warning')
            return redirect(url_for('backlog_ranking'))

        # 存储文件
        f.save(file_path_3a4)

        # 预读取文件做格式和内容判断
        if ext_3a4 == '.csv':
            df = pd.read_csv(file_path_3a4,encoding='iso-8859-1',nrows=3)
        elif ext_3a4 == 'xlsx':
            df = pd.read_excel(file_path_3a4,encoding='iso-8859-1',nrows=3)
        else:
            add_user_log(user=login_user, location='Backlog ranking', user_action='Run',
                         summary='Wong file type used: {}'.format(file_path_3a4))

            msg = '3a4 file format error! Only accept .csv or .xlsx!'
            flash(msg,'warning')
            return redirect(url_for('backlog_ranking'))

        # 检查文件是否包含需要的列：
        if not np.all(np.in1d(col_3a4_must_have_backlog_ranking, df.columns)):
            flash('File format error! Following required \
                                        columns not found in 3a4 data: {}'.format(
                    str(np.setdiff1d(col_3a4_must_have_backlog_ranking, df.columns))),
                    'warning')
            del df
            gc.collect()
            return redirect(url_for('backlog_ranking'))

        try:
            df_3a4 = read_3a4_parse_dates(file_path_3a4, ['CURRENT_FCD_NBD_DATE','ORIGINAL_FCD_NBD_DATE'])

            df_3a4=basic_data_processing_backlog_ranking(df_3a4,org)
            if df_3a4.shape[0]==0:
                msg = 'Returned with empty data with input - ensure you use ther right file and right org ({}/{})'.format(filename_3a4,org)
                flash(msg,'warning')
                return redirect(url_for('backlog_ranking'))

            # read smartsheet priorities
            ss_exceptional_priority,df_removal = read_backlog_priority_from_smartsheet(df_3a4,login_user)

            # Remove and send email notification for ss removal from exceptional priority smartsheet
            remove_priority_ss_from_smtsheet_and_notify(df_removal, login_user, sender=login_user + ' via DFPM tools')

            # Rank the orders
            df_3a4 = ss_ranking_overall_new_jan(df_3a4, ss_exceptional_priority, ranking_options, lowest_priority_cat,
                                       order_col='SO_SS',with_dollar=False)

            # save the file and send the email
            # send email
            if email_option == 'to_me':
                to_address = [login_user + '@cisco.com']
            else:
                to_address = read_subscription_by_site(org)
                if len(to_address)==0:
                    to_address = [login_user + '@cisco.com']
            create_and_send_3a4_backlog_ranking(df_3a4, to_address, org, login_user, login_name)

            msg='3A4 backlog ranking has been generated and sent to the defined emails!'
            flash(msg,'success')

            # summarize time
            time_stamp = pd.Timestamp.now()
            processing_time = round((time_stamp - start_time_).total_seconds() / 60, 1)

            # write program log to log file
            add_user_log(user=login_user, location='Backlog ranking', user_action='Run',
                         summary='Success: {} - {}; processing time: {}'.format(org,email_option,str(processing_time)))

            return redirect(url_for('backlog_ranking'))

        except Exception as e:
            msg = 'Error: {}'.format(str(e))
            flash(msg, 'warning')

            print(e)
            traceback.print_exc()
            add_user_log(user=login_user, location='Backlog ranking', user_action='Run',
                         summary='Error: ' + str(e))
            error_msg = '\n[' + login_user + '] Backlog_ranking: ' + org + '   ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + '\n'
            with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                file_object.write(error_msg)
            traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

            return redirect(url_for('backlog_ranking'))

    return render_template('backlog_ranking.html', form=form,user=login_name,subtitle='- Backlog Ranking')

@app.route('/config_rules_incl_excl_bupf',methods=['GET','POST'])
def config_rules_incl_excl_bupf_based():
    form=ConfigRulesInclExclBuPfBased()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    df_rule=read_table('general_config_rule_bu_pf')

    if form.validate_on_submit():
        org=form.org.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        bu = form.bu.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pf=form.pf.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        exception_main_pid=form.exception_main_pid.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pid_a=form.pid_a.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pid_b=form.pid_b.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        remark=form.remark.data.strip()

        add_incl_excl_rule_bupf(org,bu,pf, exception_main_pid, pid_a, pid_b, remark, login_user)

        df_rule=read_table('general_config_rule_bu_pf')

        msg = 'New general rule has been added'
        flash(msg, 'success')
        return redirect(url_for('config_rules_incl_excl_bupf_based'))

    return render_template('config_rules_inclusion_exclusion_bupf_based.html',
                           form=form,user=login_name,subtitle='- Config Rules',
                           login_user=login_user,
                           df_rule_header=df_rule.columns,
                           df_rule_data=df_rule.values,
                           )

@app.route('/config_rules_incl_excl_pid',methods=['GET','POST'])
def config_rules_incl_excl_pid_based():
    form=ConfigRulesInclExclPidBased()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    df_rule=read_table('general_config_rule_pid')

    if form.validate_on_submit():
        org=form.org.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        bu = form.bu.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pf=form.pf.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pid_a=form.pid_a.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pid_b=form.pid_b.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        pid_c=form.pid_c.data.upper().replace(' ', '').replace('\n', '').replace('\r', '')
        #pid_a_exception = form.pid_a_exception.data.replace(' ', '').replace('\n', '').replace('\r', '')
        #pid_b_exception = form.pid_b_exception.data.replace(' ', '').replace('\n', '').replace('\r', '')
        #pid_c_exception = form.pid_c_exception.data.replace(' ', '').replace('\n', '').replace('\r', '')
        remark=form.remark.data.strip()

        add_incl_excl_rule_pid(org,bu,pf, pid_a, pid_b, pid_c, remark, login_user)

        df_rule=read_table('general_config_rule_pid')

        msg = 'New inclusion/exclusion rule has been added'
        flash(msg, 'success')
        return redirect(url_for('config_rules_incl_excl_pid_based'))

    return render_template('config_rules_inclusion_exclusion_pid_based.html',
                           form=form,user=login_name,subtitle='- Config Rules',
                           login_user=login_user,
                           df_rule_header=df_rule.columns,
                           df_rule_data=df_rule.values,
                           )

@app.route('/config_rules_complex',methods=['GET','POST'])
def config_rules_complex():
    form=ConfigRulesComplex()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    file_path_calina = os.path.join(base_dir_tracker, 'PABU slot config rules.xlsx')
    file_path_rachel = os.path.join(base_dir_tracker, 'SRGBU SM_NIM config rules.xlsx')
    file_path_alex = os.path.join(base_dir_tracker, 'EBBU C9400 PWR_LC_SUP combination rule.xlsx')

    org_calina = pd.read_excel(file_path_calina, sheet_name='APPLICABLE_ORG').iloc[0,0]
    org_rachel = pd.read_excel(file_path_rachel, sheet_name='APPLICABLE_ORG').iloc[0,0]
    org_alex = pd.read_excel(file_path_alex, sheet_name='APPLICABLE_ORG').iloc[0,0]

    if form.validate_on_submit():
        submit_calina=form.submit_upload_calina.data
        submit_rachel = form.submit_upload_rachel.data
        submit_alex = form.submit_upload_alex.data

        if submit_calina:
            fname_calina=form.file_calina.data
            confirm_calina=form.confirm_calina.data
            if not fname_calina:
                msg = 'Select the new config rule file to upload!'
                flash(msg, 'warning')
                return redirect(url_for('config_rules_complex'))

            if not 'PABU' in fname_calina.filename:
                msg="This is for PABU rules, ensure 'PABU' in the file name you select!"
                flash(msg, 'warning')
                return redirect(url_for('config_rules_complex'))


            if login_user not in ['unknown','cagong'] + [super_user]:
                msg='Only following user is eligible to update rule for this: {}'.format('cagong')
                flash(msg,'warning')
                return redirect(url_for('config_rules_complex'))

            if not confirm_calina:
                msg = 'Select the upload and replace checkbox to confirm proceeding!'
                flash(msg, 'warning')
                return render_template('config_rules_complex.html', form=form,user=login_name,subtitle='- Config Rules')

            # 存储文件
            fname_calina.save(file_path_calina)
            msg = 'New file has been upload and rules replaced: {}'.format(fname_calina.filename)
            flash(msg, 'success')
            return redirect(url_for('config_rules_complex'))
        elif submit_rachel:
            fname_rachel=form.file_rachel.data
            confirm_rachel=form.confirm_rachel.data
            if not fname_rachel:
                msg = 'Select the new config rule file to upload!'
                flash(msg, 'warning')
                return redirect(url_for('config_rules_complex'))

            if not 'SRGBU' in fname_rachel.filename:
                msg="This is for SRGBU rules, ensure 'SRGBU' in the file name you select!"
                flash(msg, 'warning')
                return redirect(url_for('config_rules_complex'))

            if login_user not in ['unknown','rachzhan'] + [super_user]:
                msg='Only following user is eligible to update rule for this: {}'.format('rachzhan')
                flash(msg,'warning')
                return redirect(url_for('config_rules_complex'))

            if not confirm_rachel:
                msg = 'Select the upload and replace checkbox to confirm proceeding!'
                flash(msg, 'warning')
                return render_template('config_rules_complex.html', form=form,user=login_name,subtitle='- Config Rules')

            # 存储文件
            fname_rachel.save(file_path_rachel)
            msg = 'New file has been upload and rules replaced: {}'.format(fname_rachel.filename)
            flash(msg, 'success')
            return redirect(url_for('config_rules_complex'))

    return render_template('config_rules_complex.html',
                           form=form,user=login_name,subtitle='- Config Rules',
                           login_user=login_user,
                           org_calina=org_calina,
                           org_rachel=org_rachel,
                           org_alex=org_alex)


@app.route('/config_rules_main',methods=['GET','POST'])
def config_rules_main():
    form=ConfigRulesMain()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    df_error_db = read_table('history_new_error_config_record')
    df_slot = read_table('slot')

    if form.validate_on_submit():
        start_time=pd.Timestamp.now()
        submit_upload=form.submit_upload_error.data
        submit_remove=form.submit_remove_error.data
        submit_add_slot=form.submit_add_slot.data

        if submit_upload:
            file_upload_error=form.file_upload_error.data

            if not file_upload_error:
                msg='Pls select the data to upload'
                flash(msg,'warning')
                return redirect(url_for("config_rules_main"))

            # 存储文件
            filename = secure_filename(file_upload_error.filename)
            file_path = os.path.join(base_dir_uploaded,
                                             'New error config upload (' + login_user + ')' + start_time.strftime(
                                                 '%Y-%m-%d %H:%M:%S') + '.xlsx')

            file_upload_error.save(file_path)

            #Read the data
            df_upload = pd.read_excel(file_path)
            #df_upload = commonize_and_create_main_item(df_upload, 'BUSINESS_UNIT', 'main_bu')

            # check formats
            col_must_have=['ORGANIZATION_CODE','BUSINESS_UNIT','PO_NUMBER','OPTION_NUMBER','PRODUCT_ID','ORDERED_QUANTITY','REMARK']
            if not np.all(np.in1d(col_must_have, df_upload.columns)):
                msg='File format error! Following required columns not found in uploaded file: {}. Pls use the template provided.'.format(
                        str(np.setdiff1d(col_must_have, df_upload.columns)))
                flash(msg,'warning')
                return redirect(url_for("config_rules_main"))

            # get new config data and upload
            df_error_db=read_table('history_new_error_config_record')
            #df_error_db = commonize_and_create_main_item(df_error_db, 'BUSINESS_UNIT', 'main_bu')

            #df_error_db = fill_up_remark(df_error_db)
            report_po_qty=len(df_upload.PO_NUMBER.unique())
            new_config_po_qty=add_reported_po_to_tracker_and_upload_unique_new_config_to_db(df_upload, df_error_db,login_user)
            msg = 'Thank you! You reported {} PO with error configs, {} PO are new configs added to database.'.format(
                    report_po_qty, new_config_po_qty)
            flash(msg, 'success')

            # read and count again:
            if new_config_po_qty>0:
                df_error_db = read_table('history_new_error_config_record')

            # write program log to log file
            add_user_log(user=login_user, location='Manage config', user_action='Upload error config',
                             summary='Uploaded PO saved to tracker: {}; new configs saved to db: {}'.format(report_po_qty, new_config_po_qty))

            return redirect(url_for("config_rules_main"))
        elif submit_remove:
            file_remove_error=form.file_remove_error.data

            if not file_remove_error:
                msg='Pls select the data to upload'
                flash(msg,'warning')
                return redirect(url_for("config_rules_main"))

            # 存储文件
            filename = secure_filename(file_remove_error.filename)
            file_path = os.path.join(base_dir_uploaded,
                                             'Removal error config upload (' + login_user + ')' + start_time.strftime(
                                                 '%Y-%m-%d %H:%M:%S') + '.xlsx')

            file_remove_error.save(file_path)

            #Read the data
            df_remove = pd.read_excel(file_path)
            #df_remove = commonize_and_create_main_item(df_remove, 'BUSINESS_UNIT', 'main_bu')

            # check formats
            col_must_have=['ORGANIZATION_CODE','BUSINESS_UNIT','PO_NUMBER','OPTION_NUMBER','PRODUCT_ID','ORDERED_QUANTITY','REMARK']
            if not np.all(np.in1d(col_must_have, df_remove.columns)):
                msg='File format error! Following required columns not found in uploaded file: {}. Pls use the template provided.'.format(
                        str(np.setdiff1d(col_must_have, df_remove.columns)))
                flash(msg,'warning')
                return redirect(url_for("config_rules_main"))

            # find same config data from db and remove
            df_error_db=read_table('history_new_error_config_record')
            #df_error_db = commonize_and_create_main_item(df_error_db, 'BUSINESS_UNIT', 'main_bu')
            #report_po_qty=len(df_upload.PO_NUMBER.unique())
            df_error_db_remove=get_same_config_data_to_remove_from_db(df_error_db, df_remove) # use df_remove as the base
            remove_config_po_qty=len(df_error_db_remove.PO_NUMBER.unique())
            id_list=df_error_db_remove.id.values
            id_list = [str(x) for x in id_list]
            if len(id_list)>0:
                delete_record('history_new_error_config_record', id_list)
                msg = 'Thanks, we have found {} same config record in database which have been removed'.format(remove_config_po_qty)
                flash(msg, 'success')
                # read and count again:
                df_error_db = read_table('history_new_error_config_record')
            else:
                msg = 'No same config record found in database to remove.'
                flash(msg, 'info')

            # write program log to log file
            add_user_log(user=login_user, location='Manage config', user_action='Remove config',
                             summary='No of matching configs removed from database: {}'.format(remove_config_po_qty))

            return redirect(url_for("config_rules_main"))
        elif submit_add_slot:
            slot_pid=form.slot_pid.data.strip().upper()

            if slot_pid not in df_slot.SLOT_PID.values:
                add_slot(slot_pid, login_user)
                msg = 'New slot keyword added.'
                flash(msg, 'success')
            else:
                msg = 'Slot you try to add already exist.'
                flash(msg, 'info')

            df_slot = read_table('slot')

            return redirect(url_for("config_rules_main"))


    df_error_db_summary=df_error_db[df_error_db.OPTION_NUMBER==0].pivot_table(index=['ORGANIZATION_CODE','BUSINESS_UNIT'],values=['PO_NUMBER'],aggfunc=len,margins=True).reset_index()
    df_error_db_summary.rename(columns={'PO_NUMBER':'No. of error configs'},inplace=True)

    return render_template('config_rules_main.html',
                            form=form,
                            user=login_name, subtitle='- Config Rules',
                            login_user=login_user,
                           df_error_db_summary_header=df_error_db_summary.columns,
                           df_error_db_summary_data=df_error_db_summary.values,
                           df_slot_header=df_slot.columns,
                           df_slot_data=df_slot.values)


@app.route('/summary_3a4', methods=['GET', 'POST'])
def dfpm_app():
    form = Summary3a4Form()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    # read DFPM mapping
    df_dfpm_mapping = read_table('dfpm_mapping')
    df_dfpm_mapping.sort_values(by=['DFPM'],inplace=True)

    if form.validate_on_submit():
        start_time_=pd.Timestamp.now()
        submit_3a4 = form.submit_3a4.data
        submit_add_update = form.submit_add_update.data

        if submit_3a4:
            user_selection = []
            start_time=pd.Timestamp.now()
            # 通过条件判断及邮件赋值，开始执行任务
            f = form.file.data
            dfpm_3a4 = form.dfpm_3a4.data
            dfpm_3a4_option = form.dfpm_3a4_option.data
            cm_outlier = form.cm_outlier.data
            cm_outlier_org = form.cm_outlier_org.data.strip().upper()
            cm_outlier_org = cm_outlier_org.split('/')

            # 汇总task list, 判断是否有选择至少一个summary需要制作
            if dfpm_3a4:
                user_selection.append(form.dfpm_3a4.label.text)
                user_selection.append(dfpm_3a4_option)
            if cm_outlier:
                user_selection.append(form.cm_outlier.label.text)
                user_selection.append(cm_outlier_org)

            if len(user_selection) > 0:
                print('Start to create below selected reports: {}'.format(user_selection))
            else:
                flash('You should at least select one task to do!', 'warning')
                return redirect(url_for('dfpm_app'))

            # start formally
            filename_3a4 = secure_filename(f.filename)
            ext_3a4 = os.path.splitext(filename_3a4)[1]

            if ext_3a4 == '.csv':
                file_path_3a4 = os.path.join(base_dir_uploaded, '3a4 for dfpm app (' + login_user + ')' +start_time.strftime('%Y-%m-%d %H:%M:%S') + '.csv')
            elif ext_3a4 == '.xlsx':
                file_path_3a4 = os.path.join(base_dir_uploaded, '3a4 for dfpm app (' + login_user + ')' +start_time.strftime('%Y-%m-%d %H:%M:%S') + '.xlsx')
            else:
                flash('3A4 file type error: Only csv or xlsx file accepted! File you were trying to upload: {}'.format(
                        f.filename),'warning')
                return redirect(url_for('dfpm_app'))

            # 存储文件
            f.save(file_path_3a4)

            # 预读取文件做格式和内容判断
            if ext_3a4 == '.csv':
                df = pd.read_csv(file_path_3a4,encoding='iso-8859-1',nrows=3)
            elif ext_3a4 == '.xlsx':
                df = pd.read_excel(file_path_3a4,encoding='iso-8859-1',nrows=3)

            # 检查文件是否包含需要的列：
            if not np.all(np.in1d(col_3a4_must_have_dfpm, df.columns)):
                flash('File format error! Following required \
                                            columns not found in 3a4 data: {}'.format(
                        str(np.setdiff1d(col_3a4_must_have_dfpm, df.columns))),
                        'warning')
                del df
                gc.collect()

                return redirect(url_for('dfpm_app'))

            try:
                df_3a4 = read_3a4_parse_dates(file_path_3a4, date_col_to_parse_apjc)

                df_3a4=basic_data_processin_dfpm_app(df_3a4)

                # read smartsheet priorities
                ss_exceptional_priority, df_removal = read_backlog_priority_from_smartsheet(df_3a4, login_user)

                # Remove and send email notification for ss removal from exceptional priority smartsheet
                #remove_priority_ss_from_smtsheet_and_notify(df_removal, login_user, sender='APJC DF - 3a4 auto')

                # Rank the orders
                #df_3a4 = ss_ranking_overall_new_december(df_3a4, ss_exceptional_priority, ranking_col,lowest_priority_cat, order_col='SO_SS',
                #                                    new_col='ss_overall_rank')
                df_3a4 = ss_ranking_overall_new_jan(df_3a4, ss_exceptional_priority, ranking_options, lowest_priority_cat,
                                           order_col='SO_SS')

                df_3a4,ctb_error_msg=add_cm_ctb_to_3a4(df_3a4)
                addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material = create_ctb_summaries(df_3a4)

                # create summaries based on tasks
                if dfpm_3a4:
                    # create asp
                    df_asp=create_tan_asp(df_3a4)
                    # create the DFPM mapping dict
                    dfpm_mapping = generate_dfpm_mapping_dict(df_dfpm_mapping)

                    dfpm_3a4_created=create_dfpm_3a4(dfpm_mapping,dfpm_3a4_option,df_3a4,df_asp,addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material,login_user)

                    # Release the memories
                    del addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material
                    gc.collect()

                    msg='3A4 has been generated for: {}'.format(dfpm_3a4_created)
                    flash(msg,'success')
                    print(msg)

                if cm_outlier:
                    # Default only for FOC and FDO - kept for APJC outlier
                    org_name=org_name_global['APJC']
                    outlier_comparison = 7
                    # cm_emails_to进一步考虑user是否选择了特别org
                    #cm_emails = {'FOC': ['kwang2@cisco.com'],
                    #             'FDO': ['kwang2@cisco.com']}  # testing
                    cm_emails_to = {}
                    for org in cm_outlier_org:
                        site_email=read_subscription_by_site(org)
                        if len(site_email)>0:
                            cm_emails_to[org]=site_email
                        else:
                            cm_emails_to[org] = [login_user + '@cisco.com']

                    # remove options
                    df_3a4=df_3a4[df_3a4.OPTION_NUMBER==0].copy()
                    # Add outlier columns: book/schedule/pack long ageings
                    df_3a4 = add_outlier_col(df=df_3a4)

                    create_and_send_cm_3a4(df_3a4, cm_emails_to, outlier_elements,
                                           outlier_chart_foc, outlier_chart_fdo, org_name,
                                           outlier_comparison, login_user)
                    msg = 'Outlier summary has been generated and sent for: {}'.format(cm_outlier_org)
                    flash(msg, 'success')

                # summarize time
                time_stamp = pd.Timestamp.now()
                processing_time = round((time_stamp - start_time).total_seconds() / 60, 1)
                print('Completed: {} min'.format(processing_time))
                # write program log to log file
                add_user_log(user=login_user, location='DFPM app', user_action='Run', summary='User selection: {}; Processing time: {} min.'.format(user_selection, str(processing_time)))

                return redirect(url_for('dfpm_app'))
            except Exception as e:
                msg = 'Error: {}'.format(str(e))
                flash(msg, 'warning')

                traceback.print_exc()
                add_user_log(user=login_user, location='DFPM app', user_action='Run',
                             summary='User selection: {}; Error: {}'.format(user_selection,str(e)))
                error_msg = '\n[' + login_user + '] DFPM app: ' + pd.Timestamp.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + '\n'
                with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                    file_object.write(error_msg)
                traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

                return redirect(url_for('dfpm_app'))
        elif submit_add_update:
            dfpm = form.dfpm.data.strip().lower()
            org = form.dfpm_org.data.strip().upper()
            bu = form.bu_list.data.strip()
            extra_pf = form.extra_pf.data.strip().upper()
            exclusion_pf = form.exclusion_pf.data.strip().upper()

            if len(org)>3:
                msg = 'Pls only input correct org, and just one org at a time!'
                flash(msg, 'warning')
                return redirect(url_for('dfpm_app'))

            if dfpm=='' or org=='':
                msg = 'DFPM and Org are mandatory fields!'
                flash(msg, 'warning')
                return redirect(url_for('dfpm_app'))

            #bu = bu.split('/')
            #extra_pf = extra_pf.split('/')
            #exclusion_pf = exclusion_pf.split('/')

            # update database
            dfpm_org=dfpm+'_'+org
            df_dfpm_mapping.loc[:,'dfpm_org']=df_dfpm_mapping.DFPM + '_' + df_dfpm_mapping.Org
            if dfpm_org in df_dfpm_mapping.dfpm_org.values:
                update_dfpm_mapping_data(dfpm, org, bu, extra_pf, exclusion_pf,login_user)
                msg='DFPM and Org {} already exists - data updated.'.format(dfpm+'-'+org)
                flash(msg,'success')
            else:
                add_dfpm_mapping_data(dfpm, org, bu, extra_pf, exclusion_pf,login_user)
                msg='DFPM added: {}'.format(dfpm)
                flash(msg,'success')

            return redirect(url_for('dfpm_app'))


    return render_template('dfpm_app.html', form=form,user=login_name,subtitle='- DFPM 3A4',
                           df_dfpm_mapping_header=df_dfpm_mapping.columns,
                           df_dfpm_mapping_data=df_dfpm_mapping.values,)



@app.route('/documents',methods=['GET'])
def documents():
    '''
    For documentation
    :return:
    '''
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_name = 'unknown'

    fname=os.path.join(os.getcwd(),'3a4_automation_documentation.xlsx')
    df1=pd.read_excel(fname,sheet_name='3a4_col_processing')
    df2 = pd.read_excel(fname, sheet_name='3a4_summaries')

    return render_template('documents.html',
                           table_data_1=df1.values,
                           table_header_1=df1.columns,
                           table_data_2=df2.values,
                           table_header_2=df2.columns,
                           user=login_name,
                           subtitle=' - Documentation')

@app.route('/backlog',methods=['GET'])
def backlog():
    '''
    For Addressable backlog
    :return:
    '''

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_title = 'unknown'

    if '[C]' in login_title: # for c-workers
        return 'Sorry, you are not authorized to access this.'

    if login_user != 'kwang2':
        add_user_log(user=login_user, location='Backlog', user_action='Visit',
                 summary='')

    fname_apjc=os.path.join(base_dir_tracker,'apjc_history_addressable.xlsx')
    fname_emea=os.path.join(base_dir_tracker,'emea_history_addressable.xlsx')
    fname_americas=os.path.join(base_dir_tracker,'americas_history_addressable.xlsx')

    df_foc=pd.read_excel(fname_apjc,sheet_name='FOC')
    df_fdo = pd.read_excel(fname_apjc, sheet_name='FDO')
    df_jpe = pd.read_excel(fname_apjc, sheet_name='JPE')
    df_shk = pd.read_excel(fname_apjc, sheet_name='SHK')
    df_ncb = pd.read_excel(fname_apjc, sheet_name='NCB')
    df_fcz=pd.read_excel(fname_emea,sheet_name='FCZ')
    df_fve = pd.read_excel(fname_emea, sheet_name='FVE')
    df_ftx=pd.read_excel(fname_americas,sheet_name='FTX')
    df_tau = pd.read_excel(fname_americas, sheet_name='TAU')
    df_sjz = pd.read_excel(fname_americas, sheet_name='SJZ')
    df_fgu = pd.read_excel(fname_americas, sheet_name='FGU')
    df_jmx = pd.read_excel(fname_americas, sheet_name='JMX')
    df_fjz = pd.read_excel(fname_americas, sheet_name='FJZ')
    df_tsp = pd.read_excel(fname_americas, sheet_name='TSP')

    org_list = ['FOC','FDO','JPE','SHK','NCB', 'FCZ','FVE',  'FTX','TAU','SJZ','JMX','FGU','FJZ','TSP']
    for org in org_list:
        df = eval('df_' + org.lower())
        """
        df.loc['total'] = df.sum(axis=0)
        for col in df.columns[1:]:
            if df.loc['total', col] == 0:
                df.drop(col, axis=1, inplace=True)
        """
        df.fillna(0, inplace=True)
        df.sort_values(by='DATE', axis=0, ascending=False, inplace=True)
        #print(org,len(df.columns))
        df.set_index('DATE',inplace=True)
        df.loc['avg']=df.mean(axis=0)
        df.sort_values(by='avg',axis=1,ascending=False,inplace=True)
        df=df[:-1].copy() # remove avg row
        df.reset_index(inplace=True)
        df=df.iloc[:20,:18] # pick only 20 row and 18 col to show so not to mess up the format

        if org == 'FOC':
            df_foc = df
        elif org=='FDO':
            df_fdo=df
        elif org=='JPE':
            df_jpe=df
        elif org=='SHK':
            df_shk=df
        elif org=='NCB':
            df_ncb=df
        elif org == 'FCZ':
            df_fcz = df
        elif org == 'FVE':
            df_fve = df
        elif org == 'FTX':
            df_ftx = df
        elif org=='TAU':
            df_tau=df
        elif org=='SJZ':
            df_sjz=df
        elif org=='JMX':
            df_jmx=df
        elif org=='FGU':
            df_fgu=df
        elif org=='FJZ':
            df_fjz=df
        elif org=='TSP':
            df_tsp=df

    return render_template('backlog_details.html',
                           foc_header=df_foc.columns,
                           foc_data=df_foc.values,
                           fdo_header=df_fdo.columns,
                           fdo_data=df_fdo.values,
                           jpe_header=df_jpe.columns,
                           jpe_data=df_jpe.values,
                           shk_header=df_shk.columns,
                           shk_data=df_shk.values,
                           ncb_header=df_ncb.columns,
                           ncb_data=df_ncb.values,
                           fcz_header=df_fcz.columns,
                           fcz_data=df_fcz.values,
                           fve_header=df_fve.columns,
                           fve_data=df_fve.values,
                           ftx_header=df_ftx.columns,
                           ftx_data=df_ftx.values,
                           tau_header=df_tau.columns,
                           tau_data=df_tau.values,
                           sjz_header=df_sjz.columns,
                           sjz_data=df_sjz.values,
                           fgu_header=df_fgu.columns,
                           fgu_data=df_fgu.values,
                           jmx_header=df_jmx.columns,
                           jmx_data=df_jmx.values,
                           fjz_header=df_fjz.columns,
                           fjz_data=df_fjz.values,
                           tsp_header=df_tsp.columns,
                           tsp_data=df_tsp.values,
                           user=login_name
                           )

@app.route('/file_download',methods=['GET','POST'])
def file_download():
    '''For Files download'''
    now = time.time()

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_title = 'unknown'

    if '[C]' in login_title: # for c-workers
        return 'Sorry, you are not authorized to access this.'

   # get file info
    df_dfpm_3a4=get_file_info_on_drive(base_dir_output,keep_hours=100)
    df_upload=get_file_info_on_drive(base_dir_uploaded,keep_hours=100)

    return render_template('file_download.html',
                           files_dfpm_3a4=df_dfpm_3a4.values,
                           files_uploaded=df_upload.values,
                           user=login_name,
                           subtitle=' - File download')

@app.route('/o/<filename>',methods=['GET'])
def download_file_output(filename):
    f_path=base_dir_output
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/u/<filename>',methods=['GET'])
def download_file_upload(filename):
    f_path=base_dir_uploaded
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/t/<filename>',methods=['GET'])
def download_file_tracker(filename):
    f_path=base_dir_tracker
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)

@app.route('/l/<filename>',methods=['GET'])
def download_file_logs(filename):
    f_path=base_dir_logs
    login_user = request.headers.get('Oidc-Claim-Sub')
    if login_user != None:
        add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=filename)
    return send_from_directory(f_path, filename=filename, as_attachment=True)


@app.route('/subscribe', methods=['GET','POST'])
def subscribe():
    form=SubscriptionForm()

    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_title = 'unknown'

    df_subscription= read_table('subscription')
    #df_subscription.sort_values(by=['Email'], ascending=True, inplace=True)
    df_subscription.drop_duplicates(keep='last',inplace=True)

    if form.validate_on_submit():
        submit_add=form.submit_add.data
        submit_remove=form.submit_remove.data
        confirm_remove=form.confirm_remove.data
        # email addresses if for others
        email = form.email_add_other.data.lower()
        email = email.replace(' ', '').replace('\n', '').replace('\r', '')
        email = email.split(';')
        email = set(email)
        email = list(email)

        if submit_remove: # for removal of email by admin
            if confirm_remove:
                if email==['']:
                    email=[login_user + '@cisco.com']

                id_list = df_subscription[df_subscription.Email.isin(email)].id.values
                id_list=[str(x) for x in id_list]
                if len(id_list)>0:
                    delete_record('subscription', id_list)
                    emails_removed=df_subscription[df_subscription.Email.isin(email)].Email.values
                    add_user_log(user=login_user, location='Subscribe', user_action='Un-subscribe',
                                 summary='Email removed: {}'.format(emails_removed))

                    msg ='Following emails have been removed: {}'.format(emails_removed)
                    flash(msg,'Success')
                    return redirect(url_for('subscribe'))
                else:
                    msg = 'Pls put in correct/existing email to delete!'
                    flash(msg,'warning')
                    return redirect(url_for('subscribe'))
            else:
                msg = 'Select the checkbox to proceed - Related emails will be totally removed.'
                flash(msg, 'warning')
                return render_template('subscription.html', form=form, user=login_name,
                                       subtitle=' - Report Subscription',
                                       df_subscription_header=df_subscription.columns,
                                       df_subscription_data=df_subscription.values)
        elif submit_add: # adding or updating email tasks
            backlog=form.sub_backlog.data
            backlog_apjc=form.backlog_apjc.data
            backlog_emea=form.backlog_emea.data
            backlog_americas=form.backlog_americas.data

            wnbu_compliance=form.sub_wnbu_compliance.data
            wnbu_compliance_apjc=form.wnbu_compliance_apjc.data
            wnbu_compliance_emea=form.wnbu_compliance_emea.data
            wnbu_compliance_americas=form.wnbu_compliance_americas.data

            config = form.sub_config.data
            config_apjc = form.config_apjc.data
            config_emea = form.config_emea.data
            config_americas = form.config_americas.data

            ranking = form.backlog_ranking.data
            ranking_org = form.backlog_ranking_org.data.upper()



            if backlog==False and wnbu_compliance==False and config==False and ranking==False:
                msg = "Pls select at least a report to subscribe!"
                flash(msg,'warning')
                return render_template('subscription.html', form=form, user=login_name,
                                       subtitle=' - Report Subscription',
                               df_subscription_header=df_subscription.columns,
                               df_subscription_data=df_subscription.values)

            if email:
                if ',' in email or '/' in email:
                    msg="Pls separate multiple emails with ';' instead!"
                    flash(msg,'warning')
                    return render_template('subscription.html', form=form, user=login_name,
                                           subtitle=' - Report Subscription',
                               df_subscription_header=df_subscription.columns,
                               df_subscription_data=df_subscription.values)

            # aggregate the user selection of subscriptions
            task_dic={}

            if backlog:
                if backlog_apjc==False and backlog_emea==False and backlog_americas==False:
                    msg='Pls select region for Backlog report!'
                    flash(msg,'warning')
                    #return redirect(url_for('subscribe'))
                    return render_template('subscription.html', form=form,user=login_name,subtitle=' - Report Subscription',
                               df_subscription_header=df_subscription.columns,
                               df_subscription_data=df_subscription.values)
                else:
                    region = []
                    if backlog_apjc:
                        region.append('APJC')
                    if backlog_emea:
                        region.append('EMEA')
                    if backlog_americas:
                        region.append('Americas')
                    task_dic['Backlog dashboard']=region

            if wnbu_compliance:
                if wnbu_compliance_apjc == False and wnbu_compliance_emea == False and wnbu_compliance_americas == False:
                    msg = 'Pls select region for WNBU compliance report!'
                    flash(msg, 'warning')
                    # return redirect(url_for('subscribe'))
                    return render_template('subscription.html', form=form, user=login_name,subtitle=' - Report Subscription',
                               df_subscription_header=df_subscription.columns,
                               df_subscription_data=df_subscription.values)
                else:
                    region = []
                    if wnbu_compliance_apjc:
                        region.append('APJC')
                    if wnbu_compliance_emea:
                        region.append('EMEA')
                    if wnbu_compliance_americas:
                        region.append('Americas')
                    task_dic['WNBU compliance']=region

            if config:
                if config_apjc == False and config_emea == False and config_americas == False:
                    msg = 'Pls select region for Config report!'
                    flash(msg, 'warning')
                    # return redirect(url_for('subscribe'))
                    return render_template('subscription.html', form=form, user=login_name,
                                               subtitle=' - Report Subscription',
                               df_subscription_header=df_subscription.columns,
                               df_subscription_data=df_subscription.values)
                else:
                    region = []
                    if config_apjc:
                        region.append('APJC')
                    if config_emea:
                        region.append('EMEA')
                    if config_americas:
                        region.append('Americas')
                    task_dic['Config report']=region

            if ranking:
                ranking_org=ranking_org.replace(' ', '')
                ranking_org_list = ranking_org.split('/')
                ranking_org_list = [org for org in ranking_org_list if len(org)==3]
                wrong_org_list = [org for org in ranking_org_list if len(org)!=3]

                if len(ranking_org_list)==0 or len(wrong_org_list)>0:
                    msg='You have either put in NO org name or wrong org name for ranking report!'
                    flash(msg,'warning')
                    return render_template('subscription.html', form=form, user=login_name,
                                           subtitle=' - Report Subscription',
                                           df_subscription_header=df_subscription.columns,
                                           df_subscription_data=df_subscription.values)
                task_dic['Backlog ranking']=ranking_org_list

            if email!=['']:
                # identify eligible and wrong email list - return in case of errors
                uneligible_email=[]
                wrong_email=[]
                email_list=[]
                for x in email:
                    if len(x)<=11:
                        wrong_email.append(x)
                    elif x.count('@')!=1:
                        wrong_email.append(x)
                    elif '.' not in x:
                        wrong_email.append(x)
                    else:
                        email_list.append(x)

                    if backlog:
                        if 'cisco' not in x:
                            msg = 'You selected the Backlog dashboard which is for Cisco only! Pls exclude non-Cisco users and resubmit.'
                            flash(msg, 'warning')
                            return render_template('subscription.html', form=form,
                                                   user=login_name,
                                                   subtitle=' - Report Subscription',
                                                   df_subscription_header=df_subscription.columns,
                                                   df_subscription_data=df_subscription.values)

                    if ranking:
                        if 'cisco' not in x:
                            if len(ranking_org_list)>1 or \
                                    ('foxconn' in x and ranking_org_list[0] not in ['FOC','FCZ','FTX','FJZ','SJZ']) or \
                                    ('jabil' in x and ranking_org_list[0] not in ['JPE', 'JMX']) or \
                                    ('fab' in x and ranking_org_list[0] not in ['NCB']) or \
                                    ('flex' in x and ranking_org_list[0] not in ['FDO','FGU','TAU']) or \
                                    ('dbschenker' in x and ranking_org_list[0] not in ['SHK','FVE']):
                                msg = 'Non-Cisco users can only subscribe to belonged org!'
                                flash(msg, 'warning')
                                return render_template('subscription.html', form=form,
                                                       user=login_name,
                                                       subtitle=' - Report Subscription',
                                                       df_subscription_header=df_subscription.columns,
                                                       df_subscription_data=df_subscription.values)
                # return for below errors
                if len(wrong_email) > 0:
                    msg = 'Following emails looks wrong: {}'.format(wrong_email)
                    flash(msg, 'warning')
                    msg = 'None email is correct to be added, stop!'
                    flash(msg, 'warning')
                    return render_template('subscription.html', form=form,
                                           user=login_name,
                                           subtitle=' - Report Subscription',
                                           df_subscription_header=df_subscription.columns,
                                           df_subscription_data=df_subscription.values)

                if len(email_list) == 0:
                    msg = 'None email is correct to be added, stop!'
                    flash(msg, 'warning')
                    return render_template('subscription.html', form=form,
                                           user=login_name,
                                           subtitle=' - Report Subscription',
                                           df_subscription_header=df_subscription.columns,
                                           df_subscription_data=df_subscription.values)

            else: # subscribing for self case
                email_list=[login_user + '@cisco.com']

            #map task to each email
            new_sub_dict_dict={}
            for email in email_list:
                new_sub_dict_dict[email]=task_dic

            # change current sub df into a dic and compare with new_sub_dict_dict to add additonal subscription
            current_sub_dict = {}
            for row in df_subscription.itertuples():
                current_sub_dict[row.Email]=eval(row.Subscription)
            # create an updated dict that require changes
            updated_sub_dict = {}
            for email, new_sub in new_sub_dict_dict.items():
                if email in current_sub_dict.keys():
                    updated_sub = current_sub_dict[email]
                    current_sub=current_sub_dict[email] # current sub task dict
                    for task_name,new_scope_list in new_sub.items():
                        if task_name in current_sub.keys():
                            current_scope_list=current_sub[task_name] # region list in current sub
                            updated_scope_list=np.union1d(new_scope_list, current_scope_list).tolist()
                        else:
                            updated_scope_list=new_scope_list
                        updated_sub[task_name]=updated_scope_list
                else:
                    updated_sub = new_sub

                updated_sub_dict[email]=updated_sub #

            # update db
            for email,task in updated_sub_dict.items():
                if email in df_subscription.Email.values:
                    update_subscription(email, str(task), login_user)
                else:
                    add_subscription(email, str(task), login_user)

            # write program log to log file
            msg = 'Subscription for following emails have been added/updated: {}'.format(email_list)
            flash(msg, 'success')
            add_user_log(user=login_user, location='Subscribe', user_action='Subscribe',
                                 summary='Subscription details: {}'.format(new_sub_dict_dict))

        # read the table again for display
        df_subscription = read_table('subscription')

        return render_template('subscription.html', form=form,
                           user=login_name,
                           subtitle=' - Report Subscription',
                           df_subscription_header=df_subscription.columns,
                           df_subscription_data=df_subscription.values)

    return render_template('subscription.html', form=form,
                           user=login_name,
                           subtitle=' - Report Subscription',
                           df_subscription_header=df_subscription.columns,
                           df_subscription_data=df_subscription.values)


@app.route('/admin', methods=['GET','POST'])
def admin():
    form=AdminForm()

    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    login_title = request.headers.get('Oidc-Claim-Title')

    if login_user == None:
        login_user = 'unknown'
        login_title = 'unknown'

    if login_user!='kwang2':
        raise ValueError
        add_user_log(user=login_user, location='Admin', user_action='Visit',
                 summary='Why happens?')

    # get file info
    df_dfpm_3a4=get_file_info_on_drive(base_dir_output,keep_hours=100)
    df_upload=get_file_info_on_drive(base_dir_uploaded,keep_hours=100)
    df_tracker=get_file_info_on_drive(base_dir_tracker,keep_hours=10000)
    df_logs=get_file_info_on_drive(base_dir_logs,keep_hours=10000)

    # read logs
    df_log_detail = read_table('user_log')
    df_log_detail.sort_values(by=['DATE', 'TIME'], ascending=False, inplace=True)

    if form.validate_on_submit():
        fname=form.file_name.data
        if fname in df_dfpm_3a4.File_name.values:
            f_path=df_dfpm_3a4[df_dfpm_3a4.File_name==fname].File_path.values[0]
            os.remove(f_path)
            msg='{} removed!'.format(fname)
            flash(msg,'success')
        elif fname in df_upload.File_name.values:
            f_path = df_upload[df_upload.File_name == fname].File_path.values[0]
            os.remove(f_path)
            msg = '{} removed!'.format(fname)
            flash(msg, 'success')
        elif fname in df_tracker.File_name.values:
            f_path = df_tracker[df_tracker.File_name == fname].File_path.values[0]
            os.remove(f_path)
            msg = '{} removed!'.format(fname)
            flash(msg, 'success')
        else:
            msg = 'Error file name! Ensure it is in output folder,upload folder or supply folder: {}'.format(fname)
            flash(msg, 'warning')
            return redirect(url_for('admin',_external=True,_scheme='http',viewarg1=1))

    return render_template('admin.html',form=form,
                           files_dfpm_3a4=df_dfpm_3a4.values,
                           files_uploaded=df_upload.values,
                           files_tracker=df_tracker.values,
                           files_logs=df_logs.values,
                           log_details=df_log_detail.values,
                           user=login_name,
                           subtitle=' - Admin')


@app.route('/slot/<login_user>/<added_by>/<record_id>',methods=['GET'])
def delete_slot_record(login_user,added_by,record_id):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'http'

    if login_user==added_by:# or login_user==super_user:
        id_list=[str(record_id)]
        delete_record('slot', id_list)
        msg = 'Slot deleted: {}'.format(record_id)
        flash(msg, 'success')
    else:
        msg = 'You can only delete record created by you!'
        flash(msg,'warning')

    return redirect(url_for("config_rules_main", _external=True, _scheme=http_scheme, viewarg1=1))


@app.route('/pid/<login_user>/<added_by>/<record_id>',methods=['GET'])
def delete_general_config_rule_pid_record(login_user,added_by,record_id):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'http'

    if login_user==added_by:# or login_user==super_user:
        id_list=[str(record_id)]
        delete_record('general_config_rule_pid', id_list)
        msg = 'General rule deleted: {}'.format(record_id)
        flash(msg, 'success')
    else:
        msg = 'You can only delete record created by you!'
        flash(msg,'warning')

    return redirect(url_for("config_rules_incl_excl_pid_based", _external=True, _scheme=http_scheme, viewarg1=1))

@app.route('/bupf/<login_user>/<added_by>/<record_id>',methods=['GET'])
def delete_general_config_rule_bupf_record(login_user,added_by,record_id):
    if login_user == 'unknown':
        http_scheme = 'http'
    else:
        http_scheme = 'http'

    if login_user==added_by:# or login_user==super_user:
        id_list=[str(record_id)]
        delete_record('general_config_rule_bu_pf', id_list)
        msg = 'General rule deleted: {}'.format(record_id)
        flash(msg, 'success')
    else:
        msg = 'You can only delete record created by you!'
        flash(msg,'warning')

    return redirect(url_for("config_rules_incl_excl_bupf_based", _external=True, _scheme=http_scheme, viewarg1=1))


if __name__ == '__main__':
    # FLASK_RUN_HOST='0.0.0.0'
    app.run()
