'''
Ken: 2019
Flask web service app for 3a4 automated reports
'''

# add below matplotlib.use('Agg') to avoid this error: Assertion failed: (NSViewIsCurrentlyBuildingLayerTreeForDisplay()
# != currentlyBuildingLayerTree), function NSViewSetCurrentlyBuildingLayerTreeForDisplay
import matplotlib
matplotlib.use('Agg')

import time
from werkzeug.utils import secure_filename
from flask import flash,send_from_directory,render_template,request, redirect, url_for
from flask_setting import *
from blg_functions import *
from blg_settings import *
from db_add import add_user_log  # remove db and use above instead
from db_read import read_table
from db_delete import delete_record
import traceback
#from flask_bootstrap import Bootstrap

#Bootstrap(app)

@app.route('/hello',methods=['GET'])
def hello():
    return "Welcome to the auto 3a4 tool!"

@app.route('/dfpm_automation', methods=['GET', 'POST'])
def run_reports():
    form = UploadForm()
    # as these email valiable are redefined below in email_to_only check, thus have to use global to define here in advance
    # otherwise can't be used. (as we create new vaiables with _ suffix thus no need to set global variable)
    # global backlog_dashboard_emails
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'kwang2'
        login_name = 'Ken - debug'

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user,location='Home',user_action='visit',summary='')

    program_log = []
    ctb_error_msg = []
    user_selection = []
    time_details=[]

    if form.validate_on_submit():
        start_time_=pd.Timestamp.now()
        # 通过条件判断及邮件赋值，开始执行任务
        f = form.file.data
        use_existing_3a4=form.use_existing_3a4.data
        region=form.region.data
        backlog_summary = form.backlog_summary.data
        add_to_tracker=form.add_to_tracker.data
        cm_3a4 = form.cm_3a4.data
        cm_name=form.cm_name.data
        dfpm_3a4 = form.dfpm_3a4.data
        dfpm_id=form.dfpm_id.data
        wnbu_compliance = form.wnbu_compliance.data
        #ranking_logic=form.ranking_logic.data   # use the default from settings instead
        ranking_logic = 'cus_sat'

        outlier_summary = form.outlier_summary.data
        #outlier_comparison=form.outlier_comparison.data
        outlier_comparison = 7 # use default 7 days directly

        email_option=form.email_option.data


        # write program log to log file
        #add_user_log(userid, start_time.strftime('%H:%M'), None, None, '; '.join(user_selection),
        #             None, "Start running program")

        # 判断是否输入email_option:
        if email_option=='':
            flash('Pls select email sending option!','warning')
            return render_template('3a4_run_main.html', form=form)

        # 判断是否输入有效天数
        """
        if not type(outlier_comparison)==int:
            flash("You have to input outlier comparison back days as integer!", 'warning')
            return render_template('3a4_run_main.html', form=form)
        elif not outlier_comparison>0:
            flash("You have to input the outlier comparison back days greater than 1!", 'warning')
            return render_template('3a4_run_main.html', form=form)
        """
        # 判断并定义ranking_col
        ranking_col = ranking_col_cust

        # 汇总task list, 判断是否有选择至少一个summary需要制作
        user_selection.append(region + '\n')
        if dfpm_3a4:
            user_selection.append(form.dfpm_3a4.label.text + '\n')
        if cm_3a4:
            user_selection.append(form.cm_3a4.label.text + '\n')
        if backlog_summary:
            user_selection.append(form.backlog_summary.label.text + '\n')
        if outlier_summary:
            user_selection.append(form.outlier_summary.label.text + '\n')
        if wnbu_compliance:
            user_selection.append(form.wnbu_compliance.label.text + '\n')

        if len(user_selection) > 1:
            print('Start to create below selected reports:')
            for task in user_selection:
                print(task)
        else:
            flash('You should at least select one task to do!', 'warning')
            return render_template('3a4_run_main.html', form=form)

        # Add other user selection input
        parameter=''
        if backlog_summary:
            if add_to_tracker=='formal':
                parameter=parameter + 'Add addressable to tracker: YES / '
            else:
                parameter=parameter + 'Add addressable to tracker: NO / '
        if cm_3a4 and cm_name:
            parameter=parameter + 'Make CM 3a4 only for: '+ cm_name + ' / '
        if dfpm_3a4 and dfpm_id:
            parameter=parameter + 'Make DFPM 3a4 only for: ' + dfpm_id

        user_selection.append('('+ parameter + ')')


        # check if file input chosen
        if not any([use_existing_3a4,f]):
            flash('Pls upload 3a4 file or select to use existing file on server (if any).','warning')
            return render_template('3a4_run_main.html', form=form)

        # 是否使用新upload文件还是存在服务器上的文件 (uploaded no more than 8 hours ago)
        if use_existing_3a4:
            file_source='Previously uploaded 3a4 on server'
            file_list=os.listdir(app.config['UPLOAD_PATH'])
            fname_3a4=''
            for file in file_list:
                if file[:1]!='~':
                    if login_user in file:
                        fname_3a4=file

            if fname_3a4!='':
                file_path_3a4=os.path.join(app.config['UPLOAD_PATH'],fname_3a4)
                now = time.time()
                m_time = os.stat(file_path_3a4).st_mtime
                creation_time=int((now - m_time) / 60) # minutes
                if creation_time>480:
                    flash('3a4 existing for you on server was created more than 8 hours ago. To ensure it is updated pls upload new 3a4 file.', 'warning')
                    return render_template('3a4_run_main.html', form=form)
            else:
                flash('Do not find 3a4 existing for you on server. Pls upload new 3a4 file.', 'warning')
                return render_template('3a4_run_main.html', form=form)

        else:
            # 上传的文件名称并判断文件类型
            file_source='User upload'
            filename_3a4 = secure_filename(f.filename)
            ext_3a4 = os.path.splitext(filename_3a4)[1]

            # print(filename_3a4)

            if ext_3a4 == '.csv':
                file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'], '3a4 - ' + login_user + '.csv')
            elif ext_3a4 == '.xlsx':
                file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'], '3a4 - ' + login_user + '.xlsx')
            else:
                flash('3A4 file type error: Only csv or xlsx file accepted! File you were trying to upload: {}'.format(
                    f.filename),'warning')
                return render_template('3a4_run_main.html', form=form)

            # 存储文件
            f.save(file_path_3a4)



        # 根据region选项定义default值 （暂时只启用backlog_summary）
        if region=='APJC':
            org_name=org_name_apjc
            backlog_dashboard_emails=backlog_dashboard_emails_apjc
            addr_fname=addr_fname_apjc
            backlog_chart=backlog_chart_apjc
            sender='APJC DF'
        elif region=='EMEA':
            org_name = org_name_emea
            backlog_dashboard_emails = backlog_dashboard_emails_emea
            addr_fname=addr_fname_emea
            backlog_chart = backlog_chart_emea
            sender='EMEA DF'
        elif region=='Americas':
            org_name = org_name_americas
            backlog_dashboard_emails = backlog_dashboard_emails_americas
            addr_fname=addr_fname_americas
            backlog_chart = backlog_chart_americas
            sender='Americas DF'



        # 根据email_option并重新赋值各项summary发送地址
        if email_option=='to_me':
            email=[login_user + '@cisco.com']
            backlog_dashboard_emails_to = email
            apjc_outlier_emails_to = email
            wnbu_compliance_summary_emails_to = email

            # cm_emails_to进一步考虑user是否选择了特别org
            cm_emails_to={}
            if cm_name!='':
                cm_emails_to = {cm_name: email}
                msg = 'You selected CM summaries only for org: {}'.format(cm_name)
                program_log.append(msg)
                print(msg)
            else:
                for key in cm_emails.keys():
                    cm_emails_to[key]=email

            #dfpm_mapping_to进一步考虑user是否选择了特别dfpm
            dfpm_mapping_to={}
            if dfpm_id!='':
                bu = dfpm_mapping[dfpm_id]
                dfpm_mapping_to[login_user] = bu
                msg = 'You selected DFPM summaries for user: {}'.format(dfpm_id)
                program_log.append(msg)
                print(msg)
            elif dfpm_id=="" and dfpm_3a4:
                flash('When email otpion is sending to yourself, you have to select a DFPM for the DFPM 3a4 summary!','warning')
                return render_template('3a4_run_main.html', form=form)
        else:
            backlog_dashboard_emails_to = backlog_dashboard_emails
            apjc_outlier_emails_to = apjc_outlier_emails
            wnbu_compliance_summary_emails_to = wnbu_compliance_summary_emails

            # cm_emails_to进一步考虑user是否选择了特别org
            cm_emails_to = {}
            if cm_name != '':
                cm_emails_to = {cm_name: cm_emails[cm_name]}
                msg = 'You selected CM summaries only for org: {}'.format(cm_name)
                program_log.append(msg)
                print(msg)
            else:
                cm_emails_to=cm_emails

            # dfpm_mapping_to进一步考虑user是否选择了特别dfpm
            dfpm_mapping_to = {}
            if dfpm_id!='':
                bu = dfpm_mapping[dfpm_id]
                dfpm_mapping_to[login_user] = bu
                msg = 'You selected DFPM summaries for user: {}'.format(dfpm_id)
                program_log.append(msg)
                print(msg)
            else:
                dfpm_mapping_to=dfpm_mapping


        # 预读取文件做格式和内容判断
        if file_path_3a4[-4:] == '.csv':
            df = pd.read_csv(file_path_3a4,encoding='iso-8859-1')
        elif file_path_3a4[-4:] == 'xlsx':
            df = pd.read_excel(file_path_3a4,encoding='iso-8859-1')

        # 检查3a4是否包含对应region的org
        if not np.all(np.in1d(org_name[region], df.ORGANIZATION_CODE.unique())):
            flash('The 3a4 you uploaded does not contain all orgs from {}!'.format(region), 'warning')
            del df
            gc.collect()

            return render_template('3a4_run_main.html', form=form)

        # 检查文件是否包含需要的列：
        if not np.all(np.in1d(col_3a4_must_have[region], df.columns)):
            flash('File format error! Following required \
                                        columns not found in 3a4 data: {}'.format(
                    str(np.setdiff1d(col_3a4_must_have[region], df.columns))),
                    'warning')
            del df
            gc.collect()

            return render_template('3a4_run_main.html', form=form)


        # 正式开始程序; start processing the data and create summaries
        # write program log to log file
        add_user_log(user=login_user, location='Home', user_action='Run',
                     summary='Start running the program. Tasks: ' + '/'.join(user_selection))

        try:
            time_stamp = pd.Timestamp.now()
            module = 'read_3a4'
            if region == 'APJC':
                date_col = date_col_apjc
            elif region == 'EMEA' or region == 'Americas':
                date_col = date_col_emea_americas
            df_3a4 = read_3a4_parse_dates(file_path_3a4, date_col, program_log, file_source)

            # initial basic data processing
            if region=='EMEA' or region=='Americas':
                module='basic_data_processin_emea_americas'
                df_3a4=basic_data_processin_emea_americas(df_3a4)
            elif region=='APJC':
                module='basic_data_processin_apjc'
                df_3a4=basic_data_processin_apjc(df_3a4)

            module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
            msg = module + ' done:' + str(module_processing_time) + 'min'
            print(msg)
            time_details.append(msg)
            time_stamp = pd.Timestamp.now()

            # below do further data processing based on tasks
            if outlier_summary or cm_3a4:
                print('Creating the outliers...')
                module = 'add_outlier_col/create_rtv_config_col'

                # Add outlier columns: book/schedule/pack long ageings
                df_3a4 = add_outlier_col(df=df_3a4)

                # rtv config col - discarded with 3a4 without option
                #df_3a4 = create_rtv_config_col(df_3a4)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg=module + ' done:' + str(module_processing_time)+'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            if dfpm_3a4 or cm_3a4:

                #qend = decide_qend_date(qend_list)

                # read smartsheet priorities
                module = 'read_backlog_priority_from_smartsheet'
                ss_priority,removal_ss_email,df_removal = read_backlog_priority_from_smartsheet(df_3a4)

                # send email notification for ss removal from exceptional priority smartsheet
                send_email_for_priority_ss_removal(removal_ss_email, df_removal, login_user,sender='APJC DF')

                # Rank the orders
                module = 'ss_ranking_overall'
                df_3a4 = ss_ranking_overall_new_december(df_3a4, ss_priority, ranking_col,lowest_priority_cat, order_col='SO_SS',
                                                new_col='ss_overall_rank')
                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            if dfpm_3a4 or cm_3a4:
                print('Creating addressable with CTB summaries...')
                # Read CTB from smartsheet, add to 3a4, and make different summaries
                module='create_ctb_summaries'
                df_3a4, addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material, program_log, ctb_error_msg \
                                                    = create_ctb_summaries(df_3a4, program_log,org_name)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()
                #print('1', '110997874-68' in df_3a4.PO_NUMBER.values)
            if backlog_summary or dfpm_3a4:
                print('Creating backlog summaries...')
                # Make addr_df_summary for addressable snapshot and addr_df_dict for trending chart; also collect
                # addressable data to tracker.
                module='create_addressable_summary_and_comb_addressable_history'
                addr_df_summary, addr_df_dict,program_log = create_addressable_summary_and_comb_addressable_history(df_3a4,
                                                                                                           org_name,
                                                                                                           region,
                                                                                                           addr_fname,
                                                                                                           program_log)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            if backlog_summary:
                #存储addressable tracker - only when add_to_tracker and also 3a4 include all APJC org then write the addressable into tracker
                module='save_addr_tracker'
                if add_to_tracker=='formal':
                    program_log=save_addr_tracker(df_3a4,addr_df_dict, region, org_name,addr_fname, program_log)
                else:
                    program_log.append('Backlog not save to tracer per user choice')
                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            # data processing: Create WNNBU compliance hold order col
            if wnbu_compliance:
                print('Creating WNBU compliance check...')
                module='read_compliance_from_smartsheet'
                df_compliance_table, no_ship, program_log = read_compliance_from_smartsheet(df_3a4, program_log)
                module='check_compliance_for_wnbu(adding compliance hold col)'
                df_3a4, df_compliance_release, df_compliance_hold, df_country_missing = check_compliance_for_wnbu(df_3a4,
                                                                                                                  no_ship)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            # Below create summaries and send by email
            if backlog_summary:
                module='create_and_send_addressable_summaries'
                program_log = create_and_send_addressable_summaries(addr_df_summary, addr_df_dict, org_name,
                                                                    backlog_dashboard_emails_to,
                                                                    backlog_chart, region,sender,
                                                                    program_log, login_user)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

                # send tracker addressable backlog to ken as backlog - happens on Monday
                if region=='APJC':
                    download_and_send_tracker_on_monday_as_backup()


            if outlier_summary:
                module='create_and_send_outlier_summaries'
                program_log = create_and_send_outlier_summaries(df_3a4, add_to_tracker,outlier_elements, outlier_chart_apjc,
                                                                apjc_outlier_emails_to,org_name,outlier_comparison,
                                                                program_log, login_user)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            if cm_3a4:
                module='create_and_send_cm_3a4'
                program_log = create_and_send_cm_3a4(df_3a4,cm_emails_to, outlier_elements,
                                                     outlier_chart_foc,outlier_chart_fdo,org_name,
                                                     outlier_comparison, program_log,login_user)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            if wnbu_compliance:
                module='create_and_send_wnbu_compliance'
                program_log = create_and_send_wnbu_compliance(wnbu_compliance_summary_emails_to,
                                                              df_compliance_release,
                                                              df_compliance_hold,
                                                              df_country_missing,
                                                              df_compliance_table,
                                                              program_log, login_user)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()

            if dfpm_3a4:
                module='create_and_send_dfpm_3a4'
                #print('1:', '110997874-68' in df_3a4.PO_NUMBER.values)
                program_log,time_details = create_and_send_dfpm_3a4(df_3a4, dfpm_mapping_to, date_col,backlog_distribution_date_labels,
                                                                        addr_ctb_by_org_bu,
                                                                        addr_ctb_by_org_bu_pf,
                                                                        ctb_summary_for_material,
                                                                        addr_df_dict, program_log, time_details,login_user)

                module_processing_time = round((pd.Timestamp.now() - time_stamp).total_seconds() / 60, 2)
                msg = module + ' done:' + str(module_processing_time) + 'min'
                print(msg)
                time_details.append(msg)
                time_stamp = pd.Timestamp.now()


            # Release the memories
            module='Releasing memories'
            if backlog_summary:
                del addr_df_summary
            if dfpm_3a4:
                del addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material
            if backlog_summary or dfpm_3a4:
                del addr_df_dict
            if outlier_summary:
                del outlier_comparison
            if wnbu_compliance:
                del wnbu_compliance_summary_emails_to, df_compliance_release, df_compliance_hold, df_country_missing, df_compliance_table

            del df_3a4

            gc.collect()

            # summarize time
            time_stamp = pd.Timestamp.now()
            processing_time = round((time_stamp - start_time_).total_seconds() / 60, 1)
            start_time = start_time_.strftime('%H:%M')
            finish_time = time_stamp.strftime('%H:%M')

            # write program log to log file
            add_user_log(user=login_user, location='Home', user_action='Run', summary='Processing time: ' + str(processing_time) + 'min; tasks: ' + '/'.join(user_selection))

            return render_template('3a4_program_result.html',
                                   user_selection=user_selection,
                                   ctb_error_msg=ctb_error_msg,
                                   program_log=program_log,
                                   time_details=time_details,
                                   program_error='None',
                                   processing_time=processing_time,
                                   start_time=start_time,
                                   finish_time=finish_time)

        except Exception as e:
            print(module,': ', e)
            traceback.print_exc()

            add_user_log(user=login_user, location='Home', user_action='Run',
                         summary='Error: ' + str(e))
            error_msg='['+login_user + '] ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                file_object.write(error_msg)
            traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

            return render_template('3a4_program_result.html',
                                   user_selection=user_selection,
                                   ctb_error_msg=ctb_error_msg,
                                   program_log=program_log,
                                   time_details=time_details,
                                   program_error=module + ':' + str(e),
                                   processing_time='',
                                   start_time='',
                                   finish_time='')

    return render_template('3a4_run_main.html', form=form,user=login_name)



@app.route('/dfpm_3a4', methods=['GET', 'POST'])
def run_dfpm_3a4():
    form = UploadForm()
    # as these email valiable are redefined below in email_to_only check, thus have to use global to define here in advance
    # otherwise can't be used. (as we create new vaiables with _ suffix thus no need to set global variable)
    # global backlog_dashboard_emails
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'kwang2'
        login_name = 'Ken - debug'

    if form.validate_on_submit():
        start_time_=pd.Timestamp.now()
        # 通过条件判断及邮件赋值，开始执行任务
        f = form.file.data
        use_existing_3a4=form.use_existing_3a4.data

        # 判断并定义ranking_col
        ranking_col = ranking_col_cust
        org_name = org_name_apjc

        # check if file input chosen
        if not any([use_existing_3a4,f]):
            flash('Pls upload 3a4 file or select to use existing file on server (if any).','warning')
            return render_template('3a4_run_main.html', form=form)

        # 是否使用新upload文件还是存在服务器上的文件 (uploaded no more than 8 hours ago)
        if use_existing_3a4:
            file_source='Previously uploaded 3a4 on server'
            file_list=os.listdir(app.config['UPLOAD_PATH'])
            fname_3a4=''
            for file in file_list:
                if file[:1]!='~':
                    if login_user in file:
                        fname_3a4=file

            if fname_3a4!='':
                file_path_3a4=os.path.join(app.config['UPLOAD_PATH'],fname_3a4)
                now = time.time()
                m_time = os.stat(file_path_3a4).st_mtime
                creation_time=int((now - m_time) / 60) # minutes
                if creation_time>480:
                    flash('3a4 existing for you on server was created more than 8 hours ago. To ensure it is updated pls upload new 3a4 file.', 'warning')
                    return render_template('3a4_run_main.html', form=form)
            else:
                flash('Do not find 3a4 existing for you on server. Pls upload new 3a4 file.', 'warning')
                return render_template('3a4_run_main.html', form=form)

        else:
            # 上传的文件名称并判断文件类型
            file_source='User upload'
            filename_3a4 = secure_filename(f.filename)
            ext_3a4 = os.path.splitext(filename_3a4)[1]

            # print(filename_3a4)

            if ext_3a4 == '.csv':
                file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'], '3a4 - ' + login_user + '.csv')
            elif ext_3a4 == '.xlsx':
                file_path_3a4 = os.path.join(app.config['UPLOAD_PATH'], '3a4 - ' + login_user + '.xlsx')
            else:
                flash('3A4 file type error: Only csv or xlsx file accepted! File you were trying to upload: {}'.format(
                    f.filename),'warning')
                return render_template('3a4_run_main.html', form=form)

            # 存储文件
            f.save(file_path_3a4)

        # 预读取文件做格式和内容判断
        if file_path_3a4[-4:] == '.csv':
            df = pd.read_csv(file_path_3a4,encoding='iso-8859-1',nrows=3)
        elif file_path_3a4[-4:] == 'xlsx':
            df = pd.read_excel(file_path_3a4,encoding='iso-8859-1',nrows=3)

        # 检查文件是否包含需要的列：
        if not np.all(np.in1d(col_3a4_must_have['APJC'], df.columns)):
            flash('File format error! Following required \
                                        columns not found in 3a4 data: {}'.format(
                    str(np.setdiff1d(col_3a4_must_have['APJC'], df.columns))),
                    'warning')
            del df
            gc.collect()

            return render_template('3a4_run_main.html', form=form)

        try:
            program_log=[]
            time_stamp = pd.Timestamp.now()
            module = 'read_3a4'
            date_col = date_col_apjc # for parsing dates
            df_3a4 = read_3a4_parse_dates(file_path_3a4, date_col, program_log, file_source)

            module='basic_data_processin_apjc'
            df_3a4=basic_data_processin_apjc(df_3a4)
            # read smartsheet priorities
            module = 'read_backlog_priority_from_smartsheet'
            ss_priority,removal_ss_email,df_removal = read_backlog_priority_from_smartsheet(df_3a4)

            # send email notification for ss removal from exceptional priority smartsheet
            send_email_for_priority_ss_removal(removal_ss_email, df_removal, login_user,sender='APJC DF - 3a4 auto')

            # Rank the orders
            module = 'ss_ranking_overall'
            df_3a4 = ss_ranking_overall_new_december(df_3a4, ss_priority, ranking_col,lowest_priority_cat, order_col='SO_SS',
                                                new_col='ss_overall_rank')

            module='create_ctb_summaries'
            df_3a4, addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material, program_log, ctb_error_msg \
                                                    = create_ctb_summaries(df_3a4, program_log,org_name)

            module='create_and_send_dfpm_3a4'
            #print('1:', '110997874-68' in df_3a4.PO_NUMBER.values)
            program_log,time_details = create_and_send_dfpm_3a4(df_3a4, dfpm_mapping_to, date_col,backlog_distribution_date_labels,
                                                                        addr_ctb_by_org_bu,
                                                                        addr_ctb_by_org_bu_pf,
                                                                        ctb_summary_for_material,
                                                                        addr_df_dict, program_log, time_details,login_user)


            # Release the memories
            module='Releasing memories'
            del df_3a4, addr_ctb_by_org_bu, addr_ctb_by_org_bu_pf, ctb_summary_for_material
            gc.collect()






            # summarize time
            time_stamp = pd.Timestamp.now()
            processing_time = round((time_stamp - start_time_).total_seconds() / 60, 1)
            start_time = start_time_.strftime('%H:%M')
            finish_time = time_stamp.strftime('%H:%M')

            # write program log to log file
            add_user_log(user=login_user, location='Home', user_action='Run', summary='Processing time: ' + str(processing_time) + 'min; tasks: ' + '/'.join(user_selection))

            return render_template('3a4_program_result.html',
                                   user_selection=user_selection,
                                   ctb_error_msg=ctb_error_msg,
                                   program_log=program_log,
                                   time_details=time_details,
                                   program_error='None',
                                   processing_time=processing_time,
                                   start_time=start_time,
                                   finish_time=finish_time)

        except Exception as e:
            print(module,': ', e)
            traceback.print_exc()

            add_user_log(user=login_user, location='Home', user_action='Run',
                         summary='Error: ' + str(e))
            error_msg='['+login_user + '] ' + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+') as file_object:
                file_object.write(error_msg)
            traceback.print_exc(file=open(os.path.join(base_dir_logs, 'error_log.txt'), 'a+'))

            return render_template('3a4_program_result.html',
                                   user_selection=user_selection,
                                   ctb_error_msg=ctb_error_msg,
                                   program_log=program_log,
                                   time_details=time_details,
                                   program_error=module + ':' + str(e),
                                   processing_time='',
                                   start_time='',
                                   finish_time='')

    return render_template('3a4_run_main.html', form=form,user=login_name)



@app.route('/documents',methods=['GET'])
def documents():
    '''
    For documentation
    :return:
    '''
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'kwang2'
        login_name = 'Ken - debug'

    if login_user != '' and login_user != 'kwang2':
        add_user_log(user=login_user, location='Document', user_action='Visit',
                     summary='')

    fname=os.path.join(os.getcwd(),'3a4_automation_documentation.xlsx')
    df1=pd.read_excel(fname,sheet_name='3a4_col_processing')
    df2 = pd.read_excel(fname, sheet_name='3a4_summaries')

    return render_template('3a4_documents.html',
                           table_data_1=df1.values,
                           table_header_1=df1.columns,
                           table_data_2=df2.values,
                           table_header_2=df2.columns,
                           user=login_name)

@app.route('/backlog',methods=['GET'])
def backlog():
    '''
    For Addressable backlog
    :return:
    '''

    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = 'kwang2'
        login_name = 'Ken - debug'

    if login_user != '' and login_user != 'kwang2':
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
    df_jtv = pd.read_excel(fname_emea, sheet_name='JTV')
    df_ftx=pd.read_excel(fname_americas,sheet_name='FTX')
    df_tau = pd.read_excel(fname_americas, sheet_name='TAU')
    df_sjz = pd.read_excel(fname_americas, sheet_name='SJZ')
    df_fgu = pd.read_excel(fname_americas, sheet_name='FGU')
    df_jmx = pd.read_excel(fname_americas, sheet_name='JMX')
    df_fjz = pd.read_excel(fname_americas, sheet_name='FJZ')
    df_tsp = pd.read_excel(fname_americas, sheet_name='TSP')

    org_list = ['FOC','FDO','JPE','SHK','NCB', 'FCZ','FVE', 'JTV', 'FTX','TAU','SJZ','JMX','FGU','FJZ','TSP']
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
        df=df.iloc[:20,:22] # pick only 20 row and 22 col to show

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
        elif org == 'JTV':
            df_jtv = df
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
                           jtv_header=df_jtv.columns,
                           jtv_data=df_jtv.values,
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
    if login_user == None:
        login_user = 'kwang2'
        login_name = 'Ken - debug'

   # get file info
    df_dfpm_3a4=get_file_info_on_drive(base_dir_spreadsheet)
    df_upload=get_file_info_on_drive(base_dir_uploaded)

    return render_template('file_download.html',
                           files_dfpm_3a4=df_dfpm_3a4.values,
                           files_uploaded=df_upload.values,
                           user=login_name)



@app.route('/<path:file_path>',methods=['GET'])
def download_file(file_path):
    #form=FileDownloadForm()

    f_path,fname = os.path.split(file_path)
    f_path='/' + f_path
    login_user = request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = ''
        login_name = ''

    add_user_log(user=login_user, location='Download', user_action='Download file',
                 summary=fname)

    return send_from_directory(f_path, filename=fname, as_attachment=True)


@app.route('/admin', methods=['GET','POST'])
def admin():
    form=AdminForm()

    login_user=request.headers.get('Oidc-Claim-Sub')
    login_name = request.headers.get('Oidc-Claim-Fullname')
    if login_user == None:
        login_user = ''
        login_name = ''

    if login_user!='' and login_user!='kwang2':
        add_user_log(user=login_user, location='Admin', user_action='Visit',
                     summary='Warning')
        return redirect(url_for('run_reports',_external=True,_scheme='http',viewarg1=1))

    # get file info
    df_dfpm_3a4=get_file_info_on_drive(base_dir_spreadsheet)
    df_upload=get_file_info_on_drive(base_dir_uploaded)
    df_tracker=get_file_info_on_drive(base_dir_tracker)
    df_logs=get_file_info_on_drive(base_dir_logs)

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

    return render_template('3a4_admin.html',form=form,
                           files_dfpm_3a4=df_dfpm_3a4.values,
                           files_uploaded=df_upload.values,
                           files_tracker=df_tracker.values,
                           files_logs=df_logs.values,
                           log_details=df_log_detail.values,
                           user=login_name)



@app.route('/table_data',methods=['GET','POST'])
def operate_table():
    form=TableDataForm()

    if form.validate_on_submit():
        submit_query=form.submit_query.data
        submit_download=form.submit_download.data
        submit_delete=form.submit_delete.data
        table_name = form.table_name.data
        password = form.password.data
        criteria = form.criteria.data
        max_records = form.max_records.data
        show_last=form.show_last.data
        id=form.id.data

        criteria_string = None
        records_limit = None
        if criteria:
            criteria_string = criteria
        if max_records:
            records_limit = max_records

        if submit_query:
            if password=='4585':
                try:
                    df=read_table(table_name,show_last=show_last, criteria_string=criteria_string, records_limit=records_limit)
                   
                    return render_template('3a4_operate_table.html',
                                       table_data=df.values,
                                       table_header=df.columns,
                                       table_name=table_name,
                                       form=form)
                except Exception as e:
                    flash(e,'warning')

                    return render_template('3a4_operate_table.html', form=form)
            else:
                flash('Input the correct password to operate this.','warning')

                return render_template('3a4_operate_table.html',form=form)
        elif submit_download:
            if password=='4585':
                try:
                    df=read_table(table_name,show_last=show_last, criteria_string=criteria_string, records_limit=records_limit)
                except:
                    flash('Your query returned with error - check your input!','warning')
                    return render_template('3a4_operate_table.html', form=form)

                send_downloaded_table(table_name, df,show_last,criteria_string,records_limit,'APJC DFPM')
                flash('Your query result shown below sent to kwang2@cisco.com!', 'success')

                return render_template('3a4_operate_table.html',
                                       table_data=df.values,
                                       table_header=df.columns,
                                       table_name=table_name,
                                       form=form)

            else:
                flash('Input the correct password to operate this.','warning')

                return render_template('3a4_operate_table.html',form=form)

        elif submit_delete:
            if password=='4585':
                if id=='':
                    flash('Input ID to delete!','warning')
                    return render_template('3a4_operate_table.html', form=form)
                else:
                    id=id.replace(' ','')
                    if ',' in id:
                        id_list=id.split(',')
                        id_list = [int(x) for x in id_list]
                    elif '~' in id:
                        id_list = id.split('~')
                        id_list=[int(x) for x in id_list]
                        id_list=[x for x in range(id_list[0],id_list[1]+1)]
                    else:
                        id_list=[int(id)]


                    # 检查id_list是否都存在于table中
                    df = read_table(table_name, show_last=show_last, criteria_string=criteria_string, records_limit=records_limit)
                    id_df=df.id.values
                    id_not_exist=np.setdiff1d(id_list,id_df)
                    if len(id_not_exist)>0:
                        flash('These id do not exist in the table: {}'.format(id_not_exist),'warning')
                        return render_template('3a4_operate_table.html',
                                               table_data=df.values,
                                               table_header=df.columns,
                                               table_name=table_name,
                                               form=form)
                    else:
                        delete_record(table_name,id_list)
                        flash('Record ({}) is deleted from {}'.format(id_list,table_name),'success')
                        df = read_table(table_name, show_last=show_last, criteria_string=criteria_string, records_limit=records_limit)

                        return render_template('3a4_operate_table.html',
                                           table_data=df.values,
                                           table_header=df.columns,
                                           table_name=table_name,
                                           form=form)
            else:
                flash('Input the correct password operate this.','warning')
                df = read_table(table_name, show_last=show_last, criteria_string=criteria_string, records_limit=records_limit)

                return render_template('3a4_operate_table.html',
                                       table_data=df.values,
                                       table_header=df.columns,
                                       table_name=table_name,
                                       form=form)

    return render_template('3a4_operate_table.html', form=form)




if __name__ == '__main__':
    # FLASK_RUN_HOST='0.0.0.0'
    app.run()
