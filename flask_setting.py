from flask import Flask
from flask_wtf.file import FileField, FileRequired
from flask_wtf import FlaskForm
from wtforms.validators import Email, DataRequired,input_required
from wtforms import SubmitField, BooleanField, StringField,RadioField,SelectField,PasswordField,TextAreaField,SelectMultipleField
import os
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'secret string')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DB_URI') #os.getenv('DB_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Flask forms
class GlobalAppForm(FlaskForm):
    # 创建各种表单对象
    file = FileField('Please upload 3A4 file (.csv):')
    region=SelectField('Create report for selected region:',
                      choices=[('APJC','APJC'),('EMEA','EMEA'),('Americas','Americas')],
                      default='APJC')
    backlog_summary = BooleanField('Regional backlog dashboard')
    wnbu_compliance = BooleanField('WNBU PO Compliance check')
    config_check = BooleanField('ATO PO configuration check')
    running_option=SelectField('Option: ',
                             choices=[('formal','Formal - save record and email result to defined recipients'),
                                      ('test','Test - Not save record and only email to self')],
                             validators=[input_required()],
                             default='formal')
    submit = SubmitField('EXECUTE')

class Summary3a4Form(FlaskForm):
    file = FileField('Please upload 3A4 file (.csv):')
    cm_outlier = BooleanField('CM outlier summaries')
    cm_outlier_org = StringField('Org name:',default='FOC/FDO')
    dfpm_3a4 = BooleanField('DFPM 3a4 summaries')
    dfpm_3a4_option = SelectField('Option',
                                  choices=[('as_one','Create file as one'),('by_dfpm','Create separate file by DFPM')],
                                  default='by_dfpm')
    submit_3a4 = SubmitField('EXECUTE')

    # below for DFPM mapping of BU/PF
    dfpm = StringField('DFPM (required):')
    dfpm_org = StringField('Org (required):')
    bu_list = StringField("BU (optional - separate by '/'):")
    extra_pf = StringField("Include Extra PF (optional - separate by '/'):")
    exclusion_pf = StringField("Exclude PF (optional - separate by '/'):")
    submit_add_update = SubmitField('ADD/UPDATE')

class SubscriptionForm(FlaskForm):
    sub_backlog=BooleanField('Backlog summary dashboard')
    sub_wnbu_compliance = BooleanField('WNBU Compliance hold release report')
    sub_config = BooleanField('Config check result')
    backlog_apjc = BooleanField('APJC')
    backlog_emea = BooleanField('EMEA')
    backlog_americas = BooleanField('Americas')
    wnbu_compliance_apjc = BooleanField('APJC')
    wnbu_compliance_emea = BooleanField('EMEA')
    wnbu_compliance_americas = BooleanField('Americas')
    config_apjc = BooleanField('APJC')
    config_emea = BooleanField('EMEA')
    config_americas = BooleanField('Americas')
    backlog_ranking = BooleanField('Site backlog ranking report')
    backlog_ranking_org = StringField("Org name - multi-org sep by '/':")

    email_add_other=TextAreaField("Optional - email for others (multiple emails separate by ';')")
    submit_add = SubmitField('Subscribe')

    confirm_remove = BooleanField('Confirm removal!')
    submit_remove = SubmitField('Un-Subscribe')



class BacklogRankingForm(FlaskForm):
    file = FileField('Please upload 3A4 file (.csv):', validators=[input_required()])
    org = StringField('Org code:',validators=[input_required()])
    email_option = SelectField('Email option: ',
                               choices=[('to_all', 'Send to my group'), ('to_me', 'Send to ME only')],
                               validators=[input_required()],
                               default='to_all')
    submit = SubmitField('EXECUTE')

class TableDataForm(FlaskForm):
    # Read data from table and delete records
    table_name=SelectField('Select table:',
                           choices=[('',''),
                                    ('user_log','user_log'),
                                    ('error_log','error_log'),
                                    ],
                           default='user_log',
                            validators = [DataRequired()],
                           )
    password=PasswordField('Password: ',validators=[DataRequired()])
    criteria=TextAreaField('Searching criteria (optional): ')
    max_records=StringField('Max records to show (optional): ',default=5)
    show_last=BooleanField('Show the last records',default=True)
    submit_query=SubmitField('EXECUTE QUERY')
    submit_download=SubmitField('DOWNLOAD')
    id=TextAreaField('Input ID to delete(sep by , or ~)')
    submit_delete=SubmitField('DELETE ID')

class FileDownloadForm(FlaskForm):
    # download the 3a4 spreadsheet by DFPM id
    fname=StringField('Input file name to download:',validators=[DataRequired()])
    submit_download=SubmitField('DOWNLOAD')
    download_data=SelectField('Select data to download:',
                              choices=[('created_3a4','created_3a4'),
                                       ('uploaded_3a4','uploaded_3a4'),
                                       ('tracker','tracker')],
                              default='created_3a4',
                              validators=[DataRequired()])



class AdminForm(FlaskForm):
    file_name=StringField(validators=[DataRequired()])
    submit_delete=SubmitField('Delete')

class ConfigRules(FlaskForm):
    submit_download_pabu = SubmitField('Download rule file')
    file_pabu = FileField('Select rule file:')
    confirm_pabu = BooleanField('Confirm to replace with new file!')
    submit_upload_pabu = SubmitField('Upload rule file')

    submit_download_srgbu = SubmitField('Download rule file')
    file_srgbu = FileField('Select rule file:')
    confirm_srgbu = BooleanField('Confirm to replace with new file!')
    submit_upload_srgbu = SubmitField('Upload rule file')

    org_pid_rule=StringField('ORG*:',default='FOC;FDO')
    bu_pid_rule=StringField('BU:',default='SRGBU')
    pf_pid_rule=TextAreaField("PF*:",default='4200ISR;4300ISR')
    pid_a_pid_rule=StringField('PID_A*:',default='-AC;-DC')
    pid_b_pid_rule=StringField('PID_B:',default='XYZ;ABC')
    pid_c_pid_rule=StringField('PID_C:',default='CDE;WER')
    pid_a_exception_pid_rule = TextAreaField('PID_A_EXCEPTION:',default='sdfwr-AC;tewr-DC')
    pid_b_exception_pid_rule = TextAreaField('PID_B_EXCEPTION:',default='werw-XYZ;qwqw-ABC')
    pid_c_exception_pid_rule = TextAreaField('PID_C_EXCEPTION:',default='tutyu-CDE;erer-WER')
    remark_pid_rule=TextAreaField('Remark:')
    submit_pid_rule=SubmitField('Add rule')

    org_bupf_rule = StringField('ORG*:',default='FOC;FDO')
    bu_bupf_rule = StringField('BU:',default='SRGBU')
    pf_bupf_rule = TextAreaField("PF*:",default='4200ISR;4300ISR')
    exception_main_pid_bupf_rule = TextAreaField('EXCEPTION_MAIN_PID:',default='ENCS5406P/K9;ENCS5412P/K9')
    pid_a_bupf_rule = StringField('PID_A:',default='-AC;-DC')
    pid_b_bupf_rule = StringField('PID_B:',default='XYZ;ABC')
    remark_bupf_rule = TextAreaField('Remark:',default='Mandatory AC/DC PSU')
    submit_bupf_rule = SubmitField('Add rule')



# Database tables

class UserLog(db.Model):
    '''
    User logs db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    USER_NAME=db.Column(db.String(10))
    DATE=db.Column(db.Date)
    TIME=db.Column(db.String(8))
    LOCATION=db.Column(db.String(10))
    USER_ACTION=db.Column(db.String(20))
    SUMMARY=db.Column(db.Text)

class DfpmMapping(db.Model):
    '''
    Email setting db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    DFPM=db.Column(db.String(10))
    Org=db.Column(db.String(3))
    BU=db.Column(db.String(40))
    Extra_PF=db.Column(db.String(40))
    Exclusion_PF=db.Column(db.String(40))
    Added_by=db.Column(db.String(10))
    Added_on=db.Column(db.Date)

class Subscription(db.Model):
    '''
    Email setting db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    Email=db.Column(db.String(40))
    Subscription=db.Column(db.String(200))
    Added_by = db.Column(db.String(10))
    Added_on = db.Column(db.Date)

class GeneralConfigRulePid(db.Model):
    '''
    Email setting db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    ORG = db.Column(db.String(20))
    BU = db.Column(db.String(15))
    PF=db.Column(db.String(60))
    PID_A=db.Column(db.String(30))
    PID_B = db.Column(db.String(30))
    PID_C = db.Column(db.String(30))
    PID_A_EXCEPTION = db.Column(db.String(100))
    PID_B_EXCEPTION = db.Column(db.String(100))
    PID_C_EXCEPTION = db.Column(db.String(100))
    REMARK = db.Column(db.String(100))
    Added_by = db.Column(db.String(10))
    Added_on = db.Column(db.Date)

class GeneralConfigRuleBuPf(db.Model):
    '''
    Email setting db table
    '''
    id=db.Column(db.Integer,primary_key=True)
    ORG=db.Column(db.String(20))
    BU=db.Column(db.String(15))
    PF=db.Column(db.String(60))
    EXCEPTION_MAIN_PID =db.Column(db.String(100))
    PID_A = db.Column(db.String(30))
    PID_B = db.Column(db.String(30))
    REMARK = db.Column(db.String(100))
    Added_by = db.Column(db.String(10))
    Added_on = db.Column(db.Date)