from flask_setting import *

def delete_record(table_name,id_list):
    '''
    delete a list of records from a defined table
    '''
    # first change the table name to table class
    table_name=table_name.split('_')
    table_name=[x.title() for x in table_name]
    table_name=''.join(table_name)
    table_class=eval(table_name)

    for id in id_list:
        record=table_class.query.get(id)

        db.session.delete(record)
        db.session.commit()

    print('records deleted')


if __name__=='__main__':
    #table_name='dfpm_tool_addressable_backlog'
    #id_list=range(1,29965)
    delete_record(table_name,id_list)
    #pass
