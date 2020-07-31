from .models import BRDQuestions, Answers, CSRMappings, BRDLoadAttempts, TableCustomerId
import surveygizmo as sg
import json
import urllib.request
import mysql.connector
import os.path
import datetime

parent_path = '\\\\ssnj-isilon01\\astweb\\brd_buddy'

v_host = "ssnj-qadb03"
v_database ="ASTDEVDB_STACK1_01"
v_user = "qatester"
v_password = "Billtrust1"

# connect to CSR db
mydb = mysql.connector.connect(host=v_host, database=v_database, user=v_user, password=v_password)

def getSurveyData(surveyID, responseID):
    # Connect to Surveygizmo
    client = sg.SurveyGizmo(
    api_version='v5',

    # Authentication tokens
    api_token = "bf5834d3ce040aa545e2c982d586191db9512186675574c169",
    api_token_secret = "A9yZuGcXzmHTc"
    )

    try:
        survey = client.api.surveyresponse.get(int(surveyID),responseID)
        # Save the survey questions and answers
        survey_answers = survey["data"]["survey_data"]

        # Create a dictionary to store question id - answer pairs
        answers_to_map = dict()

        # iterate through the questions and map question id to answer
        for num, info in survey_answers.items():
            if info["shown"] == True:
                if 'answer' in info:
                    answers_to_map.update({info['id'] : info['answer']}) 
                if 'subquestions' in info:
                    temp_dict = info['subquestions']
                    for k, v in temp_dict.items():
                        if 'answer' in v:
                            answers_to_map.update({k : v['answer']})    

        answers_to_map.pop(279)
    except:
        return {}
    return answers_to_map

    
def mapping(ans_dict, resp_id): 

    # status vars
    status = 'FAILED'
    query_count = 0

    # save customer id, pcase number and username of the load attempt to use when framing update and audit statements
    cust_id = BRDLoadAttempts.objects.filter(response_id=resp_id).values('customer_id')[0]['customer_id']
    username = BRDLoadAttempts.objects.filter(response_id=resp_id).values('username')[0]['username']
    pcase_num = BRDLoadAttempts.objects.filter(response_id=resp_id).values('pcase_num')[0]['pcase_num']

    # create directory to hold output files
    path = os.path.join(parent_path, f'{pcase_num}') 
    if not os.path.isdir(path):
        os.mkdir(path)
    

    # create files to write DLM, audits and manual entries to
    dml = os.path.join(path, f"{pcase_num}_dml.txt")
    audit = os.path.join(path, f"{pcase_num}_audit.txt")
    manual = os.path.join(path, f"{pcase_num}_manual.txt")

    dml_file = open(dml, 'w')
    audit_file = open(audit, 'w')
    manual_file = open(manual, 'w')

    if mydb.is_connected():
        db_Info = mydb.get_server_info()
        cursor = mydb.cursor()

        # loop through sg dict
        for sg_id, answer in ans_dict.items():

            # if the sg question exists in the mapping table
            if BRDQuestions.objects.filter(surveygizmo_id=sg_id).count() != 0:

                # get the questions id
                q_id = BRDQuestions.objects.filter(surveygizmo_id=sg_id).values('id')[0]['id']

                # get the mapping and answer data
                mapping = CSRMappings.objects.filter(map_parents=q_id).values()[0]
                answer = Answers.objects.filter(ans_parent=q_id).values()[0]

                # save the relevant info
                table = mapping['table_ref']
                col = mapping['col_name']
                csr_setting = mapping['csr_setting']
                new_value = answer['csr_value']

                # set of logic that will be used if fsicustomersettings does not need to be updated
                if 'fsiatom' not in table:
                    cust_id_query = TableCustomerId.objects.filter(table_ref=table).values()[0]
                    cust_identifier = cust_id_query['cust_id_name']
                    
                    # if the answer was manually entered, make sure that text is used in the query
                    if 'String' in new_value or 'Numeric' in new_value:
                        new_value = ans_dict[sg_id]

                    if 'list' in new_value or 'provide' in new_value:
                        new_value = 'Y'
                    
                    # fetching the old value
                    query =f'SELECT {col} FROM {table} WHERE {cust_identifier} = {cust_id}' 
                
                    cursor = mydb.cursor()
                    cursor.execute(query)
                    records = cursor.fetchall()

                    # if the old and new value are different then generate the UDPATE and audit statement
                    for row in records:
                        old_val = row[0]
                        if not isinstance(old_val, str):
                            old_val = str(old_val)
                        if old_val != new_value:
                            dml_file.write(f'UPDATE {table} SET {col} = \'{new_value}\', UpdateUser = {pcase_num} WHERE {cust_identifier} = {cust_id};' + '\n')
                            query_count = query_count + 1
                            audit_statement = f"\"{username} changed {csr_setting} from \'{old_val}\' to \'{new_value}\'\" "
                            audit_update = f"INSERT INTO fsicsraudits (EventDate, EventUser, IPAddress, CustomerId, Description) VALUE({datetime.datetime.now()}, brdbuddy, http://ssnj-devast01/, {cust_id}, {audit_statement})"
                            audit_file.write(audit_update + '\n')
                                
                # if fsiatom is referenced in the column there is some extra logic that needs to be used to generate the UPDATE statement
                else:
                    update_query, old_valueAtom, new_valueAtom = fsiatom(table, col, csr_setting, new_value, cust_id)
                    audit_statement = f"\"{username} changed {csr_setting} from \'{old_valueAtom}\' to \'{new_valueAtom}\'\" "
                    audit_update = f"INSERT INTO fsicsraudits (EventDate, EventUser, IPAddress, CustomerId, Description) VALUE({datetime.datetime.now()}, brdbuddy, http://ssnj-devast01/, {cust_id}, {audit_statement})"
                    dml_file.write(update_query + '\n')
                    audit_file.write(audit_update + '\n')

            # if the mapping for the question does not exist, the Surveygizmo question id will be written the to manual entry file
            else:
                manual_file.write("Survey Question " + str(sg_id) + " is missing a mapping!" + '\n')

    # close the files
    dml_file.close()
    audit_file.close()
    manual_file.close()

    # calculating the status
    if query_count == 0:
        status = 'FAILED'
    else:
        status = 'PARTIAL'
    return status, pcase_num

# separate logic for mappings that use fsiatom
def fsiatom (table, col, setting, val, cust_id):
    # the column and value will have an ID called the AtomId
    setting_query = f'(SELECT AtomId FROM fsiatom WHERE AtomValue = \'{col}\')'
    value_query = f'(SELECT AtomId FROM fsiatom WHERE AtomValue = \'{val}\' LIMIT 1)'
    settingAtom = 0
    valueAtom = 0
    
    cursor = mydb.cursor()

    # get the setting AtomId
    cursor.execute(setting_query)
    settings_records = cursor.fetchall()
    for row in settings_records:
        settingAtom = row[0]
    
    # get the setting AtomId
    cursor.execute(value_query)
    values_records = cursor.fetchall()
    for row in values_records:
        valueAtom = row[0]

    # query to get the old ValueAtom
    audit_query = f'SELECT ValueAtom FROM fsicustomersettings WHERE SettingsAtom = {settingAtom} AND CustomerId = {cust_id}' 
    old_valueAtom = 0

    cursor.execute(audit_query)
    old_values_records = cursor.fetchall()

    # if the ValueAtom does not exist the row in fsicustomersettings does not exist either so we need to insert that!
    if not old_values_records:
        pathAtom_query = f'SELECT DISTINCT PathAtom FROM fsicustomersettings WHERE SettingsAtom = {settingAtom};'

        for row in values_records:
            pathAtom = row[0]

        return f'INSERT INTO fsicustomersettings (SettingsStatus, CustomerId, SettingsAtom, ValueAtom, PathAtom) VALUES (5, {cust_id}, {settingAtom}, {valueAtom}, {pathAtom});', '', valueAtom

    # if the ValueAtom does exist, we can go ahead and generate the UPDATE statment
    else:
        for row in values_records:
            old_valueAtom = row[0]

        return f'UPDATE fsicustomersettings SET ValueAtom = {valueAtom} WHERE SettingsAtom = {settingAtom} AND CustomerId = {cust_id};', old_valueAtom, valueAtom
        



