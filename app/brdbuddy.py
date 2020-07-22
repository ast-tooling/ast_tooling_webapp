from .models import BRDQuestions, Answers, CSRMappings, BRDLoadAttempts, TableCustomerId
import surveygizmo as sg
import json
import urllib.request
import mysql.connector
import os.path

v_host = "ssnj-qadb03"
v_database ="ASTDEVDB_STACK1_01"
v_user = "qatester"
v_password = "Billtrust1"

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

    
def mapping(ans_dict, resp_id, survey_id): 

    # status vars
    status = 'FAILED'
    query_count = 0

    # save customer id, pcase number and username of the load attempt to use when framing update and audit statements
    cust_id = BRDLoadAttempts.objects.filter(response_id=resp_id).values('customer_id')[0]['customer_id']
    pcase_num = BRDLoadAttempts.objects.filter(response_id=resp_id).values('pcase_num')[0]['pcase_num']
    username = BRDLoadAttempts.objects.filter(response_id=resp_id).values('username')[0]['username']

    try:
        # create files to write DLM, audits and manual entries to
        dml = os.path.join(f'Z:\IT Documents\QA\{pcase_num}', f"{pcase_num}_dml.txt")
        audit = os.path.join(f'Z:\IT Documents\QA\{pcase_num}', f"{pcase_num}_audit.txt")
        manual = os.path.join(f'Z:\IT Documents\QA\{pcase_num}', f"{pcase_num}_manual.txt")

        dml_file = open(dml, 'w')
        audit_file = open(audit, 'w')
        manual_file = open(manual, 'w')

        # connect to CSR db
        mydb = mysql.connector.connect(host=v_host, database=v_database, user=v_user, password=v_password)

        if mydb.is_connected():
            db_Info = mydb.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = mydb.cursor()

            count = 1
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

                    # UPDATE THESE TO THE CORRECT VALUES
                    audit_table = 'csr_data'
                    audit_col = 'audit_history'

                    if table is not None and 'fsiatom' not in table:
                        cust_id_query = TableCustomerId.objects.filter(table_ref=table).values()[0]
                        cust_identifier = cust_id_query['cust_id_name']
                        
                        # if the answer was manually entered, make sure that text is used in the query
                        if 'String' in new_value or 'Numeric' in new_value:
                            new_value = ans_dict[sg_id]

                        if 'list' in new_value or 'provide' in new_value:
                            new_value = 'Y'
                        
                        query = "SELECT " + col + " FROM " + table + " WHERE " + cust_identifier + " = " + str(cust_id)
                    
                        cursor = mydb.cursor()
                        cursor.execute(query)
                        records = cursor.fetchall()

                        for row in records:
                            old_val = row[0]
                            if not isinstance(old_val, str):
                                old_val = str(old_val)
                            if old_val != new_value:
                                dml_file.write("UDPATE " + table + " SET " + col + " = " + "\'" + new_value + "\'" + ", " + "UpdateUser " + " = " + str(pcase_num) + " WHERE " + cust_identifier + " = " + str(cust_id) + ";" + '\n')
                                query_count = query_count + 1
                                audit_statement = f"\"{username} changed {csr_setting} from \'{old_val}\' to \'{new_value}\'\" "
                                audit_update = f"INSERT INTO {audit_table} ({audit_col}) VALUE({audit_statement})"
                                audit_file.write(audit_update + '\n')
                                    
                    else:
                        # add logic for custom table logic 
                        manual_file.write("check the mapping for question " + str(sg_id) + '\n')

                else:
                    manual_file.write("Survey Question " + str(sg_id) + " is missing a mapping!" + '\n')
    except:
        return "Invalid P-case number!"

    # close the files
    dml_file.close()
    audit_file.close()
    manual_file.close()

    # calculating the status
    if query_count == 0:
        status = 'FAILED'
    else:
        status = 'PARTIAL'
    return status

