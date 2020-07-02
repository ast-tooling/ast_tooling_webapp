from .models import BRDQuestions, Answers, CSRMappings, BRDLoadAttempts, BRDLoadInfo
import surveygizmo as sg
import json
import urllib.request

update = "UPDATE "
set_text = " SET "
where = " WHERE customerID = "
values = " VALUES "

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

    
def mapping(ans_dict, resp_id): # def mapping(ans_dict, resp_id, survery_id)

    # variables for status 
    status = 'FAILED'
    query_count = 0

    # open the files where the queries and failed answers will be stored
    f = open("queries.txt", "w")
    m = open("manual.txt", "w")

    load_attempt_id = BRDLoadAttempts.objects.filter(response_id=resp_id).values('customer_id')[0]['customer_id']
    pcase_num = BRDLoadAttempts.objects.filter(response_id=resp_id).values('pcase_num')[0]['pcase_num']
    username = BRDLoadAttempts.objects.filter(response_id=resp_id).values('username')[0]['username']

    f.write("pcase_num: " + str(pcase_num) + '\n')
    f.write("username: " + username + '\n')
    m.write("username: " + username + '\n')
    m.write("pcase_num: " + str(pcase_num) + '\n')
    
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
            value = answer['csr_value']

            # if the answer was manually entered, make sure that text is used in the query
            if 'String' in value or 'Numeric' in value:
                value = ans_dict[sg_id]

            # frame the UPDATE statement
            f.write(update + table + set_text + col + " = " + "\'" + value + "\'" + where + str(load_attempt_id) + ";" + '\n')
            # update the number of queries generated
            query_count + 1
        
        # if the sg question does not exist in the mapping table, write the id and answer to another file which the BAs can then use to manually enter the settings
        else:
            m.write(str(sg_id) + " : " + str(answer) + '\n')
            # m.write( "FAIL" + '\n')
    
    # close the files
    f.close()
    m.close()

    # calculating the status
    if len(ans_dict) == query_count:
        status = 'SUCCESS'
    else:
        status = 'PARTIAL'
    
    return status

