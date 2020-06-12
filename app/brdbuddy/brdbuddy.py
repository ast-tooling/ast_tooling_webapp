import surveygizmo as sg
import json

def getSurveyData(responseID):
    # Connect to Surveygizmo
    client = sg.SurveyGizmo(
    api_version='v5',

    # Authentication tokens
    api_token = "bf5834d3ce040aa545e2c982d586191db9512186675574c169",
    api_token_secret = "A9yZuGcXzmHTc"
    )
    print ("Got here")
    # Get and save the survey (pass in survey id and response id)
    survey = client.api.surveyresponse.get('4623162',responseID)

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

    return answers_to_map

# def mappingEngine(ans_dict):
    # for sg_id, answer in ans_dict.items():
    #     if the key exists in BRDQuestions
    #         insert into BRDLoadInfo
    #         find the csr tab/setting/val as well as the table and col 
    #         insert these
    #         mapping status set to TRUE
    #     else
    #         insert into BRDLoadInfo
    #         mapping status set to FALSE
