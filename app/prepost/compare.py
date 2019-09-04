from __future__ import print_function,unicode_literals
from pprint import pprint
import pickle
import pandas as pd
import os.path
import time
from functools import wraps
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient import discovery
import pymongo
from pymongo import MongoClient
from pymongo import MongoReplicaSetClient
from pprint import pprint
import gspread_dataframe as gd
# User Info
import os
import json
from itertools import tee
from collections import OrderedDict
import mysql.connector
from base64 import b64decode
# import pyodbc
from memory_profiler import profile
from . import sheet_requests

def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        timerList.append("Total time running %s: %s seconds" %
               (function.__name__, str(t1-t0))
               )
        return result
    return function_timer

@fn_timer
def InitSQLClient(dStack={},master=False):
    userName = os.getenv('username')
    sqlConnFile = r"C:\\Users\\%s\\AppData\\Roaming\\SQLyog\\sqlyog.ini" % userName
    inFile = open(sqlConnFile, 'rt')
    foundConnection = False
    connections = {}
    for line in inFile.readlines():
        if "[Connection" in line:
            foundConnection = True
            currentConnection = line.strip()
            connections[currentConnection] = {}
        if foundConnection:
            if line.startswith("Host="):
                connections[currentConnection]["Host"] = line.split("=")[1].strip()
            elif line.startswith("User="):
                connections[currentConnection]["User"] = line.split("=")[1].strip()
            elif line.startswith("Password="):
                connections[currentConnection]["Password"] = line.split("=")[1].strip()
    inFile.close()
    userName = ""
    password = ""
    for connection in connections.values():
        if "Host" in connection:
            if connection["Host"].upper() == "IMDB":
                if "User" in connection:
                    userName = connection["User"]
                if "Password" in connection:
                    password = connection["Password"]
    #print(userName, password)

    # SQLyog stores passwords with base 64 encoding so we must decode it
    decodedPassword = decode_password(password)
    # TODO why are we connecting to both stage & prod, should it be a flag?
    # TODO need to figure out a why to have users connect through webserver,
    # probably be generic name
    if master:
        imdb_mysqlClient = mysql.connector.connect(
            host="imdb",
            user=userName,
            passwd=decodedPassword,
            database="billingmaster"
        )
        reportdb_mysqlClient = mysql.connector.connect(
            host="reportdb",
            user=userName,
            passwd=decodedPassword,
            database="billingmaster"
        )
    else:
        imdb_mysqlClient = mysql.connector.connect(
            host="imdb",
            user=userName,
            passwd=decodedPassword,
            database=dStack['imdb']
        )
        reportdb_mysqlClient = mysql.connector.connect(
            host="reportdb",
            user=userName,
            passwd=decodedPassword,
            database=dStack['reportdb']
        )
    mysqlClient =  {"imdb"      : imdb_mysqlClient,
                    "reportdb"  : reportdb_mysqlClient}
    return mysqlClient

@fn_timer
def InitMongoClient():
    ###############################
    # START: GET USER CREDENTIALS #
    userName = os.getenv('username')
    userPassword = ''
    roboPath = "C:\\Users\\%s\\.3T\\robo-3t\\1.3.1\\robo3t.json" % userName

    print ("Validating user credentials...")
    try:
        with open(roboPath) as json_file:
            connectionData = json.load(json_file)
            for connection in connectionData['connections']:
                # AS_061719: update below line to check for different hosts
                if connection['serverHost'].split('.')[0] in ("ssnj-immongodb01","ssnj-immongodb02","ssnj-immongodb03"):
                    for cred in connection['credentials']:
                        userName = cred['userName']
                        userPassword = cred['userPassword']
    except:
        userName = "cweakley"
        userPassword = "vn6oCdWK"
    # END: GET USER CREDENTIALS #
    #############################

    ##################################
    # START: CONNECT TO MONGO CLIENT #
    print ("Connecting to mongo client...")
    imdb_MongoClient = MongoClient(["ssnj-immongodb01:10001", "ssnj-immongodb02:10001", "ssnj-immongodb03:10001"],
                                            userName = userName,
                                            password = userPassword,
                                            authSource = 'docpropsdb',
                                            authMechanism = 'SCRAM-SHA-1')
    imdb_fsidocprops = imdb_MongoClient.docpropsdb.fsidocprops

    reportdb_MongoClient = MongoClient(["prmreportdb01:10001"],
                                            userName = userName,
                                            password = 'gEvSnvCy',
                                            authSource = 'docpropsdb',
                                            authMechanism = 'SCRAM-SHA-1')
    reportdb_fsidocprops = reportdb_MongoClient.docpropsdb.fsidocprops

    fsidocprops =  {"imdb"      : imdb_fsidocprops,
                    "reportdb"  : reportdb_fsidocprops}

    return fsidocprops

    # END: CONNECT TO MONGO CLIENT #
    ################################

    ##################################
    #  START: CONNECT TO SQL SERVER  #

@fn_timer
def InitSqlServerConn(server='dnco-stc2bsql.billtrust.local',database='carixDataProcessing',trusted_conn_bool='yes'):
    print('Connecting using windows auth...')
    conn = pyodbc.connect(driver='{SQL Server}',
                          server=server,
                          database=database)
    cursor = conn.cursor()
    return cursor
    print('Connected, cursor object returned...')
    #   END: CONNECT TO SQL SERVER   #
    ##################################
    
@fn_timer
def decode_password(encoded):
    # print('encoded password is %s' % encoded)
    # TODO, update '==' to check length of encoded var; should be multiple of 4
    # see https://gist.github.com/perrygeo/ee7c65bb1541ff6ac770
    if len(encoded) % 4 != 0:
        if len(encoded) % 4 == 2:
            encoded += '==='
        elif len(encoded) % 4 == 1:
            encoded += '=='
        elif len(encoded) % 4 == 3:
            encoded += '='
    tmp = bytearray(b64decode(encoded))
    for i in range(len(tmp)):
        tmp[i] = rotate_left(tmp[i], 8)
    return tmp.decode('utf-8')

def rotate_left(num, bits):
    bit = num & (1 << (bits-1))
    num <<= 1
    if(bit):
        num |= 1
    num &= (2**bits-1)
    return num

@fn_timer
def GetCoversheetDocIds(mysqlClient, arguments):
    # We want to ignore any document that was created as a coversheet
    coversheetDocIds = {}
    preOrPost = "prechange"
    for args in ((arguments['preId'], arguments['preEnv']), (arguments['postId'], arguments['postEnv'])):
        mysqlCursor = mysqlClient[args[1]].cursor()
        mysqlCursor.execute("SELECT documentId FROM fsidocument fsidoc WHERE customerid = %s AND batchid = %s \
                          AND (fsidoc.FFDId IN (SELECT FFDId FROM fsiFFD WHERE customerId = %s AND itemType = 'O') \
                          OR FFDId = 88908 OR fsidoc.OriginalBatchId IS NOT NULL)" % (arguments['custId'], args[0], arguments['custId']))
        # convert from tuple generator of Longs to Int list
        # TODO - would it be better to pass the cursor around until we need to access the results to prevent memory limits?
        coversheetDocIds[preOrPost] = list(int(i[0]) for i in mysqlCursor.fetchall())
        preOrPost = "postchange"   
    return (coversheetDocIds)

@fn_timer
def GetFSIDocumnetInfo(mysqlClient, arguments):

    destTypes = {"C" : "Coversheet",
                 "D" : "Print and Ebill",
                 "E" : "Ebill",
                 "I" : "Invoice Central",
                 "G" : "OB 10",
                 "P" : "Pull - P",
                 "Q" : "Pull - Q",
                 "R" : "Pull - R",
                 "S" : "Print",
                 "T" : "Pull - T",
                 "V" : "Email",
                 "X" : "Fax"}

    fsiDocumentInfo = {}
    preOrPost = "prechange"

    for args in ((arguments['preId'], arguments['preEnv']), (arguments['postId'], arguments['postEnv'])):

        mysqlCursor = mysqlClient[args[1]].cursor()
        mysqlCursor.execute("SELECT documentId, FFDId, DestType, PageCount FROM fsidocument WHERE customerid = %s AND batchid = %s \
                          AND (FFDId NOT IN (SELECT FFDId FROM fsiFFD WHERE customerId = %s AND itemType = 'O') \
                          AND FFDId != 88908)" % (arguments['custId'], args[0], arguments['custId']))

        batchInfo = {}
        # TODO - would it be better to pass the cursor around until we need to access the results to prevent memory limits?
        for document in mysqlCursor.fetchall():
            batchInfo[str(document[0])] = { "FFDID"    : str(document[1]),
                                            "BT_ROUTE" : destTypes[str(document[2])],
                                            "PAGECOUNT": str(document[3])}
        if batchInfo == {}:
            print("Did not find any record in fsidocument for customerId: %s, batchId: %s" % (arguments['custId'], arguments[0]))
            sys.exit()

        fsiDocumentInfo[preOrPost] = batchInfo
        preOrPost = "postchange"

    return fsiDocumentInfo

@fn_timer
def GoogleAPIAuthorization():
    # add 5 vars below, updaed in func as needed
    base = os.path.abspath(os.path.dirname(__file__))
    token_fname = 'token.pickle'
    creds_fname = 'credentials.json'
    token_path = os.path.join(base,token_fname)
    creds_path = os.path.join(base,creds_fname)
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            print('token file found at %s' % token_path)
            creds = pickle.load(token, encoding='latin1')
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                creds_path, SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(os.path.join(base,'token.pickle'), 'wb') as token:
            pickle.dump(creds, token)
    service = discovery.build('sheets', 'v4', credentials=creds)
    return service

# Write a single range of values out
@fn_timer
def UpdateSingleRange(service, values, startPos, sheetName, spreadsheetId, printData=False, value_input_option="RAW", insertDataOption="OVERWRITE"):
    #value_input_option = "RAW" #input raw string data, no formulas, dates, currency, ect.
    startPos = sheetName + '!' + startPos
    print("Updating spreadsheet id: %s" % spreadsheetId)
    print("Starting at cell: %s" % startPos)
    # input paramater 'values' holds a list of row data, lets update 1000 rows at a time (only use even numbers)
    rowCount = len(values)
    rowsPerUpdate = 5000
    for i in range(0, rowCount, rowsPerUpdate):
        if i + rowsPerUpdate > rowCount: # add remaining orphan updates
            print("Updating rows %s through %s" % (str(i), str(rowCount)))
            body = {'values': values[i:rowCount]}
        else: # add batch updates
            print("Updating rows %s through %s" % (str(i), str(i + rowsPerUpdate)))
            if printData:
                print(values)
            body = {'values': values[i:i + rowsPerUpdate]}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheetId, range=startPos,
            valueInputOption=value_input_option, body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))
        # update starting position
        startPos = startPos.split("!")[1]
        startCol = list(filter(str.isalpha, str(startPos)))[0]
        startRow = int(''.join(list(filter(str.isdigit, str(startPos))))) + rowsPerUpdate
        startPos = "%s!%s%s" % (sheetName, startCol, startRow)

@fn_timer
def SendUpdateRequests(service, requests, spreadsheetId):
    body = {'requests': requests}
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId,body=body).execute()
    return response

def AddChangedCellLink(service, changedDocProps, sheetId, sheetName, arguments):
    temp = arguments['spreadsheetURL'][:arguments['spreadsheetURL'].rfind("edit#gid=")]
    url = temp + "edit#gid=" + str(sheetId)
    for changedProp in changedDocProps:
        if changedProp not in ("", "(none)"):
            firstChangeA1 = GetA1Notation(changedDocProps[changedProp]["column"], changedDocProps[changedProp]["row"])
            colLabelA1 = GetA1Notation(changedDocProps[changedProp]["column"], 0)
            value = "=HYPERLINK(\"%s&range=%s\", \"%s\")" % (url, firstChangeA1, "Find Change")
            UpdateSingleRange(service, [[value,]], colLabelA1, sheetName, arguments['spreadsheetId'], value_input_option="USER_ENTERED")    


def GetA1Notation(columnIndex, rowIndex):
    quot, rem = divmod(columnIndex, 26)
    return((chr(quot-1 + ord('A')) if quot else '') +
           (chr(rem + ord('A')) + str(rowIndex+1)))    


@fn_timer
def QueryMongo(fsidocprops, coversheetDocIds, arguments):

    print("Querying for prechange and postchange doc props...")

    prechangePropsGen = fsidocprops[arguments['preEnv']].find({'batchId': arguments['preId'], 'customerId': arguments['custId'], 'documentId': {'$nin': coversheetDocIds['prechange']}},
        {'_id':0, 'batchId':0, 'customerId':0, 'size':0, 'seq':0, 'lockId':0})
    postchangePropsGen = fsidocprops[arguments['postEnv']].find({'batchId': arguments['postId'], 'customerId': arguments['custId'], 'documentId': {'$nin': coversheetDocIds['postchange']}},
        {'_id':0, 'batchId':0, 'customerId':0, 'size':0, 'seq':0, 'lockId':0})

    print("Query finished... CustomerId: %s  Prechange: %s  Postchange: %s" % (arguments['custId'], arguments['preId'], arguments['postId']))
    return(prechangePropsGen, postchangePropsGen)

@fn_timer
def MergeBatchData(prechangeProps, postchangeProps, fsiDocumentInfo, arguments):
    print("Starting MergerBatchData...")
    fsiDocumentProps = ["FFDID", "BT_ROUTE", "PAGECOUNT"]
    docPropLabels = ["FFDID", "BT_ROUTE", "PAGECOUNT"]

    prechangeProps = list(prechangeProps)
    postchangeProps = list(postchangeProps)    

    # Add all doc props from our prechange and postchange batches to a list of doc prop names
    for batch in (prechangeProps, postchangeProps):
        for document in batch:
            for prop in document.get('properties'):
                docPropName = prop.get('k')
                if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                    if not docPropName.endswith("_COL") and docPropName not in arguments['ignoredProps']:
                        if docPropName not in docPropLabels:
                            docPropLabels.append(str(docPropName))

    # Sort the labels and add DOCUMENTID, ACCOUNT_NUMBER and INVOICE_NUMBER to the front\
    docPropLabels.sort()
    props = ["BT_ROUTE", "INVOICE_NUMBER", "ACCOUNT_NUMBER"]
    #props = ["ACCOUNT_NUMBER", "TOTAL_DUE"]
    for prop in props:
        if prop in docPropLabels:
            docPropLabels.remove(prop)
            docPropLabels.insert(0, prop)
    docPropLabels.insert(0, "")
    docPropLabels.insert(0, "DOCUMENTID")

    # masterPropList will contain our final structure of pre and post doc props with a masterKey
    # masterKey is currently ''.join(ACCOUNT_NUMBER INVOICE_NUMBER) but should optionally be user defined
    # {ACCOUNT_NUMBERINVOICE_NUMBER: [prechangePropValue, postchangePropValue], [prechangePropValue, postchangePropValue], ...}
    masterPropList = OrderedDict()
    # START PRECHANGE PROPS
    # Doc props can be split across multiple mongo Objects, this means documentId cannot be used as a unique identifier
    # Here we remove and combine these split db objects and add them back into our original list
    # See Example: db.getCollection('fsidocprops').find({"customerId":2001, "batchId":13811669, "documentId":4315315279})
    for batch in (prechangeProps, postchangeProps):
        splitObjects = {}
        removeThese = []
        for i, document in enumerate(batch):
            if document.get('pages') > 1:
                docId = document.get('documentId')
                removeThese.append(i)
                if docId not in splitObjects:
                    splitObjects[docId] = document
                else:
                    splitObjects[docId].get('properties').extend(document.get('properties'))
        for index in sorted(removeThese, reverse = True):
            batch.pop(index)
        for docId in splitObjects.values():
            batch.extend([docId])

    count = 0
    # These are the doc props that will be used as a unique key to match up pre/post documents
    # In the future this should be defaulted to acc num and inv num with the option for user override
    #propKeys = ["ACCOUNT_NUMBER", "INVOICE_NUMBER"]  "PO_NUMBER", "BT_ROUTE", "TOTAL_DUE"
    #propKeys = ["ACCOUNT_NUMBER", "INVOICE_NUMBER", "TOTAL_DUE", "FFDID"]
    for document in prechangeProps:
        # Get master key before starting
        masterKey = []
        for prop in arguments['masterKeyProps']:
            if prop in fsiDocumentProps:
                masterKey.append(fsiDocumentInfo['prechange'][str(document.get('documentId'))].get(prop))
            else:
                for docProp in document.get('properties'):
                    if prop == docProp.get('k'):
                        if prop == "FILENAME":
                            masterKey.append(docProp.get('v').split("\\")[-1])
                        else:
                            masterKey.append(docProp.get('v'))
        #print(masterKey)
        masterKey = '~'.join(masterKey)
        count += 1

        #print(masterKey)
        #sys.exit()


        # Exit if we were not able to find either account number or invoice number
        if masterKey == '':
            print("Not able to find any doc prop keys in the prechange batch")
            print(str(count))
            sys.exit()
        if False:
            if masterKey in masterPropList:
                print("Found a duplicate masterkey within the prechange batch: ", masterKey)
                print("Master key components:", '~'.join(arguments['masterKeyProps']))
                sys.exit()

        #print (docProps)
        for docPropLabel in docPropLabels:
            if docPropLabel == "DOCUMENTID":
                masterPropList[masterKey] = [[str(document.get('documentId')), '']]
            elif docPropLabel in fsiDocumentProps:
                #print(fsiDocumentInfo['prechange'][str(document.get('documentId'))][docPropLabel])
                masterPropList[masterKey].append([fsiDocumentInfo['prechange'][str(document.get('documentId'))][docPropLabel], ''])
            else:
                tempPropValues = ['', '']
                if docPropLabel != "":
                    for prop in document.get('properties'):
                        propName = prop.get('k')
                        if propName == docPropLabel:
                            if propName == "FILENAME":
                                tempPropValues[0] = prop.get('v').replace('<BR>', '\n')[:5000].split("\\")[-1]
                            else:
                                tempPropValues[0] = prop.get('v').replace('<BR>', '\n')[:5000] #google sheets limits cell data to 5000 chars
                            break
                masterPropList[masterKey].append(tempPropValues)
        # END PRECHANGE PROPS

    # START POSTCHANGE PROPS
    misMatchCount = 0
    for document in postchangeProps:
        misMatch = False
        # Get master key before starting
        masterKey = []
        for prop in arguments['masterKeyProps']:
            if prop in fsiDocumentProps:
                masterKey.append(fsiDocumentInfo['postchange'][str(document.get('documentId'))].get(prop))
            else:
                for docProp in document.get('properties'):
                    if prop == docProp.get('k'):
                        if prop == "FILENAME":
                            masterKey.append(docProp.get('v').split("\\")[-1])
                        else:
                            masterKey.append(docProp.get('v'))
        masterKey = '~'.join(masterKey)

        # Exit if we were not able to find either account number or invoice number
        if masterKey == '':
            print("Not able to find either account number or invoice number in the postchange batch")
            sys.exit()
        elif not masterKey in masterPropList:
            print("Postchange masterkey not found in prechange masterkeylist, adding mismatched key.")
            print(masterKey)
            misMatchCount += 1
            masterPropList[masterKey] = []
            noMatch = 'NO MATCH'
            for label in docPropLabels:
                masterPropList[masterKey].append([noMatch,''])
                noMatch = ''
            misMatch = True

        for i, docPropLabel in enumerate(docPropLabels):
            if docPropLabel == "DOCUMENTID":
                masterPropList[masterKey][i][1] = str(document.get('documentId'))
            elif docPropLabel in fsiDocumentProps:
                masterPropList[masterKey][i][1] = fsiDocumentInfo['postchange'][str(document.get('documentId'))][docPropLabel]
            elif docPropLabel != "":
                for prop in document.get('properties'):
                    propName = prop.get('k')
                    if propName == docPropLabel:
                        if propName == "FILENAME":
                            masterPropList[masterKey][i][1] = prop.get('v').replace('<BR>', '\n')[:5000].split("\\")[-1]
                        else:
                            masterPropList[masterKey][i][1] = prop.get('v').replace('<BR>', '\n')[:5000] #google sheets limits cell data to 5000 chars
                        break
        if True:
            if misMatchCount > ((len(prechangeProps) + len(postchangeProps)) / 4) \
              or misMatchCount > len(prechangeProps) * .75 \
              or misMatchCount > len(postchangeProps) * .75:
                print("ERROR: More than half of the total document count are mismatched, or more than 75% of either the pre or post change documents " \
                      "are mismatched, check prechange and postchange batch ids.")
                sys.exit()
    print(misMatchCount, len(prechangeProps), len(postchangeProps))
    #print("Time Elapsed: %s" % (time.time() - startTime))

    return(docPropLabels, masterPropList, misMatchCount, len(prechangeProps), len(postchangeProps))

@fn_timer
def MergeToDataFrame(prechangePropsGen, postchangePropsGen, fsiDocumentInfo, arguments, service):
    print("Starting MergeToDataFrame...")
    fsiDocumentProps = ["FFDID", "BT_ROUTE", "PAGECOUNT"]
    docPropLabels = ["FFDID", "BT_ROUTE", "PAGECOUNT"]

    prechangeProps = {}
    postchangeProps = {}
    batchInfo = {"preBatchCount"    : 0,
                 "postBatchCount"   : 0,
                 "misMatchCount"    : 0,
                 "fileList"         : set(),}
    fileList = {}
    # Add all doc props from our prechange and postchange batches to a list of doc prop names
    preOrPost = "prechange"
    batchCount = "preBatchCount"
    for batch in ((prechangePropsGen, prechangeProps), (postchangePropsGen, postchangeProps)):
        # Doc props can be split across multiple mongo Objects, this means documentId cannot be used as a unique identifier
        # Here we remove and later combine these split db objects
        # See Example: db.getCollection('fsidocprops').find({"customerId":2001, "batchId":13811669, "documentId":4315315279})
        splitObjects = {} # keep track of documents that are split across multiplie mongo objects
        for document in batch[0]:
            isSplitObject = False
            bFoundFileName = False
            docProps = {} # used to temp store our docProp label and values
            docId = str(document.get('documentId'))
            docProps['DOCUMENTID'] = docId

            # Keep track of how many documents are in each batch
            batchInfo[batchCount] += 1

            for prop in document.get('properties'):
                docPropName = prop.get('k').strip()
                #print(docPropName)
                if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                    if "s" in prop: # Once we find the first columnar property we can break
                        #print("Found first col prop, breaking properties loop at: %s, Line: %s" % (docPropName, prop.get('s')))
                        break
                    elif "vref" in prop: #vref props are too large to store in regular mongo documents so we skip them
                        pass    
                    elif docPropName not in arguments['ignoredProps']:
                        if not bFoundFileName:  # ASK ABOUT PERFORMANCE OF THIS TYPE OF IF STRUCTURE
                            if docPropName == "FILENAME":
                                docProps[docPropName] = prop.get('v').split("\\")[-1]  # only grab the file name and not the full path
                                batchInfo["fileList"].add(docProps[docPropName])
                                bFoundFileName = True
                            else:
                                #try:
                                docProps[docPropName] = prop.get('v').replace('<BR>', '\n')[:5000]
                                #except:
                                    # There can be a null value for 'v' so we need to catch the error
                                    # ex: db.getCollection('fsidocprops').find({'customerId':2496, 'batchId':13855795, 'documentId': 4392914925})
                                #    docProps[docPropName] = ''
                                       
                        else:
                            docProps[docPropName] = prop.get('v').replace('<BR>', '\n')[:5000]
                        # Create a list of all unique doc prop names across both pre and post
                        if docPropName not in docPropLabels:
                            docPropLabels.append(str(docPropName))

            # Add our fsiDocument values
            for docPropName in fsiDocumentProps:
                docProps[docPropName] = fsiDocumentInfo[preOrPost][docId][docPropName]

            # if pages > 1, document is split across multiple mongo objects and should be combined
            if document.get('pages') > 1:
                splitObjects.setdefault(docId, {}).update(docProps)
                # Each time we find a split doc we need to subtract one from our total doc count
                batchInfo[batchCount] -= 1
            else:
                # Add to our OrderedDict with filename and docid as the key which can be used to properly sort objects
                batch[1].setdefault(docProps['FILENAME'], {}).update({docProps['DOCUMENTID']:docProps})

        # clean up our splitObjects
        for documentId in splitObjects:
            batch[1].setdefault(splitObjects[documentId]['FILENAME'], {}).update({documentId:splitObjects[documentId]})

        preOrPost = "postchange"
        batchCount = "postBatchCount"

    # Sort the labels and add DOCUMENTID, ACCOUNT_NUMBER and INVOICE_NUMBER to the front
    docPropLabels.sort()
    props = ["BT_ROUTE", "INVOICE_NUMBER", "ACCOUNT_NUMBER"]
    for prop in props:
        if prop in docPropLabels:
            docPropLabels.remove(prop)
            docPropLabels.insert(0, prop)
    docPropLabels.insert(0, "")
    docPropLabels.insert(0, "DOCUMENTID")




    # Time to create our masterPropList which should be a 2d array structured exactly how our data will appear as a table in gsheets
    masterPropList = []
    i = 1
    for fileName in prechangeProps:
        #print("length of prechange docs: " + str(len(prechangeProps[fileName])))
        for docId in sorted(prechangeProps[fileName].keys()):
            propList = []
            for docPropLabel in docPropLabels:
                if docPropLabel in prechangeProps[fileName][docId]:
                    propList.append(prechangeProps[fileName][docId][docPropLabel])
                else:
                    propList.append("")
            masterPropList.extend([propList, []])
        if fileName in postchangeProps:
            #print("length of postchange docs: " + str(len(postchangeProps[fileName])))
            for docId in sorted(postchangeProps[fileName].keys()):
                propList = []
                for docPropLabel in docPropLabels:
                    if docPropLabel in postchangeProps[fileName][docId]:
                        propList.append(postchangeProps[fileName][docId][docPropLabel])
                    else:
                        propList.append("")
     
                masterPropList[i] = propList 
                i += 2

    for fileName in postchangeProps:
        if fileName not in prechangeProps:
            #masterPropList.extend([[],[]])
            print(len(masterPropList))
            for docId in sorted(postchangeProps[fileName].keys()):
                propList = []
                for docPropLabel in docPropLabels:
                    if docPropLabel in postchangeProps[fileName][docId]:
                        propList.append(postchangeProps[fileName][docId][docPropLabel])
                    else:
                        propList.append("")
                masterPropList.extend([[],propList])
                #i += 2

    return (docPropLabels, masterPropList, batchInfo, arguments)

    ### PANDAS BULLSHIT ###
    #prechangeDf = pd.DataFrame(prechangeProps.values())
    #postchangeDf = pd.DataFrame(postchangeProps.values())
    #masterPropDf = pd.concat([prechangeDf, postchangeDf], ignore_index=True)

    #pd.set_option('display.max_rows', 500)
    #pd.set_option('display.max_columns', 500)
    #pd.set_option('display.width', 1000)
    #masterPropDf = masterPropDf.sort_values(by=['9999_MASTER_KEY_9999'])
    #print(masterPropDf.columns)
    ### PANDAS BULLSHIT ###

@fn_timer
def CreateCompareTab(docPropLabels, masterPropList, batchInfo, arguments, service):

    #rowCount = ((len(prechangeProps) + len(postchangeProps)) * 2) + 2
    rowCount = len(masterPropList)+2
    spreadsheetId = arguments['spreadsheetId']
    addSheetResponse = SendUpdateRequests(service, sheet_requests.AddCompareSheet(rowCount, spreadsheetId, service), spreadsheetId)
    sheetId = addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('sheetId')
    sheetName = str(addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('title'))

    # Add docprop labels to new tab
    UpdateSingleRange(service, [docPropLabels], "B2", sheetName, spreadsheetId)

    #ws = gc.open(sheetName).worksheet(sheetId)
    #existing = gd.get_as_dataframe(ws)
    #updated = existing.append(masterPropList)
    #gd.set_with_dataframe(ws, updated)

    #sys.exit()

    rows = []
    startRowNum = 3
    currentRowNum = 3
    startColIndex = 2
    currentColIndex = 2
    
    #print(masterPropList)

    endColIndex = startColIndex + len(masterPropList[0]) - 1 # subract 2 because we dont include docid or pre/post number
    colIndexes = list(range(startColIndex, endColIndex+1)) #keep track of columns labels that have already turned red due to mismatch

    #sys.exit()

    print ("Setting column widths...")
    #print("Time Elapsed: %s" % (time.time() - startTime))
    requests = sheet_requests.SetAutoColumnWidth(startColIndex, endColIndex, sheetId)
    SendUpdateRequests(service, requests, spreadsheetId)

    # Add docprop values to new tab
    UpdateSingleRange(service, masterPropList, "B3", sheetName, spreadsheetId)

    prechangeRange = []
    postchangeRange = []

    dpLabelEqual = []
    dpLabelNotEqual = []

    borderRange =[{ "sheetId": sheetId,
                    "startColumnIndex": 0,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": 1,
                    "endRowIndex": 2 }]

    # NEW COLOR RANGES
    dpValuesEq = {}
    dpValuesNe = []
    misMatchedPair = []
    dpValuesEqual = True
    dpNeCols = [] # cell range of dp labels to turn red
    noChangeHideRow = []
    noChangeHideCol = []
    changedDocProps = {}
    numOfChangedPairs = 0
    compareNumber = 1
    compareNumbers = []

    print("Number of Doc Props:", len(docPropLabels))
    for docCounter, document in enumerate(masterPropList):
        # ADD DATA
        dpRowEq = True
        if (docCounter+1) % 2 != 0:

            # REDO THIS BULLSHIT
            row1 = ["PRE.%06d" % compareNumber]
            row2 = ["POS.%06d" % compareNumber]
            compareNumbers.append(row1)
            compareNumbers.append(row2)
            # REDO THIS BULLSHIT

            for propCounter, docPropValue in enumerate(document):
                #print(docPropValue)
                #row1.append(docPropValue[0]) #prechange
                #row2.append(docPropValue[1]) #postchange
                if docCounter < len(masterPropList) - 1:
                    if docPropValue != masterPropList[docCounter+1][propCounter]:
                        if docPropLabels[currentColIndex-2] != "DOCUMENTID":
                            if docPropValue == "NO MATCH":
                                print("DocId", docPropValue)
                                misMatchedPair.append({ "sheetId": sheetId,
                                                        "startColumnIndex": currentColIndex - 1 ,
                                                        "endColumnIndex": endColIndex,
                                                        "startRowIndex": startRowNum - 2,
                                                        "endRowIndex": startRowNum})
                                break
                            dpRowEq = False
                            # Store a list of all doc props that were changed
                            if docPropLabels[currentColIndex-2] not in changedDocProps:
                                changedDocProps[docPropLabels[currentColIndex-2]] = {"documents": ["Changed Document Pairs:", "PRE.%06d - POS.%06d" % (compareNumber, compareNumber)],
                                                                                     "column": currentColIndex-1,
                                                                                     "row": currentRowNum-1}
                            else:
                                changedDocProps[docPropLabels[currentColIndex-2]]["documents"].append("PRE.%06d - POS.%06d" % (compareNumber, compareNumber))
                            if currentColIndex in colIndexes:
                                dpNeCols.append({   "sheetId": sheetId,
                                                    "startColumnIndex": currentColIndex - 1 ,
                                                    "endColumnIndex": currentColIndex,
                                                    "startRowIndex": startRowNum - 2,
                                                    "endRowIndex": startRowNum-1})
                                colIndexes.remove(currentColIndex)
                            if dpValuesEqual:
                                dpValuesNe.append({ "sheetId": sheetId,
                                                    "startColumnIndex": currentColIndex - 1 ,
                                                    "endColumnIndex": currentColIndex,
                                                    "startRowIndex": currentRowNum - 1,
                                                    "endRowIndex": currentRowNum + 1})
                                dpValuesEqual = False
                            elif not dpValuesEqual:
                                dpValuesNe[-1]["endColumnIndex"] = currentColIndex
                                dpValuesNe[-1]["endRowIndex"] = currentRowNum + 1

                    else:
                        dpValuesEqual = True
                currentColIndex += 1

            #############################

            # used for summary statement at top of page
            if not dpRowEq:
                numOfChangedPairs += 1
            # Reset bool for each pair
            dpValuesEqual = True
            # Build our 2D Array of row data
            #rows.append(row1)
            #rows.append(row2)

            dpLabelEqual.append("D%d=D%d" % (currentRowNum, currentRowNum+1))
            dpLabelNotEqual.append("D%d<>D%d" % (currentRowNum, currentRowNum+1))
            borderRange.append({ "sheetId": sheetId,
                                 "startColumnIndex": 0,
                                 "endColumnIndex": endColIndex,
                                 "startRowIndex": currentRowNum,
                                 "endRowIndex": currentRowNum+1})
            #requests = AddCompFormatRule(service, requests, endColIndex, currentRowNum-1)
            #AddCompFormatRule(service, "=D%s" % str(currentRowNum), endColIndex, currentRowNum)
            #####
            # Keep track of all rows that should be hidden because there was no change seen between pre and post for that pair
            if dpRowEq and arguments['noChangeRows'] == 'hide':
                if len(noChangeHideRow) > 0: # only check previous row if we have added at least one
                    if noChangeHideRow[-1][1] == currentRowNum-1:
                        noChangeHideRow[-1][1] = currentRowNum+1 # extend previous range by our current pair
                    else:
                        noChangeHideRow.extend([[currentRowNum-1, currentRowNum+1]]) # start a new range
                else:
                    noChangeHideRow.extend([[currentRowNum-1, currentRowNum+1]]) # add our first range
            currentRowNum += 2
            compareNumber += 1
            currentColIndex = 2 # rest col index after each pair is added to rows

            #######################################

    # Add compare index numbers to column A
    UpdateSingleRange(service, compareNumbers, "A3", sheetName, spreadsheetId)

    # Add rows of data to sheet
    #UpdateSingleRange(rows, "A%s" % str(startRowNum), sheetName)
    #print("Time Elapsed: %s" % (time.time() - startTime))
    # Set all dp cells to green to start
    print("Setting all cells to green...")
    dpValuesEq = {  "sheetId": sheetId,
                    "startColumnIndex": startColIndex,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 1,
                    "endRowIndex": currentRowNum - 1}

    requests = sheet_requests.AddGreenBackground(dpValuesEq)
    SendUpdateRequests(service, requests, spreadsheetId)
    # Now change all mismatched value pairs to red
    print("Changing cells to red...")
    if len(dpValuesNe) > 0:
        requests = sheet_requests.AddRedBackground(dpValuesNe)
        SendUpdateRequests(service, requests, spreadsheetId)
    else:
        print("No differences found between the two batchs...")

    print("Setting all labels to green or red...")
    dpEqCols = [{   "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 2,
                    "endRowIndex": startRowNum - 1}]
    requests = sheet_requests.AddDPLabelBackground(dpEqCols, dpNeCols)
    SendUpdateRequests(service, requests, spreadsheetId)


    print ("Adding row borders...")
    requests = sheet_requests.AddRowBorders(service, borderRange)
    SendUpdateRequests(service, requests, spreadsheetId)

    print ("Setting font to Calibri...")
    requests = sheet_requests.SetFont(service, endColIndex, sheetId)
    SendUpdateRequests(service, requests, spreadsheetId)

    #print ("Adding conditional formatting rules for each document...")
    #requests = AddDPCompFormatRule(prechangeRange, postchangeRange)
    # Send conditional formatting requests
    #SendUpdateRequests(service, requests, spreadsheetId)
    # Send alternating colors request
    print("Adding alternating colors and batch information...")
    SendUpdateRequests(service, sheet_requests.AddAlternatingColors(sheetId), spreadsheetId)
    #changedDPIndexes = docProp
    #changedDocProps = [docPropLabels[i] for i in dpNeCols] # Get dp labels that saw a change in any pre/post pair

    AddChangedCellLink(service, changedDocProps, sheetId, sheetName, arguments)

    # Add batch information
    prePost = ("Pre:  " + str(arguments['preId']), "Post:  " + str(arguments['postId']), "", "BATCH COMPARE STATS")
    csrId = ("CSR ID:  " + str(arguments['custId']), "DOCUMENTID")
    UpdateSingleRange(service, [prePost], "A1", sheetName, arguments['spreadsheetId'])
    UpdateSingleRange(service, [csrId], "A2", sheetName, arguments['spreadsheetId'])

    requests = sheet_requests.AddBatchInformation(batchInfo['preBatchCount'], batchInfo['postBatchCount'], batchInfo['misMatchCount'], numOfChangedPairs - batchInfo['misMatchCount'], changedDocProps, sheetId, sheetName, arguments)
    SendUpdateRequests(service, requests, spreadsheetId)

    # INSERT COLUMN
    #requests = AddColumn(2, 3, sheetId)
    #SendUpdateRequests(service, requests, spreadsheetId)

    # Add pair color to column C, either red or green
    requests = sheet_requests.AddPairColor(dpValuesNe, dpValuesEq, sheetId)
    SendUpdateRequests(service, requests, spreadsheetId)

    if misMatchedPair != []:
        requests = sheet_requests.AddGrayBackground(misMatchedPair)
        SendUpdateRequests(service, requests, spreadsheetId)

    if arguments['noChangeRows'] == 'hide':
        print("Hiding rows that saw no change from pre to post...")
        requests = sheet_requests.HideNoChangeRows(noChangeHideRow, sheetId)
        if requests != []:
            SendUpdateRequests(service, requests, spreadsheetId)

    if arguments['noChangeCols'] == 'hide':
        for currentColIndex in colIndexes:
            if docPropLabels[currentColIndex-2] not in ("DOCUMENTID", "ACCOUNT_NUMBER", "INVOICE_NUMBER", "BT_ROUTE", ""):
                    noChangeHideCol.append([currentColIndex -1, currentColIndex])
        print("Hiding cols that saw no change from pre to post...")
        requests = sheet_requests.HideNoChangeCols(noChangeHideCol, sheetId)
        if requests != []:
            SendUpdateRequests(service, requests, spreadsheetId)

    #print("Adding summary information...")
    #summaryStatement = "Prechange Document Count: %s | Postchange Document Count: %s | Number of Mismatched Documents: %s | Number of Pairs with Change: %s" \
    #                    % (numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount)
    #UpdateSingleRange([[summaryStatement, ""]], "C1")



    #print("Time Elapsed: %s" % (time.time() - startTime))
    print("Mission successful...")


@fn_timer
def CreateDPCompareTab(docPropLabels, masterPropList, misMatchCount, numOfPreDocs, numOfPostDocs, arguments, service):

    print("Start CreateDPCompareTab: %s" % (time.time() - startTime))

    spreadsheetId = arguments['spreadsheetId']
    rowCount = (len(masterPropList) * 2) + 2
    addSheetResponse = SendUpdateRequests(service, sheet_requests.AddCompareSheet(rowCount, spreadsheetId, service), spreadsheetId)
    sheetId = addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('sheetId')
    sheetName = str(addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('title'))

    print("sheet id: ", sheetId)
    print("sheet name: ", sheetName)

    UpdateSingleRange(service, [docPropLabels], "B2", sheetName, spreadsheetId)
    rows = []
    startRowNum = 3
    currentRowNum = 3
    startColIndex = 2
    currentColIndex = 2
    #
    endColIndex = startColIndex + len(masterPropList[list(masterPropList.keys())[0]]) - 1 # subract 1 because we dont include docid or pre/post number
    colIndexes = list(range(startColIndex, endColIndex+1)) #keep track of columns labels that have already turned red due to mismatch

    print ("Setting column widths...")
    #print("Time Elapsed: %s" % (time.time() - startTime))
    requests = sheet_requests.SetAutoColumnWidth(startColIndex, endColIndex, sheetId)
    SendUpdateRequests(service, requests, spreadsheetId)

    #sys.exit()

    prechangeRange = []
    postchangeRange = []

    dpLabelEqual = []
    dpLabelNotEqual = []

    borderRange =[{ "sheetId": sheetId,
                    "startColumnIndex": 0,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": 1,
                    "endRowIndex": 2 }]

    # NEW COLOR RANGES
    dpValuesEq = {}
    dpValuesNe = []
    dpValuesEqual = True
    dpNeCols = [] # cell range of dp labels to turn red
    noChangeHideRow = []
    noChangeHideCol = []
    misMatchedPair = []
    changedDocProps = {}
    numOfChangedPairs = 0
    compareNumber = 1

    print("Number of Doc Props:", len(docPropLabels))
    for documentPair in masterPropList.values():
        # ADD DATA
        row1 = ["PRE.%06d" % compareNumber]
        row2 = ["POS.%06d" % compareNumber]
        dpRowEq = True
        misMatched = False
        for docPropValue in documentPair:
            #print(docPropValue)
            row1.append(docPropValue[0]) #prechange
            row2.append(docPropValue[1]) #postchange
            if docPropValue[0] != docPropValue[1]:
                if docPropValue[0] == "NO MATCH":
                    misMatched = True
                    misMatchedPair.append({ "sheetId": sheetId,
                                            "startColumnIndex": currentColIndex,
                                            "endColumnIndex": endColIndex,
                                            "startRowIndex": currentRowNum - 1,
                                            "endRowIndex": currentRowNum + 1})
                else:
                    if docPropLabels[currentColIndex-2] != "DOCUMENTID" and not misMatched:
                        dpRowEq = False
                        # Store a list of all doc props that were changed
                        if docPropLabels[currentColIndex-2] not in changedDocProps:
                            changedDocProps[docPropLabels[currentColIndex-2]] = {"documents": ["Changed Document Pairs:", "PRE.%06d - POS.%06d" % (compareNumber, compareNumber)],
                                                                                 "column": currentColIndex-1,
                                                                                 "row": currentRowNum-1}
                        else:
                            changedDocProps[docPropLabels[currentColIndex-2]]["documents"].append("PRE.%06d - POS.%06d" % (compareNumber, compareNumber))
                        if currentColIndex in colIndexes:
                            dpNeCols.append({   "sheetId": sheetId,
                                                "startColumnIndex": currentColIndex - 1 ,
                                                "endColumnIndex": currentColIndex,
                                                "startRowIndex": startRowNum - 2,
                                                "endRowIndex": startRowNum-1})
                            colIndexes.remove(currentColIndex)

                        if dpValuesEqual:
                            dpValuesNe.append({ "sheetId": sheetId,
                                                "startColumnIndex": currentColIndex - 1 ,
                                                "endColumnIndex": currentColIndex,
                                                "startRowIndex": currentRowNum - 1,
                                                "endRowIndex": currentRowNum + 1})
                            dpValuesEqual = False
                        elif not dpValuesEqual:
                            dpValuesNe[-1]["endColumnIndex"] = currentColIndex
                            dpValuesNe[-1]["endRowIndex"] = currentRowNum + 1
            else:
                dpValuesEqual = True
            currentColIndex += 1
        # used for summary statement at top of page
        if not dpRowEq:
            numOfChangedPairs += 1
        # Reset bool for each pair
        dpValuesEqual = True
        # Build our 2D Array of row data
        #if dpRowEq and arguments['noChangeRows'] == 'exclude':
        #    pass
        #else:
        rows.append(row1)
        rows.append(row2)

        dpLabelEqual.append("D%d=D%d" % (currentRowNum, currentRowNum+1))
        dpLabelNotEqual.append("D%d<>D%d" % (currentRowNum, currentRowNum+1))
        borderRange.append({ "sheetId": sheetId,
                             "startColumnIndex": 0,
                             "endColumnIndex": endColIndex,
                             "startRowIndex": currentRowNum,
                             "endRowIndex": currentRowNum+1})
        #requests = AddCompFormatRule(service, requests, endColIndex, currentRowNum-1)
        #AddCompFormatRule(service, "=D%s" % str(currentRowNum), endColIndex, currentRowNum)
        #####
        # Keep track of all rows that should be hidden because there was no change seen between pre and post for that pair
        if dpRowEq and arguments['noChangeRows'] == 'hide':
            if len(noChangeHideRow) > 0: # only check previous row if we have added at least one
                if noChangeHideRow[-1][1] == currentRowNum-1:
                    noChangeHideRow[-1][1] = currentRowNum+1 # extend previous range by our current pair
                else:
                    noChangeHideRow.extend([[currentRowNum-1, currentRowNum+1]]) # start a new range
            else:
                noChangeHideRow.extend([[currentRowNum-1, currentRowNum+1]]) # add our first range
        currentRowNum += 2
        compareNumber += 1
        currentColIndex = 2 # rest col index after each pair is added to rows

    # Add rows of data to sheet
    UpdateSingleRange(service, rows, "A%s" % str(startRowNum), sheetName, spreadsheetId)
    #print("Time Elapsed: %s" % (time.time() - startTime))
    # Set all dp cells to green to start
    print("Setting all cells to green...")
    dpValuesEq = {  "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 1,
                    "endRowIndex": currentRowNum}
    requests = sheet_requests.AddGreenBackground(dpValuesEq)
    SendUpdateRequests(service, requests, spreadsheetId)
    # Now change all mismatched value pairs to red
    print("Changing cells to red...")
    if len(dpValuesNe) > 0:
        requests = sheet_requests.AddRedBackground(dpValuesNe)
        SendUpdateRequests(service, requests, spreadsheetId)
    else:
        print("No differences found between the two batchs...")

    print("Setting all labels to green or red...")
    dpEqCols = [{   "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 2,
                    "endRowIndex": startRowNum - 1}]
    requests = sheet_requests.AddDPLabelBackground(dpEqCols, dpNeCols)
    SendUpdateRequests(service, requests, spreadsheetId)

    print ("Adding row borders...")
    requests = sheet_requests.AddRowBorders(service, borderRange)
    SendUpdateRequests(service, requests, spreadsheetId)

    print ("Setting font to Calibri...")
    requests = sheet_requests.SetFont(service, endColIndex, sheetId)
    SendUpdateRequests(service, requests, spreadsheetId)
    #print ("Adding conditional formatting rules for each document...")
    #requests = AddDPCompFormatRule(prechangeRange, postchangeRange)
    # Send conditional formatting requests
    #SendUpdateRequests(service, requests, spreadsheetId)
    # Send alternating colors request
    print("Adding alternating colors and batch information...")
    SendUpdateRequests(service, sheet_requests.AddAlternatingColors(sheetId), spreadsheetId)
    #changedDPIndexes = docProp
    #changedDocProps = [docPropLabels[i] for i in dpNeCols] # Get dp labels that saw a change in any pre/post pair

    AddChangedCellLink(service, changedDocProps, sheetId, sheetName, arguments)

    # Add batch information
    prePost = ("Pre:  " + str(arguments['preId']), "Post:  " + str(arguments['postId']), "", "BATCH COMPARE STATS")
    csrId = ("CSR ID:  " + str(arguments['custId']), "DOCUMENTID")
    UpdateSingleRange(service, [prePost], "A1", sheetName, arguments['spreadsheetId'])
    UpdateSingleRange(service, [csrId], "A2", sheetName, arguments['spreadsheetId'])

    requests = sheet_requests.AddBatchInformation(numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount, changedDocProps, sheetId, sheetName, arguments)
    SendUpdateRequests(service, requests, spreadsheetId)

    requests = sheet_requests.AddPairColor(dpValuesNe, dpValuesEq, sheetId)
    SendUpdateRequests(service, requests, spreadsheetId)

    if misMatchedPair != []:
        requests = sheet_requests.AddGrayBackground(misMatchedPair)
        SendUpdateRequests(service, requests, spreadsheetId)

    if arguments['noChangeRows'] == 'hide':
        print("Hiding rows that saw no change from pre to post...")
        requests = sheet_requests.HideNoChangeRows(noChangeHideRow, sheetId)
        if requests != []:
            SendUpdateRequests(service, requests, spreadsheetId)

    if arguments['noChangeCols'] == 'hide':
        for currentColIndex in colIndexes:
            if docPropLabels[currentColIndex-2] not in ("DOCUMENTID", "ACCOUNT_NUMBER", "INVOICE_NUMBER", "BT_ROUTE", ""):
                    noChangeHideCol.append([currentColIndex -1, currentColIndex])
        print("Hiding cols that saw no change from pre to post...")
        requests = sheet_requests.HideNoChangeCols(noChangeHideCol, sheetId)
        if requests != []:
            SendUpdateRequests(service, requests, spreadsheetId)  

    #print("Adding summary information...")
    #summaryStatement = "Prechange Document Count: %s | Postchange Document Count: %s | Number of Mismatched Documents: %s | Number of Pairs with Change: %s" \
    #                    % (numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount)
    #UpdateSingleRange([[summaryStatement, ""]], "C1")



    #print("Time Elapsed: %s" % (time.time() - startTime))
    print("Mission successful...")
    sys.exit()


def run(argv):
    if len(argv) != 4:
        print("Command line arguments not given, using values hardcoded within run() function...")

        spreadsheetURL = 'https://docs.google.com/spreadsheets/d/1rvOMtucwaM5kD8j4GOxpHAMQnh0U064GMv8oUazq4Kw/edit#gid=0'
        spreadsheetId = spreadsheetURL.split('/')[-2]

        arguments = {"custId"           : 2546,
                     "preId"            : 13849961,
                     "preEnv"           : "imdb", # imdb or reportdb, imdb should be default
                     "postId"           : 13850115,
                     "postEnv"          : "imdb", # imdb or reportdb, imdb should be default
                     "spreadsheetURL"   : spreadsheetURL,
                     "spreadsheetId"    : spreadsheetId,
                     "compareLogic"     : "masterKey", # docId or masterKey
                     'noChangeCols'     : 'hide', # show, hide or exclude
                     'noChangeRows'     : 'hide', # show, hide or exclude
                     'masterKeyProps'   : ['ACCOUNT_NUMBER', 'INVOICE_NUMBER', 'TOTAL_DUE', 'BT_ROUTE', 'FFDID'],
                     'ignoredProps'     : ['FILEDATE', 'FILE_PREFIX', 'SIG_BMP', 'XML_DATA', 'BT_PRINT_FILE_NAME', 'BILLING_ADDRESS_BEG1', 'BILLING_ADDRESS_BEG2',
                                           'BILLING_ADDRESS_END1', 'BILLING_ADDRESS_END2', 'BILLING_ADDRESS_ZIP4', 'BILLING_ADDRESS_ZIP5', 'BILLING_ADDRESS_CITY',
                                           'BILLING_ADDRESS_STATE', 'ROWIMG', 'JOB_ID']}
        pprint(arguments)
    else:
        spreadsheetId = argv[3].split('/')[-2]
        arguments = {"custId"           : int(argv[0]),
                     "preId"            : int(argv[1]),
                     "preEnv"           : "imdb", # imdb or reportdb, imdb should be default
                     "postId"           : int(argv[2]),
                     "postEnv"          : "imdb", # imdb or reportdb, imdb should be default
                     "spreadsheetURL"   : argv[3],
                     "spreadsheetId"    : spreadsheetId,
                     "compareLogic"     : "docId",
                     'noChangeCols'     : 'hide',
                     'noChangeRows'     : 'hide',
                     'masterKeyProps'   : ['ACCOUNT_NUMBER', 'INVOICE_NUMBER', 'TOTAL_DUE', 'BT_ROUTE', 'FFDID'],
                     'ignoredProps'     : ['FILEDATE', 'FILE_PREFIX', 'SIG_BMP', 'XML_DATA', 'BT_PRINT_FILE_NAME', 'BILLING_ADDRESS_BEG1', 'BILLING_ADDRESS_BEG2',
                                           'BILLING_ADDRESS_END1', 'BILLING_ADDRESS_END2', 'BILLING_ADDRESS_ZIP4', 'BILLING_ADDRESS_ZIP5', 'BILLING_ADDRESS_CITY',
                                           'BILLING_ADDRESS_STATE', 'ROWIMG', 'JOB_ID']}
        print("Proceeding with the following command line arguments...")
        pprint(arguments)

    #sys.exit()

    # connect to Sql Server, TODO this should be a cmd line arg
    # setting as false, not all users have windows auth, need to add generic
    # gmcuser creds
    bConnToSqlServer = False
    if bConnToSqlServer:
        sqlServerCursor = InitSqlServerConn()
    # Get list of Coversheet FFDIds
    mysqlClient = InitSQLClient()
    coversheetDocIds = GetCoversheetDocIds(mysqlClient, arguments)

    # Get ffdid, routing and pagecount from fsidocument, returns two lists of dicts, a prechange and postchange
    fsiDocumentInfo = GetFSIDocumnetInfo(mysqlClient, arguments)

    # Init mongo client, returns the fsidocprops collection to query against
    fsidocprops = InitMongoClient()

    # masterKey compareLogic creates a unique identifier based on doc prop values in order to make documents in the prechange
    # to their respective document in the postchange batch
    if arguments["compareLogic"] == "masterKey":
        prePostDocProps = GetDocProps(fsidocprops, coversheetDocIds, arguments)
        # Merge pre and post batches, returns list = [docPropLabels, masterPropList, misMatchCount, numOfPreDocs, numOfPostDocs]
        mergedData = MergeBatchData(prePostDocProps[0], prePostDocProps[1], fsiDocumentInfo, arguments)
        # Add all information to our google sheet and format cells accordingly
        CreateDPCompareTab(mergedData[0], mergedData[1], mergedData[2], mergedData[3], mergedData[4], arguments)

    # docId compareLogic matches prechange docs to postchange by using documentId, the lowest docId from pre batch matches
    # the lowest docId from post batch
    elif arguments["compareLogic"] == "docId":
        prePostDocProps = QueryMongo(fsidocprops, coversheetDocIds, arguments)
        # Merge pre and post batches, returns list = [docPropLabels, masterPropList, misMatchCount, numOfPreDocs, numOfPostDocs]
        mergedData = MergeToDataFrame(prePostDocProps[0], prePostDocProps[1], fsiDocumentInfo, arguments)
        # Add all information to our google sheet and format cells accordingly
        CreateCompareTab(mergedData[0], mergedData[1], mergedData[2], arguments)

    for timer in timerList:
        print(timer)

# Used to calc processing time
startTime = time.time()
timerList = []

# Authorize Google Sheets API credentials and build arguments['service']
# asimon :: removing the google auth piece as it should only be ran once at runtime
# creds = GoogleAPIAuthorization()
# arguments['service'] = discovery.build('sheets', 'v4', credentials=creds)

# List of properties that we never want to include in our compare
#arguments['ignoredProps'] = ( 'FILEDATE', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME', 'BILLING_ADDRESS_BEG1', 'BILLING_ADDRESS_BEG2',
#                        'BILLING_ADDRESS_END1', 'BILLING_ADDRESS_END2', 'BILLING_ADDRESS_ZIP4', 'BILLING_ADDRESS_ZIP5', 'BILLING_ADDRESS_CITY',
#                        'BILLING_ADDRESS_STATE', 'ROWIMG', 'JOB_ID')