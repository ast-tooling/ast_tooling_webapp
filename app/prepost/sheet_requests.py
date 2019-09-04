import time
from functools import wraps

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


def AddColumn(startIndex, endIndex, sheetId):
    requests = [
    {
      "insertDimension": {
        "range": {
          "sheetId": sheetId,
          "dimension": "COLUMNS",
          "startIndex": startIndex,
          "endIndex": endIndex
        },
        "inheritFromBefore": True
      }
    }]
    return requests


def AddPairColor(dpValuesNe, dpValuesEq, sheetId):
    requests = [
    {
      "updateDimensionProperties": {
        "range": {
          "sheetId": sheetId,
          "dimension": "COLUMNS",
          "startIndex": 2,
          "endIndex": 3
        },
        "properties": {
          "pixelSize": 5
        },
        "fields": "pixelSize"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 2,
          "endColumnIndex": 3,
          "startRowIndex": 0,
          "endRowIndex": 2
        },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.8,  # Gray
              "green": 0.8,
              "red": 0.8,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    }]

    requests.append(
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 2,
          "endColumnIndex": 3,
          "startRowIndex": dpValuesEq["startRowIndex"],
          "endRowIndex": dpValuesEq["endRowIndex"]
        },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.588,  # Dark green if equal
              "green": 0.815,
              "red": 0.568,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    })

    for pair in dpValuesNe:
        requests.append(
        {
          "repeatCell": {
            "range": {
              "sheetId": sheetId,
              "startColumnIndex": 2,
              "endColumnIndex": 3,
              "startRowIndex": pair["startRowIndex"],
              "endRowIndex": pair["endRowIndex"]
            },
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.44,  # Dark red if not equal
                  "green": 0.44,
                  "red": 0.874,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests


def AddGrayBackground(misMatchedPair):
    requests = []
    for pair in misMatchedPair:
        requests.append(
        {
          "repeatCell": {
            "range": pair,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.8,  # Light gray if mismatched
                  "green": 0.8,
                  "red": 0.8,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests


def AddGreenBackground(dpValuesEq):
    requests = [
    {
      "repeatCell": {
        "range": dpValuesEq,
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.827451,  # Light green if equal
              "green": 0.91764706,
              "red": 0.8509804,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    }]
    return requests


def AddRedBackground(dpValuesNe):
    requests = []
    for cellRange in dpValuesNe:
        requests.append(
        {
          "repeatCell": {
            "range": cellRange,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.8,  # Light red if not equal
                  "green": 0.8,
                  "red": 0.95686175,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests


def AddDPLabelBackground(dpEqCols, dpNeCols):
    requests = []
    for cellRange in dpEqCols:
        requests.append(
        {
          "repeatCell": {
            "range": cellRange,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.588,  # Dark green if equal
                  "green": 0.815,
                  "red": 0.568,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    for cellRange in dpNeCols:
        requests.append(
        {
          "repeatCell": {
            "range": cellRange,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.44,  # Dark red if not equal
                  "green": 0.44,
                  "red": 0.874,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests


def AddAlternatingColors(sheetId):
    requests = [{
       'addBanding':{
          'bandedRange':{
             'range':{
                'sheetId':sheetId,
                'startRowIndex':2,
                'startColumnIndex':0,
                'endColumnIndex':2,
             },
             'rowProperties':{
                'firstBandColor':{
                   'red':1,
                   'green':.89,
                   'blue':.74,
                },
                'secondBandColor':{
                   'red':.776,
                   'green':.905,
                   'blue':1,
                }
             },
          },
       },
    },
    {
       'updateSheetProperties':{
          'properties':{
             'sheetId':sheetId,
             'gridProperties':{
                'frozenRowCount':2
             }
          },
          'fields':'gridProperties.frozenRowCount',
       }
    },
    {
       'updateSheetProperties':{
          'properties':{
             'sheetId':sheetId,
             'gridProperties':{
                'frozenColumnCount':3
             }
          },
          'fields':'gridProperties.frozenColumnCount',
       }
    },]
    return requests


def SetFont(service, endColumnIndex, sheetId):
    requests =  [
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 0,
          "endColumnIndex": endColumnIndex
        },
        "cell": {
          "userEnteredFormat": {
            "textFormat": {
              "fontFamily": "Calibri",
            }
          }
        },
        "fields": "userEnteredFormat(textFormat)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 0,
          "endColumnIndex": 2,
          "startRowIndex": 1,
          "endRowIndex": 2
        },
        "cell": {
          "userEnteredFormat": {
          "backgroundColor": {
              "red": 0.8,
              "green": 0.8,
              "blue": 0.8
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startRowIndex": 0,
          "endRowIndex": 2
        },
        "cell": {
          "userEnteredFormat": {
            "textFormat": {
              "fontFamily": "Calibri",
              "bold": True,
            }
          }
        },
        "fields": "userEnteredFormat(textFormat)"
      }
    },
    ]
    return requests


def AddRowBorders(service, borderRange):
    requests = []
    for ranges in borderRange:
        requests.append({'updateBorders': {'range': ranges, 'bottom': {"style": "SOLID",
                                                                       "width": 2,
                                                                       "color": {'red': 0,
                                                                               'green': 0,
                                                                               'blue': 0,}}}})
    return requests


def AddBatchInformation(numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs, changedDocProps, sheetId, sheetName, arguments):
    note = "Prechange Document Count:\n    %s\n"    \
           "Postchange Document Count:\n    %s\n"   \
           "Mismatched Document Count:\n    %s\n"   \
           "Changed Pre/Post Pair Count:\n    %s\n" \
           "List of changed Doc Props:"             \
           % (numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs)
    if len(changedDocProps) == 0:
        note += "\n    (none)"
    for changedProp in changedDocProps:
        note += "\n    %s" % changedProp
    requests = [
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 0,
          "endColumnIndex": 1,
          "startRowIndex": 0,
          "endRowIndex": 1
          },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.776,  # Prechange
              "green": 0.905,
              "red": 1,
            },
          },
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 1,
          "endColumnIndex": 2,
          "startRowIndex": 0,
          "endRowIndex": 1
          },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 1,  # Postchange
              "green": 0.89,
              "red": 0.74,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 3,
          "endColumnIndex": 4,
          "startRowIndex": 0,
          "endRowIndex": 1
          },
        "cell": {
          "note" : note
        },
        "fields": "note"
      }
    },]
    for changedProp in changedDocProps.values():
        if changedProp != "":
            requests.append(    {
                                  "repeatCell": {
                                    "range": {
                                      "sheetId": sheetId,
                                      "startColumnIndex": changedProp["column"],
                                      "endColumnIndex": changedProp["column"]+1,
                                      "startRowIndex": 1,
                                      "endRowIndex": 2
                                      },
                                    "cell": {
                                      "note" : "\n".join(changedProp["documents"]),
                                      "userEnteredFormat": {
                                        "textFormat": {
                                          "foregroundColor": {
                                            "red":   0,
                                            "green": 0,
                                            "blue":  0,
                                          },
                                          "underline": False,
                                          "fontFamily": "Calibri",
                                          "bold": True,
                                        },
                                      },
                                    },
                                    "fields": "note, userEnteredFormat(textFormat)"
                                  }
                                })
    return requests


def HideNoChangeRows(noChangeHideRow, sheetId):
    requests = []
    for startIndex, endIndex in noChangeHideRow:
        requests.append({
          'updateDimensionProperties': {
            "range": {
              "sheetId": sheetId,
              "dimension": 'ROWS',
              "startIndex": startIndex,
              "endIndex": endIndex,
            },
            "properties": {
              "hiddenByUser": True,
            },
            "fields": 'hiddenByUser',
        }})
    return requests


def HideNoChangeCols(noChangeHideCol, sheetId):
    requests = []
    for startIndex, endIndex in noChangeHideCol:
        requests.append({
          'updateDimensionProperties': {
            "range": {
              "sheetId": sheetId,
              "dimension": 'COLUMNS',
              "startIndex": startIndex,
              "endIndex": endIndex,
            },
            "properties": {
              "hiddenByUser": True,
            },
            "fields": 'hiddenByUser',
        }})
    return requests


def AddCompareSheet(rowCount, spreadsheetId, service):
    print("Start: Add new sheet to google doc")
    request = service.spreadsheets().get(spreadsheetId=spreadsheetId, fields="sheets.properties")
    response = request.execute()
    sheetNumbers = [0]
    for sheet in response.get('sheets'):
        sheetName = str(sheet.get('properties').get('title'))
        if "DP COMPARE" in sheetName:
            if sheetName.split(" ")[-1].isdigit():
                sheetNumbers.append(int(sheetName.split(" ")[-1]))
    newSheetNumber = max(sheetNumbers) + 1
    title = "DP COMPARE %d" % newSheetNumber
    print("New sheet name: %s" % title)
    # Alternate colors of added sheets
    red = 0.55
    green = 1.0
    blue = 0.64
    if max(sheetNumbers) % 2 == 0:
        red = 0.22
        green = 0.19
        blue = 1.0

    requests = [{"addSheet": {"properties": {"title": title,
                                             "gridProperties": {"rowCount": rowCount,
                                                                "columnCount": 25},
                                                                "tabColor": { "red": red,
                                                                              "green": green,
                                                                              "blue": blue}}}}]
    return requests


def SetColumnWidth(startIndex, endIndex, sheetId):
    requests = [{"updateDimensionProperties":{"range":{ "sheetId": sheetId,
                                                        "dimension": "COLUMNS",
                                                        "startIndex": startIndex,
                                                        "endIndex": endIndex},
                                                        "properties":{  "pixelSize": 160},
                                                                        "fields": "pixelSize"}}]
    return requests


def SetAutoColumnWidth(startIndex, endIndex, sheetId):
    requests = [{"autoResizeDimensions":{"dimensions":{"sheetId": sheetId,
                                                      "dimension": "COLUMNS",
                                                      "startIndex": startIndex,
                                                      "endIndex": endIndex}}}]
    return requests        



            