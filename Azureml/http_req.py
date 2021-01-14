
import urllib.request
import urllib  # urllib.request and urllib.error for Python 3.X
import json

data ={
    "Claim_Nr": 1350302,
    "Client_Claim": "034124149",
    "CLAIM_DATE": 1556035200000,
    "Claim_Place_Code": "4",
    "Claim_Place_Zone_Code": "\\u05d1\\u05d0\\u05e8",
    "Fualt_Insured_Code": 4.0,
    "Vehicle_Towed": 0.0,
    "Police_arrive": "nodata",
    "Photos_taken": "nodata"
}

# data = {
#     "Inputs": {
#         "input1":
#         [
#             {
#                 'column1': "value1",
#                 'column2': "value2",
#                 'column3': "value3"
#             }
#         ],
#     },
#     "GlobalParameters":  {}
# }

body = str.encode(json.dumps(data))
print(body)
# Replace this with the URI and API Key for your web service
url = 'http://13.69.251.224:80/api/v1/service/quitoaksservice/score'
api_key = '?'
headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}

# "urllib.request.Request(url, body, headers)" for Python 3.X
req =  urllib.request.Request(url, body, headers)
print (req)
try:
    # "urllib.request.urlopen(req)" for Python 3.X
    response =  urllib.request.urlopen(req)

    result = response.read()
    print(result)
# "urllib.error.HTTPError as error" for Python 3.X
except urllib.error.HTTPError as error:
    print("The request failed with status code: " + str(error.code))

    # Print the headers - they include the request ID and the timestamp, which are useful for debugging the failure
    print(error.info())
    print(json.loads(error.read()))
