import requests
import json


def insertion_hubspot(FICHIER_OUTPUT,api_key,data):
    # insert your api key here
    url = "https://api.hubapi.com/crm/v3/imports"
    headers = {
      'content-type': 'application/json',
      'authorization': 'Bearer %s' % api_key
    }
    data =  data


    datastring = json.dumps(data)

    payload = {"importRequest": datastring}

    # current_dir = os.path.dirname(__file__)
    # relative_path = "./test_import.csv"

    # absolute_file_path = os.path.join(current_dir, relative_path)

    files = [
        ('files', open(FICHIER_OUTPUT, 'r',encoding='utf-8'))
    ]
    print(files)


    response = requests.request("POST", url, data=payload, files=files,headers=headers)
    print(response.encoding)
    print(response.text.encode('utf8'))
    print(response.status_code)