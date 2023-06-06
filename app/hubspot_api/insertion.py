from httplib2 import Authentication
import requests
import json


def insertion_hubspot(FICHIER_OUTPUT,api_key,data):
    # insert your api key here
    url = "https://api.hubapi.com/crm/v3/imports"
    headers = {
      # 'content-type': 'multipart/form-data',
      # 'accept': 'application/json',
      'Authorization': 'Bearer %s' % api_key
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
    
    # Check if the request was successful
    import_id = None
    # Check if the request was successful
    if response.status_code == 200 or response.status_code == 202:
      print("Response status code : ",response.status_code)
      # print("response headers ",response.headers)
      # print("response encoding ",response.encoding)
      # print("response text : ",response.text.encode('utf8'))
      # Get the import ID from the response headers
      response_dict = json.loads(response.text.encode('utf8'))
      import_id = response_dict['id']
      print(f"Import ID: {import_id}")
    else:
      print(f"Error: {response.status_code}")
        
    # print("response text : ",response.text.encode('utf8'))
    return import_id
