import requests
import json


def insertion_hubspot(FICHIER_OUTPUT,api_key):
    # insert your api key here
    url = "https://api.hubapi.com/crm/v3/imports?hapikey="+str(api_key)

    data = {
    "name": "Test import by api w OWNER",
    "files": [
        {
        "fileName": "output_bundle.csv",
        "fileFormat": "CSV",
        "dateFormat": "DAY_MONTH_YEAR",
        "fileImportPage": {
            "hasHeader": True,
            "columnMappings": [
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Nom",
                "propertyName": "name",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Type principal",
                "propertyName": "secteurs_d_activite",
                "idColumnType": None
            },
    #           {
    #             "columnObjectTypeId": "0-2",
    #             "columnName": "Tous les types",
    #             "propertyName": "Tous_les_types",
    #             "idColumnType": None
    #           },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Site internet (url racine)",
                "propertyName": "website",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Téléphone",
                "propertyName": "phone",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Téléphone international",
                "propertyName": "telephone_international",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Fuseau horaire",
                "propertyName": "timezone",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Adresse 1",
                "propertyName": "address",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Ville",
                "propertyName": "city",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Code postal",
                "propertyName": "zip",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Division de niveau 1",
                "propertyName": "state",
                "idColumnType": None
            },
    #             {
    #             "columnObjectTypeId": "0-2",
    #             "columnName": "Division de niveau 2",
    #             "propertyName": "Division_de_niveau_2",
    #             "idColumnType": None
    #           },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Pays",
                "propertyName": "country",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Email",
                "propertyName": "emails",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Page de contact 1",
                "propertyName": "pages_de_contacts",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Lien_réseaux_sociaux",
                "propertyName": "lien_reseaux_sociaux",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "owner",
                "propertyName": "hubspot_owner_id",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "leadStatus",
                "propertyName": "hs_lead_status",
                "idColumnType": None
            },
            ]
        }
        }
    ]
    }


    datastring = json.dumps(data)

    payload = {"importRequest": datastring}

    # current_dir = os.path.dirname(__file__)
    # relative_path = "./test_import.csv"

    # absolute_file_path = os.path.join(current_dir, relative_path)

    files = [
        ('files', open(FICHIER_OUTPUT, 'r',encoding='utf-8'))
    ]
    print(files)


    response = requests.request("POST", url, data=payload, files=files)
    print(response.encoding)
    print(response.text.encode('utf8'))
    print(response.status_code)