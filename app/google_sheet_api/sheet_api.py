from __future__ import print_function
from datetime import date

import os.path
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1eOB8EAMsjkXuTK2bUnhvOm2N0PIbFO2eb8gfxCoa_b8'
SAMPLE_RANGE_NAME = 'A1:AA1000'


def connexion_gs():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'Credentials/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        # print('Name, Major:')
        # for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            # print('%s, %s' % (row[0], row[0]))
        
        
        return values,service

        
    except HttpError as err:
        print(err)



        
def add_sheet(nom,origine,nombre):   
    
    values,service = connexion_gs()
    
    ligne_suivante = len(values)
    
    REF = 'L'+str(ligne_suivante)
    Nom_Listing = nom
    Origine = origine
    Date = str(date.today())
    Nombre = nombre
    dico = [REF,Nom_Listing,Origine,Date,Nombre]
    # print(dico)
    df2 = pd.DataFrame([dico],columns=values[0])
    # print(df2)
    
    def Export_Data_To_Sheets():
        response_date = service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            valueInputOption='RAW',
            range='A'+str(len(values)+1)+':E',
            body=dict(
                majorDimension='ROWS',
                values=df2.T.reset_index().T.values.tolist()[1:])
        ).execute()
        print('Sheet successfully Updated')

    Export_Data_To_Sheets()
    return REF
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
