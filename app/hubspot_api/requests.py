# from msilib.schema import Error
import requests
import json
import os


def get_owner(api_key):
    url = "https://api.hubapi.com/crm/v3/owners"
    headers = {
      'content-type': 'application/json',
      'authorization': 'Bearer %s' % api_key
    }
    response = requests.request("GET", url,headers=headers)
    print(response.content)
    return response


class owner:
    id=0
    nb_cmpn = 0
    pourcentage_cmpn = 0
    
def getOwners_nb_cmpn(id_owner,api_key): #&hapikey="+str(api_key)
    url = "https://api.hubapi.com/crm/v3/objects/companies/search?properties=hubspot_owner_id"
    headers={
        'Content-type':'application/json', 
        'authorization': 'Bearer %s' %api_key

    }
    body = {
      "filterGroups": [
        {
          "filters": [
            {
              "value": id_owner,
              "propertyName": "hubspot_owner_id",
              "operator": "EQ"
            },
            {
              "value": "NEW",
              "propertyName": "hs_lead_status",
              "operator": "EQ"
            }
          ],

        }
      ],

      "limit": 30,
      "after": 0
    }
    response = requests.request("POST", url,json=body,headers=headers)
    
    
    print("total cpnm = "+str(response.json()['total']))
    return response.json()['total']



def assign_owner(df,list_owner):
    try:
        taille = len(df)
        cpt = 0
        ligne_prec = 0
        df=df.assign(owner="0")
        for i in list_owner :
            print("i id : "+str(i.id))
            if(cpt<len(list_owner)-1):
                # print("taille : "+str(taille) + " pourcentage : "+str(i.pourcentage_cmpn)+ ' = '+ str(int(taille * (i.pourcentage_cmpn/100))))
                j = 0
                range_top = int(taille * (i.pourcentage_cmpn/100))
                for j in range(ligne_prec,ligne_prec+range_top):
                    df['owner'][j] = i.id
                    # print(str(df['owner'][j]) + "pour j = " + str(j))
                # print("range = "+str(ligne_prec)+ ' to ' + str(ligne_prec+range_top))

                cpt_toj = int(taille * (i.pourcentage_cmpn/100))
                ligne_prec = ligne_prec + cpt_toj
            else:
                for j in range(ligne_prec,taille):
                    df['owner'][j] = i.id
                    # print(str(df['owner'][j]) + "pour j = " + str(j))

                # print("range else = "+str(ligne_prec)+ ' to ' + str(taille))

            cpt+=1
    except:
        print('Error in assign_owner' + str("!"))
    # print(df['owner'])
    return df
  
def getBacklList(apikey,after=0): 
    
  url = "https://api.hubapi.com/crm/v3/objects/companies/search"
  headers={
      'Content-type':'application/json', 
      'authorization': 'Bearer %s' %apikey
  }
  body = {
      "filterGroups": [
      {
          "filters": [
          {
              "value": "true",
              "propertyName": "blacklist",
              "operator": "EQ"
          },
          {
              "value": "Dead",
              "propertyName": "hs_lead_status",
              "operator": "EQ"
          },
          ],

      }
      ],

      "limit": 30,
      "after": after
  }
  response = requests.request("POST", url,json=body,headers=headers)
  # print("total cpnm = "+str(response.json()['total']))
  return response.json()

