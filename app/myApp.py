# import os
# from msilib.schema import Error
from asyncio import wait
import datetime
import json
import pandas as pd
import numpy as np
import copy

import requests
from urlParser.parser import extract_domain
from csvReader.reader import read_csv_with_autodetect
import netoyage_CDProject.netoyage_cdp as ncdp
from hubspot_api.requests import owner
from interface.interface import my_app
import netoyage_scrapio.netoyage as nsrap
import netoyage_manageo.netoyage_mng as mng
import hubspot_api.requests as req
import hubspot_api.insertion as insert
import os
from os import environ, path
import google_sheet_api.sheet_api as sa

import re
import unidecode
import hubspot_api.requests as hs

import time
from hubspot import HubSpot
from hubspot.crm.objects import SimplePublicObjectInput
import hubspot
from pprint import pprint
from hubspot.crm.companies import ApiException

def main():
    hapikey="pat-eu1-91242dbb-e9d6-4b77-852f-fdd31151e049"#"pat-eu1-26e9ca6f-6614-4320-b7eb-15b6ec1d25fe" cpt test
    bundle_dir = path.abspath(path.dirname(__file__)) 
    path_to_dat = path.join(bundle_dir,"OUTPUT")
    try:
        os.mkdir(path_to_dat)
    except OSError as error:
        print(error)
    path_to_dat = path.join(path_to_dat,"output_bundle.csv")

    print(path_to_dat)

    
    FICHIER_OUTPUT = path_to_dat    
    # FICHIER_INPUT = "../EDITEUR_LOGICIELS.CSV"
    # FICHIER_INPUT = "../agence-de-voyage-paca.csv"
    FICHIER_INPUT=''
    FICHIER_INPUT_CONTACT=''

    list_owner = []
    owner_selected = ""
    
    flag = 0
    df_csv = pd.DataFrame(None)
    #récupération de la liste d'owner pour l'attribution du listing
    response = req.get_owner(hapikey)
    nb_owner = len(response.json()['results'])
    for i in range(nb_owner):
        x=response.json()['results'][i]['email']
        list_owner.append(x)
    
    # list_owner.append("azertyuiop@gmail.com")
    #Lancement de l'app
    print("Fichier input before : "+FICHIER_INPUT)
    os.environ['hapikey'] = hapikey
    os.environ['list_owner'] = str(list_owner)
    os.environ['FICHIER_INPUT'] = FICHIER_INPUT
    os.environ['owner_selected'] = owner_selected
    os.environ["range_bot"] = "0" if os.environ.get("range_bot") == None else os.environ.get("range_bot")
    os.environ["range_top"] = "100" #Par défaut de 0 à 100
    
    # ? For manageo contact
    os.environ['FICHIER_INPUT_CONTACT'] = FICHIER_INPUT_CONTACT
    os.environ["range_bot_mngC"] = "0" if os.environ.get("range_bot_mngC") == None else os.environ.get("range_bot_mngC")
    os.environ["range_top_mngC"] = "100" #Par défaut de 0 à 100
    # ? -----------------
    
    my_app()
    
    
    FICHIER_INPUT = os.environ.get("FICHIER_INPUT")
    owner_selected = os.environ.get("owner_selected")
    hapikey = os.environ.get("hapikey")
    # name_fichier = os.environ.get("name_fichier")
    FICHIER_INPUT_CONTACT = os.environ.get('FICHIER_INPUT_CONTACT') #Manageo contact if exist

    print("Fichier input after : "+FICHIER_INPUT)
    print("Owener selected after : "+owner_selected)
    print("Hapikey after : "+hapikey)

    ####################
    df_contact = None
    if(os.environ.get('manageo') == 'True'):
        
        try:
            df_csv = pd.read_csv (str(FICHIER_INPUT),encoding='utf8', sep=";")
        except:
            print("Error manageo")
         #IF contact file exist
        if (FICHIER_INPUT_CONTACT != ''):
            try:
                df_csv_c = pd.read_csv (str(FICHIER_INPUT_CONTACT),encoding='utf8',sep=";" )
            except:
                print("Error manageo contact")
               
            print(df_csv_c.columns)
            df_contact = pd.DataFrame(df_csv_c)
        
    else:
    
        # try:
        #     df_csv = pd.read_csv (str(FICHIER_INPUT),encoding='utf8', )
        # except:
        #     try:
        #         df_csv = pd.read_csv (str(FICHIER_INPUT),sep="\t",encoding='utf8')     
        #     except:
        #         try:
        #             df_csv = pd.read_csv (str(FICHIER_INPUT),sep=";",encoding='utf8')
        #         except :
        #             try:
        #                 df_csv = pd.read_csv (str(FICHIER_INPUT),encoding='latin-1', )
        #             except:
        #                 try:
        #                     df_csv = pd.read_csv (str(FICHIER_INPUT),sep=";",encoding='latin-1')
        #                 except:
        #                     try:
        #                         df_csv = pd.read_csv (str(FICHIER_INPUT),sep="\t",encoding='latin-1')
        #                     except :
        #                         print('read_csv error : '+str("!"))
        
        df_csv = read_csv_with_autodetect(FICHIER_INPUT)
    if df_csv is not None:
        print("\nMy columns : \n",df_csv.columns)
    df = pd.DataFrame(df_csv)
           
    
    if(os.environ.get("manageo") == "True"):
        netoyage_manageo(df,FICHIER_OUTPUT,hapikey,owner_selected,df_contact)
    else:
        print("\n\n DF columns : ",len(df.columns))

        if('Google ID' in df.columns):
            flag = 0
            netoyage_scraperIo(df,FICHIER_OUTPUT,hapikey,owner_selected)
        else: 
            flag = 1
            netoyage_CdProject(df,FICHIER_OUTPUT,hapikey,owner_selected)
        
        


def assign_random_owner(hapikey,df_clean,nomTeams = "Sales"):
    response = req.get_owner(hapikey)
    nb_owner = len(response.json()['results'])
    list_owner = []
    for i in range(nb_owner):
        try:
            for t in response.json()['results'][i]['teams']:
                if t["name"]==nomTeams:

                    x = req.owner()
                    x.id=response.json()['results'][i]['id']
                    x.nb_cmpn = req.getOwners_nb_cmpn(x.id,hapikey)
                    x.id=response.json()['results'][i]['email']
                    print("x id : "+str(x.id))
                    list_owner.append(x)
                else:
                    raise
        except:
            print(str(i)+"  N'est pas dans la team "+nomTeams)


    total_nv_cmpn_restante = 0
    moyenne_nb_cmpn = 0
    for item in list_owner:
        total_nv_cmpn_restante+=item.nb_cmpn
        moyenne_nb_cmpn+=item.nb_cmpn
    moyenne_nb_cmpn = moyenne_nb_cmpn / len(list_owner)
    print("nb total nv cmpn restante : " + str(total_nv_cmpn_restante))

    #On essaye de lisser le pourcentage pour prendre en compte les écarts
    if(moyenne_nb_cmpn > 0):
        coef_lissage = len(df_clean) / moyenne_nb_cmpn
    else:
        coef_lissage = 0
    print('coef lissage ' + str(coef_lissage))


    for item in list_owner:
        if(total_nv_cmpn_restante!=0):
            item.pourcentage_cmpn = ((item.nb_cmpn+coef_lissage)/(total_nv_cmpn_restante+(coef_lissage*len(list_owner)))) * 100    
        else:
            item.pourcentage_cmpn = ( (len(df_clean)/(len(list_owner)))/ (len(df_clean)) ) * 100

        print(str(item.id) + ' : ' + str(item.pourcentage_cmpn))
        
    list_owner.sort(reverse = False,key=lambda x: x.pourcentage_cmpn) #On commence par le plus grand pourcentage
    #On doit maintenant inverser les pourcentages actuels avec ceux à recevoir (on échange le plus grand avec le plus petit, etc)
    i = 0
    j = len(list_owner)-1
    while (i<len(list_owner) and j>i):
        tmp = list_owner[i].pourcentage_cmpn
        list_owner[i].pourcentage_cmpn = list_owner[j].pourcentage_cmpn
        list_owner[j].pourcentage_cmpn = tmp
        i+=1
        j-=1
        
    df_clean_tmp = req.assign_owner((df_clean.copy()).reset_index(drop=True),list_owner)
    print(df_clean_tmp)
    return df_clean_tmp

def assign_specified_owner(df_clean,owner):
    list_owner = []
    list_owner.append(req.owner())
    list_owner[0].id=owner
    return req.assign_owner((df_clean.copy()).reset_index(drop=True),list_owner)
    
def add_refListing(df,name_fichier,origine):
    ref = sa.add_sheet(name_fichier,origine,len(df))
    df = df.assign(Ref=ref)
    
    # df = df.assign(Ref="TRY")
    df = df.assign(Nom_listing=name_fichier)
    return df,ref


    

def netoyage_CdProject(df,FICHIER_OUTPUT,hapikey,owner_selected):
    df_CDProject_tmp = df.copy()
    (df_CDProject_tmp.drop_duplicates(subset =["Téléphone 1","Nom"], keep = 'first', inplace=True))
    df_CDProject_tmp['Nom'] = df_CDProject_tmp.apply(lambda x: x['Nom'] if (not pd.isna(x['Email']) or (not pd.isna(x["Téléphone 2"]))
                                                            or (not pd.isna(x["Téléphone 1"]))) else np.nan, axis = 1)
    df_CDProject_tmp = df_CDProject_tmp[df_CDProject_tmp['Nom'].notna()]

    ##BLACK LIST
    df_CDProject_tmp = removeBlackList(df_CDProject_tmp,hapikey)
    
    
    #Clean tel 1
    df_CDProject_tmp = df_CDProject_tmp.rename(columns={"Téléphone 1":"Téléphone"})

    df_CDProject_tmp['Téléphone'] = df_CDProject_tmp.apply(lambda x: str(int(float(x['Téléphone']))) if(not pd.isna(x["Téléphone"]))
                                                        else str(x["Téléphone"]) ,axis=1)
    df_CDProject_tmp['Téléphone'] = df_CDProject_tmp.apply(lambda x: '+'+str(x["Téléphone"]) if( not pd.isna(x['Téléphone']) )
                                                                                                else str(x['Téléphone']),axis=1)

    df_CDProject_tmp = nsrap.netoyage_tel(df_CDProject_tmp)

    #CLean tel 2

    df_CDProject_tmp['Téléphone 2'] = df_CDProject_tmp.apply(lambda x: str(int(float(x['Téléphone 2']))) if(not pd.isna(x["Téléphone 2"]))
                                                        else str(x["Téléphone 2"]) ,axis=1)
    df_CDProject_tmp['Téléphone 2'] = df_CDProject_tmp.apply(lambda x: '+'+str(x["Téléphone 2"]) if( not pd.isna(x['Téléphone 2']) )
                                                                                                else str(x['Téléphone 2']),axis=1)

    df_CDProject_tmp = ncdp.netoyage_tel2(df_CDProject_tmp)


        #Adresse
    df_CDProject_tmp = df_CDProject_tmp.rename(columns={"Adresse 2":"Adresse"})
    df_CDProject_tmp = df_CDProject_tmp.rename(columns={"Adresse 1":"Adresse complémentaire"})


    #Clean effectif
    df_CDProject_tmp['Effectif'] = df_CDProject_tmp.apply(lambda x: x["Effectif"] if (x["Effectif"] != "0" and x["Effectif"]!= "Inconnu")
                                                        else '',axis = 1)

    #Activités
    df_CDProject_tmp = df_CDProject_tmp.rename(columns={"Activité.1":"Activités secondaires"})
    df_CDProject_tmp = ncdp.netoyage_activites(df_CDProject_tmp)

    #Unique EMail
    df_CDProject_tmp = df_CDProject_tmp.assign(Unique_Email="")
    df_CDProject_tmp["Unique_Email"] = df_CDProject_tmp.apply(lambda x: str(x["Email"]).split(" ")[0],axis = 1)
    
    #Netoyage global
    df_CDProject_tmp = ncdp.globalClean_CDProject(df_CDProject_tmp)

    print("TAILLE AVANT : "+str(len(df_CDProject_tmp)))
    print("RANGE : "+str(int(os.environ.get("range_bot")))+str(int(os.environ.get("range_top"))))
    ## A GERER ##
    df_CDProject_tmp = df_CDProject_tmp.iloc[int(os.environ.get("range_bot")):int(os.environ.get("range_top"))] #Mettre un bouton pour gérer ça
    # df_clean = df_clean.iloc[0:220]
    print("TAILLE APRES : "+str(len(df_CDProject_tmp)))

    # RANDOM DATAFRAME
    df_CDProject_tmp = df_CDProject_tmp.sample(frac=1).reset_index(drop=True)
    print("TAILLE APRES après : "+str(len(df_CDProject_tmp)))


    #Spr des nan
    df_CDProject_tmp = df_CDProject_tmp.replace(np.nan,'')
    df_CDProject_tmp = df_CDProject_tmp.replace('nan','')
    df_CDProject_tmp = df_CDProject_tmp.replace('NaN','')
    


    
    
    #Spr doublons
    df_CDProject_tmp.drop_duplicates(subset =["Téléphone","Nom","Email"], keep = 'first', inplace=True)
    df_CDProject_tmp = df_CDProject_tmp[(df_CDProject_tmp['Téléphone'] != '' ) | (df_CDProject_tmp['Email'] !='') | (df_CDProject_tmp['Téléphone 2']!= '')]

    # API Call
    # ASSIGN Des owner
    if(owner_selected=="random"):
        df_CDProject_tmp = assign_random_owner(hapikey,df_CDProject_tmp)
    else:
        df_CDProject_tmp = assign_specified_owner(df_CDProject_tmp,owner_selected)    
    
    #Ajout last col (lead status)
    df_CDProject_tmp = df_CDProject_tmp.assign(leadStatus="NEW")
    df_CDProject_tmp = df_CDProject_tmp.assign(id="NAN")
    df_CDProject_tmp['id'] = df_CDProject_tmp['Nom']+df_CDProject_tmp['Téléphone']
    
    df_CDProject_tmp,_ = add_refListing(df_CDProject_tmp,os.environ.get("name_fichier"),"CdProject")

    # EXPORT CSV
    df_CDProject_tmp.to_csv(FICHIER_OUTPUT,index=False,encoding='utf-8')

    print("TO CSV OK !!")
    data = {
    "name": os.environ.get("name_fichier"),
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
                "columnName": "Adresse",
                "propertyName": "address2",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Adresse complémentaire",
                "propertyName": "address",
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
                "columnName": "Ville",
                "propertyName": "city",
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
                "columnName": "Téléphone suplémentaire",
                "propertyName": "telephones",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "NAF",
                "propertyName": "naf",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "SIRET",
                "propertyName": "siret",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Effectif",
                "propertyName": "effectif",
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
                "columnName": "Responsable",
                "propertyName": "responsable",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Activité",
                "propertyName": "secteurs_d_activite",
                "idColumnType": None
            },
                
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Type principal",
                "propertyName": "activites_secondaire",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Unique_Email",
                "propertyName": "email",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "provenance",
                "propertyName": "provenance_donnee",
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
                #UNIQUE DOUBLONS
                {
                "columnObjectTypeId": "0-2",
                "columnName": "id",
                "propertyName": "id_unique",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Ref",
                "propertyName": "ref",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Nom_listing",
                "propertyName": "nom_du_listing",
                "idColumnType": None
            },
                
                
            ]
        }
        }
    ]
    }
    
    # EXPORT HUBSPOT
    insert.insertion_hubspot(FICHIER_OUTPUT,hapikey,data)
    


def netoyage_scraperIo(df,FICHIER_OUTPUT,hapikey,owner_selected):
    


    # DEBUT NETOYAGE
    df_telClean = nsrap.netoyage_telInternational(df)
    df_telClean = nsrap.netoyage_tel(df_telClean)
    df_telClean = nsrap.netoyage_pageContacts(df_telClean)
    df_RSClean = nsrap.netoyage_pageLienRS(df_telClean)
    df_EmailClean = nsrap.netoyage_email(df_RSClean)
    df_addrClean = nsrap.netoyage_addr(df_EmailClean)
    df_zipClean = nsrap.netoyage_codeZip(df_addrClean)
    df_clean = df_zipClean
    
    
    try:
        df_clean = nsrap.globalClean(df_clean)
    except KeyError:
            print("ERROR : "+str(ValueError))
            
            
    print("TAILLE AVANT : "+str(len(df_clean)))
    print("RANGE : "+str(int(os.environ.get("range_bot")))+str(int(os.environ.get("range_top"))))
    ## A GERER ##
    df_clean = df_clean.iloc[int(os.environ.get("range_bot")):int(os.environ.get("range_top"))] #Mettre un bouton pour gérer ça
    # df_clean = df_clean.iloc[0:220]
    print("TAILLE APRES : "+str(len(df_clean)))

    # RANDOM DATAFRAME
    df_clean = df_clean.sample(frac=1).reset_index(drop=True)
    print("TAILLE APRES après : "+str(len(df_clean)))

    #Spr des nan
    df_clean = df_clean.replace(np.nan,'')
    df_clean = df_clean.replace('nan','')
    df_clean = df_clean.replace('NaN','')
    



    
    #Spr doublons
    df_clean.drop_duplicates(subset =["Téléphone","Nom"], keep = 'first', inplace=True)
    df_clean = df_clean[(df_clean['Téléphone'] != '' ) | (df_clean['Email'] !='') | (df_clean['Téléphone suplémentaire']!= '')
                   | (df_clean['Site internet (url racine)']!= '') | (df_clean['Page de contact 1']!= '')]

    #URL Only ! (Second unique id)
    df.clean = df_clean[df_clean['Site internet (url racine)'] != '']
    

    # ? BLACLIST
    df_clean = removeBlackList(df_clean,apikey=hapikey)

    # API Call
    # ASSIGN Des owner
    if(owner_selected=="random"):
        df_clean = assign_random_owner(hapikey,df_clean)
    else:
        df_clean = assign_specified_owner(df_clean,owner_selected)    
    #Ajout last col (lead status)
    df_clean = df_clean.assign(leadStatus="NEW")
    df_clean = df_clean.assign(id="NAN")
    df_clean['id'] = df_clean['Nom']+df_clean['Téléphone']
    df_clean['url_unique'] = df_clean['Site internet (url racine)'].apply(extract_domain)
    # print(df_clean['id'])
    # print(df_clean['url_unique'])

    df_clean,_ = add_refListing(df_clean,os.environ.get("name_fichier"),"scrap.io")
    # EXPORT CSV
    df_clean.to_csv(FICHIER_OUTPUT,index=False,encoding='utf-8')

    print("TO CSV OK !!")
    
    data = {
    "name": os.environ.get("name_fichier"),
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
                "columnName": "Téléphone suplémentaire",
                "propertyName": "telephones",
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
                "columnName": "Etat",
                "propertyName": "state",
                "idColumnType": None
            },

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
                "columnName": "Unique_Email",
                "propertyName": "email",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "provenance",
                "propertyName": "provenance_donnee",
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
                #UNIQUE DOUBLONS
            {
                "columnObjectTypeId": "0-2",
                "columnName": "id",
                "propertyName": "id_unique",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "url_unique",
                "propertyName": "url_unique",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Ref",
                "propertyName": "ref",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Nom_listing",
                "propertyName": "nom_du_listing",
                "idColumnType": None
            },
                
            ]
        }
        }
    ]
    }
    # EXPORT HUBSPOT
    insert.insertion_hubspot(FICHIER_OUTPUT,hapikey,data)
    
def netoyage_manageo(df,FICHIER_OUTPUT,hapikey,owner_selected,df_contact=None):
    #NETOYAGE MANAGEO selection des colonnes
    manageoDF = pd.DataFrame(df,columns=['Siret','Raison sociale', 'Adresse normée ligne 4', 'Ville', 'Code postal', 'Libellé activité','Code activité', 'Date de création',
                                            'Tranche Effectif INSEE','Téléphone', "Site Web","Facebook","Twitter","LinkedIn", 'Email', "Chiffre d'affaires" , 'Nom dirigeant principal', 'Prénom dirigeant principal'])
    manageoDFC = None
    if(df_contact is not None):
        manageoDFC = pd.DataFrame(df_contact,columns=['Civilité','Nom', 'Prénom', 'Fonction', 'Raison sociale', 'Téléphone',"Site Web","Facebook","Twitter","LinkedIn",'Email'])

    # df_comp,dfc = mng.test()
    df_comp = manageoDF.copy()
    if(manageoDFC is not None):
        dfc = manageoDFC.copy()
    else:
        dfc = None
        
            
    #DF COMP NETOYAGE
    #rename colonne Raison sociale in Nom
    df_comp = df_comp.replace(np.nan,'')
    df_comp = df_comp.replace('nan','')
    df_comp = df_comp.replace('NaN','')
    df_comp['Téléphone'] = df_comp['Téléphone'].apply(mng.format_phone)
    df_comp['Date de création'] = df_comp['Date de création'].apply(mng.getAnneeCreation)
    df_comp = df_comp.assign(responsable="NAN")
    df_comp['responsable'] = df_comp['Nom dirigeant principal'].astype(str) +" "+ df_comp["Prénom dirigeant principal"]
    df_comp = df_comp.drop(['Nom dirigeant principal','Prénom dirigeant principal'],axis=1)     
    df_comp.rename(columns={'Adresse normée ligne 4':'adresse'}, inplace=True)
    df_comp.rename(columns={'Libellé activité':'la'}, inplace=True)
    df_comp.rename(columns={'Code activité':'ca'}, inplace=True)
    df_comp.rename(columns={'Date de création':'dc'}, inplace=True)
    df_comp = mng.netoyage_email(df_comp)
    # ----------------- #
    
    # ? BLACK LIST
    df_comp.rename(columns={'Raison sociale':'Nom'}, inplace=True)
    df_comp = removeBlackList(df_comp,apikey=hapikey)
    
    #! Gestion de la taille des fichiers d'imports
    print("TAILLE AVANT : "+str(len(df_comp)))
    print("RANGE : "+str(int(os.environ.get("range_bot")))+str(int(os.environ.get("range_top"))))
    ## A GERER ##
    df_comp = df_comp.iloc[int(os.environ.get("range_bot")):int(os.environ.get("range_top"))] #Mettre un bouton pour gérer ça
    # df_clean = df_clean.iloc[0:220]
    print("TAILLE APRES : "+str(len(df_comp)))

    # RANDOM DATAFRAME
    df_comp = df_comp.sample(frac=1).reset_index(drop=True)
    print("TAILLE APRES après : "+str(len(df_comp)))
    
    # * Gestion des contacts
    if(dfc is not None):
        print("TAILLE AVANT CONTACT: "+str(len(dfc)))
        print("RANGE : "+str(int(os.environ.get("range_bot_mngC")))+str(int(os.environ.get("range_top_mngC"))))
        ## A GERER ##
        dfc = dfc.iloc[int(os.environ.get("range_bot_mngC")):int(os.environ.get("range_top_mngC"))] #Mettre un bouton pour gérer ça
        # df_clean = df_clean.iloc[0:220]
        print("TAILLE APRES CONTACT: "+str(len(dfc)))

        # RANDOM DATAFRAME
        dfc = dfc.sample(frac=1).reset_index(drop=True)
        print("TAILLE APRES après CONTACT : "+str(len(dfc)))
    
    
    
    #! ----------------- #
    
    
    
    if(dfc is not None):
        #DFC NETOYAGE
        dfc = dfc.replace(np.nan,'')
        dfc = dfc.replace('nan','')
        dfc = dfc.replace('NaN','')
        dfc['Téléphone'] = dfc['Téléphone'].apply(mng.format_phone)
        
        # ----------------- #
    
    
    #Ajout last col (lead status)
    if(owner_selected=="random"):
        df_comp = assign_random_owner(hapikey,df_comp)
    else:
        df_comp = assign_specified_owner(df_comp,owner_selected)
        if(dfc is not None):
            dfc = assign_specified_owner(dfc,owner_selected)
        
    df_comp = df_comp.assign(leadStatus="NEW")
    df_comp = df_comp.assign(id="NAN")
    df_comp['id'] = df_comp['Nom']+df_comp['Téléphone']
    df_comp['url_unique'] = df_comp['Site Web'].apply(extract_domain)

    print(df_comp['id'])
    # df_clean = add_refListing(df_clean,os.environ.get("name_fichier"),"manageo")
    
    if(dfc is not None):
        dfc = dfc.assign(leadStatus="NEW")
        dfc = dfc.assign(id="NAN") 
        dfc.rename(columns={"Raison sociale": "companyID"}, inplace=True)
        dfc['id'] = dfc['Nom']+dfc['companyID']+dfc['Téléphone']+dfc['Email']
        
        dfc = dfc.drop_duplicates(subset=["id"], keep="first")



    
    df_comp,ref = add_refListing(df_comp,os.environ.get("name_fichier"),"Manageo")

    # EXPORT CSV
    df_comp.to_csv(FICHIER_OUTPUT,index=False,encoding='utf-8')
        
            
    
    
    #EXPORT HUBSPOT 
    data = {
    "name": os.environ.get("name_fichier"),
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
                "columnName": "Siret",
                "propertyName": "siret",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Nom",
                "propertyName": "name",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "adresse",
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
                "columnName": "la",
                "propertyName": "secteurs_d_activite",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "ca",
                "propertyName": "code_activite",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "dc",
                "propertyName": "founded_year",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Tranche Effectif INSEE",
                "propertyName": "effectif",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Téléphone",
                "propertyName": "phone",
                "idColumnType": None
            },
            #RAJOUT ENTREPRISE 2.0
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Site Web",
                "propertyName": "website",
                "idColumnType": None
            },#"Facebook";"Twitter";"LinkedIn"
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Facebook",
                "propertyName": "facebook_company_page",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Twitter",
                "propertyName": "twitterhandle",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "LinkedIn",
                "propertyName": "linkedin_company_page",
                "idColumnType": None
            },
            #---------------------
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Email",
                "propertyName": "emails",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Chiffre d'affaires",
                "propertyName": "annualrevenue",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "responsable",
                "propertyName": "responsable",
                "idColumnType": None
            },
            {
                "columnObjectTypeId": "0-2",
                "columnName": "Unique_Email",
                "propertyName": "email",
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
                #UNIQUE DOUBLONS
            {
                "columnObjectTypeId": "0-2",
                "columnName": "id",
                "propertyName": "id_unique",
                "idColumnType": None
            },{
                "columnObjectTypeId": "0-2",
                "columnName": "url_unique",
                "propertyName": "url_unique",
                "idColumnType": None
            },

            {
                "columnObjectTypeId": "0-2",
                "columnName": "Ref",
                "propertyName": "ref",
                "idColumnType": None
            },
                {
                "columnObjectTypeId": "0-2",
                "columnName": "Nom_listing",
                "propertyName": "nom_du_listing",
                "idColumnType": None
            },
            
                
            ]
        }
        }
    ]
    }


    # EXPORT HUBSPOT
    import_id = insert.insertion_hubspot(FICHIER_OUTPUT,hapikey,data)
    
    
    # wait = input("Appuyez sur une touche pour continuer . . . ")
    #Attendre la fin de l'importation
    if import_id != None:
        try:
            while req.check_import_status(hapikey,import_id) != "DONE":
                time.sleep(10)
        except:
            print("Erreur d'importation")
            pass
        
    if(dfc is not None):
        time.sleep(10)#For get all Companies
        myCompanies = getAllCompanies(hapikey,ref)
        for index, row in dfc.iterrows():
            company_name = row['companyID']
            if(company_name in myCompanies['properties.name'].values):
                idx = myCompanies[myCompanies['properties.name'] == company_name].index[0]
                company_id = myCompanies['id'][idx]
            else:
                company_id = None
            dfc.at[index, 'companyID'] = company_id
        
        
        dfc = dfc.dropna(subset=['companyID'])
        dfc_copy = dfc.copy()
        dfc.drop(['companyID'], axis=1, inplace=True)
        dfc.to_csv(FICHIER_OUTPUT,index=False,encoding='utf-8')
        
        #! Stop l'importation des contacts sans entreprises
        # enterprise_list = df_comp['Nom'].unique().tolist()
        # # Iterate over the contacts and import only if the associated enterprise is in the list
        # for cmp in dfc['Raison sociale']:
        #     if cmp not in enterprise_list:
        #         # import contact
        #! ------------------------------------------------   
        
        data_contact = {
            "name": os.environ.get("name_fichier")+"contacts",
            "files": [
                {
                "fileName": "output_bundle.csv",
                "fileFormat": "CSV",
                "dateFormat": "DAY_MONTH_YEAR",
                "fileImportPage": {
                    "hasHeader": True,
                    "columnMappings": [
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Civilité",
                            "propertyName": "salutation",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Nom",
                            "propertyName": "lastname",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Prénom",
                            "propertyName": "firstname",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Fonction",
                            "propertyName": "job_function",
                            "idColumnType": None
                        },
                        # {
                        #     "columnObjectTypeId": "0-2",
                        #     "columnName": "companyID",
                        #     "propertyName": "hs_object_id",
                        #     "idColumnType": None
                        # },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Téléphone",
                            "propertyName": "phone",
                            "idColumnType": None
                        },
                        #RAJOUT ENTREPRISE 2.0
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Site Web",
                            "propertyName": "website",
                            "idColumnType": None
                        },#"Facebook";"Twitter";"LinkedIn"
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Facebook",
                            "propertyName": "hs_facebook_click_id",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Twitter",
                            "propertyName": "twitterhandle",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "LinkedIn",
                            "propertyName": "lgm_linkedinurl",
                            "idColumnType": None
                        },
                        #---------------------
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Email",
                            "propertyName": "email",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "owner",
                            "propertyName": "hubspot_owner_id",
                            "idColumnType": None
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "leadStatus",
                            "propertyName": "hs_lead_status",
                            "idColumnType": None
                        },
                            #UNIQUE DOUBLONS
                            {
                            "columnObjectTypeId": "0-1",
                            "columnName": "id",
                            "propertyName": "id_unique",
                            "idColumnType": None
                        },
                    ]
                }
            }
        ]
        }
        
        
        # #!--------
        # wait = input("Appuyez sur une touche pour continuer . . . ")
        import_id = insert.insertion_hubspot(FICHIER_OUTPUT,hapikey,data_contact)
        if import_id != None:
            try:
                while req.check_import_status(hapikey,import_id) != "DONE":
                    time.sleep(10)
            except:
                print("Erreur d'importation")
                pass
            
        contacts = getAllContacts(hapikey)
        companies_id = []
        contacts_id = []
        for index, row in dfc_copy.iterrows():
            if row['id'] in contacts['properties.id_unique'].values:
                company_id = row.get('companyID')
                idx = contacts[contacts['properties.id_unique'] == row['id']].index[0]
                contact_id = contacts['id'][idx]
                if company_id is not None:
                    # req.setAssociation(hapikey, contact_id, company_id)
                    companies_id.append(company_id)
                    contacts_id.append(contact_id)
                else:
                    print(f"No company ID found for contact {contact_id}, skipping company association.")
            else:
                print(f"No contact found for  {row['id']}, skipping company association.")
        req.setAssociation(hapikey, contacts_id, companies_id)

def getCompany(company_name,apikey):

#! Ne par chercher par nom, get all company et ensuite faire un filtre sur le nom
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
                "value": company_name,
                "propertyName": "name",
                "operator": "EQ"
            },

            ],

        }
        ],

        "limit": 30,
        "after": 0
    }
    response = requests.request("POST", url,json=body,headers=headers)

    # Vérification de la réponse de l'API
    if response.status_code == 200:
        response_data = json.loads(response.content.decode('utf-8'))
        if len(response_data['results']) > 0:
            # Récupération de l'ID de l'entreprise correspondante
            company_id = response_data['results'][0]['id']
        else:
            # L'entreprise n'a pas été trouvée
            company_id = None
    else:
        # La requête a échoué
        company_id = None
        # print('Erreur: Impossible de récupérer l\'ID de l\'entreprise')
    return company_id

def getAllCompanies(apikey,ref):
    companies = []
    NOW = datetime.datetime.now()
    ONE_HOUR_AGO = NOW - datetime.timedelta(minutes=10)
    EPOCH_ONE_HOUR_AGO = int(ONE_HOUR_AGO.timestamp() * 1000)
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    headers={
        'Content-type':'application/json', 
        'authorization': 'Bearer %s' %apikey
    }
    after = 0
    body = {
        "filterGroups": [
        {
            "filters": [
                {
                    "value": ref,
                    "propertyName": "ref",
                    "operator": "EQ"
                },
            ],

        }],
        "properties": [
            "id_unique",
            "hs_object_id",
            "name"
        ],
        "limit": 100,
        "after": after
    }
    response = requests.request("POST", url,json=body,headers=headers)
    response.raise_for_status()

    data = response.json()
    print("total cpnm = "+str(data['total']))

    companies.extend(data['results'])
    time.sleep(2)
    while data.get("paging"):
        after = data['paging']['next']['after']
        body = {
        "filterGroups": [
        {
            "filters": [
                {
                    "value": ref,
                    "propertyName": "ref",
                    "operator": "EQ"
                },
            ],

        }],
            "properties": [
                "id_unique",
                "hs_object_id",
                "name"
            ],
            "limit": 100,
            "after": after
        }
        response = requests.request("POST", url,json=body,headers=headers)
        response.raise_for_status()
        data = response.json()
        companies.extend(data['results'])

        time.sleep(2)
        # Normalisation du JSON en DataFrame
    df = pd.json_normalize(companies)

    print("myDF",df)
    try:
        # Sélection des colonnes souhaitées
        df = df.loc[:, ['id', 'properties.name','properties.id_unique']]    
    except:
        print("erreur")
    return df
    
    
def getAllContacts(apikey):
    companies = []
    NOW = datetime.datetime.now()
    ONE_HOUR_AGO = NOW - datetime.timedelta(minutes=10)
    EPOCH_ONE_HOUR_AGO = int(ONE_HOUR_AGO.timestamp() * 1000)
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers={
        'Content-type':'application/json', 
        'authorization': 'Bearer %s' %apikey
    }
    after = 0
    body = {
        "filterGroups": [
        {
            "filters": [
                {
                    "value": EPOCH_ONE_HOUR_AGO,
                    "propertyName": "createdate",
                    "operator": "GT"
                },
            ],

        }],
        "properties": [
            "id_unique",
        ],
        "limit": 100,
        "after": after
    }
    response = requests.request("POST", url,json=body,headers=headers)
    response.raise_for_status()

    data = response.json()
    print("total contact = "+str(data['total']))

    companies.extend(data['results'])
    time.sleep(2)

    while data.get("paging"):
        after = data['paging']['next']['after']
        body = {
        "filterGroups": [
        {
            "filters": [
                {
                    "value": EPOCH_ONE_HOUR_AGO,
                    "propertyName": "createdate",
                    "operator": "GT"
                },
            ],

        }],
            "properties": [
                "id_unique",
            ],
            "limit": 100,
            "after": after
        }
        response = requests.request("POST", url,json=body,headers=headers)
        response.raise_for_status()
        data = response.json()
        companies.extend(data['results'])

        time.sleep(2)

        # Normalisation du JSON en DataFrame
    df = pd.json_normalize(companies)

    # Sélection des colonnes souhaitées
    df = df.loc[:, ['id','properties.id_unique']]    
    return df
    
def getContact(contact_UniqueID,apikey):
#! Ne par chercher par nom, get all contact et ensuite faire un filtre sur le nom

    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers={
        'Content-type':'application/json', 
        'authorization': 'Bearer %s' %apikey
    }
    body = {
        "filterGroups": [
        {
            "filters": [
            {
                "value": contact_UniqueID,
                "propertyName": "id_unique",
                "operator": "EQ"
            },

            ],

        }
        ],

        "limit": 30,
        "after": 0
    }
    response = requests.request("POST", url,json=body,headers=headers)

    # Vérification de la réponse de l'API
    if response.status_code == 200:
        response_data = json.loads(response.content.decode('utf-8'))
        if len(response_data['results']) > 0:
            # Récupération de l'ID de l'entreprise correspondante
            contact_id = response_data['results'][0]['id']
        else:
            # L'entreprise n'a pas été trouvée
            contact_id = None
    else:
        # La requête a échoué
        contact_id = None
        print('Erreur: Impossible de récupérer l\'ID de l\'entreprise')
    return contact_id

def myBacklList(apikey):
    myrep = hs.getBacklList(apikey)
    print("total cpnm = "+str(myrep['total']))
    blackList = []
    try:
        while myrep.get('paging'):
            print("after = "+str(myrep['paging']['next']['after']))
            after = myrep['paging']['next']['after']
            for i in myrep['results']:
                blackList.append(i) #AGERER ICI
            myrep = hs.getBacklList(apikey,after)
        else:
            for i in myrep['results']:
                blackList.append(i)
    except Exception as e:
        print("end of list ",e)
    return blackList


def removeBlackList(df,apikey):
    blackList = myBacklList(apikey)
    df['name_normalized'] = df['Nom'].apply(lambda x: unidecode.unidecode(re.sub(r'[^\w\s_]', '', x.lower()).replace('_',' ')))
    # Créer une liste contenant les noms normalisés de la blacklist
    blacklist_names = [unidecode.unidecode(re.sub(r'[^\w\s_]', '', item['properties']['name'].lower()).replace('_',' ')) for item in blackList]

    print(blacklist_names)
    # Sélectionner les lignes du dataframe qui ont une valeur dans la colonne "name_normalized" qui est dans la liste de la blacklist
    # df_filtered = df[~df['name_normalized'].isin(blacklist_names)]
    
    # Sélectionner les lignes du DataFrame qui ont une valeur dans la colonne "name_normalized"
    # qui contient une chaîne de caractères de la liste de la blacklist
    
    # df_filtered = df[df['name_normalized'].str.contains('|'.join(blacklist_names))]
    # séparés par un espace (\b pour matcher les limites de mots)
    blacklist_regex = r'\b(?:%s)\b' % '|'.join(map(re.escape, blacklist_names))

    df_filtered = df[df['name_normalized'].str.contains(blacklist_regex)]
    # Filtrer les noms d'entreprises blacklistées
    df_blacklisted = df[df['name_normalized'].str.contains(blacklist_regex)]

    # Créer une liste de tuples contenant les noms d'entreprises blacklistées 
    # et les correspondances de blacklist_names qui les ont faites figurer dans la liste noire
    blacklisted_names = [(name, [item for item in blacklist_names if re.search(r'\b%s\b' % re.escape(item), name)]) for name in df_blacklisted['name_normalized']]

    # Afficher la liste de noms d'entreprises blacklistées et leurs correspondances de blacklist_names
    for name, matches in blacklisted_names:
        print(f"{name} a été blacklisté en raison de : {matches}")

    # Supprimer la colonne "name_normalized" du dataframe final
    df = df.drop(df_filtered.index)
    df = df.drop('name_normalized', axis=1)

    return df
    
if __name__ == "__main__":
    print("Lancement du programme . . . ")
    main()
else:
    print("Not working")