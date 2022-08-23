# import os
# from msilib.schema import Error
import pandas as pd
import numpy as np
import copy
import netoyage_CDProject.netoyage_cdp as ncdp
from hubspot_api.requests import owner
from interface.interface import my_app
import netoyage_scrapio.netoyage as nsrap
import hubspot_api.requests as req
import hubspot_api.insertion as insert
import os
from os import environ, path
import google_sheet_api.sheet_api as sa



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
    # try:
    #     from PyInstaller.utils.hooks import collect_data_files
    #     collect_data_files(package=path_to_dat)
    #     print("Exec à partir du .exe")
    # except ModuleNotFoundError as error:
    #     print("Exec à partir du .py : "+str(error))
    
    FICHIER_OUTPUT = path_to_dat    
    # FICHIER_INPUT = "../EDITEUR_LOGICIELS.CSV"
    # FICHIER_INPUT = "../agence-de-voyage-paca.csv"
    FICHIER_INPUT=''
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
    my_app()
    FICHIER_INPUT = os.environ.get("FICHIER_INPUT")
    owner_selected = os.environ.get("owner_selected")
    hapikey = os.environ.get("hapikey")
    name_fichier = os.environ.get("name_fichier")


    print("Fichier input after : "+FICHIER_INPUT)
    print("Owener selected after : "+owner_selected)
    print("Hapikey after : "+hapikey)

    ####################
    
    
    
    try:
        df_csv = pd.read_csv (str(FICHIER_INPUT),encoding='utf8', )
    except:
        try:
            df_csv = pd.read_csv (str(FICHIER_INPUT),sep=";",encoding='utf8')
        except:
            try:
                df_csv = pd.read_csv (str(FICHIER_INPUT),sep="\t",encoding='utf8')
            except :
                try:
                    df_csv = pd.read_csv (str(FICHIER_INPUT),encoding='latin-1', )
                except:
                    try:
                        df_csv = pd.read_csv (str(FICHIER_INPUT),sep=";",encoding='latin-1')
                    except:
                        try:
                            df_csv = pd.read_csv (str(FICHIER_INPUT),sep="\t",encoding='latin-1')
                        except :
                            print('read_csv error : '+str("!"))

    print(df_csv.columns)
    df = pd.DataFrame(df_csv)
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
    # ref = sa.add_sheet(name_fichier,origine,len(df))
    # df = df.assign(Ref=ref)
    
    df = df.assign(Ref="TRY")
    df = df.assign(Nom_listing=name_fichier)
    return df


    

def netoyage_CdProject(df,FICHIER_OUTPUT,hapikey,owner_selected):
    df_CDProject_tmp = df.copy()
    (df_CDProject_tmp.drop_duplicates(subset =["Téléphone 1","Nom","Email"], keep = 'first', inplace=True))
    df_CDProject_tmp['Nom'] = df_CDProject_tmp.apply(lambda x: x['Nom'] if (not pd.isna(x['Email']) or (not pd.isna(x["Téléphone 2"]))
                                                            or (not pd.isna(x["Téléphone 1"]))) else np.nan, axis = 1)
    df_CDProject_tmp = df_CDProject_tmp[df_CDProject_tmp['Nom'].notna()]


    
    
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
    df_CDProject_tmp["Unique_Email"] = df_CDProject_tmp.apply(lambda x: x["Email"].split(" ")[0],axis = 1)
    
    #Netoyage global
    df_CDProject_tmp = ncdp.globalClean_CDProject(df_CDProject_tmp)

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
    df_CDProject_tmp = add_refListing(df_CDProject_tmp,os.environ.get("name_fichier"),"CdProject")

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


    # API Call
    # ASSIGN Des owner
    if(owner_selected=="random"):
        df_clean = assign_random_owner(hapikey,df_clean)
    else:
        df_clean = assign_specified_owner(df_clean,owner_selected)    
    #Ajout last col (lead status)
    df_clean = df_clean.assign(leadStatus="NEW")
    df_clean = add_refListing(df_clean,os.environ.get("name_fichier"),"scrap.io")
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
    # insert.insertion_hubspot(FICHIER_OUTPUT,hapikey,data)
    
    
    
    
    
    
if __name__ == "__main__":
    print("Lancement du programme . . . ")
    main()
else:
    print("Not working")