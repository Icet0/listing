# import os
from msilib.schema import Error
import pandas as pd
import numpy as np
import copy
from hubspot_api.requests import owner
from interface.interface import my_app
import netoyage_scrapio.netoyage as nsrap
import hubspot_api.requests as req
import hubspot_api.insertion as insert
import os
from os import path




def main():
    hapikey="eu1-d719-6d0e-44d8-8841-fa8980bbf069"
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
    os.environ['list_owner'] = str(list_owner)
    os.environ['FICHIER_INPUT'] = FICHIER_INPUT
    os.environ['owner_selected'] = owner_selected
    my_app()
    FICHIER_INPUT = os.environ.get("FICHIER_INPUT")
    owner_selected = os.environ.get("owner_selected")
    print("Fichier input after : "+FICHIER_INPUT)
    print("Owener selected after : "+owner_selected)

    ####################
    
    
    
    try:
        df_csv = pd.read_csv (str(FICHIER_INPUT),encoding='latin-1', )
    except:
        try:
            df_csv = pd.read_csv (str(FICHIER_INPUT),sep=";",encoding='latin-1')
        except Error:
            print('read_csv error : '+str(Error))

    print(df_csv.columns)
    df = pd.DataFrame(df_csv)
    if('Google ID' in df.columns):
        flag = 0
        netoyage_scraperIo(df,FICHIER_OUTPUT,hapikey,owner_selected)
    else: 
        flag = 1
        netoyage_CdProject(df,FICHIER_OUTPUT,hapikey,owner_selected)
        
        

    
    # if(flag):
    #     #On lance le nétoyage CD_Project
    #     netoyage_CdProject(df,FICHIER_OUTPUT,hapikey)
    # elif(not flag):
    #     df.drop(df.tail(3).index, 
    #         inplace = True)
    #     #On lance le nétoyage scrap.io
    #     netoyage_scraperIo(df,FICHIER_OUTPUT,hapikey)
        


def assign_random_owner(hapikey,df_clean,):
    response = req.get_owner(hapikey)
    nb_owner = len(response.json()['results'])
    list_owner = []
    for i in range(nb_owner):
        x = req.owner()
        x.id=response.json()['results'][i]['email']
        print("x id : "+str(x.id))
        x.nb_cmpn = req.getOwners_nb_cmpn(x.id,hapikey)
        list_owner.append(x)


    total_nv_cmpn_restante = 0
    moyenne_nb_cmpn = 0
    for item in list_owner:
        total_nv_cmpn_restante+=item.nb_cmpn
        moyenne_nb_cmpn+=item.nb_cmpn
    moyenne_nb_cmpn = moyenne_nb_cmpn / len(list_owner)
    print(total_nv_cmpn_restante)

    #On essaye de lisser le pourcentage pour prendre en compte les écarts
    if(moyenne_nb_cmpn > 0):
        coef_lissage = len(df_clean) / moyenne_nb_cmpn
    else:
        coef_lissage = 0


    # tab =[]
    # for item in list_owner:
    #     tab.append(item.nb_cmpn)
    # print(tab)
    # std = np.std(tab, axis=None)
    # coef_variation = std/moyenne_nb_cmpn
    # print("coef_variation : "+str(coef_variation))




    for item in list_owner:
        if(total_nv_cmpn_restante!=0):
            item.pourcentage_cmpn = ((item.nb_cmpn+coef_lissage)/(total_nv_cmpn_restante+(coef_lissage*len(list_owner)))) * 100    
        else:
            item.pourcentage_cmpn = 0
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
        
    df_clean = req.assign_owner(df_clean,list_owner)
    return df_clean

def assign_specified_owner(df_clean,owner):
    list_owner = []
    list_owner.append(req.owner())
    list_owner[0].id=owner
    return req.assign_owner(df_clean,list_owner)
    
    

def netoyage_CdProject(df,FICHIER_INTPUT,hapikey,owner_selected):
    print("Hello world")
    


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
            
    # RANDOM DATAFRAME
    df_clean = df_clean.sample(frac=1).reset_index(drop=True)
    
    #Spr des nan
    df_clean = df_clean.replace(np.nan,'')
    df_clean = df_clean.replace('nan','')
    df_clean = df_clean.replace('NaN','')
    


    # API Call
    # ASSIGN Des owner
    if(owner_selected=="random"):
        df_clean = assign_random_owner(hapikey,df_clean)
    else:
        df_clean = assign_specified_owner(df_clean,owner_selected)    
    
    #Ajout last col (lead status)
    df_clean = df_clean.assign(leadStatus="NEW")
    
    #Spr doublons
    df_clean.drop_duplicates(subset =["Téléphone","Nom","Email"], keep = 'first', inplace=True)

    # EXPORT CSV
    df_clean.to_csv(FICHIER_OUTPUT,index=False,encoding='utf-8')

    print("TO CSV OK !!")

    # EXPORT HUBSPOT
    insert.insertion_hubspot(FICHIER_OUTPUT,hapikey)
    
    
    
    
    
    
if __name__ == "__main__":
    print("Lancement du programme . . . ")
    main()
else:
    print("Not working")