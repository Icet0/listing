
import pandas as pd
import numpy as np


def clean_TelInternational(input):
    if(not pd.isna(input) and input !='nan'):
        sInit = str(int(input))
        if(len(sInit) == 9):
            #def comme fr par défaut
            nb_pays = 2
            s = sInit
            s = ' '.join(s[i:i+2] for i in range(1, 9, 2))
            final = "+33 "+sInit[0]+' '+s
            
            
        else:
            
            ref_fr = 11 #2num après le +
            nb_pays = len(sInit)-ref_fr+2
            s = sInit[nb_pays+1:]
            s = ' '.join(s[i:i+2] for i in range(0, len(s), 2))
            final = "+"+sInit[0:nb_pays]+' '+sInit[nb_pays]+' '+s
        return final

    else:
        return "NaN"
    
def netoyage_tel(df):
    df_tmp = df.copy()
    for r in df.iterrows():
        tmp = str(r[1]["Téléphone"])
        if(tmp[0]!='n' and tmp[1]!='n'):
            tmp = tmp[1:]
            df_clean1 = clean_TelInternational(tmp.replace(' ',''))
        else:
            df_clean1 = np.nan
        df_tmp['Téléphone'].loc[r[0]] = df_clean1
        
    return df_tmp
    
def netoyage_telInternational(df):
    
    df_telClean = df.copy()
    for r in df.iterrows():
        df_clean1 = clean_TelInternational(r[1]["Téléphone international"])
    #     r[1]["Téléphone international"] = df_clean1
        df_telClean["Téléphone international"].loc[r[0]] = df_clean1
    return df_telClean


def netoyage_pageContacts(input):
    input["Page de contact 1"] = input.apply(lambda x : x["Page de contact 1"] +' '+ x["Page de contact 2"] 
                                                     if not pd.isna(x["Page de contact 2"])
                                                     else x["Page de contact 1"],axis=1)
    input["Page de contact 1"] = input.apply(lambda x : x["Page de contact 1"] +' '+ x["Page de contact 3"] 
                                                         if not pd.isna(x["Page de contact 3"])
                                                         else x["Page de contact 1"],axis=1)
    input["Page de contact 1"] = input.apply(lambda x : x["Page de contact 1"] +' '+ x["Page de contact 4"] 
                                                         if not pd.isna(x["Page de contact 4"])
                                                         else x["Page de contact 1"],axis=1)
    return input


def netoyage_pageLienRS(input):
    input = input.assign(Lien_réseaux_sociaux = np.nan)

    input["Lien_réseaux_sociaux"] = input.apply(lambda x : x["Tous les liens facebook"] +' '+ x["Tous les liens Youtube"] 
                                                         if not pd.isna(x["Tous les liens Youtube"])  and not pd.isna(x["Tous les liens facebook"])
                                                         else x["Tous les liens facebook"] if not pd.isna(x["Tous les liens facebook"])
                                                                                                   else x["Tous les liens Youtube"]
                                                                                                         if (not pd.isna(x["Tous les liens Youtube"]))
                                                                                                         else np.nan,axis=1)
                                                
    input["Lien_réseaux_sociaux"] = input.apply(lambda x : str(x["Lien_réseaux_sociaux"]) +' '+ x["Tous les liens Twitter"] 
                                                         if not pd.isna(x["Tous les liens Twitter"])
                                                         else x["Lien_réseaux_sociaux"]
                                                            if not pd.isna(x['Lien_réseaux_sociaux'])
                                                            else np.nan,axis=1)
    input["Lien_réseaux_sociaux"] = input.apply(lambda x : str(x["Lien_réseaux_sociaux"]) +' '+ x["Tous les liens Instagram"] 
                                                         if not pd.isna(x["Tous les liens Instagram"])
                                                         else x["Lien_réseaux_sociaux"]
                                                            if not pd.isna(x['Lien_réseaux_sociaux'])
                                                            else np.nan,axis=1)
    input["Lien_réseaux_sociaux"] = input.apply(lambda x : str(x["Lien_réseaux_sociaux"]) +' '+ x["Tous les liens Linkedin"] 
                                                         if not pd.isna(x["Tous les liens Linkedin"])
                                                         else x["Lien_réseaux_sociaux"]
                                                            if not pd.isna(x['Lien_réseaux_sociaux'])
                                                            else np.nan,axis=1)
    input["Lien_réseaux_sociaux"] = input.apply(lambda x: str(x["Lien_réseaux_sociaux"]).replace('\/','/'),axis=1)
    input["Lien_réseaux_sociaux"] = input.apply(lambda x: str(x["Lien_réseaux_sociaux"]).replace('["',''),axis=1)
    input["Lien_réseaux_sociaux"] = input.apply(lambda x: str(x["Lien_réseaux_sociaux"]).replace('"]',''),axis=1)                                            
    input["Lien_réseaux_sociaux"] = input.apply(lambda x: str(x["Lien_réseaux_sociaux"]).replace('","',' '),axis=1) 


    
    return input



# def netoyage_email(input):
#     input["Email"] = input.apply(lambda x : x["Email"] +' '+ x["Email 2"] 
#                                                          if not pd.isna(x["Email"])  and not pd.isna(x["Email 2"]) and x["Email 2"] not in x["Email"]
#                                                          else x["Email"] if not pd.isna(x["Email"])
#                                                                                                    else x["Email 2"]
#                                                                                                          if (not pd.isna(x["Email 2"]))
#                                                                                                          else np.nan,axis=1)
                                                
#     input["Email"] = input.apply(lambda x : x["Email"] +' '+ x["Email 3"] 
#                                                          if not pd.isna(x["Email 3"]) and x["Email 3"] not in x["Email"]
#                                                          else x["Email"]
#                                                             if not pd.isna(x['Email'])
#                                                             else np.nan,axis=1)
    
#     input["Email"] = input.apply(lambda x : x["Email"] +' '+ x["Email 4"] 
#                                                          if not pd.isna(x["Email 4"]) and x["Email 4"] not in x["Email"]
#                                                          else x["Email"]
#                                                             if not pd.isna(x['Email'])
#                                                             else np.nan,axis=1)
    
#     input["Email"] = input.apply(lambda x : x["Email"] +' '+ x["Email 5"] 
#                                                          if not pd.isna(x["Email 5"]) and x["Email 5"] not in x["Email"]
#                                                          else x["Email"]
#                                                             if not pd.isna(x['Email'])
#                                                             else np.nan,axis=1)
    
#     return input

def netoyage_email(input):
    mail_ban = ['prenom.nom@domaine.com','mail@mail.com']

    input["Email"] = input.apply(lambda x : str(x["Email"]) +' '+ str(x["Email 2"]) 
                                                         if not pd.isna(x["Email"])  and not pd.isna(x["Email 2"]) and x["Email 2"] not in x["Email"] 
                                 and x["Email"] not in mail_ban and x['Email 2'] not in mail_ban
                                                         else x["Email"] if not pd.isna(x["Email"]) and x["Email"] not in mail_ban
                                                                                                   else x["Email 2"]
                                                                                                         if (not pd.isna(x["Email 2"]) and x['Email 2'] not in mail_ban)
                                                                                                         else np.nan,axis=1)
                                                
    input["Email"] = input.apply(lambda x : str(x["Email"]) +' '+ str(x["Email 3"]) 
                                                         if not pd.isna(x["Email 3"]) and str(x["Email 3"]) not in str(x["Email"]) and str(x['Email 3']) not in mail_ban and str(x["Email"]) not in mail_ban
                                                         else x["Email"]
                                                                if (not pd.isna(x['Email']) and x["Email"] not in mail_ban)
                                                                else np.nan,axis=1)

    input["Email"] = input.apply(lambda x : str(x["Email"]) +' '+ str(x["Email 4"]) 
                                                         if not pd.isna(str(x["Email 4"])) and str(x["Email 4"]) not in str(x["Email"]) 
                                 and str(x['Email 4']) not in mail_ban and str(x["Email"]) not in mail_ban
                                                         else str(x["Email"])
                                                            if not pd.isna(str(x['Email'])) and str(x["Email"]) not in mail_ban
                                                            else np.nan,axis=1)
    
    input["Email"] = input.apply(lambda x : str(x["Email"]) +' '+ str(x["Email 5"]) 
                                                         if not pd.isna(str(x["Email 5"])) and str(x["Email 5"]) not in str(x["Email"]) 
                                 and str(x['Email 5']) not in mail_ban and str(x["Email"]) not in mail_ban
                                                         else str(x["Email"])
                                                            if not pd.isna(str(x['Email'])) and str(x["Email"]) not in mail_ban
                                                            else np.nan,axis=1)
    
    input["Email"] = input.apply(lambda x: x["Email"].replace(" nan",''),axis=1)
    input["Email"] = input.apply(lambda x: x["Email"].replace("nan",''),axis=1)

    return input

def netoyage_addr(input):
    input["Adresse 1"] = input.apply(lambda x : x["Adresse 1"] +' ; '+ x["Adresse 2"] 
                                                         if not pd.isna(x["Adresse 1"])  and not pd.isna(x["Adresse 2"]) and x["Adresse 2"] not in x["Adresse 1"]
                                                         else x["Adresse 1"] if not pd.isna(x["Adresse 1"])
                                                                                                   else x["Adresse 2"]
                                                                                                         if (not pd.isna(x["Adresse 2"]))
                                                                                                         else np.nan,axis=1)
    return input



def netoyage_codeZip(input):
    input["Code postal"] = input.apply(lambda x: str(x["Code postal"]),axis=1)
    input["Code postal"] = input.apply(lambda x: str(x["Code postal"]).replace('.0',''),axis=1)
    for i,item in input["Code postal"].iteritems():
        while(len(item)<5 and item != 'nan'):
            item = '0'+item
            input["Code postal"][i]=item
    
    return input


def globalClean(input):
    input = input.drop(['Google ID','Est fermé','Site internet','Page de contact 2','Page de contact 3',
                       'Page de contact 4','Page de contact 5','Tous les liens facebook','Tous les liens Youtube',
                       'Tous les liens Twitter','Tous les liens Instagram','Tous les liens Linkedin',
                       'Technologies du site','Pixels publicitaires du site','Adresse complète',
                       'Meta generator du site','Langue du site','District','Meta description du site',
                       'Meta keywords du site', 'Meta image du site','Caractéristiques',
                       "Heures d'ouverture",'Est revendiqué','Titre du site',
                       'Toutes les photos','Occupation','Photo 1','Photo 2',"Nombre d'avis par note",
                       'Nombre de photos',"Nombre d'avis","Note des avis",
                       "Gamme de prix","ID des avis","Lien Instagram",
                       'Lien Linkedin','Lien Youtube','Lien Twitter','Lien Facebook',
                       'ID du propriétaire','Email 2','Email 3','Email 4','Email 5','Adresse 2',
                       'Longitude','Latitude','Lien',
                       'Division de niveau 2','Nom du propriétaire','Tous les emails','Toutes les pages de contact',
                        'Code pays','Tous les types','État'
                       ],axis=1)
    
    input = input.rename(columns={"Type principal":"Activité"})
    input = input.rename(columns={"Téléphone international":"Téléphone suplémentaire"})
    input = input.rename(columns={"Division de niveau 1":"Etat"})
    input = input.assign(provenance="scrap.io")

    
        
    return input