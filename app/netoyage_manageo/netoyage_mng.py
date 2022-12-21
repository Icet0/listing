import pandas as pd
import numpy as np
import re
import math
import os

def test():
    #MERGE Contacts and Company on Nom company
    myJson = [{
        "nom company":"Nom de l'entreprise 1",
        "Type principal":"Secteur A 1",
        "Site internet (url racine)":"https://facebook.com",
        "Téléphone":"0695342229",

    },
    {
        "nom company":"Nom de l'entreprise 2",
        "Type principal":"Secteur A 2",
        "Site internet (url racine)":"https://facebook.com",
        "Téléphone":"0695342229",
 
    },
    {
        "nom company":"Nom de l'entreprise 3",
        "Type principal":"Secteur A 3",
        "Site internet (url racine)":"https://facebook.com",
        "Téléphone":"0695342229",

    }]
    
    myJson2 = [{
        "company":"Nom de l'entreprise 1",
        "Nom contact":"Nom contact 1",
        "Prenom contact":"Prénom contact 1",
    },
    {
        "company":"Nom de l'entreprise 2",
        "Nom contact":"Nom contact 2",
        "Prenom contact":"Prénom contact 2",
    },
    ]
    dfc = pd.json_normalize(myJson2)

    df = pd.json_normalize(myJson)
    print(df)
    return df,dfc


#FUNCTION TO CLEAN THE DATA


def is_nan(x):
    return math.isnan(x)

def format_phone(phone):
    if(not is_nan(phone)):
        phone = int(phone)
        phone = '0'+str(phone)
        print(phone)
        # Enlève tous les caractères qui ne sont pas des chiffres
        prefixe = '+33'
        numero = phone.replace(' ', '')
        numero = numero.replace('-', '')
        numero = numero.replace('.', '')
        numero = prefixe + numero[1:]
        print("numero",numero)
        numero_formatte = numero[0:3] + ' ' + numero[3]+' '+' '.join([numero[i:i+2] for i in range(4, len(numero), 2)])
        print("numero_formatte",numero_formatte)
        return numero_formatte
    else:
        return phone
    
    
    
    
#FUNCTION TO CLEAN THE DATA END


    
def main():
    df_comp = pd.read_csv ("company.csv",encoding='utf8',sep=";")
    df_comp = pd.DataFrame(df_comp,columns=["Raison sociale","Adresse normée ligne 4","Adresse normée ligne 6","Ville","Code postal",
                                      "Libellé activité","Code activité","Date de création","Nom dirigeant principal","Prénom dirigeant principal",
                                      "Tranche Effectif INSEE","Tranche Effectif établissement","Téléphone","Email","Chiffre d'affaires"])
    print(df_comp["Email"])
    
    df_cont = pd.read_csv ("contact.csv",encoding='utf8',sep=";")
    df_cont = pd.DataFrame(df_cont,columns=[
        "Civilité","Nom","Prénom","Fonction","Raison sociale","Téléphone","Email"
    ])
    # print(df_cont[df_comp["Nom"].duplicated()==True])
    
    # clean des données
    df_comp['Téléphone'] = df_comp['Téléphone'].apply(format_phone)
    df_cont['Téléphone'] = df_cont['Téléphone'].apply(format_phone)
    
# main()