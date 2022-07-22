import numpy as np
import pandas as pd
from netoyage_scrapio.netoyage import clean_TelInternational


def netoyage_tel2(df):
    df_tmp = df.copy()
    for r in df.iterrows():
        tmp = str(r[1]["Téléphone 2"])

        if(tmp[0]!='n' and tmp[1]!='n'):
            tmp = tmp[1:]
            df_clean1 = clean_TelInternational(tmp.replace(' ',''))
        else:
            df_clean1 = np.nan
        df_tmp['Téléphone 2'].loc[r[0]] = df_clean1
        
    return df_tmp




def netoyage_activites(input):
    try:
        input["Activités secondaires"] = input.apply(lambda x : str(x["Activités secondaires"]) +' / '+ str(x["Activité.2"]) 
                                                            if not pd.isna(x["Activités secondaires"])  and not pd.isna(x["Activité.2"]) and x["Activité.2"] not in x["Activités secondaires"] 
                                                            else x["Activités secondaires"] if not pd.isna(x["Activités secondaires"])
                                                                                                    else x["Activité.2"]
                                                                                                            if (not pd.isna(x["Activité.2"]))
                                                                                                            else np.nan,axis=1)
                                                    
        input["Activités secondaires"] = input.apply(lambda x : str(x["Activités secondaires"]) +' / '+ str(x["Activité.3"]) 
                                                            if not pd.isna(x["Activité.3"]) and str(x["Activité.3"]) not in str(x["Activités secondaires"])
                                                            else x["Activités secondaires"]
                                                                    if (not pd.isna(x['Activités secondaires']))
                                                                    else np.nan,axis=1)

        input["Activités secondaires"] = input.apply(lambda x : str(x["Activités secondaires"]) +' / '+ str(x["Activité.4"]) 
                                                            if not pd.isna(str(x["Activité.4"])) and str(x["Activité.4"]) not in str(x["Activités secondaires"]) 
                                                            else str(x["Activités secondaires"])
                                                                if not pd.isna(str(x['Activités secondaires']))
                                                                else np.nan,axis=1)

        
        input["Activités secondaires"] = input.apply(lambda x: x["Activités secondaires"].replace(" nan",''),axis=1)
        input["Activités secondaires"] = input.apply(lambda x: x["Activités secondaires"].replace("nan",''),axis=1)
    except:
        input = input.assign(actsecondaire="")
        input = input.rename(columns={"actsecondaire":"Activités secondaires"})

        print("error dans nétoyage activité")
    return input



def globalClean_CDProject(input):
    try:
        if("Activité.2" in input.columns):
            if("Activité.3" in input.columns):
                if("Activité.4" in input.columns):
                    input = input.drop(['Fax','Activité.2','Activité.3','Activité.4'
                            ],axis=1).reset_index(drop=True)
                else:
                    input = input.drop(['Fax','Activité.2','Activité.3'
                            ],axis=1).reset_index(drop=True)
            else:
                input = input.drop(['Fax','Activité.2'
                            ],axis=1).reset_index(drop=True)
            
        else:
            print("\n\ndrop fax\n\n")
            input = input.drop(['Fax'
                            ],axis=1).reset_index(drop=True)
    except:
        print("error dans global clean cd project")    
    input = input.assign(provenance="CDProject")

    return input


