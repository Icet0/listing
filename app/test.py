import pandas as pd

###INSERTION DE L'ANCIEN FICHIER NO CRM DANS HUBSPOT



def main():
    df = pd.read_csv ("nocrmDealsUTF8.csv",sep=";",encoding="utf8")
    data = (df[['Lead','Status','Amount','Téléphone','E-mail','Site','User','Last_update']])
    print(len(
        data[data["Status"]=="won"]
    ))
    data = data.assign(Provenance = "NO-CRM")
    data = data.assign(Pipeline = "Pipeline Sales")
    data = data.assign(MEDIA = "Le Point")


    print(pd.unique(data['Status']))
    data["Status"] = data["Status"].apply(lambda x : "Qualifié pour acheter" if (x == "standby" or x == "todo")
                      else "Fermé perdu" if (x == "lost" or x == "cancelled")
                      else "Fermé gagné" if x == "won"
                      else x
                    )
    print(pd.unique(data['Status']))
    data.to_csv("final_nocrm.csv",index=False)
main()