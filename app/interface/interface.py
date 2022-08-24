from argparse import Action
from cgitb import text
from faulthandler import disable
from glob import glob
#from msilib.schema import Error
from tkinter.filedialog import askopenfilename
from tkinter import *
from tkinter.messagebox import askokcancel
from turtle import width 
from tkinter.filedialog import *
from tkinter.messagebox import showinfo
from traitlets import default
import os
import pandas as pd

from hubspot_api.requests import get_owner
# from myApp import list_owner

# from app import FICHIER_INPUT
# from app import owner_selected

print("In interface py")
def my_app():
    print(os.environ.get("list_owner"))
    list_owner = []
    str_tmp = ''
    for i in str(os.environ.get("list_owner")).split(','):
        str_tmp = i.replace("'",'')
        str_tmp = str_tmp.replace("[",'')
        str_tmp = str_tmp.replace("]",'')
        str_tmp = str_tmp.replace(" ",'')

        list_owner.append(str_tmp)
    FICHIER_INPUT = os.environ.get("FICHIER_INPUT")
    owner_selected = os.environ.get("owner_selected")
    fenetre = Tk()
    fenetre.geometry("800x500")

    name = StringVar()
    mon_fichier_cours = StringVar()
    ma_liste_owner = StringVar(value=list_owner)
    owner_selected_int = StringVar()
    range_top = StringVar(value=os.environ.get("range_top"))
    range_bot = StringVar(value=os.environ.get("range_bot"))

    def choisir_fichier():
        name_tmp = askopenfilename()   # lance la fenêtre
        print (name_tmp)
        name.set(name_tmp)
        name_tmp = name
        name_tmp = name_tmp.get().split('/')[-1]
        if('.csv' in name_tmp) or ('.CSV' in name_tmp):
            mon_fichier_cours.set(name_tmp)
            os.environ['name_fichier']= mon_fichier_cours.get()
            changeState(b_valRange)
            
            #GET NB LIGNES 
            df_csv = pd.DataFrame(None)
            name_csv = name.get()
            print(name_csv)
            print(r"%s" %name_csv)
            try:
                df_csv = pd.read_csv (r"%s" %name_csv,encoding='latin-1', )
            except :
                try:
                    df_csv = pd.read_csv (r"%s" %name_csv,sep=";",encoding='latin-1')
                except :
                    try:
                        df_csv = pd.read_csv (r"%s" %name_csv,sep="\t",encoding='latin-1')
                    except :
                        try:
                            df_csv = pd.read_csv (r"%s" %name_csv,encoding='utf8', )
                        except :
                            try:
                                df_csv = pd.read_csv (r"%s" %name_csv,sep=";",encoding='utf8')
                            except :
                                try:
                                    df_csv = pd.read_csv (r"%s" %name_csv,sep="\t",encoding='utf8')
                                except :
                                    print('read_csv error : '+str("!"))
                                    choisir_fichier()

            
            df = pd.DataFrame(df_csv)
            os.environ['range_top'] = str(len(df))
            range_top.set(os.environ.get('range_top'))
            
            

            #-------------
            
        else:
            mon_fichier_cours.set("Mauvaise extention de fichier")
            
            
            
    def fermer_fenetre():
        fenetre.destroy
        
        
    def changeState(btn):
        if (btn['state'] == DISABLED):
            btn['state'] = NORMAL
        else:
            btn['state'] = NORMAL

    def disableState(btn):
        btn['state'] = DISABLED

        
    def openNewWindow():
        
        
            # handle event
        def items_selected(event):
            """ handle item selected event
            """
            # get selected indices
            selected_indices = listSel.curselection()
            # get selected items
            selected_langs = ",".join([listSel.get(i) for i in selected_indices])
            msg = f'You selected: {selected_langs}'

            showinfo(
                title='Information',
                message=msg)
            owner_selected_int.set(selected_langs)
            print("owner selected : "+owner_selected_int.get())
            changeState(b_valider)
            newWindow.destroy()
        def random_choice():
            msg = f'You selected: Random'

            showinfo(
                title='Information',
                message=msg)
            owner_selected_int.set("random")
            print("owner selected : "+owner_selected_int.get())
            changeState(b_valider)

            newWindow.destroy()
        
        # Toplevel object which will
        # be treated as a new window
        newWindow = Toplevel(fenetre)
    
        # sets the title of the
        # Toplevel widget
        newWindow.title("New Window")
    
        # sets the geometry of toplevel
        newWindow.geometry("400x400")
    
        # A Label widget to show in toplevel
        Label(newWindow,
            text ="This is a new window").grid(row=0,column=0)   
        
        yDefilB = Scrollbar(newWindow, orient='vertical')
        yDefilB.grid(row=1, column=1, sticky='ns')

        xDefilB = Scrollbar(newWindow, orient='horizontal')
        xDefilB.grid(row=3, column=0, sticky='ew')

        listSel = Listbox(newWindow,
            listvariable=ma_liste_owner,
            xscrollcommand=xDefilB.set,
            yscrollcommand=yDefilB.set)
        listSel.grid(row=1, column=0, sticky='nsew')
        xDefilB['command'] = listSel.xview
        yDefilB['command'] = listSel.yview
        listSel.bind('<<ListboxSelect>>', items_selected)
        
        bouton = Button(newWindow, text="Répartition random",command=random_choice)
        bouton.grid(column=2,row=1)
        


        
        
    def erreur_running():
        flag = askokcancel("Erreur durant l'importation", "un message d'erreur est survenu durant l'importation du csv : "+name.get()+"\n pour owner : "+owner_selected_int.get(), default="cancel")
        if(flag):
            fenetre.destroy()

    def valider():
        FICHIER_INPUT = name.get()
        owner_selected = owner_selected_int
        os.environ["FICHIER_INPUT"] = FICHIER_INPUT
        os.environ["owner_selected"] = owner_selected.get()
        fenetre.destroy()


    frm = Frame(fenetre,padx=10,pady=10,)
    frm.grid(column=5,row=5)
    Label(frm, text="Choisissez un csv à importer",width=40).grid(column=1, row=0,ipadx=5,ipady=5,sticky=N)
    b_choix = Button(frm, text="Choisir fichier", width=20, command=choisir_fichier).grid(column=1,row=3,padx=5,pady=5)
    Label(frm,textvariable=mon_fichier_cours).grid(column=1,row=4)
    b_running = Button(frm, text="Running",width=20, command=openNewWindow,state=DISABLED)
    b_running.grid(column=1, row=5,)
    Label(frm,text="",textvariable=owner_selected_int).grid(column=1,row=6)
    b_valider = Button(frm, text="Valider",width=20, command=valider,state=DISABLED)
    b_valider.grid(column=1, row=7)
    Button(frm, text="Fermer",width=20, command=fenetre.destroy).grid(column=1, row=8,)

    frm.bind_class("special")

    Label(frm, text="Modifier la clef API",width=40).grid(column=2, row=1,ipadx=5,ipady=5,sticky=N)
    hapikey = StringVar(value=os.environ.get("hapikey"))
    


    def estOK():
        print("hapikey get after inster :  "+hapikey.get())
        os.environ["hapikey"]=hapikey.get()
        try:
            list_owner = []
            response = get_owner(hapikey.get())
            nb_owner = len(response.json()['results'])
            for i in range(nb_owner):
                x=response.json()['results'][i]['email']
                list_owner.append(x)
            ma_liste_owner.set(value=list_owner)
            disableState(b_valider)
        except :
            print("error get owner in interface")
    
    text = Entry(frm,textvariable = hapikey,validate='key',)
    text.grid(column=2,row=3,padx=15,pady=5)
    b_val = Button(frm,text="Valider",command=estOK).grid(column=2,row=4)


    def rangeOK():
        print(text_top)
        os.environ['range_top']=text_top.get()
        print("range top get !!!!!!! : "+text_top.get())
        os.environ['range_bot']=text_bot.get()
        changeState(b_running)

    Label(frm, text="Modifier la taille du fichier à importer",width=40).grid(column=2, row=4,ipadx=5,ipady=5,sticky=N)
    text_bot = Entry(frm,textvariable = range_bot)
    text_bot.grid(column=2,row=5,padx=15,pady=5)
    text_top = Entry(frm,textvariable = range_top)
    text_top.grid(column=3,row=5,padx=15,pady=5)
    b_valRange = Button(frm,text="Valider",command=rangeOK,state=DISABLED)
    b_valRange.grid(column=2,row=6)

    




    fenetre.mainloop()

# my_app()
