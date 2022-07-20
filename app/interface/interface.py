from argparse import Action
from cgitb import text
from faulthandler import disable
from glob import glob
from tkinter.filedialog import askopenfilename
from tkinter import *
from tkinter.messagebox import askokcancel
from turtle import width 
from tkinter.filedialog import *
from tkinter.messagebox import showinfo
from traitlets import default
import os

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
    fenetre.geometry("700x500")

    name = StringVar()
    mon_fichier_cours = StringVar()
    ma_liste_owner = StringVar(value=list_owner)
    owner_selected_int = StringVar()
    def choisir_fichier():
        name_tmp = askopenfilename()   # lance la fenêtre
        print (name_tmp)
        name.set(name_tmp)
        name_tmp = name
        name_tmp = name_tmp.get().split('/')[-1]
        if('.csv' in name_tmp) or ('.CSV' in name_tmp):
            mon_fichier_cours.set(name_tmp)
            changeState(b_running)
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
    
    text = Entry(frm,textvariable = hapikey,validate='key',).grid(column=2,row=3,padx=15,pady=5)
    b_val = Button(frm,text="Valider",command=estOK).grid(column=2,row=4)
    # text.insert('1.0',hapikey.get())
    os.environ["hapikey"]=hapikey.get()





    fenetre.mainloop()

# my_app()