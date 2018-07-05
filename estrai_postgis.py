# coding: utf-8

import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from shapely.geometry import Point
import shapely.wkb
import seaborn as sns
import numpy as np
import sys
import pdb
from functools import reduce
from datetime import datetime


# In[3]:
def estrai_dati(yymmddI,yymmddF):
    '''
    Funzione per estrarre i dati vettoriali (non temporali) e raster (temporali) associati
    alle centraline di pm10.
    :param yymmddI: data di inizio per i dati temporali (serie di pm10 e parametri raster) nella forma yyyy-mm-dd
    :param yymmddF: data di fine per i dati temporali (serie di pm10 e parametri raster) nella forma yyyy-mm-dd
    :returns: un dataframe con i dati di pm10 della centralina nel periodo compreso tra yymmddI e yymmddF e i dati
    vettoriali (non temporali) e raster (temporali) associati al centroide più vicino alla centralina.
    '''

    annoI=yymmddI[0:4]
    annoF=yymmddF[0:4]

    try:
        data1=datetime.date(datetime.strptime(yymmddI,"%Y-%m-%d"))
    except ValueError as v:
        sys.exit(1)
        
    try:
        data2=datetime.date(datetime.strptime(yymmddF,"%Y-%m-%d"))
    except ValueError as v:
        sys.exit(1)
      
        

    if (data2 < data1):
        sys.exit("yymmddF deve essere posteriore a yymmddI")
    
    
    #calendario completo
    calendarioCompleto=pd.Series(pd.date_range(start=data1,end=data2,freq="D"))

    #calendario relativo al periodo di interesse
    calendario=pd.Series(pd.date_range(start=yymmddI,end=yymmddF,freq="D"))


    # In[4]:


    #lista_bande mi dice la posizione dei layer nei vari rasters
    lista_bande=np.where(np.in1d(calendarioCompleto.tolist(),calendario.tolist()))
    lista_bande=lista_bande[0].tolist()


    # In[5]:


    anni=np.unique(calendario.dt.year)


    # In[6]:


    #parametri di interesse
    parametri=("t2m","tp","sp","pbl00","pbl12","u10","v10","ndvi","dust","aod550")


    # In[7]:


    motore=create_engine("postgres://guido:postgres2015@localhost/asiispra")


    # In[8]:


    #questa funzione utilizzava una query
    #def estraiDati(lista,parametro,band):
    #    '''Funzione per estrarre i dati raster dal database postgis in corrispondenza delle stazioni'''
    #    if band <0 or band > 366:
    #        sys.exit("Numero di banda errato")

        #query: solo le centraline che ci interessano, sottoinsieme di tutte quelle dispoinili
    #    myquery="SELECT id_centralina,idcell,St_value({0}.rast,{1},geom) AS {0},geom FROM rgriglia.{0},centraline.pm10_con_dati WHERE St_intersects(geom,{0}.rast)".format(parametro,band)
    #    dati=pd.read_sql_query(myquery,con=motore)
    #    dati['banda']=banda
    #    lista.append(dati)
    #    return None


    # In[44]:


    #questa funzione richiama la funzione rgriglia.get_raster_value in postgresql
    def estraiRasterDati(lista,parametro,band):
        '''Funzione per estrarre i dati raster dal database postgis in corrispondenza delle stazioni'''
        if band <0 or band > 366:
            sys.exit("Numero di banda errato")

        #query: solo le centraline che ci interessano, sottoinsieme di tutte quelle dispoinili
        myquery="SELECT * FROM rgriglia.get_raster_value({0},{1})".format("'"+parametro+"'",band)
        dati=pd.read_sql_query(myquery,con=motore)

        dati['banda']=band
        #la tabella postgresql restituisce una colonna "valore" che vogliamo rinominare con il nome del parametro
        dati.rename(columns={"valore": parametro},inplace=True)
        lista.append(dati)
        return None


    # In[45]:


    listaFinale=[]

    for param in parametri:
        listaOut=[]
        
        for banda in lista_bande:

            estraiRasterDati(lista=listaOut,parametro=param,band=(banda+1))
        out=pd.concat(listaOut,axis=0,ignore_index=True)
        listaFinale.append(out)


    # In[46]:


    #reduce dei vari data frame
    cenPM10=reduce(lambda x,y: pd.merge(x,y,how="left",on=["id_centralina","idcell","geom","banda"]),listaFinale)


    # In[48]:


    #creazione della colonna geometry
    geom=cenPM10.apply(lambda x: shapely.wkb.loads(x['geom'],hex=True),axis=1)


    # In[49]:


    cenPM10.geom=geom


    # In[50]:


    #contiene per le centraline i valori dei rasters
    cenPM10=gpd.GeoDataFrame(cenPM10,crs="+init=epsg:32632",geometry='geom')


    # In[51]:


    #centraline che hanno dati
    id_centraline=str(tuple(np.unique(cenPM10.id_centralina).tolist()))
    #crea tupla con anno mese e giorno per query
    yymmdd=tuple(calendario.dt.date.astype(str).tolist())


    # In[52]:


    #acquisizione dati del PM10
    #for id in id_centraline:
    myquery='''SELECT id_centralina,
                      data_record,
                      data_record_value 
                FROM 
                      serie.pm10 
                WHERE 
                      (id_centralina IN {0}) AND (data_record IN {1})'''.format(id_centraline,yymmdd)
    #valori giornalieri del pm10
    pm10=pd.read_sql_query(con=motore,sql=myquery)



    # In[53]:


    pm10.data_record=pd.to_datetime(pm10.data_record,format="%Y-%m-%d")
    pm10.data_record=pm10.data_record.dt.date

    # In[54]:


    #se sto considerando più di un anno non posso creare la banda con dt.dayofyear
    #perchè anno dopo anno si ripeterebbero il numero di "banda"
    A=calendarioCompleto.dt.date.tolist() 
    B=np.unique(pm10.data_record.tolist())
    #non posso assehnare np.nan altrimenti dopo assegnando a "banda" un intero numpy genererebbe un errore,
    #assegno come valore iniziale -1
    pm10['banda']=-1


    # In[55]:


    for bb in B:
        quali=np.where(np.isin(A,bb))[0].tolist()
        if not len(quali):
            sys.exit("Nessuna corrispondenza trovata!")
        pm10.loc[pm10.data_record==bb,'banda']=quali[0]+1

   
    pm10.banda=pm10.banda.astype(int)

   
    # In[56]:


    #uniamo i dati del pm10 con i valori raster
    serie=pd.merge(pm10,cenPM10,how="left",on=["id_centralina","banda"])


    # In[57]:


    #acquisizione dei dati dai centroidi di griglia
    parametriPuntuali=pd.read_sql_query(con=motore,sql="SELECT * FROM vgriglia.get_vector_value()")


    # In[58]:


    #uniamo i dati vettoriali con i dati raster
    dati=pd.merge(serie,parametriPuntuali,how="inner",left_on="idcell",right_on="id")


    # In[59]:


    return dati

