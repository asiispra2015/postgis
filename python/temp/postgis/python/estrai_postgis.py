# coding: utf-8

import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely.geometry import Point
import shapely.wkb
import numpy as np
import sys
from functools import reduce
from datetime import datetime

#su rasters
parametriRasters={"temperature": "t2m",
                  "precipitation": "tp",
                  "surface pressure": "sp",
                  "planet boundary layer 00": "pbl00",
                  "planet boundary layer 12": "pbl12",
                  "u wind": "u10",
                  "v wind": "v10",
                  "ndvi": "ndvi",
                  "dust": "dust",
                  "aod cams": "aod550",
		  "wind direction": "wdir",
		  "wind speed": "wspeed"	
                 }


# In[3]:
def estrai_dati(yymmddI,yymmddF,parametriRAST=parametriRasters):
    
    '''Funzione per estrarre i dati vettoriali (non temporali) e raster (temporali- giornalieri) associati alle centraline di pm10.
    --- Parametri ---
        yymmddI: data di inizio per i dati temporali (serie di pm10 e parametri raster) nella forma yyyy-mm-dd
        yymmddF: data di fine per i dati temporali (serie di pm10 e parametri raster) nella forma yyyy-mm-dd
        parametriRAST: parametri temporali che vogliamo estrarre. I parametri vettoriali vengono estratti tutti.
    --- Returns ---    
        un dataframe con i dati di pm10 delle centraline nel periodo compreso tra yymmddI e yymmddF, i dati
        vettoriali (non temporali) e raster (temporali) associati al centroide della griglia di ciascuna centralina.
    '''
    
    import pandas as pd
    from geopandas import GeoDataFrame
    from sqlalchemy import create_engine
    from shapely.geometry import Point
    import shapely.wkb
    import numpy as np
    from sys import exit
    from functools import reduce
    from datetime import datetime
    

    #i dati nel database riguardano il 2015. annoI e annoF vanno cambiati solo se cambiano i dati sul database
    annoI,annoF=("2015","2015")
    giornoI,giornoF=(annoI+"-01-01",annoI+"-12-31")

    try:
        data1=datetime.date(datetime.strptime(yymmddI,"%Y-%m-%d"))
    except ValueError as v:
        exit(1)
        
    try:
        data2=datetime.date(datetime.strptime(yymmddF,"%Y-%m-%d"))
    except ValueError as v:
        exit(1)
      
        

    if (data2 < data1):
        sys.exit("yymmddF deve essere posteriore a yymmddI")
    
    
    #calendario completo del 2015 o del periodo che coprono i dati sul database postgis
    calendarioCompleto=pd.Series(pd.date_range(start=giornoI,end=giornoF,freq="D"))

    #calendario relativo al periodo di interesse passato alla funzione
    calendario=pd.Series(pd.date_range(start=yymmddI,end=yymmddF,freq="D"))

    #le date in "calendario" rientrano tutte nel periodo coperto dai dati sul database postgis?? Date oltre
    #la disponibilità sul database postgis vanno eliminate
    calendarioEffettivo=calendario.loc[(calendario >= giornoI) & (calendario<= giornoF)]
    # In[4]:


    #lista_bande: devo trovare i layer (nei raster temporali) che corrispondono al periodo dato da "calendarioEffettivo"
    #In pratica: sto dicendo..quale layer corrisponde (ad esempio) al 2015-03-04?
    lista_bande=np.where(np.in1d(calendarioCompleto.tolist(),calendarioEffettivo.tolist()))
    lista_bande=lista_bande[0].tolist()

    if len(lista_bande)==0:
        exit("Nessuna delle date passate è disponibile nel database postgis, esco!")
    elif len(lista_bande) < len(calendario):
        print("Alcune delle date richieste non sono disponibili sul database postgis!")


    #gli anni in calendarioEffettivo     
    anni=np.unique(calendarioEffettivo.dt.year)


    # In[7]:
    motore=create_engine("postgres://guido:postgres2015@localhost/asiispra")


    #questa funzione richiama la funzione rgriglia.get_raster_value in postgresql
    def estraiRasterDati(parametro,band=1):
        
        ''' Nessun controllo sulla banda. Già precedentemente sono state filtrate le date in modo di non avere date
            fuori dal range dati su database postgis
            --- Parametri ---
                parametro: parametro raster da acquisire
                band: banda da cui acquisire il parametro. Default=1
            --- Return ---
                data frame con i dati rasters
        '''
        
        #query: solo le centraline che ci interessano, sottoinsieme di tutte quelle dispoinili
        myquery="SELECT * FROM rgriglia.get_raster_value({0},{1})".format("'"+parametro+"'",band)
        dati=pd.read_sql_query(myquery,con=motore)

        dati['banda']=band
        #la tabella postgresql restituisce una colonna "valore" che vogliamo rinominare con il nome del parametro
        dati.rename(columns={"valore": parametro},inplace=True)
        return dati


    # In[45]:


    listaFinale=[]

    for nome_esteso,param in parametriRAST.items():

        listaOut=[estraiRasterDati(parametro=param,band=(banda+1)) for banda in lista_bande ]           
        out=pd.concat(listaOut,axis=0,ignore_index=True)
        listaFinale.append(out)


    #reduce dei vari data frame
    cenPM10=reduce(lambda x,y: pd.merge(x,y,how="left",on=["id_centralina","idcell","geom","banda"]),listaFinale)


    #creazione della colonna geometry
    geom=cenPM10.apply(lambda x: shapely.wkb.loads(x['geom'],hex=True),axis=1)

    cenPM10.geom=geom


    #contiene per le centraline i valori dei rasters
    cenPM10=GeoDataFrame(cenPM10,crs="+init=epsg:32632",geometry='geom')

    #centraline che hanno dati
    id_centraline=str(tuple(np.unique(cenPM10.id_centralina).tolist()))
    
    
    #acquisizione dati giornalieri del PM10
    
    #crea la sequenza di date da passare a postgresql, sequenza racchiusa tra parentesi e con le date tra apici
    stringa_date_per_query="("+ ",".join("'{0}'".format(w) for w in calendarioEffettivo.dt.date.astype(str).tolist() ) + ")"

    myquery='''SELECT id_centralina,
                      data_record,
                      data_record_value 
                FROM 
                      serie.pm10 
                WHERE 
                      (id_centralina IN {0}) AND (data_record IN {1})'''.format(id_centraline,stringa_date_per_query)
    
    #valori giornalieri del pm10
    pm10=pd.read_sql_query(con=motore,sql=myquery)


    pm10.data_record=pd.to_datetime(pm10.data_record,format="%Y-%m-%d")
    pm10.data_record=pm10.data_record.dt.date

    #se sto considerando più di un anno non posso creare la banda con dt.dayofyear perchè anno dopo anno si 
    #ripeterebbero il numero di "banda". Ovvero: dt.dayofyear mi dice che un "primo gennaio" è il numero 1 dell'anno ma se io
    #ho due anni di dati il secondo primo gennaio non lo devo più identificare con "1" ma con 367.
    A=calendarioCompleto.dt.date.tolist() 
    B=np.unique(pm10.data_record.tolist())
    
    #non posso inizializzare il campo "banda" con np.nan: np.nan è di tipo numeric mentre le bande sono identificate da interi
    #assegnando np.nan a "banda" numpy genera un errore quando poi vado ad assegnare un intero. Inizializziamo con -1
    pm10['banda']=-1


    for bb in B:
        quali=np.where(np.isin(A,bb))[0].tolist()
        if not len(quali):
            sys.exit("Nessuna corrispondenza trovata!")
        pm10.loc[pm10.data_record==bb,'banda']=quali[0]+1

   
    pm10.banda=pm10.banda.astype(int)

   
    #uniamo i dati del pm10 con i valori raster
    serie=pd.merge(pm10,cenPM10,how="left",on=["id_centralina","banda"])


    #acquisizione dei dati dai centroidi di griglia: sfrutta la funzione in plsql vgriglia.get_vector_value() memorizzata su postgis
    parametriPuntuali=pd.read_sql_query(con=motore,sql="SELECT * FROM vgriglia.get_vector_value()")

    #uniamo i dati vettoriali con i dati raster
    dati=pd.merge(serie,parametriPuntuali,how="inner",left_on="idcell",right_on="id")


    return dati

