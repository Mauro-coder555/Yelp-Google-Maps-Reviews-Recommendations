import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
import importlib

import utils as ut
import ETL_functions as etl

importlib.reload(ut)

def process_file(bucket,ruta): # Retorna verdadero  o falso si todo sale bien o mal
   
    data = ut.descargar_archivo_gcs(bucket,ruta)

    # Cargar el archivo como un DataFrame de pandas
    try:
        df = pd.read_csv(io.BytesIO(data)) # DF A PARTIR DEL NUEVO ARCHIVO QUE INGRESO EL CLIENTE
    except FileNotFoundError:
        return False
    except pd.errors.EmptyDataError:
        return False
    print(df.columns)
    
    tipo_archivo = ut.asignar_tipo_archivo(ruta)
    if tipo_archivo is None:
        return False

    if not etl.check_rows_yelp(df,tipo_archivo):
        return False

    # Casos para hacer cosas en función del parámetro
    if tipo_archivo == "business":       
        processed_blob_path = "processed/yelp/business_clean_prueba.csv"

        #Filtrar los establecimientos por estados específicos
        df = df[(df['state'] == 'FL') | (df['state'] == 'CA') | (df['state'] == 'NV')]

        #Abrir dataframe para guardar luego los ids de negocios nuevos que se sumaran al archivo base
        data_business_unique_ids = ut.obtener_data_archivo_a_actualizar_csv(bucket,'used_ids/unique_business_ids.csv')
        unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))

        # Proceso ETL

        data_archivo_a_actualizar = ut.obtener_data_archivo_a_actualizar_csv(bucket,processed_blob_path)
        df_a_actualizar = pd.read_csv(io.BytesIO(data_archivo_a_actualizar))
        df_final = etl.procesar_nulos_duplicados(df_a_actualizar,df, unique_ids,unique_ids,bucket, tipo_archivo)
            
        ut.save_in_storage(bucket,processed_blob_path,df_final)

        pass
    elif tipo_archivo == "review":
        processed_blob_path = "data/processed/yelp/review_clean.csv"
        #Abrir dataframe para guardar luego los ids de negocios nuevos que se sumaran al archivo base
        data_business_unique_ids = ut.obtener_data_archivo_a_actualizar_csv(bucket,'used_ids/unique_business_ids.csv')
        unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))

        df = df[df['business_id'].isin(unique_ids['business_id'])]

        # Proceso ETL
        etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids,user_unique_ids,bucket, tipo_archivo)

        ut.save_in_storage(bucket,processed_blob_path,df)

        pass
    elif tipo_archivo == "tip":
        processed_blob_path = "processed/yelp/tip_clean.csv"
        #Abrir dataframe para guardar luego los ids de negocios nuevos que se sumaran al archivo base
        data_business_unique_ids = ut.obtener_data_archivo_a_actualizar_csv(bucket,'used_ids/unique_business_ids.csv')
        unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))
        df = df[df['business_id'].isin(unique_ids['business_id'])]

        # Proceso ETL
        etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids,user_unique_ids,bucket, tipo_archivo)
            
        ut.save_in_storage(bucket,processed_blob_path,df)

        pass
    elif tipo_archivo == "checkin":
        processed_blob_path = "processed/yelp/checkin_clean.csv"

        #Abrir dataframe para guardar luego los ids de negocios nuevos que se sumaran al archivo base
        data_business_unique_ids = ut.obtener_data_archivo_a_actualizar_csv(bucket,'used_ids/unique_business_ids.csv')
        unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))

        df = df[df['business_id'].isin(unique_ids['business_id'])]
      
        # Proceso ETL
        etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path), df, unique_ids, user_unique_ids,bucket, tipo_archivo)
            
        ut.save_in_storage(bucket,processed_blob_path,df)

        pass
    elif tipo_archivo == "user":
        processed_blob_path = "processed/yelp/user_clean.csv"

        #Abrir dataframe para guardar luego los ids de usuarios nuevos que se sumaran al archivo base
        data_business_unique_ids = ut.obtener_data_archivo_a_actualizar_csv(bucket,'used_ids/unique_user_ids.csv')
        user_unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))

        # Proceso ETL
        etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path), df, unique_ids, user_unique_ids,bucket, tipo_archivo)
            
        ut.save_in_storage(bucket,processed_blob_path,df)
        pass

    print("chau")    
    ut.borrar_archivo_nuevo(bucket)
    return True
    

process_file("business")