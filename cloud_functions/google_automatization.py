import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
import importlib

import utils as ut

importlib.reload(ut)


def process_file(tipo_archivo):
 # Descargar los datos del blob y cargarlos en un DataFrame
    bucket = ut.set_config()
    ruta_carpeta = ut.obtener_ruta_archivo_nuevo_csv(bucket)   
    original_blob_path = ruta_carpeta  # Esto supone que solo hay un archivo en la carpeta "new/"
    original_blob = bucket.blob(original_blob_path)
    data = original_blob.download_as_bytes()

     # Cargar el archivo como un DataFrame de pandas
    try:
        df = pd.read_csv(io.BytesIO(data)) # DF A PARTIR DEL NUEVO ARCHIVO QUE INGRESO EL CLIENTE
    except FileNotFoundError:
        return False
    except pd.errors.EmptyDataError:
        return False
    

 # Casos para hacer cosas en función del parámetro
    if tipo_archivo == "metadata_sitios":       
        processed_blob_path = "processed/google_maps/metadata_sitios_clean.csv"

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
    elif tipo_archivo == "review_estados":
        processed_blob_path = "data/processed/yelp/review_estados_clean.csv"
        #Abrir dataframe para guardar luego los ids de negocios nuevos que se sumaran al archivo base
        data_business_unique_ids = ut.obtener_data_archivo_a_actualizar_csv(bucket,'used_ids/unique_business_ids.csv')
        unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))

        df = df[df['business_id'].isin(unique_ids['business_id'])]

        # Proceso ETL
        etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids,user_unique_ids,bucket, tipo_archivo)

        ut.save_in_storage(bucket,processed_blob_path,df)

        pass