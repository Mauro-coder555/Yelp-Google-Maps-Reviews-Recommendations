import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
import importlib

import utils as ut
import ETL_functions as etl


def process_file(ruta_archivo, parametro):

    if not etl.check_rows:
        return False
    else:
        # Descargar los datos del blob y cargarlos en un DataFrame
        bucket = ut.set_config

        original_blob_path = ruta_archivo
        original_blob = bucket.blob(original_blob_path)
        data = original_blob.download_as_bytes()


        # Cargar el archivo como un DataFrame de pandas
        try:
            df = pd.read_csv(io.BytesIO(data)) # DF A PARTIR DEL NUEVO ARCHIVO QUE INGRESO EL CLIENTE
        except FileNotFoundError:
            return "El archivo no pudo ser encontrado."
        except pd.errors.EmptyDataError:
            return "El archivo está vacío."
        
        # Casos para hacer cosas en función del parámetro
        if parametro == "business":

            processed_blob_path = "yelp/processed/business.csv"

            #Filtrar los establecimientos por estados específicos
            df = df[(df['state'] == 'FL') | (df['state'] == 'CA') | (df['state'] == 'NV')]

            #Guardar los distintos establecimientos de los estados ya filtrados
            unique_ids = pd.read_csv('unique_ids.csv')

            # Realizar la fusión basada en la columna 'business_id'
            merged_data = pd.merge(unique_ids, df, on='business_id', how='left')
            # Eliminar los duplicados manteniendo solo la última aparición
            merged_data.drop_duplicates(subset=['business_id'], keep='last', inplace=True)
            # Guardar el resultado en un nuevo archivo si lo deseas
            merged_data.to_csv('unique_ids.csv', index=False)
            
            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, merged_data, parametro)
            
            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif parametro == "review":
            processed_blob_path = "yelp/processed/review.csv"
            #Filtrar las reseñas por estados específicos obtenidos a partir de los negocios       
            unique_ids = pd.read_csv('unique_ids.csv')
            df = df[df['business_id'].isin(unique_ids['business_id'])]
            
            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, merged_data, parametro)

            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif parametro == "tip":
            processed_blob_path = "yelp/processed/tip.csv"
            #Filtrar los consejos por estados específicos obtenidos a partir de los negocios       
            unique_ids = pd.read_csv('unique_ids.csv')
            df = df[df['business_id'].isin(unique_ids['business_id'])]

            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, merged_data, parametro)
            
            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif parametro == "checkin":
            processed_blob_path = "yelp/processed/checkin.csv"

            #Filtrar los consejos por estados específicos obtenidos a partir de los negocios       
            unique_ids = pd.read_csv('unique_ids.csv')
            df = df[df['business_id'].isin(unique_ids['business_id'])]

            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, merged_data, parametro)

            
            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif parametro == "user":
            processed_blob_path = "yelp/processed/user.csv"

            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, merged_data, parametro)
            
            ut.save_in_storage(bucket,processed_blob_path,df)
            pass

        return True