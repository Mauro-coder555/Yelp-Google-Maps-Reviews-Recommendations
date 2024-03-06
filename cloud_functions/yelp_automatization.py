import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
import importlib

import utils as ut
import ETL_functions as etl


def process_file(tipo_archivo):
    
    ruta_archivo = ut.obtener_ruta_archivo_nuevo_csv()
    if ruta_archivo:
        print("Ruta del archivo CSV encontrado:", ruta_archivo)
    else:
        print("No se encontró ningún archivo CSV dentro de la carpeta 'new/yelp'.")
        return False


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
        if tipo_archivo == "business":

            processed_blob_path = "processed/yelp/business_clean.csv"

            #Filtrar los establecimientos por estados específicos
            df = df[(df['state'] == 'FL') | (df['state'] == 'CA') | (df['state'] == 'NV')]

            #Guardar los distintos establecimientos de los estados ya filtrados
            unique_ids = pd.read_csv('data_tools/unique_business_ids.csv')

            # Realizar la fusión basada en la columna 'business_id'
            merged_data = pd.merge(unique_ids, df, on='business_id', how='left')
            # Eliminar los duplicados manteniendo solo la última aparición
            merged_data.drop_duplicates(subset=['business_id'], keep='last', inplace=True)
            # Guardar el resultado en un nuevo archivo si lo deseas
            merged_data.to_csv('data_tools/unique_business_ids.csv', index=False)
            
            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, merged_data, tipo_archivo)
            
            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif tipo_archivo == "review":
            processed_blob_path = "data/processed/yelp/review_clean.csv"
            #Filtrar las reseñas por estados específicos obtenidos a partir de los negocios       
            unique_ids = pd.read_csv('data_tools/unique_business_ids.csv')
            df = df[df['business_id'].isin(unique_ids['business_id'])]
            
            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids, tipo_archivo)

            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif tipo_archivo == "tip":
            processed_blob_path = "processed/yelp/tip_clean.csv"
            #Filtrar los consejos por estados específicos obtenidos a partir de los negocios       
            unique_ids = pd.read_csv('data_tools/unique_business_ids.csv')
            df = df[df['business_id'].isin(unique_ids['business_id'])]

            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids, tipo_archivo)
            
            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif tipo_archivo == "checkin":
            processed_blob_path = "processed/yelp/checkin_clean.csv"

            #Filtrar los consejos por estados específicos obtenidos a partir de los negocios       
            unique_ids = pd.read_csv('data_tools/unique_business_ids.csv')
            df = df[df['business_id'].isin(unique_ids['business_id'])]

            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids, tipo_archivo)
            
            ut.save_in_storage(bucket,processed_blob_path,df)

            pass
        elif tipo_archivo == "user":
            processed_blob_path = "processed/yelp/user_clean.csv"

            # Proceso ETL
            etl.procesar_nulos_duplicados(ut.cargar_df(processed_blob_path),df, unique_ids, tipo_archivo)
            
            ut.save_in_storage(bucket,processed_blob_path,df)
            pass

        ut.borrar_archivo_nuevo()
        return True