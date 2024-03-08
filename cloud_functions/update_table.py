import os
from google.cloud import bigquery
from google.cloud import storage
import utils as ut
from google.oauth2 import service_account

def actualizar_tabla(event, context, nombre_tabla,nombre_archivo): 

    # Ruta al archivo JSON de credenciales
    credentials_path = "../credentials/eminent-cycle-415715-3ef9bde04901.json"

    # Crear credenciales a partir del archivo JSON
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Inicializar el cliente de BigQuery
    bigquery_client = bigquery.Client(credentials=credentials)
   
    # Obtener la ruta del archivo en el bucket
    ruta_archivo = "gs://yelp-ggmaps-data/processed/yelp/"+ nombre_archivo

    # Cargar datos desde el archivo en el bucket a BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = bigquery_client.load_table_from_uri(
        ruta_archivo, nombre_tabla, job_config=job_config
    )  # API request

    load_job.result()  # Espera a que se complete la carga

    print(f"Los datos se han cargado correctamente en la tabla {nombre_tabla} desde {ruta_archivo}.")

    return "Proceso de actualización de tabla completado."


nombre_archivo = "business_clean_prueba_BIG_QUERY.csv"
nombre_tabla = "eminent-cycle-415715.prueba_id_business.BUSINESS"
actualizar_tabla(None,None,nombre_tabla,nombre_archivo)


