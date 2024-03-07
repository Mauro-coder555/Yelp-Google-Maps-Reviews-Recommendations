from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import os

def set_config():
    # Configuración de Google Cloud Storage
    bucket_name = "yelp-ggmaps-data"

    # Ruta al archivo JSON de credenciales
    credentials_path = "../credentials/eminent-cycle-415715-3ef9bde04901.json"

    # Crear credenciales a partir del archivo JSON
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Crear cliente de almacenamiento con las credenciales
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)

    return bucket

def save_in_storage(bucket,path,df):
     #Exportar / Guardar el DataFrame filtrado ya definito / procesado
       
        processed_blob_path = path
        processed_blob = bucket.blob(processed_blob_path)
        csv_data = df.to_csv(index=False).encode('utf-8')

        processed_blob.upload_from_string(csv_data, content_type='text/csv')

        print(f"El DF se guardó correctamente en {processed_blob_path}")

def cargar_df(path):
    try:
        # Cargar el archivo en un DataFrame
        df = pd.read_csv(path)  # Cambiar a read_excel si es un archivo Excel
        return df
    except Exception as e:
        print("Error al cargar el DataFrame:", e)
        return None
    
def obtener_ruta_archivo_nuevo_csv(bucket):
    ruta_carpeta = "new/"  # Ruta de la carpeta que contiene el archivo
    blobs_en_carpeta = bucket.list_blobs(prefix=ruta_carpeta)
    lista_archivos = [blob.name for blob in blobs_en_carpeta if '.' in blob.name[len(ruta_carpeta):]]
    if len(lista_archivos) == 1:  # Verifica si hay exactamente un archivo en la carpeta
        return lista_archivos[0]
    else:
        raise ValueError("La carpeta 'new/' no contiene un único archivo.")
    
def obtener_ruta_archivo_a_actualizar_csv(bucket, ruta):
    blob = bucket.blob(ruta)
    data = blob.download_as_bytes()
    return data
    
def borrar_archivo_nuevo(bucket):
    # Ruta de la carpeta principal
    carpeta_principal = "new/"

    try:
        # Lista los blobs dentro de la carpeta principal en el bucket
        blobs = bucket.list_blobs(prefix=carpeta_principal)

        # Elimina cada archivo dentro de la carpeta
        for blob in blobs:
            # Verifica si el blob es un archivo (no una carpeta)
            if not blob.name.endswith('/'):
                blob.delete()

        print("Se han borrado todos los archivos dentro de la carpeta 'new/'.")
    except Exception as e:
        print(f"No se pudo eliminar los archivos de la carpeta 'new/': {e}")