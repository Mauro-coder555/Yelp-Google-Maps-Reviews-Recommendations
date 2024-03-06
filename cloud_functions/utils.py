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
    
def obtener_ruta_archivo_nuevo_csv():
    # Ruta de la carpeta principal
    carpeta_principal = "new/"

    # Lista los archivos dentro de la carpeta principal
    archivos = os.listdir(carpeta_principal)

    # Encuentra el archivo CSV dentro de la lista de archivos
    archivo_csv = next((archivo for archivo in archivos if archivo.endswith(".csv")), None)

    # Si se encontró un archivo CSV, devuelve la ruta completa
    if archivo_csv:
        ruta_completa = os.path.join(carpeta_principal, archivo_csv)
        return ruta_completa
    else:
        return None
    
def borrar_archivo_nuevo():
    # Ruta de la carpeta principal
    carpeta_principal = "new/"

    try:
        # Elimina todos los archivos dentro de la carpeta
        for archivo in os.listdir(carpeta_principal):
            ruta_archivo = os.path.join(carpeta_principal, archivo)
            if os.path.isfile(ruta_archivo):
                os.remove(ruta_archivo)
        print("Se han borrado todos los archivos dentro de la carpeta 'new/'.")
    except FileNotFoundError:
        print("La carpeta 'new/yelp' no existe.")
    except OSError as e:
        print(f"No se pudo eliminar los archivos de la carpeta 'new/': {e}")