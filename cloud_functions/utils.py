from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import reverse_geocoder as rg

def set_config(bucket_name):    

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

def descargar_archivo_gcs(bucket_nombre, ruta_archivo):
     # Ruta al archivo JSON de credenciales
    credentials_path = "../credentials/eminent-cycle-415715-3ef9bde04901.json"

    # Crear credenciales a partir del archivo JSON
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Crear cliente de almacenamiento con las credenciales
    cliente = storage.Client(credentials=credentials)

    # Obtener el bucket
    bucket = cliente.bucket(bucket_nombre)

    # Obtener el blob (archivo) dentro del bucket
    blob = bucket.blob(ruta_archivo)

    # Descargar el contenido del archivo como bytes
    contenido_bytes = blob.download_as_bytes()

    return contenido_bytes    

    
def obtener_data_archivo_a_actualizar_csv(bucket, ruta):
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


def asignar_tipo_archivo(ruta):
    if "yelp" in ruta:
        if "business" in ruta:
            tipo_archivo = "business"
        elif "checkin" in ruta:
            tipo_archivo = "checkin"
        elif "tip" in ruta:
            tipo_archivo = "tip"
        elif "review" in ruta:
            tipo_archivo = "review"
        elif "sitio" in ruta:
            tipo_archivo = "sitio"
        else:
            tipo_archivo = None
    elif "google" in ruta:
        if "review" in ruta:
            tipo_archivo = "review"
        elif "sitio" in ruta:
            tipo_archivo = "sitio"
        else:
            tipo_archivo = None
    else:
        tipo_archivo = None

    return tipo_archivo

def corregir_ubicaciones(df):
    # Aplicar la función obtener_ubicacion a las columnas latitude y longitude
    df['ubicacion'] = df.apply(lambda row: obtener_ubicacion(row['latitude'], row['longitude']), axis=1)
    
    # Actualizar los campos 'state' y 'city' basados en las nuevas columnas generadas
    df['state'] = df['ubicacion'].apply(lambda x: x['estado'])
    df['city'] = df['ubicacion'].apply(lambda x: x['ciudad'])
    
    # Eliminar la columna 'ubicacion' generada auxiliar
    df = df.loc[:, df.columns != 'ubicacion']
    
    return df


# Función para obtener estado y ciudad a partir de latitud y longitud
def obtener_ubicacion(latitud, longitud):
    try:
        # Obtener información de ubicación utilizando Reverse Geocoder
        results = rg.search((latitud, longitud))

        # Extraer el estado y la ciudad de los resultados
        estado = results[0]['admin1']
        ciudad = results[0]['name']

        return {"estado": estado, "ciudad": ciudad}
    except Exception:
        # Si no se encuentra la ubicación, devolver valores vacíos
        return {"estado": "sin datos", "ciudad": "sin datos"}