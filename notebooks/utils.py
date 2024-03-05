from google.oauth2 import service_account
from google.cloud import storage

def set_config():
    # Configuración de Google Cloud Storage
    bucket_name = "yelp-ggmaps-data"

    # Ruta al archivo JSON de credenciales
    credentials_path = "C:/Users/Work/Downloads/eminent-cycle-415715-3ef9bde04901.json"

    # Crear credenciales a partir del archivo JSON
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Crear cliente de almacenamiento con las credenciales
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)

    return bucket