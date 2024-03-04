import os
from google.cloud import storage

ALLOWED_EXTENSIONS = {".csv", ".json", ".pkl", ".parquet"}

def storage_client(credentials_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    return storage.Client()

def allowed_file(filename):
    return any(filename.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS)

def is_file_uploaded(bucket_name, file_name):
    if not allowed_file(file_name):
        return False

    client = storage_client("C:/Users/Work/Downloads/eminent-cycle-415715-3ef9bde04901.json")
    bucket = client.bucket(bucket_name)
    
    # Verificar si el archivo ya existe en Cloud Storage
    blob = bucket.blob(file_name)
    return blob.exists()

if __name__ == "__main__":
    bucket_name = "yelp-ggmaps-data"
    file_name_to_check = "webapp/nombre_del_archivo_que_quieres_verificar.txt"  # Cambia esto con el nombre del archivo a verificar
    if is_file_uploaded(bucket_name, file_name_to_check):
        print(f"El archivo {file_name_to_check} ya existe en Cloud Storage o no es un formato permitido. No se puede subir.")
    else:
        print(f"El archivo {file_name_to_check} no existe en Cloud Storage y es de un formato permitido. Puede subirlo.")