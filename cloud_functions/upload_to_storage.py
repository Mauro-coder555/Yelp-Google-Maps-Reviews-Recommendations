import os
from google.cloud import storage
import backoff

def storage_client(credentials_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    return storage.Client()

def list_blob_names(bucket_name):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name)
    return [blob.name for blob in blobs]

@backoff.on_exception(backoff.expo, Exception, max_tries=8)
def upload_blob(blob, file_path):
    blob.upload_from_filename(file_path)

def upload_to_storage(local_folder_path, bucket_name, credentials_path):
    client = storage_client(credentials_path)
    bucket = client.get_bucket(bucket_name)

    # Mostrar archivos en el almacenamiento antes de la subida
    print("Archivos en Cloud Storage antes de la subida:")
    cloud_files_before = list_blob_names(bucket_name)
    for cloud_file in cloud_files_before:
        print(f" - {cloud_file}")

    allowed_extensions = [".json", ".pkl", ".parquet"]
    uploaded_files = []

    def upload_files_recursive(folder_path, prefix="raw"):
        for entry in os.scandir(folder_path):
            if entry.is_file() and any(entry.name.endswith(ext) for ext in allowed_extensions):
                local_file_path = entry.path
                blob = bucket.blob(f"{prefix}/{os.path.relpath(local_file_path, folder_path)}")

                try:
                    print(f"Subiendo {entry.name}...")
                    upload_blob(blob, local_file_path)
                    uploaded_files.append(entry.name)
                    print(f"{entry.name} subido exitosamente.")
                except Exception as e:
                    print(f"Error al subir {entry.name}: {e}")
            elif entry.is_dir():
                upload_files_recursive(entry.path, prefix)

    upload_files_recursive(local_folder_path)

    if uploaded_files:
        print("\nArchivos subidos exitosamente a storage:")
        for uploaded_file in uploaded_files:
            print(f" - {uploaded_file}")
    else:
        print("\nNo se encontraron archivos para subir.")

    # Mostrar archivos en el almacenamiento después de la subida
    print("\nArchivos en Cloud Storage después de la subida:")
    cloud_files_after = list_blob_names(bucket_name)
    for cloud_file in cloud_files_after:
        print(f" - {cloud_file}")

if __name__ == "__main__":
    credentials_path = "C:/Users/Work/Downloads/eminent-cycle-415715-3ef9bde04901.json"
    local_folder_path = "data"
    cloud_storage_bucket = "yelp-ggmaps-data"
    upload_to_storage(local_folder_path, cloud_storage_bucket, credentials_path)