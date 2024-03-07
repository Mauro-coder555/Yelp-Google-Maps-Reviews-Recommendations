import os
from google.cloud import storage

def storage_client(credentials_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    return storage.Client()

def upload_blob(bucket, file_path, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def upload_to_storage(local_folder_path, bucket_name, credentials_path):
    client = storage_client(credentials_path)
    bucket = client.get_bucket(bucket_name)

    print("Archivos en Cloud Storage antes de la subida:")
    for blob in bucket.list_blobs():
        print(f" - {blob.name}")

    allowed_extensions = [".json", ".pkl", ".parquet"]
    uploaded_files = []

    def upload_files_recursive(folder_path, prefix="new"):
        for entry in os.scandir(folder_path):
            if entry.is_file() and any(entry.name.endswith(ext) for ext in allowed_extensions):
                local_file_path = entry.path
                destination_blob_name = f"{prefix}/{entry.name}"

                try:
                    print(f"Subiendo {entry.name}...")
                    upload_blob(bucket, local_file_path, destination_blob_name)
                    uploaded_files.append(entry.name)
                    print(f"{entry.name} subido exitosamente.")
                except Exception as e:
                    print(f"Error al subir {entry.name}: {e}")

    upload_files_recursive(local_folder_path)

    if uploaded_files:
        print("\nArchivos subidos exitosamente a storage:")
        for uploaded_file in uploaded_files:
            print(f" - {uploaded_file}")
    else:
        print("\nNo se encontraron archivos para subir.")

    print("\nArchivos en Cloud Storage después de la subida:")
    for blob in bucket.list_blobs():
        print(f" - {blob.name}")

if __name__ == "__main__":
    credentials_path = "../credentials/eminent-cycle-415715-3ef9bde04901.json"
    local_folder_path = "data"  # Cambia según tu estructura
    cloud_storage_bucket = "yelp-ggmaps-data"
    upload_to_storage(local_folder_path, cloud_storage_bucket, credentials_path)