import functions_framework
import utils as ut
import etl_functions as etl
import yelp_etl
import google_maps_etl

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    
    nombre_archivo = cloud_event.data["name"]

    bucket_entrada_nombre = cloud_event.context["bucket"]
    bucket_entrada = ut.get_bucket(bucket_entrada_nombre)
    
    bucket_salida_nombre = 'yelp_gmaps_work'
    bucket_salida = ut.get_bucket(bucket_salida_nombre)


    if "sitio" in nombre_archivo:
        if verificar_archivo_existente(bucket_salida, "business_merged"):
            fusionar_business_googlep_yelp(bucket_salida,"business_merged.csv", "sitio.csv")
        else:
            generar_business_merged(bucket_salida,"sitio.csv")
    

    elif "business" in nombre_archivo:
        if verificar_archivo_existente(bucket_salida, "business_merged"):
            fusionar_business_googlep_yelp(bucket_salida,"business_merged.csv", "business.csv")
        else:
            generar_business_merged(bucket_salida,"business_csv")

       
    #Funcion que actualiza la tabla en bisquery a partir del archivo (Ya esta hecha)




def fusionar_business_googlep_yelp(bucket,ruta_archivo_base, nombre_archivo):
    #Mirar en nombre_archivo si es un sitio de google o un business de yelp, para saber que columnas tenemos

    ut.save_in_storage(bucket,"business_merged", df) # Generar df final en esta funcion



def verificar_archivo_existente(bucket, archivo):
#Verifica si un archivo existe en un bucket de Cloud Storage.
    try:
        # Verificar si el archivo existe en el bucket
        blob = bucket.blob(archivo)
        return blob.exists()
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return False

def generar_business_merged(bucket,nombre_archivo):
    """Copia un archivo dentro del mismo bucket."""
    try:
        # Copiar el archivo dentro del mismo bucket
        blob = bucket.blob(nombre_archivo)
        new_blob = bucket.copy_blob(blob, bucket, "business_merged.csv")
        return new_blob.exists()
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return False
