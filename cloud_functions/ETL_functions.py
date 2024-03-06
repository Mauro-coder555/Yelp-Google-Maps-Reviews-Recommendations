import pandas as pd
import numpy as np

def check_rows(ruta_archivo, parametro):
    # Diccionario que contiene los nombres de columnas correspondientes a cada tabla

    # Basta que tengan todas las columnas mencionadas, se ignoraran las columnas sobrantes
    columnas = {
        "business": ["business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "is_open", "attributes", "categories", "hours"],
        "review": ["review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool"],
        "tip": ["text", "date", "compliment_count", "business_id", "user_id"],
        "checkin": ["business_id", "date"],
        "user": ["user_id", "name", "review_count", "yelping_since", "friends", "useful", "funny", "cool", "fans", "elite", "average_stars", "compliment_hot", "compliment_more", "compliment_profile", "compliment_cute", "compliment_list", "compliment_note", "compliment_plain", "compliment_cool", "compliment_funny", "compliment_writer", "compliment_photos"]
    }
    
    # Verificar si el parámetro es válido
    if parametro not in columnas:
        return "El parámetro especificado no es válido."
    
    # Cargar el archivo como un DataFrame de pandas
    try:
        df = pd.read_csv(ruta_archivo)
    except FileNotFoundError:
        return "El archivo no pudo ser encontrado."
    except pd.errors.EmptyDataError:
        return "El archivo está vacío."
    
    # Verificar si el DataFrame contiene todas las columnas correspondientes al parámetro
    if all(col in df.columns for col in columnas[parametro]):
        return True
    else:
        return False    


def procesar_nulos_duplicados(df_base,df_nuevo,df_unique_ids,tipo):        

        # Casos para hacer cosas en función del parámetro
        if tipo == "business":
            # Definir criterio de integridad de fila 70% de fila debe tener datos no nulos
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Reemplazar nulos en columnas categóricas con "sin datos"
            df_nuevo[['name', 'address', 'city', 'state', 'postal_code', 'attributes', 'categories', 'hours']] = df_nuevo[['name', 'address', 'city', 'state', 'postal_code', 'attributes', 'categories', 'hours']].fillna('sin datos')

            # Eliminar filas con valores nulos en 'stars' o 'review_count'
            df_nuevo = df_nuevo.dropna(subset=['stars', 'review_count'])

            # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)

            # Eliminar filas duplicadas basadas en 'business_id' dejando solo la que mayor cantidad de reviews tenga
            df_concat = df_concat.loc[df_concat.groupby('business_id')['reviews_count'].idxmax()]

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)

             # Encontrar los business_id únicos en df_concat
            business_id_concat_unicos = df_concat['business_id'].unique()

            # Filtrar los business_id que no están en df_unique_ids
            business_id_no_en_unique = np.setdiff1d(business_id_concat_unicos, df_unique_ids['business_id'])

            # Crear un DataFrame con los business_id que no están en df_unique_ids
            df_nuevos_business_id = pd.DataFrame({'business_id': business_id_no_en_unique})

            # Combinar df_nuevos_business_id con df_unique_ids
            df_unique_actualizado = pd.concat([df_unique_ids, df_nuevos_business_id], ignore_index=True)

            # Actualizar ids unicos
            df_unique_actualizado.to_csv("data_tools/unique_ids.csv", index=False)

            pass
        elif tipo == "review":
            user_unique_ids = pd.read_csv('data_tools/unique_user_ids.csv') 

            # Realizar la comprobación de ids existentes
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_ids['business_id']) & df_nuevo['user_id'].isin(user_unique_ids['user_id'])]

            # Definir criterio de integridad de fila
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Eliminar filas con valores nulos en columnas clave y fecha
            df_nuevo = df_nuevo.dropna(subset=['review_id', 'user_id', 'business_id', 'date', 'stars', 'text'])

            # Imputar valores nulos en el DataFrame
            df_nuevo.fillna('sin datos', inplace=True)    

            # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)

            # Eliminar filas duplicadas, conservando la entrada con la fecha más reciente en caso de duplicados en review_id
            df_concat = df_concat.sort_values('date', ascending=False).drop_duplicates('review_id')

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)

            pass
        elif tipo == "tip":
            user_unique_ids = pd.read_csv('data_tools/unique_user_ids.csv')

            # Realizar la comprobación de ids únicos
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_ids['business_id']) & df_nuevo['user_id'].isin(user_unique_ids['user_id'])]

            # Definir criterio de integridad de fila
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Eliminar filas con valores nulos en columnas mas importantes
            df_nuevo = df_nuevo.dropna(subset=['text', 'date', 'compliment_count', 'business_id', 'user_id'])

            # Imputar valores nulos en el DataFrame
            df_nuevo.fillna('sin datos', inplace=True)            

            # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)

            # Eliminar filas duplicadas, conservando la entrada con la fecha más reciente en caso de duplicados en business_id y user_id
            df_concat = df_concat.sort_values('date', ascending=False).drop_duplicates(['business_id', 'user_id'])

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)
            pass
        elif tipo == "checkin":          
            # Eliminar filas con valores nulos en business_id o date
            df_nuevo = df_nuevo.dropna(subset=['business_id', 'date'])

            # Realizar la comprobación de ids únicos
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_ids['business_id'])]

            # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)

            # Eliminar duplicados de business_id, manteniendo la fila con la lista de fechas más larga
            df_concat['date_length'] = df_concat['date'].apply(len)  # Calcular la longitud de las listas de fechas
            df_concat = df_concat.sort_values(by=['business_id', 'date_length'], ascending=[True, False])  # Ordenar por business_id y longitud de la lista
            df_concat = df_concat.drop_duplicates(subset='business_id', keep='first')  # Mantener la primera fila para cada business_id (la más larga)

            pass
        elif tipo == "user":
            # Eliminar filas con valores nulos en user_id o review_count
            df = df.dropna(subset=['user_id', 'review_count'])

            # Definir criterio de integridad de fila
            threshold = int(0.7 * len(df.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Imputar el resto de los valores nulos con "sin datos"
            df_nuevo = df_nuevo.fillna("sin datos")

             # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)

            # Eliminar duplicados de user_id, conservando la fila con el mayor conteo de reviews
            df_concat = df_concat.sort_values(by=['user_id', 'review_count'], ascending=[True, False])
            df_concat = df_concat.drop_duplicates(subset='user_id', keep='first')

            # Encontrar los user_id únicos en df_concat
            user_id_concat_unicos = df_concat['user_id'].unique()

            # Filtrar los user_id que no están en user_unique_ids
            user_unique_ids = pd.read_csv('data_tools/unique_user_ids.csv')

            user_id_no_en_unique = np.setdiff1d(user_id_concat_unicos, user_unique_ids['user_id'])

            # Crear un DataFrame con los user_id que no están en user_unique_ids
            df_nuevos_user_id = pd.DataFrame({'user_id': user_id_no_en_unique})

            # Combinar df_nuevos_user_id con df_unique_ids
            df_unique_actualizado = pd.concat([user_unique_ids, df_nuevos_user_id], ignore_index=True)

            # Actualizar ids unicos
            df_unique_actualizado.to_csv("data_tools/user_unique_ids.csv", index=False)

            pass

        return df_concat
        
def concatenar_dataframes(df_base, df_nuevo):
    # Concatena los DataFrames
    df_concat = pd.concat([df_base, df_nuevo], ignore_index=True)

    # Elimina las duplicaciones
    df_concat = df_concat.drop_duplicates()

    return df_concat
        