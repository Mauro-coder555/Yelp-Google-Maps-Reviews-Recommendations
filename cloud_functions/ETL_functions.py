import pandas as pd
import numpy as np
import utils as ut

def check_rows(df, parametro):
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
        return False
        
    # Verificar si el DataFrame contiene todas las columnas correspondientes al parámetro
    if all(col in df.columns for col in columnas[parametro]):
        return True
    else:
        print("what")
        return False    


def procesar_nulos_duplicados(df_base,df_nuevo,df_unique_business_ids,user_unique_ids,bucket,tipo):        

        # Casos para hacer cosas en función del parámetro
        if tipo == "business":
            # Definir criterio de integridad de fila 70% de fila debe tener datos no nulos
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Reemplazar nulos en columnas categóricas con "sin datos"
            df_nuevo.loc[:, ['name', 'address', 'city', 'state', 'postal_code', 'attributes', 'categories', 'hours', 'is_open']] = df_nuevo.loc[:, ['name', 'address', 'city', 'state', 'postal_code', 'attributes', 'categories', 'hours', 'is_open']].fillna('sin datos')

            # Eliminar filas con valores nulos en 'business_id ', 'stars' o 'review_count'
            df_nuevo = df_nuevo.dropna(subset=['business_id', 'stars', 'review_count', 'latitude', 'longitude'])

            # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)
            
            # Eliminar filas duplicadas basadas en 'business_id' dejando solo la que mayor cantidad de reviews tenga
            df_concat = df_concat.loc[df_concat.groupby('business_id')['review_count'].idxmax()]

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)

           # Identificar business_id únicos presentes en df_concat pero no en df_unique_business_ids
            nuevos_business_ids = set(df_concat['business_id']) - set(df_unique_business_ids['business_id'])

            # Filtrar filas de df_concat que contienen los nuevos business_id
            nuevos_datos = df_concat[df_concat['business_id'].isin(nuevos_business_ids)]

            # Concatenar los nuevos datos al DataFrame df_unique_business_ids, manteniendo solo la columna "business_id"
            df_unique_business_ids = pd.concat([df_unique_business_ids, nuevos_datos[['business_id']]], ignore_index=True)

            # Eliminar duplicados en el dataframe df_unique_business_ids
            df_unique_business_ids = df_unique_business_ids.drop_duplicates(subset=['business_id'])

            # Guardar el dataframe actualizado en el archivo CSV

            ut.save_in_storage(bucket,"used_ids/unique_business_ids.csv",df_unique_business_ids)
            

            pass
        elif tipo == "review":          

            # Realizar la comprobación de ids existentes, no agregamos reseñas que tengas id's de negocios/usuarios que no existan en la base de antemano
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_business_ids['business_id']) & df_nuevo['user_id'].isin(user_unique_ids['user_id'])]

            # Definir criterio de integridad de fila
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Eliminar filas con valores nulos en algunas columnas de importancia
            columnas_clave = ['review_id', 'user_id', 'business_id', 'date', 'stars', 'text']
            df_nuevo = df_nuevo.loc[df_nuevo[columnas_clave].notnull().all(axis=1)]

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

            # Realizar la comprobación de ids únicos
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_business_ids['business_id']) & df_nuevo['user_id'].isin(user_unique_ids['user_id'])]

            # Definir criterio de integridad de fila
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Eliminar filas con valores nulos en columnas mas importantes
            columnas = ['text', 'date', 'compliment_count', 'business_id', 'user_id']
            df_nuevo = df_nuevo.loc[df_nuevo[columnas].notnull().all(axis=1)]

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
            df_nuevo = df_nuevo.loc[(df_nuevo['business_id'].notnull()) & (df_nuevo['date'].notnull())]

            # Realizar la comprobación de ids únicos
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_business_ids['business_id'])]

            # Concatenar dataframe base con dataframe nuevo
            df_concat = concatenar_dataframes(df_base, df_nuevo)

            # Eliminar duplicados de business_id, manteniendo la fila con la lista de fechas más larga
            df_concat['date_length'] = df_concat['date'].apply(len)  # Calcular la longitud de las listas de fechas
            df_concat = df_concat.sort_values(by=['business_id', 'date_length'], ascending=[True, False])  # Ordenar por business_id y longitud de la lista
            df_concat = df_concat.drop_duplicates(subset='business_id', keep='first')  # Mantener la primera fila para cada business_id (la más larga)

            # Eliminar la columna 'date_length' creada de manera auxiliar al final del proceso
            df_concat = df_concat.drop('date_length', axis=1)

            pass
        elif tipo == "user":
            
            # Eliminar filas con valores nulos en user_id o review_count
            df_nuevo = df_nuevo.loc[(df_nuevo['user_id'].notnull()) & (df_nuevo['review_count'].notnull())]

            # Definir criterio de integridad de fila
            threshold = int(0.7 * len(df_nuevo.columns))

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

            user_id_no_en_unique = np.setdiff1d(user_id_concat_unicos, user_unique_ids['user_id'])

            # Crear un DataFrame con los user_id que no están en user_unique_ids
            df_nuevos_user_id = pd.DataFrame({'user_id': user_id_no_en_unique})

            # Combinar df_nuevos_user_id con df_unique_ids
            df_unique_actualizado = pd.concat([user_unique_ids, df_nuevos_user_id], ignore_index=True)

            # Actualizar ids unicos de usuarios
            ut.save_in_storage(bucket,"used_ids/unique_user_ids.csv",df_unique_actualizado)

            pass

        return df_concat

def concatenar_dataframes(df_base, df_nuevo):
    # Concatena los DataFrames
    df_concat = pd.concat([df_base, df_nuevo], ignore_index=True)
    
    # Elimina las duplicaciones
    df_concat = df_concat.drop_duplicates()

    return df_concat
        