# Librerias necesarias 
import pandas as pd
import os


#FUNCIONES:

def combinar_csv_interacciones(ruta):
    """
    Combina todos los archivos CSV en una carpeta específica en un solo DataFrame,
    utilizando el separador adecuado (;).

    Parámetros:
        ruta (str): Ruta de la carpeta donde se encuentran los archivos CSV.

    Retorna:
        pd.DataFrame: DataFrame combinado con los datos de todos los CSV en la carpeta.
        Si no se encuentran archivos válidos, retorna None.
    """
    # Lista para almacenar los DataFrames procesados
    dataframes = []

    # Verificar si la ruta existe
    if os.path.exists(ruta):
        # Iterar sobre todos los archivos en la carpeta
        for archivo in os.listdir(ruta):
            # Verificar si el archivo tiene extensión .csv
            if archivo.endswith('.csv'):
                print(f"Procesando archivo: {archivo}")
                
                # Cargar el archivo en un DataFrame con el separador correcto
                try:
                    df = pd.read_csv(os.path.join(ruta, archivo), sep=';', on_bad_lines='skip', low_memory=False)
                    # Agregar el DataFrame a la lista
                    dataframes.append(df)
                except Exception as e:
                    print(f"Error al procesar el archivo {archivo}: {e}")
        
        # Combinar todos los DataFrames si existen
        if dataframes:
            df_combinado = pd.concat(dataframes, ignore_index=True)
            return df_combinado
        else:
            print("No se encontraron archivos CSV válidos en la carpeta.")
            return None
    else:
        print("La carpeta no existe.")
        return None

    
def combinar_csv_y_agregar_fecha(ruta):
    """
    Combina todos los archivos CSV en una carpeta específica en un solo DataFrame
    y agrega una columna con una fecha extraída del nombre del archivo.

    Parámetros:
        ruta (str): Ruta de la carpeta donde se encuentran los archivos CSV.

    Retorna:
        pd.DataFrame: DataFrame combinado con los datos de todos los CSV en la carpeta,
                      incluyendo una columna "Fecha" extraída del nombre de los archivos.
                      Si no se encuentran archivos válidos, retorna None.
    """
    # Lista para almacenar DataFrames junto con la fecha
    dataframes = []

    # Verifica si la carpeta existe
    if os.path.exists(ruta):
        # Itera sobre todos los archivos en la carpeta
        for archivo in os.listdir(ruta):
            # Verifica si es un archivo CSV
            if archivo.endswith('.csv'):
                print(f"Procesando archivo: {archivo}")
                
                # Extrae la parte completa de la fecha del nombre del archivo
                nombre_sin_extension = os.path.splitext(archivo)[0]
                partes = nombre_sin_extension.split('-')  # Divide el nombre por guion

                # Verifica que haya al menos tres partes
                if len(partes) >= 3:
                    fecha = "-".join(partes[-3:])  # Toma las últimas tres partes y las une con guiones
                    print(f"Fecha extraída: {fecha}")
                    
                    # Carga el archivo CSV en un DataFrame
                    try:
                        df = pd.read_csv(os.path.join(ruta, archivo), sep=";", on_bad_lines='skip')
                        # Agrega la fecha como una nueva columna al DataFrame
                        df['fecha'] = fecha
                        # Guarda el DataFrame en la lista
                        dataframes.append(df)
                    except Exception as e:
                        print(f"Error al procesar el archivo {archivo}: {e}")
                else:
                    print(f"No se pudo identificar la fecha en el archivo: {archivo}")
        # Combina todos los DataFrames si existen
        if dataframes:
            df_combinado = pd.concat(dataframes, ignore_index=True)
            return df_combinado
        else:
            print("No se encontraron archivos CSV válidos en la carpeta.")
            return None
    else:
        print("La carpeta no existe.")
        return None

    
def procesar_dataframe_usuarios(dataframe):
    """
    Procesa un DataFrame de usuarios renombrando la columna 'id' a 'idusuarios'
    y conservando únicamente las columnas especificadas.

    Parámetros:
        dataframe (pd.DataFrame): El DataFrame que se desea procesar.

    Retorna:
        pd.DataFrame: El DataFrame procesado con las columnas ajustadas.
    """
    # Renombrar la columna 'id' a 'idusuarios'
    dataframe.rename(columns={'id': 'idusuarios'}, inplace=True)
    
    # Conservar solo las columnas especificadas
    columnas_a_conservar = ['idusuarios', 'active', 'registered', 'city', 'country', 'fecha']
    dataframe = dataframe.loc[:, columnas_a_conservar]
    
    return dataframe


def mapear_ciudades_a_estados(df, estados_venezuela):
    """
    Mapea las ciudades en la columna 'city' a sus estados correspondientes
    según el diccionario proporcionado.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene la columna 'city'.
        estados_venezuela (dict): Diccionario que relaciona los estados con sus ciudades.

    Retorna:
        pd.DataFrame: DataFrame con la columna 'city' corregida.
    """
    # Crear un diccionario inverso donde cada ciudad apunta a su estado
    ciudad_a_estado = {ciudad.lower(): estado for estado, ciudades in estados_venezuela.items() for ciudad in ciudades}
    
    # Aplicar el mapeo en la columna 'city'
    df['city'] = df['city'].str.lower().map(ciudad_a_estado).fillna(df['city'])
    
    return df


    
def rellenar_valores_nulos(df):
    """
    Rellena los valores nulos en la columna 'city' con 'DESCONOCIDOS'.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene la columna 'city'.

    Retorna:
        pd.DataFrame: DataFrame con los valores nulos de 'city' rellenados.
    """
    df['city'] = df['city'].fillna("Desconocidos")
    return df
    
    

def procesar_dataframe_interacciones(df):
    """
    Procesa un DataFrame seleccionando columnas específicas y renombrando una columna.

    Parámetros:
        df (pd.DataFrame): DataFrame a procesar.

    Retorna:
        pd.DataFrame: El DataFrame procesado.
    """
    # Columnas a conservar
    columnas_deseadas = ['id', 'user', 'channel', 'from', 'duration', 'server', 'type', 'device', 'shared', 'state']
    
    # Seleccionar las columnas deseadas
    df = df.loc[:, columnas_deseadas]
    
    # Renombrar la columna 'user' a 'idusuarios'
    df.rename(columns={'user': 'idusuarios'}, inplace=True)
    
    return df




def leer_unico_csv_canales(ruta):
    """
    Lee un único archivo CSV en una carpeta específica y lo carga en un DataFrame.

    Parámetros:
        ruta (str): Ruta de la carpeta donde se encuentra el archivo CSV.

    Retorna:
        pd.DataFrame: DataFrame cargado desde el archivo CSV.
                      Si no se encuentra el archivo o hay un error, retorna None.
    """
    # Verifica si la ruta existe
    if os.path.exists(ruta):
        # Lista los archivos en la carpeta
        archivos = [archivo for archivo in os.listdir(ruta) if archivo.endswith('.csv')]
        
        # Asegúrate de que solo hay un archivo CSV en la carpeta
        if len(archivos) == 1:
            archivo = archivos[0]
            print(f"Procesando archivo: {archivo}")
            
            # Intentar leer el archivo CSV
            try:
                df = pd.read_csv(os.path.join(ruta, archivo), sep=';', on_bad_lines='skip', low_memory=False)
                return df
            except Exception as e:
                print(f"Error al procesar el archivo {archivo}: {e}")
                return None
        elif len(archivos) == 0:
            print("No se encontró ningún archivo CSV en la carpeta.")
            return None
        else:
            print("Hay más de un archivo CSV en la carpeta. Por favor, asegúrate de que solo haya uno el mas actulizado.")
            return None
    else:
        print("La carpeta no existe.")
        return None
    
    
    
def seleccionar_columnas_canales(df):
    """
    Selecciona columnas específicas de un DataFrame.

    Parámetros:
        df (pd.DataFrame): DataFrame de entrada.

    Retorna:
        pd.DataFrame: DataFrame con solo las columnas seleccionadas.
    """
    # Definir las columnas deseadas
    columnas_deseadas = ['id', 'active', 'type', 'name', 'externalId', 'epgProviderId', 'channelGroup']
    
    # Verificar que las columnas existan en el DataFrame
    columnas_existentes = [col for col in columnas_deseadas if col in df.columns]
    
    # Seleccionar las columnas existentes
    df_modificado = df[columnas_existentes]
    
    return df_modificado


def guardar_dataframe_interactivo(df, ruta_carpeta):
    """
    Guarda un DataFrame en un archivo CSV en una carpeta específica.
    Solicita al usuario el nombre del archivo. Si la carpeta no existe, la crea.

    Parámetros:
        df (pd.DataFrame): El DataFrame a guardar.
        ruta_carpeta (str): Ruta de la carpeta donde se guardará el archivo. Por defecto es 'new_data'.

    Retorna:
        str: Ruta completa del archivo guardado.
    """
    # Pedir al usuario el nombre del archivo
    nombre_archivo = input("Por favor, introduce el nombre del archivo (con extensión .csv): ")
    
    # Asegurarse de que el nombre del archivo termine con '.csv'
    if not nombre_archivo.endswith('.csv'):
        nombre_archivo += '.csv'
    
    # Verificar si la carpeta existe; si no, crearla
    if not os.path.exists(ruta_carpeta):
        os.makedirs(ruta_carpeta)
        print(f"Carpeta creada: {ruta_carpeta}")
    else:
        print(f"Carpeta existente: {ruta_carpeta}")
    
    # Construir la ruta completa del archivo
    ruta_completa = os.path.join(ruta_carpeta, nombre_archivo)
    
    # Guardar el DataFrame en un archivo CSV con el separador ';'
    df.to_csv(ruta_completa, sep=';', index=False)
    print(f"Archivo guardado en: {ruta_completa}")
    
    return ruta_completa


#diccionario de parseo 

estados_venezuela = {
    "Amazonas": ["puerto ayacucho", "guayana"],
    "Anzoátegui": ["el trigre", "anaco", "barcelona", "el tigrito", "lecheria", "puerto la cruz", "aragua de barcelona"],
    "Apure": ["san fernando de apure", "san fernando", "araure"],
    "Aragua": ["maracay", "palo negro", "turmero", "maeacay", "la victoria", "cagua", "la morita1", "aragua", "la victoria edo aragua", "maracay.", "el consejo estado aragua", "santa rita maracay", "turmero, estado aragua", "la victoria aragua", "maracay- turmero", "palo negrro", "santa rita aragua", "yagua", "palo negro, aragua", "el limón", "maracay edo aragua", "msracay", "msracay", "palo negro - aragua", "maracay aragua", "caguas"],
    "Barinas": ["barinas"],
    "Bolívar": ["puerto ordaz", "ciudad bolivar", "ciudad guayana", "ciudad bolívar"],
    "Carabobo": ["moron", "valencia", "guacara", "montalban", "puerto cabello", "carabobo", "naguanagua", "puerto cabello carabobo", "carrizal", "san diego", "los guayos", "valencia edo. carabobo", "valencia, naguanagua", "valencia. venezuela", "valencia. venezuela", "san joaquin", "san diego de los altos", "valencia estado carabobo", "valencia, los guayos", "valencia, edo. carabobo", "san joaquin, estado carabobo", "ciudad alianza", "naguanagua. estado carabobo", "puerto cabell", "san joaquin*", "san joaquin", "valencia, carabobo", "nagauanagua", "ciudad zamora mzna. 4-f", "san digo", "san diego carabobo", "nagua", "urb. el bosque. valencia edo. carabobo", "guacara edo carabobo", "puerto cabello", "valenciq","valencia,  naguanagua","valencia , los guayos","san joaquín","guacaipuro","puerto  cabello"],
    "Cojedes": ["san carlos", "tinaco", "tinaquillo"],
    "Delta Amacuro": ["tucupita"],
    "Desconocidos": ["NaN","desconocido", "primero de mayo", "venezuela", "urb. los cocos sur", "la vaquera", "cjto. res. ops torre 3", "468", "el consejo", "palavecino", "allen", "san rafael de carvajal,/ carvajal", "11201663", "mario briceño iragorry", "cordero lomas blanca", "la concepcion", "13912257", "barcel", "el concejo", "libertad", "30", "275", "hhhhhh", "plc", "la concepción", "parroquia santa teresa", "peribeca", "san rafael del mojan", "4", "ciudad", "ed. mar azul t-b"],
    "Falcón": ["punto fijo", "churuguara", "coro", "chichiriviche", "la vela de coro", "santa ana de coro"],
    "Guárico": ["san juan de los morros", "altagracia de orituco"],
    "Lara": ["carora", "cabudare", "barquisimeto", "barquisimeto gxtest2al", "barquisimeto gxtes2al", "cabudare estado lara", "iribarren", "cabude", "barquisimeto*", "bqto"],
    "Mérida": ["merida", "mérida", "el vigia", "ejido, mérida", "mérida, venezuela"],
    "Miranda": ["guanares", "charallave", "los teques", "guatire", "miranda", "san antonio de los altos", "carrizal, estado miranda", "guarenas, miranda", "cuarenas", "guarenas, edo. miranda", "ciudad casarapa", "ocumare del tuy", "mariche", "guatires", "san antonio", "san josé d/los altos", "gurenas", "san josé de los altos", "los teqes", "guatire, edo miranda, venezuela", "*los teques", "guatire, edo. miranda. venezuela", "altos mirandinos", "higuerote"],
    "Monagas": ["maturin", "maturín"],
    "Nueva Esparta": ["margarita", "porlamar", "pampatar", "catia la mar"],
    "Portuguesa": ["guanare", "acarigua"],
    "Sucre": ["cumana", "cumanacoa", "municipio sucre", "carupano", "cumaná", "sucre"],
    "Táchira": ["san cristobal", "san cristóbal", "tachira", "capacho", "tariba", "los rastrojos", "cordero", "táriba", "caneyes", "táchira", "tucape", "san rafael cárdenas tachira venezuela", "san cristobal edo tachira", "capacho viejo", "san cristobal de tac"],
    "Trujillo": ["valera"],
    "Vargas (La Guaira)": ["la guaira", "vargas", "maiquetía, estado la guaira"],
    "Yaracuy": ["san felipe", "chivacoa", "yaritagua"],
    "Zulia": ["cabimas", "maracaibo", "zulia", "ciudad ojeda", "santa rita", "ciudad ojeda.", "ojeda", "maraxaibo", "maracaibo*"],
    "Distrito Capital (Caracas)": ["caracas", "caracass", "el hatillo", "csracas", "la tahona", "edif virrey, calle caurimare , caracas", "gran caracas", "caracas venezuela", "caricuao", "baruta", "libertador", "caracas gxtest2al", "distrito capital", "caracs", "caracas (miranda)", "maracas", "el hatillo-la unión", "baracas", "chacao", "la candelaria", "carcas", "municipio el hatillo", "caracas-baruta", "ccs", "la candelaria mbi", "el hatillo la unión corralito", "caraacs", "lagunillas", "caracas09966564", "caracas,", "caracas, el hatillo", "lagunillas", "urb. libertadores", "cara as", "caracas (d.c.)"],
    "Internacionales": ["santiago", "la paz", "palmira", "ejido", "bogota", "sn francisco", "chile", "zamora", "miami", "san francisco", "girardot", "san. francisco"]
}




    
    
    
    
    
  