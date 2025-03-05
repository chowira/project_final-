from funciones import funciones as fun
import pandas as pd
import os
from funciones.variables_folder.variables import estados_venezuela
# Usar



# Especifica la ruta de la carpeta
carpeta = "./data/Usuarios"
carpeta_2 = "./data/tabala_de_hechos/Diciembre/Diciembre"
carpeta_3 = "./data/canales"
carpeta_4="./data/new_data"






if __name__ == '__main__':
    df_interacciones = fun.combinar_csv_interacciones(carpeta_2)
    df_interacciones_porcesado = fun.procesar_dataframe_interacciones(df_interacciones)
    fun.guardar_dataframe_interactivo(df_interacciones_porcesado,carpeta_4)
    df_usuarios = fun.combinar_csv_y_agregar_fecha(carpeta)
    df_usuarios_colum = fun.procesar_dataframe_usuarios(df_usuarios)
    df_usuarios_map = fun.mapear_ciudades_a_estados(df_usuarios_colum,estados_venezuela)
    df_usuarios_map_nulos = fun.rellenar_valores_nulos(df_usuarios_map)
    fun.guardar_dataframe_interactivo(df_usuarios_map_nulos, carpeta_4)
    df_canales = fun.leer_unico_csv_canales(carpeta_3)
    df_canales_new = fun.seleccionar_columnas_canales(df_canales)
    fun.guardar_dataframe_interactivo(df_canales_new,carpeta_4)