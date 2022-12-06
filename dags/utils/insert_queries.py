from utils.file_util import cargar_datos

"""
QUERIES MUJERES
"""

# city insertion
def insert_query_mineria_mujeres(**kwargs):
    
    insert = f"INSERT INTO mineria_mujeres (idMin,recurso,cantidad,unidades) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idMin}\',\'{row.recurso}\',{row.cantidad},\'{row.unidades}\');\n"
        return insertQuery
    except:
        return ""

# customer insertion
def insert_query_educacion_mujeres(**kwargs):
    insert = f"INSERT INTO educacion_mujeres (idEdu,coberturaNeta) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idEdu}\',{row.coberturaNeta});\n"
        return insertQuery
    except:
        return ""

# date insertion
def insert_query_demografia_mujeres(**kwargs):
    # To Do: recuerden que tratar con variables de tipo "DATE" en sql se hace uso de la instrucción TO_DATE. ejemplo: TO_DATE('31-12-2022','DD-MM-YYYY')
    insert = f"INSERT INTO demografia_mujeres (idDem,cantMujeres,porcentajeMujeres) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idDem}\',{row.cantMujeres},{row.porcentajeMujeres});\n"
        return insertQuery
    except:
        return ""

# employee insertion
def insert_query_violencia_mujeres(**kwargs):
    # To Do
    insert = f"INSERT INTO violencia_mujeres (idVio,homiMujer,violenciaMuj,tasaVioMuj) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idVio}\',{row.homiMuj},{row.violenciaMuj},{row.tasaVioMuj});\n"
        return insertQuery
    except:
        return ""


    
# fact order insert
def insert_query_municipio_mujeres(**kwargs):
    # To Do
    insert = f"INSERT INTO municipio_mujeres (id,nombre,departamento,anio,idMin,idEdu,idDem,idVio) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.id}\',\'{row.nombre}\',\'{row.departamento}\',{row.anio},\'{row.idMin}\',\'{row.idEdu}\',\'{row.idDem}\',\'{row.idVio}\');\n"
        return insertQuery
    except:
        return ""


"""
QUERIES VIOLENCIA
"""

# city insertion
def insert_query_mineria_violencia(**kwargs):
    
    insert = f"INSERT INTO mineria_violencia (idMin,recurso,cantidad,unidades) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idMin}\',\'{row.recurso}\',{row.cantidad},\'{row.unidades}\');\n"
        return insertQuery
    except:
        return ""

# customer insertion
def insert_query_educacion_violencia(**kwargs):
    insert = f"INSERT INTO educacion_violencia (idEdu,coberturaNeta) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idEdu}\',{row.coberturaNeta});\n"
        return insertQuery
    except:
        return ""

# date insertion
def insert_query_demografia_violencia(**kwargs):
    # To Do: recuerden que tratar con variables de tipo "DATE" en sql se hace uso de la instrucción TO_DATE. ejemplo: TO_DATE('31-12-2022','DD-MM-YYYY')
    insert = f"INSERT INTO demografia_violencia (idDem,cantPersonas) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idDem}\',{row.cantPersonas});\n"
        return insertQuery
    except:
        return ""

# employee insertion
def insert_query_violencia_violencia(**kwargs):
    # To Do
    insert = f"INSERT INTO violencia_violencia (idVio,cSecuestros,cDesplazadas,cDesplaExp,cHomicidios) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.idVio}\',{row.cSecuestros},{row.cDesplazadas},{row.cDesplaExp},{row.cHomicidios});\n"
        return insertQuery
    except:
        return ""


    
# fact order insert
def insert_query_municipio_violencia(**kwargs):
    # To Do
    insert = f"INSERT INTO municipio_violencia (id,nombre,departamento,anio,idMin,idEdu,idDem,idVio) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.id}\',\'{row.nombre}\',\'{row.departamento}\',{row.anio},\'{row.idMin}\',\'{row.idEdu}\',\'{row.idDem}\',\'{row.idVio}\');\n"
        return insertQuery
    except:
        return ""