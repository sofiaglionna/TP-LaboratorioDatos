import pandas as pd
import duckdb as dd

ArchivoEP = pd.read_csv("datasets/Iniciales/Datos_por_departamento_actividad_y_sexo.csv")
ArchivoEE = pd.read_csv("datasets/Iniciales/2022_padron_oficial_establecimientos_educativos.csv", header=6)
ArchivoPoblacion = pd.read_csv("datasets/Iniciales/padron_poblacion.csv", header=11)
ArchivoActividadesEstablecimientos = pd.read_csv("datasets/Iniciales/actividades_establecimientos.csv")

Departamento = """
                SELECT DISTINCT in_departamentos AS departamento_id,departamento,provincia_id,provincia
                FROM ArchivoEP
                """
dfDepartamento = dd.query(Departamento).df()


#cantidad de varones que emplea cada rubro por departamento en 2022
VaronesEmpleados = """
                    SELECT anio, clae6,in_departamentos AS departamento_id, empleo AS varones, empresas_exportadoras
                    FROM ArchivoEP
                    WHERE  anio = 2022 AND genero = 'Varones'
                    """
dfVaronesEmpleados=dd.query(VaronesEmpleados).df()       

#cantidad de mujeres que emplea cada rubro por departamento en 2022
MujeresEmpleados = """ 
                    SELECT anio, clae6,in_departamentos AS departamento_id, empleo AS mujeres, empresas_exportadoras
                    FROM ArchivoEP
                    WHERE anio = 2022 AND genero = 'Mujeres'
                    """
dfMujeresEmpleadas=dd.query(MujeresEmpleados).df()

#junto ambas tablas de varones y mujeres 
EP_con_nulls = """
                    SELECT dfVaronesEmpleados.clae6, dfVaronesEmpleados.departamento_id, dfVaronesEmpleados.varones, dfMujeresEmpleadas.mujeres, dfVaronesEmpleados.empresas_exportadoras
                    FROM dfVaronesEmpleados
                    LEFT OUTER JOIN dfMujeresEmpleadas
                    ON dfVaronesEmpleados.clae6 = dfMujeresEmpleadas.clae6 AND dfVaronesEmpleados.departamento_id = dfMujeresEmpleadas.departamento_id

                    UNION
                    
                    SELECT dfMujeresEmpleadas.clae6, dfMujeresEmpleadas.departamento_id, dfVaronesEmpleados.varones, dfMujeresEmpleadas.mujeres, dfMujeresEmpleadas.empresas_exportadoras
                    FROM dfMujeresEmpleadas
                    LEFT OUTER JOIN dfVaronesEmpleados
                    ON dfVaronesEmpleados.clae6 = dfMujeresEmpleadas.clae6 AND dfVaronesEmpleados.departamento_id = dfMujeresEmpleadas.departamento_id
                    """
dfEP_con_nulls = dd.query(EP_con_nulls).df()

EP = """
        SELECT clae6, departamento_id, empresas_exportadoras,
        CASE WHEN varones IS NOT NULL THEN varones ELSE 0 END AS varones,
        CASE WHEN mujeres IS NOT NULL THEN mujeres ELSE 0 END AS mujeres                     
        FROM dfEP_con_nulls
        """
dfEP = dd.query(EP).df()


"""
EPaux lo usamos para ver que en EP no borraramos filas de 2022. Basicamente vemos cuantos departamentos y clae6 
(que son la superclave del archivo original si no se tiene en cuenta el año) hay sin repeticiones por varones y mujeres
y vemos si coincide con lo que nos quedo en EP. Ya que en esta juntamos varones y mujeres en una misma fila por 
departamento y clae6.
"""
EPaux = """
        SELECT DISTINCT in_departamentos, clae6
        FROM ArchivoEP
        WHERE anio = 2022
        
    """
dfEPaux = dd.query(EPaux).df()


EEconDepartamentoPorNombre = """
                                SELECT cueanexo,departamento,SNU,"SNU - INET","Secundario - INET", "Nivel inicial - Jardín maternal","Nivel inicial - Jardín de infantes", Primario,Secundario
                                FROM ArchivoEE
                                """
dfEEconDepartamentoPorNombre = dd.query(EEconDepartamentoPorNombre).df()      

EE = """
        SELECT cueanexo,departamento_id,SNU,"SNU - INET","Secundario - INET", "Nivel inicial - Jardín maternal","Nivel inicial - Jardín de infantes", Primario,Secundario     
        FROM dfEEconDepartamentoPorNombre
        LEFT OUTER JOIN dfDepartamento
        ON dfEEconDepartamentoPorNombre.departamento = dfDepartamento.Departamento
      """
dfEE = dd.query(EE).df()

#Normalizamos tipos numéricos en columnas del padrón educativo 
cols_a_numericas = [
    "SNU",
    "SNU - INET",
    "Secundario - INET",
    "Nivel inicial - Jardín maternal",
    "Nivel inicial - Jardín de infantes",
    "Primario",
    "Secundario"
]

for col in cols_a_numericas:
    dfEE[col] = pd.to_numeric(dfEE[col], errors="coerce").fillna(0).astype("int64")

# Aseguramos que departamento_id quede como entero
dfEE["departamento_id"] = pd.to_numeric(dfEE["departamento_id"], errors="coerce").astype("Int64")
dfEE.dtypes

#%%
#corregir población
Poblacion_con_nombre = """
                       SELECT "  de Edad" AS departamento_id, "Unnamed: 1" AS Edad, "Unnamed: 2" AS Casos,"Unnamed: 3" AS "%"
                        FROM ArchivoPoblacion
                        """

dfPoblacion_con_nombre=dd.query(Poblacion_con_nombre).df()
dfPoblacion_con_nombre['provincia_id'] = None




dfPoblacion_con_nombre.dropna(subset=['Edad'], inplace=True)
dfPoblacion_con_nombre.reset_index(drop=True, inplace=True)
dfPoblacion_con_nombre.loc[55530, 'Casos'] = "Nacional"

#%%
"""
En departamento tengo varios departamentos con mismo nombre por lo que solo el nombre no me distingue entre ellos
pero dentro de la misma provincia no pueden existir 2 departamentos de igual nombre. Por eso me guardo el provincia_id
formado por los primeros 2 digitos del codigo de Area
"""
departamento_id = 0
AREA = 0
for i, row in dfPoblacion_con_nombre.iterrows():
    dfPoblacion_con_nombre.loc[i, 'departamento_id'] = departamento_id
    dfPoblacion_con_nombre.loc[i, 'provincia_id'] = AREA
    if row['Casos'] == 'Casos':
        departamento_id= dfPoblacion_con_nombre.iloc[i-1]['Casos']
        AREA= dfPoblacion_con_nombre.iloc[i-1]['Edad'][7:9]
for i, row in dfPoblacion_con_nombre.iterrows():
    if row['Casos'] == "Casos":
        dfPoblacion_con_nombre.drop(i,inplace=True)
        dfPoblacion_con_nombre.drop(i-1,inplace=True)
dfPoblacion_con_nombre.reset_index(drop=True, inplace=True)

dfDepartamento['provincia_id'] = dfDepartamento['provincia_id'].astype(str)

Poblacion = """
SELECT 
    dfDepartamento.departamento_id,
    dfPoblacion_con_nombre.Edad,
    dfPoblacion_con_nombre.Casos,
    CAST(
        REPLACE(
            REPLACE(dfPoblacion_con_nombre."%", '%', ''), 
        ',', '.') 
    AS DOUBLE) AS porcentaje_num
FROM dfPoblacion_con_nombre
LEFT OUTER JOIN dfDepartamento
  ON departamento = dfPoblacion_con_nombre.departamento_id
 AND (dfPoblacion_con_nombre.provincia_id = dfDepartamento.provincia_id 
      OR '0'||dfDepartamento.provincia_id = dfPoblacion_con_nombre.provincia_id)
"""
dfPoblacion = dd.query(Poblacion).df()

dfPoblacion.loc[12185:12287,'departamento_id'] = 6651
dfPoblacion.loc[26525:26623,'departamento_id'] = 22126
dfPoblacion.dropna(subset=['departamento_id'], inplace=True)
dfPoblacion.reset_index(drop=True, inplace=True)

#%% Actividades establecimiento
EP_con_desc = """
                SELECT DISTINCT
                    ArchivoEP.clae6,
                    ArchivoActividadesEstablecimientos.clae6_desc
                FROM ArchivoEP
                LEFT JOIN ArchivoActividadesEstablecimientos
                ON ArchivoEP.clae6 = ArchivoActividadesEstablecimientos.clae6
              """
dfEP_con_desc = dd.query(EP_con_desc).df()

# Export
dfEP_con_desc.to_csv("EP_con_desc.csv", index=False, encoding="utf-8")
dfDepartamento.to_csv("df_Departamento.csv", index=False,encoding ="utf-8")
dfPoblacion.to_csv("df_Poblacion.csv", index=False,encoding ="utf-8")
dfEE.to_csv("df_EE.csv", index=False,encoding ="utf-8")
dfEP.to_csv("df_EP.csv", index=False,encoding ="utf-8")