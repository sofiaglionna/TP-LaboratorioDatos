import pandas as pd
import duckdb as dd

# ============================================
# IMPORTAMOS CSV (DataFrames del DER)
# ============================================

dfDepartamento = pd.read_csv("datasets/Finales/df_Departamento.csv")
dfEE = pd.read_csv("datasets/Finales/df_EE.csv")
dfEP = pd.read_csv("datasets/Finales/df_EP.csv")
dfPoblacion = pd.read_csv("datasets/Finales/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/Finales/EP_con_desc.csv")
dfProvincia = pd.read_csv("datasets/Finales/df_Provincia.csv")

# ============================================
# CORREGIMOS TIPOS DE DATOS
# ============================================

dfEE["departamento_id"] = pd.to_numeric(dfEE["departamento_id"], errors="coerce").astype("Int64")

# Asegurar tipos de Población
dfPoblacion["departamento_id"] = pd.to_numeric(dfPoblacion["departamento_id"], errors="coerce").astype("Int64")
dfPoblacion["Edad"]            = pd.to_numeric(dfPoblacion["Edad"], errors="coerce")
dfPoblacion["Casos"]           = pd.to_numeric(dfPoblacion["Casos"], errors="coerce")

# Asegurar tipos de EE (por si vinieron como texto)
cols_ee = ["SNU","SNU - INET","Secundario - INET","Nivel inicial - Jardín maternal",
           "Nivel inicial - Jardín de infantes","Primario","Secundario"]
for c in cols_ee:
    dfEE[c] = pd.to_numeric(dfEE[c], errors="coerce").fillna(0).astype("int64")
dfEE["departamento_id"] = pd.to_numeric(dfEE["departamento_id"], errors="coerce").astype("Int64")


# ============================================
# 1.i
# ============================================

# Contamos los Establecimientos Educativos que hay en cada departamento.
CantEE = """
    SELECT
        departamento_id,
        SUM("Nivel inicial - Jardín maternal" + "Nivel inicial - Jardín de infantes") AS Jardines,
        SUM("Primario") AS Primarios,
        SUM("Secundario - INET" + "Secundario") AS Secundarios,
        SUM("SNU" + "SNU - INET") AS SNU
        FROM dfEE
        GROUP BY departamento_id
"""
dfCantEE = dd.query(CantEE).df()


# Calculamos las poblaciones de cada rango de edad según nivel educativo por departamento siguiendo el siguiente criterio:
# Población Jardín = 0 - 5
# Población Primaria = 6 - 12
# Población Secundaria = 13 - 18
# SNU (Educación Adultos) = 19+ 

PoblacionXJardinEnDpto = """
    SELECT 
        departamento_Id,
        SUM(Casos) AS "Poblacion Jardin"
        FROM dfPoblacion
        WHERE Edad < 6
        GROUP BY departamento_id
        """
dfPoblacionXJardinEnDpto = dd.query(PoblacionXJardinEnDpto).df()

PoblacionXPrimarioEnDpto = """
    SELECT 
        departamento_Id,
        SUM(Casos) AS "Poblacion Primaria"
        FROM dfPoblacion
        WHERE Edad > 5 AND Edad < 13
        GROUP BY departamento_id
        """
dfPoblacionXPrimarioEnDpto = dd.query(PoblacionXPrimarioEnDpto).df()

PoblacionXSecundarioEnDpto = """
    SELECT 
        departamento_Id,
        SUM(Casos) AS "Poblacion Secundaria"
        FROM dfPoblacion
        WHERE Edad > 12 AND Edad < 19
        GROUP BY departamento_id
        """
dfPoblacionXSecundarioEnDpto = dd.query(PoblacionXSecundarioEnDpto).df()

PoblacionXSNUEnDpto = """
    SELECT 
        departamento_Id,
        SUM(Casos) AS "Poblacion Adultos"
        FROM dfPoblacion
        WHERE Edad > 18
        GROUP BY departamento_id
        """
dfPoblacionXSNUEnDpto = dd.query(PoblacionXSNUEnDpto).df()

# Ahora juntamos la información de Dpto, Cant de EE por Dpto y Población x Dpto (Relacionando todas estas tablas por departamento_id)
# Vamos a usar alias porque si no son muy largos los nombres: 

i = """
    SELECT 
        p.provincia, 
        d.departamento, 
        ee.Jardines, 
        pj."Poblacion Jardin", 
        ee.Primarios, 
        pp."Poblacion Primaria", 
        ee.Secundarios, 
        ps."Poblacion Secundaria", 
        ee.SNU, 
        pa."Poblacion Adultos"
    FROM dfDepartamento AS d
    LEFT JOIN dfProvincia AS p ON d.provincia_id = p.provincia_id
    INNER JOIN dfCantEE AS ee ON d.departamento_id = ee.departamento_id
    INNER JOIN dfPoblacionXJardinEnDpto AS pj ON d.departamento_id = pj.departamento_id
    INNER JOIN dfPoblacionXPrimarioEnDpto AS pp ON d.departamento_id = pp.departamento_id
    INNER JOIN dfPoblacionXSecundarioEnDpto AS ps ON d.departamento_id = ps.departamento_id
    INNER JOIN dfPoblacionXSNUEnDpto AS pa ON d.departamento_id = pa.departamento_id
    ORDER BY p.provincia ASC
    """
dfi = dd.query(i).df()


# ======================
# 1.ii
# ======================

# Contamos cant empleados por departamento (varones y mujeres juntos).
trabajadoresXDepartamento = """
    SELECT 
        departamento_id,
        SUM(varones + mujeres) AS total
    FROM dfEP
    GROUP BY departamento_id
"""
dftrabajadoresXDepartamento = dd.query(trabajadoresXDepartamento).df()


# Ahora juntamos provincia (dfProvincia), departamento (dfDepartamento) y la cant de empleados de dftrabajadoresXDepartamento.
# Relacionamos todo mediante departamento_id
ii = """
        SELECT  dfProvincia.Provincia,departamento,total
        FROM  dftrabajadoresXDepartamento
        INNER JOIN dfDepartamento
        ON dftrabajadoresXDepartamento.departamento_id = dfDepartamento.departamento_id
        INNER JOIN dfProvincia
        ON dfDepartamento.provincia_id = dfProvincia.provincia_id
        ORDER BY dfProvincia.Provincia, total DESC
"""

dfii = dd.query(ii).df()

# =====================
# 1.iii
# ======================

# Contamos la cant de empresas exportadoras por departamento que emplean mujeres.
# Aclaración: Los departamentos que no tengan empresas exportadoras y/o no tengan empleadas mujeres no se verán en la tabla. 
EExportadorasMujeres = """
                    SELECT departamento_id, empresas_exportadoras
                    FROM dfEP
                    WHERE empresas_exportadoras > 0 AND mujeres > 0
"""
dfEExportadorasMujeres = dd.query(EExportadorasMujeres).df()

TotalExportadorasMujeresXDpto = """
                    SELECT departamento_id,
                    SUM (empresas_exportadoras) AS Cant_Expo_Mujeres
                    FROM dfEExportadorasMujeres
                    GROUP BY departamento_id
"""
dfTotalExportadorasMujeresXDpto = dd.query(TotalExportadorasMujeresXDpto).df()

# Ahora queremos la cant total de EE, sin tener en cuenta el nivel, por dpto. Es decir cant total de EE x cada depto.
# Para ello, utilizamos el df del punto 1.i simplemento sumando todos los EE de cada nivel.

EEtotalesXDepto = """
          SELECT departamento_id, SUM(Jardines + Primarios + Secundarios + SNU) AS "Total Establecimientos Educativos"
          FROM dfCantEE
          GROUP BY departamento_id
"""
dfEEtotalesXDepto = dd.query(EEtotalesXDepto).df()

# También vamos a necesitar la población total x departamento.

PoblacionXDepto = """
            SELECT departamento_id, SUM(Casos) AS Poblacion
            FROM dfPoblacion
            GROUP BY departamento_id
"""
dfPoblacionXDepto = dd.query(PoblacionXDepto).df()

iiiconNULLS = """
      SELECT 
          p.provincia AS Provincia,
          d.departamento AS Departamento,
          em.Cant_Expo_Mujeres,
          dfEEtotalesXDepto."Total Establecimientos Educativos" AS Cant_EE,
          dfPoblacionXDepto.Poblacion
      FROM dfDepartamento AS d
      INNER JOIN dfProvincia AS p ON d.provincia_id = p.provincia_id
      LEFT JOIN dfTotalExportadorasMujeresXDpto AS em ON d.departamento_id = em.departamento_id
      INNER JOIN dfEEtotalesXDepto ON d.departamento_id = dfEEtotalesXDepto.departamento_id
      INNER JOIN dfPoblacionXDepto ON d.departamento_id = dfPoblacionXDepto.departamento_id  
      ORDER BY Cant_EE DESC, Cant_Expo_Mujeres DESC, Provincia ASC, Departamento ASC
"""
dfiiiconNULLS = dd.query(iiiconNULLS).df()
# Hacemos un LEFT JOIN en la tabla de dfTotalExportadorasMujeresXDpt para que sigan apareciendo aquellos departamentos sin exportadoras con empleo femenino.
# Al hacer esto, nos aparecerán NULLS en aquellos departamentos sin exportadoras con empleo femenino, entonces reemplazamos los NULLS simplemente con ceros:
iii = """
      SELECT Provincia, 
      Departamento, 
      IFNULL(Cant_Expo_Mujeres, 0) AS Cant_Expo_Mujeres,
      Cant_EE,
      Poblacion
      FROM dfiiiconNULLS
      """
dfiii = dd.query(iii).df()
#%%
# ======================
# 1.iv
# ======================

# Calculamos promedio de los puestos de trabajo de los departamentos de la cada provincia:

# Trabajadores totales por provincia: Utilizamos trabajadoresXDepartamento del item ii:
trabajadoresXProvinciaRepetidos = """
      SELECT d.provincia_id, txd.total AS trabajadores
      FROM dfDepartamento as d
      INNER JOIN dftrabajadoresXDepartamento AS txd 
      ON d.departamento_id = txd.departamento_id
"""
dftrabajadoresXProvinciaRepetidos = dd.query(trabajadoresXProvinciaRepetidos).df()

trabajadoresXProvincia = """
      SELECT p.provincia_id AS Provincia, SUM(trabajadores) AS cant_empleos
      FROM dftrabajadoresXProvinciaRepetidos as txp
      INNER JOIN dfProvincia AS p 
      ON txp.provincia_id = p.provincia_id
      GROUP BY p.Provincia_id
      ORDER BY p.Provincia_id
"""
dftrabajadoresXProvincia= dd.query(trabajadoresXProvincia).df()

# Necesitamos la cantidad de Departamentos por Provincia para calcular la cant de empleo promedio por departamento:
cantDeptosXProvincia = """
      SELECT p.provincia_id AS Provincia, COUNT(*) AS cant_departamentos
      FROM dfDepartamento AS d
      INNER JOIN dfProvincia AS p 
      ON d.provincia_id = p.provincia_id 
      GROUP BY p.Provincia_id
      ORDER BY p.Provincia_id
"""
dfcantDeptosXProvincia= dd.query(cantDeptosXProvincia).df()


# Ahora ya tenemos la información para calcular el promedio de puestos de trabajo por Departamento;
# Tenemos el total de puestos de trabajo por provincias (dftrabajadoresXProvincia)
# Y tenemos la cant de Deptartamentos por provincia (dfcantDeptosXProvincia)

#creo un dataFrame con las provincias y una columna vacia para asignar el promedio
PromedioPorProvincia = """
            SELECT provincia_id
            FROM dfProvincia
            ORDER BY provincia_id
"""
dfPromedioPorProvincia = dd.query(PromedioPorProvincia).df()
dfPromedioPorProvincia['Promedio'] = 0
for i,row in dftrabajadoresXProvincia['cant_empleos'].items():
    cantDepartamentosi = dfcantDeptosXProvincia.loc[i,'cant_departamentos']
    promProvinciai= row/cantDepartamentosi
    dfPromedioPorProvincia.loc[i,'Promedio'] = promProvinciai
    
#Ahora hacemos una tabla de provincia,departamento,clae6,cant_empleados (en el departamento),promedio trabajadores en provincia
# con un inner join. Luego nos quedamos solo con los casos donde cant_empleados>promedio y recortamos el clae6

#%% 





