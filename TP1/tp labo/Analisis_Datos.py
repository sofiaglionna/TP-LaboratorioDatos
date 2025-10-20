import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt

"""
===============================================================================
                    Trabajo Práctico 01 - Laboratorio de Datos
===============================================================================
2do. Cuatrimestre - 2025

Integrantes del grupo:
---------------------
- Felix Soriano
- Sofia Glionna
- Ramiro Gantman

Descripción:
-------------
En este archivo (Analisis_Datos.py) se encuentran todos los ejercicios de análisis
de los datos. Primero (1) los reportes utilizando sólo consultas SQL y luego (2) los
gráficos utilizando herramientas de visualización (matplotlib) a partir también de colsultas SQL.
===============================================================================
"""

# ============================================
# IMPORTAMOS CSV (DataFrames del DER)
# ============================================

dfDepartamento = pd.read_csv("datasets/TablasModelo/df_Departamento.csv")
dfEE = pd.read_csv("datasets/TablasModelo/df_EE.csv")
dfEP = pd.read_csv("datasets/TablasModelo/df_EP.csv")
dfPoblacion = pd.read_csv("datasets/TablasModelo/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/TablasModelo/EP_con_desc.csv")
dfProvincia = pd.read_csv("datasets/TablasModelo/df_Provincia.csv")

# ============================================
# CORREGIMOS TIPOS DE DATOS
# ============================================

dfEE["departamento_id"] = pd.to_numeric(dfEE["departamento_id"], errors="coerce").astype("Int64")

# cambiamos los tipos de datos de Población
dfPoblacion["departamento_id"] = pd.to_numeric(dfPoblacion["departamento_id"], errors="coerce").astype("Int64")
dfPoblacion["Edad"]            = pd.to_numeric(dfPoblacion["Edad"], errors="coerce")
dfPoblacion["Casos"]           = pd.to_numeric(dfPoblacion["Casos"], errors="coerce")

# Convertimos los tipos de datos de EE
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

# ======================
# 1.iv
# ======================

# Primero calculamos promedio de los puestos de trabajo de los departamentos de la cada provincia:

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

# Creamos un dataFrame con las provincias y una columna vacía para asignar el promedio:
PromedioPorProvincia = """
            SELECT *
            FROM dfProvincia
            ORDER BY provincia_id
"""
dfPromedioPorProvincia = dd.query(PromedioPorProvincia).df()
dfPromedioPorProvincia['Promedio'] = 0
for i,row in dftrabajadoresXProvincia['cant_empleos'].items():
    cantDepartamentosi = dfcantDeptosXProvincia.loc[i,'cant_departamentos']
    promProvinciai= row/cantDepartamentosi
    dfPromedioPorProvincia.loc[i,'Promedio'] = promProvinciai
    
# Ahora hacemos una tabla de provincia, departamento, clae6, cant_empleados (en el departamento), promedio trabajadores en provincia con un join. 
# Luego nos quedamos solo con los casos donde cant_empleados > promedio y recortamos el clae6.
empleadosPorDeptoyProv = """
        SELECT dfPromedioPorProvincia.Provincia, departamento, dfDepartamento.Departamento_id,total
        FROM dfDepartamento
        LEFT OUTER JOIN dfPromedioPorProvincia
        ON dfDepartamento.Provincia_id = dfPromedioPorProvincia.Provincia_id
        LEFT OUTER JOIN dftrabajadoresXDepartamento
        ON dfDepartamento.Departamento_id = dftrabajadoresXDepartamento.departamento_id
        WHERE total>Promedio
"""
dfempleadosPorDeptoyProv = dd.query(empleadosPorDeptoyProv).df()

trabajadoresxclae6ydepto = """
        SELECT departamento_id,clae6,
        SUM(varones + mujeres) AS trabajadores
        FROM dfEP
        GROUP BY departamento_id,clae6
"""
dftrabajadoresxclae6ydepto = dd.query(trabajadoresxclae6ydepto).df()

MaxTrabajadoresPorClaeEnDepartamento = """
        SELECT departamento_id, ANY_VALUE(clae6) AS clae3, trabajadores
        FROM dftrabajadoresxclae6ydepto
        WHERE (departamento_id, trabajadores) IN (
            SELECT departamento_id, MAX(trabajadores)
            FROM dftrabajadoresxclae6ydepto
            GROUP BY departamento_id)
        GROUP BY departamento_id, trabajadores
        ORDER BY departamento_id
"""
dfMaxTrabajadoresPorClaeEnDepartamento = dd.query(MaxTrabajadoresPorClaeEnDepartamento).df()

for i,row in dfMaxTrabajadoresPorClaeEnDepartamento['clae3'].items():
    if len(str(row)) == 5:
        res = '0'+str(row)[0:2]
    else:
        res = str(row)[0:3]
    dfMaxTrabajadoresPorClaeEnDepartamento.loc[i,'clae3'] = res
    
iv = """
        SELECT provincia AS Provincia,departamento AS Departamento,clae3 AS CLAE3,trabajadores AS "Cant. empleos"
        FROM dfempleadosPorDeptoyProv
        LEFT OUTER JOIN dfMaxTrabajadoresPorClaeEnDepartamento
        ON dfempleadosPorDeptoyProv.Departamento_id = dfMaxTrabajadoresPorClaeEnDepartamento.departamento_id
"""
dfiv=dd.query(iv).df()


#%%#######################################################
# ======================= GRAFICOS =======================
# ########################################################


# ======================
# 2.i
# ======================

# Utilizando la tabla creada para el ej 1.iv (dftrabajadoresXProvincia), le ponemos nombre a las provincias, ya que esta tiene el id.

EmpleadosXProvincia = """
            SELECT p.provincia, cant_empleos
            FROM dftrabajadoresXProvincia AS tp
            INNER JOIN dfProvincia AS p
            ON tp.Provincia = p.provincia_id
            ORDER BY cant_empleos DESC 
"""
dfEmpleadosXProvincia=dd.query(EmpleadosXProvincia).df()


plt.figure(figsize=(10, 6))
plt.bar(dfEmpleadosXProvincia['provincia'], dfEmpleadosXProvincia['cant_empleos'])
plt.title('Cantidad de empleos por provincia', fontsize=14, fontweight='bold')
plt.xlabel('Provincia', fontsize=12)
plt.ylabel('Cantidad de empleados', fontsize=12)
plt.xticks(rotation=45, ha='right') # Esto es para rotar los nombres pq si no se amontonan todos.

#%%=====================
# 2.ii
# ======================

# Para el siguiente gráfico nos va a convenir usar un gráfico de dispersión donde cada
# punto sea un departamento, el eje x la población del grupo, el eje y la cant de EE de
# ese grupo, y cada grupo con distinto color (siendo cada grupo jardin, primario, secundario, SNU).
# Para ello usamos el df creado en el punto 1.i

plt.figure(figsize=(8,6))

plt.scatter(dfi['Poblacion Jardin'], dfi['Jardines'], s=20,color='green', label='Jardines')
plt.scatter(dfi['Poblacion Primaria'], dfi['Primarios'], s=20,color='blue', label='Primarios')
plt.scatter(dfi['Poblacion Secundaria'], dfi['Secundarios'], s=20,label='Secundarios', color='orange')
plt.scatter(dfi['Poblacion Adultos'], dfi['SNU'], color='red', s=20,label='SNU')
# con s=20 hacemos más chicos los puntos
plt.xscale('log') # algunas provincias tienen poblaciones adultas mucho mayores, entonces usamos escala log en el eje x (poblacion)

plt.xlabel('Población')
plt.ylabel('Cantidad de Establecimientos Educativos')
plt.title('Relación entre población y cantidad de EE por nivel educativo x departamento')
plt.legend() #cuadradito con nombres de los niveles educativos
plt.grid(True)
plt.show()


#%%=====================
# 2.iii
# ======================

#FALTA HACER QUE ESTE ORDENADO POR MEDIANA

EEPorDepartamento = dfEEtotalesXDepto
departamentoConProvincia = """
            SELECT departamento_id, provincia
            FROM dfDepartamento
            LEFT OUTER JOIN dfProvincia
            ON dfDepartamento.Provincia_id = dfProvincia.Provincia_id
"""
dfdepartamentoconProvincia = dd.query(departamentoConProvincia).df()

EEPorDepartamentoyProvincia ="""
        SELECT provincia, "Total Establecimientos Educativos"
        FROM EEPorDepartamento
        LEFT OUTER JOIN dfdepartamentoconProvincia
        ON dfdepartamentoconProvincia.departamento_id = EEPorDepartamento.departamento_id
"""

dfEEPorDepartamentoyProvincia = dd.query(EEPorDepartamentoyProvincia).df()

EEenProvincia= {}
for i,row in dfEEPorDepartamentoyProvincia.iterrows():
    provincia = row['provincia']
    if provincia not in EEenProvincia.keys():
        EEenProvincia[provincia] = [dfEEPorDepartamentoyProvincia.loc[i,'Total Establecimientos Educativos']]
    else:
        EEenProvincia[provincia].append(dfEEPorDepartamentoyProvincia.loc[i,'Total Establecimientos Educativos'])

provinciasaux = list(EEenProvincia.keys())
EEPorProvaux = list(EEenProvincia.values())

#ordeno EE PorProv de menor a mayor para calcular mediana
Provinciasordenadas = []
EEPorProvordenados = []
indices = []
#No ordena bien. No ordena las provincias por EE, se debe meter adentro de cada lista en EEPorProvaux
#Entonces no se moverian las provincias ya que ordeno los EE dentro de cada provincia
for j in EEPorProvaux:
    indicej = 0
    for i in EEPorProvaux:
        if j>i:
            indicej+=1
    indices.append(indicej)

for i in range(0,len(indices)):
    for j in range (0,len(indices)):
        if i == indices[j]:
            EEPorProvordenados.append(EEPorProvaux[j])
            Provinciasordenadas.append(provinciasaux[j])
    
#calculo medianas de EEPorProv
medianas = []
for j in EEPorProvordenados:
    if len(j)%2 != 0:
        medianas.append(j[(int(len(j)-1)//2)])
    else:
        medianas.append((j[int(len(j)//2)]+j[int((len(j)//2))-1])/2)
print(medianas)
#reordeno Provincias y EEPorProv de mayor a menor mediana
indicesMedianas = []
Provincias = []
EEPorProv = []
for j in medianas:
    indicej = 0
    for i in medianas:
        if j>i:
            indicej+=1
    indicesMedianas.append(indicej)
print(indicesMedianas)
for i in range(0,len(indicesMedianas)):
    for j in range (0,len(indicesMedianas)):
        if i == indicesMedianas[j]:
            EEPorProv.append(EEPorProvordenados[j])
            Provincias.append(Provinciasordenadas[j])

fig, ax = plt.subplots(figsize=(10, 6))
VP = ax.boxplot(EEPorProv, labels= Provincias, patch_artist=True)
ax.tick_params(axis='x', rotation=45, labelsize=8)
for label in ax.get_xticklabels():
    label.set_ha('right')
plt.tight_layout()
plt.show()

#%%=====================
# 2.iv
# ======================

# Calculamos a partir de dfCantEE la cantidad de EE totales por departamento:

CantEETotales = """
        SELECT departamento_id, SUM(Jardines + Primarios + Secundarios + SNU) AS Cantidad_EE
        FROM dfCantEE
        GROUP BY departamento_id
"""
dfCantEETotales = dd.query(CantEETotales).df()

# Utilizamos el df "dftrabajadoresXDepartamento" creado para el punto 1.ii
# y la población total por departamento"dfPoblacionXDepto".

# Juntamos los 3 dfs:
EE_Empleados_Poblacion = """
        SELECT pd.departamento_id, pd.Poblacion, t.total AS Empleados, EE.Cantidad_EE AS EE
        FROM dfPoblacionXDepto AS pd
        INNER JOIN dftrabajadoresXDepartamento AS t ON pd.departamento_id = t.departamento_id
        INNER JOIN dfCantEETotales AS EE ON pd.departamento_id = EE.departamento_id
        ORDER BY pd.departamento_id
"""
dfEE_Empleados_Poblacion = dd.query(EE_Empleados_Poblacion).df()

# Ahora hacemos la cuenta para 1000 habitantes:
# Dividimos por la poblacion en ese dpto y multiplicamos por 1000.

dfEE_Empleados_Poblacion["EE_por_1000hab"] = dfEE_Empleados_Poblacion["EE"] / dfEE_Empleados_Poblacion["Poblacion"] * 1000
dfEE_Empleados_Poblacion["Empleados_por_1000hab"] = dfEE_Empleados_Poblacion["Empleados"] / dfEE_Empleados_Poblacion["Poblacion"] * 1000
# Observación: En departamentos como el 2007 (COMUNA 1) la cantidad de empleados supera la población.
# Esto es correcto, y nos indica que los empleados de COMUNA 1 no son residentes de ahi mismo.

# Ahora graficamos:
plt.figure(figsize=(8,6))
plt.scatter(dfEE_Empleados_Poblacion["EE_por_1000hab"], dfEE_Empleados_Poblacion["Empleados_por_1000hab"], color='red', alpha=0.7, s=25)

plt.xlabel("Establecimientos Educativos cada 1000 habitantes")
plt.ylabel("Empleados cada 1000 habitantes")
plt.title("Relación entre EE y trabajadores por cada 1000 habitantes (por departamento)")

plt.xscale('log') # Es util para verlo nosotros pero para el final lo sacaría.
plt.yscale('log')

plt.grid(True)
plt.tight_layout()
plt.show()




#%%=====================
# 2.v
# ======================

# Obtenemos todas las clae6 y la cantidad de mujeres que trabajan en cada clave
df_mujeres_por_clae6 = dfEP.groupby('clae6', as_index=False)['mujeres'].sum().sort_values("mujeres", ascending=False)

#Sacamos todos los valores que son 0
df_mujeres_por_clae6_final = df_mujeres_por_clae6[df_mujeres_por_clae6["mujeres"] > 0]

# Obtenemos las 5 clae6 con mayor y menor cantidad de mujeres
mayores_5 = df_mujeres_por_clae6_final.head(5)
menores_5 = df_mujeres_por_clae6_final.tail(5)

# Concatenamos los dfs del punto anterior
finales_10 = pd.concat([mayores_5, menores_5])

# Unimos el dataset anterior con sus respectivas descripciones
df_mujeres_por_clae6_con_desc = pd.merge(finales_10, dfEP_con_desc[["clae6_desc","clae6"]], on="clae6", how="left")

# Calculamos el promedio de mujeres sobre el total de empleados

cant_mujeres = dfEP["mujeres"].sum()
cant_varones = dfEP["varones"].sum()

promedio_mujeres_sobre_total = (cant_mujeres/(cant_mujeres + cant_varones))*100

# --- Datos ---
descs = df_mujeres_por_clae6_con_desc["clae6_desc"].tolist()
vals  = df_mujeres_por_clae6_con_desc["mujeres"].tolist()

# función para cortar texto cada N caracteres
def cortar_lineas(texto, ancho=18):
    palabras = texto.split()
    lineas, linea_actual = [], ""
    for p in palabras:
        if len(linea_actual) + len(p) + 1 <= ancho:
            linea_actual += (" " if linea_actual else "") + p
        else:
            lineas.append(linea_actual)
            linea_actual = p
    if linea_actual:
        lineas.append(linea_actual)
    return "\n".join(lineas)

labels_multilinea = [cortar_lineas(t, ancho=18) for t in descs]

# --- Gráfico estilo Excel ---
fig, ax = plt.subplots(figsize=(14, 7))
ax.bar(range(len(vals)), vals, width=0.55, color="cornflowerblue")

ax.grid(axis='y', alpha=0.3)
ax.axhline(promedio_mujeres_sobre_total, color="black", linestyle="--", linewidth=1)

ax.set_xticks(range(len(vals)))
ax.set_xticklabels(labels_multilinea, fontsize=8, ha='center')

ax.tick_params(axis='x', pad=10)
plt.subplots_adjust(bottom=0.45)  # aumentá si se corta el texto

ax.set_title("Participación femenina por actividad económica")
ax.set_ylabel("Cantidad de empleos femeninos")
ax.set_xlabel("")

plt.tight_layout()
plt.show()
