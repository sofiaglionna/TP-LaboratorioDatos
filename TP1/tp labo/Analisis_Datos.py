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
de los datos. Primero (1) se muestran los reportes utilizando sólo consultas SQL y luego (2) los
gráficos utilizando herramientas de visualización (matplotlib) a partir también de colsultas SQL.
===============================================================================
"""

# ============================================
# IMPORTAMOS CSV (DataFrames del DER)
# ============================================

dfDepartamento = pd.read_csv("datasets/TablasModelo/df_Departamento.csv")
dfEE = pd.read_csv("datasets/TablasModelo/df_EE.csv")
#Me aseguro de pasar clae3 como string porque sino al ser numeros duckdb lo interpreta automaticamente como int
#modificando los valores
dfEP = pd.read_csv("datasets/TablasModelo/df_EP.csv", dtype={"clae3": str})
dfPoblacion = pd.read_csv("datasets/TablasModelo/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/TablasModelo/EP_con_desc.csv")
dfProvincia = pd.read_csv("datasets/TablasModelo/df_Provincia.csv")

# ============================================
# ============= 1. REPORTES SQL ==============
# ============================================

# ======================
# 1.i
# ======================

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

#calculo el promedio de trabajadores por provincia
PromedioPorProvincia = """
      SELECT p.provincia_id,p.provincia, AVG(trabajadores) AS Prom_empleados
      FROM dftrabajadoresXProvinciaRepetidos as txp
      INNER JOIN dfProvincia AS p 
      ON txp.provincia_id = p.provincia_id
      GROUP BY p.Provincia_id,p.provincia
      ORDER BY p.Provincia_id
"""
dfPromedioPorProvincia= dd.query(PromedioPorProvincia).df()
    
# Hace una tabla de provincia, departamento, cant_empleados (en el departamento) con un join. 
# conservando solo con los departamentos donde cant_empleados > promedio de su provincia
empleadosPorDeptoyProv = """
        SELECT dfPromedioPorProvincia.Provincia, departamento, dfDepartamento.Departamento_id,total
        FROM dfDepartamento
        LEFT OUTER JOIN dfPromedioPorProvincia
        ON dfDepartamento.Provincia_id = dfPromedioPorProvincia.Provincia_id
        LEFT OUTER JOIN dftrabajadoresXDepartamento
        ON dfDepartamento.Departamento_id = dftrabajadoresXDepartamento.departamento_id
        WHERE total>Prom_empleados
"""
dfempleadosPorDeptoyProv = dd.query(empleadosPorDeptoyProv).df()

#Hago un tabla con departamento,clae6, clae3 y cantidad de empleados en dicha clae6 en ese departamento
trabajadoresxclae6ydepto = """
        SELECT departamento_id,clae6, clae3,
        SUM(varones + mujeres) AS trabajadores
        FROM dfEP
        GROUP BY departamento_id,clae6,clae3
"""
dftrabajadoresxclae6ydepto = dd.query(trabajadoresxclae6ydepto).df()

#Por departamento solo conservo la clae6 con mayor numero de empleados y su clae 3 asociada
MaxTrabajadoresPorClaeEnDepartamento = """
        SELECT departamento_id, ANY_VALUE(clae6) AS clae6, trabajadores,clae3,
        FROM dftrabajadoresxclae6ydepto
        WHERE (departamento_id, trabajadores) IN (
            SELECT departamento_id, MAX(trabajadores)
            FROM dftrabajadoresxclae6ydepto
            GROUP BY departamento_id)
        GROUP BY departamento_id, trabajadores,clae3
        ORDER BY departamento_id
"""
dfMaxTrabajadoresPorClaeEnDepartamento = dd.query(MaxTrabajadoresPorClaeEnDepartamento).df()
#paso el departamento_id de MaxTrabajadoresPorClaeEnDepartamento al nombre del departamento y de la provincia a la que pertenece
iv = """
        SELECT provincia AS Provincia,departamento AS Departamento,clae3 AS CLAE3,trabajadores AS "Cant. empleos"
        FROM dfempleadosPorDeptoyProv
        LEFT OUTER JOIN dfMaxTrabajadoresPorClaeEnDepartamento
        ON dfempleadosPorDeptoyProv.Departamento_id = dfMaxTrabajadoresPorClaeEnDepartamento.departamento_id
"""
dfiv=dd.query(iv).df()


# ============================================
# =============== 2. GRAFICOS ================
# ============================================


# ======================
# 2.i
# ======================

# Utilizando la tabla creada para el ej 1.iv (dftrabajadoresXProvinciaRepetidos). Hacemos una tabla con la suma
#de todos los trabajadores en una provincia
trabajadoresXProvincia = """
        SELECT provincia_id, SUM(trabajadores) AS cant_empleos
        FROM dftrabajadoresXProvinciaRepetidos
        GROUP BY provincia_id
"""

dftrabajadoresXProvincia = dd.query(trabajadoresXProvincia).df()


#le ponemos nombre a las provincias, ya que esta tiene el id_provincia.
EmpleadosXProvincia = """
            SELECT p.provincia, cant_empleos
            FROM dftrabajadoresXProvincia AS tp
            INNER JOIN dfProvincia AS p
            ON tp.provincia_id = p.provincia_id
            ORDER BY cant_empleos DESC 
"""
dfEmpleadosXProvincia=dd.query(EmpleadosXProvincia).df()


plt.figure(figsize=(10, 6))
plt.bar(dfEmpleadosXProvincia['provincia'], dfEmpleadosXProvincia['cant_empleos'])
plt.title('Cantidad de empleos por provincia', fontsize=14, fontweight='bold')
plt.xlabel('Provincia', fontsize=12)
plt.ylabel('Cantidad de empleados (millones)', fontsize=12)
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

# Juntamos departamentos con sus provincias para tenerlos con nombre:
EEPorDepartamento = dfEEtotalesXDepto
departamentoConProvincia = """
            SELECT departamento_id, provincia
            FROM dfDepartamento
            LEFT OUTER JOIN dfProvincia
            ON dfDepartamento.Provincia_id = dfProvincia.Provincia_id
"""
dfdepartamentoconProvincia = dd.query(departamentoConProvincia).df()

# Establecimientos educativos por departamento y la provincia a la que pertenecen.
EEPorDepartamentoyProvincia ="""
        SELECT provincia, "Total Establecimientos Educativos"
        FROM EEPorDepartamento
        LEFT OUTER JOIN dfdepartamentoconProvincia
        ON dfdepartamentoconProvincia.departamento_id = EEPorDepartamento.departamento_id
"""

dfEEPorDepartamentoyProvincia = dd.query(EEPorDepartamentoyProvincia).df()

# Por cada provincia creamos una lista con el numero de EE en cada departamento (no conservamos a que departamento se refiere).
# solo conservamos el numero de EE.
EEenProvincia= {}
for i,row in dfEEPorDepartamentoyProvincia.iterrows():
    provincia = row['provincia']
    if provincia not in EEenProvincia.keys():
        EEenProvincia[provincia] = [dfEEPorDepartamentoyProvincia.loc[i,'Total Establecimientos Educativos']]
    else:
        EEenProvincia[provincia].append(dfEEPorDepartamentoyProvincia.loc[i,'Total Establecimientos Educativos'])

provinciasaux = list(EEenProvincia.keys())
EEPorProvaux = list(EEenProvincia.values())

EEPorProvordenados = []

# Ordenamos los departamentos dentro de cada provincia de menor a mayor en cantidad de EE.
def Buscarmaximo (v):
    maximo=0
    for i in v:
        if i>maximo:
            maximo=i
    return maximo

def ordenar (v):
    res = []
    maximo = Buscarmaximo(v)
    for j in range(0,len(v)):
        minimo = maximo
        for i in v:
            if i < minimo and i not in res:
                minimo = i
        res.append(minimo)
    return res

for j in EEPorProvaux:
    EEPorProvordenados.append(ordenar(j))
    
# Calculamos medianas de cada provincia
medianas = []
for j in EEPorProvordenados:
    if len(j)%2 != 0:
        medianas.append(j[(int(len(j)-1)//2)])
    else:
        medianas.append((j[int(len(j)//2)]+j[int((len(j)//2))-1])/2)
        
# Reordenamos Provincias y EEPorProv de menor a mayor mediana
indicesMedianas = []
Provincias = []
EEPorProv = []
for j in medianas:
    indicej = 0
    for i in medianas:
        if j>i:
            indicej+=1
    indicesMedianas.append(indicej)

for i in range(0,len(indicesMedianas)):
    for j in range (0,len(indicesMedianas)):
        if i == indicesMedianas[j]:
            EEPorProv.append(EEPorProvordenados[j])
            Provincias.append(provinciasaux[j])

# Finalmente graficamos:
fig, ax = plt.subplots(figsize=(10, 6))
ax.boxplot(EEPorProv, tick_labels= Provincias, patch_artist=True)
ax.set_xticklabels(Provincias, rotation=45, ha='right')
plt.xlabel('Provincia')
plt.ylabel('Cantidad de establecimientos educativos')
plt.title('Cantidad de establecimientos educativos por provincia')
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

# Finalmente graficamos:
plt.figure(figsize=(8,6))
plt.scatter(dfEE_Empleados_Poblacion["EE_por_1000hab"], dfEE_Empleados_Poblacion["Empleados_por_1000hab"], color='red', alpha=0.7, s=25)

plt.xlabel("Establecimientos Educativos cada 1000 habitantes")
plt.ylabel("Empleados cada 1000 habitantes")
plt.title("Relación entre EE y trabajadores por cada 1000 habitantes (por departamento)")

plt.xscale('log') # (Es util para verlo nosotros pero para el final capaz lo sacaría)
plt.yscale('log')

plt.grid(True)
plt.show()


#%%=====================
# 2.v
# ======================

# Obtenemos todas las clae6 y la cantidad de mujeres y trabajadores totales que trabajan en cada clae
df_mujeres_por_clae6 = dfEP.groupby('clae6', as_index=False)['mujeres'].sum().sort_values("mujeres", ascending=False)
trabajadores_y_mujeres_por_clae = """
            SELECT clae6, SUM (varones+mujeres) AS "Total de empleados",SUM(mujeres) AS "Mujeres empleadas"
            FROM dfEP
            GROUP BY clae6
            HAVING SUM(mujeres) > 0
"""
dftrabajadores_y_mujeres_por_clae=dd.query(trabajadores_y_mujeres_por_clae).df()

porcentaje_mujeres_por_clae = """
                    SELECT clae6, ("Mujeres empleadas" * 100.0 / "Total de empleados") AS Porcentaje_mujeres
                    FROM dftrabajadores_y_mujeres_por_clae
                    ORDER BY porcentaje_mujeres
"""

dfporcentaje_mujeres_por_clae=dd.query(porcentaje_mujeres_por_clae).df()

#calculo el porcentaje Promedio de trabajadoras mujeres por clae6
promedioTotalValor = """
            SELECT (SUM("Mujeres empleadas") * 100.0) / SUM("Total de empleados") AS valor
            FROM dftrabajadores_y_mujeres_por_clae
"""
dfpromedioTotalValor = dd.query(promedioTotalValor).df()
promedio = dfpromedioTotalValor.iloc[0,0]

#junto los que tienen mayor porcentaje de mujeres y los que tienen menor
dfmaxYmin_porcentaje = pd.DataFrame()
dfmaxYmin_porcentaje = pd.concat([
    dfporcentaje_mujeres_por_clae[0:5],
    dfporcentaje_mujeres_por_clae[932::],
])
dfmaxYmin_porcentaje.reset_index(drop=True, inplace=True)

maxYmin_Porcentaje_con_Nombre = """
            SELECT clae6_desc AS clae6 ,Porcentaje_mujeres
            FROM dfmaxYmin_porcentaje
            INNER JOIN dfEP_con_desc
            ON dfmaxYmin_porcentaje.clae6 = dfEP_con_desc.clae6
            ORDER BY Porcentaje_mujeres ASC
"""
Clae6_con_Porcentaje_de_Mujeres = dd.query(maxYmin_Porcentaje_con_Nombre).df()

Clae6_con_Porcentaje_de_Mujeres.reset_index(drop=True, inplace=True)

descs = Clae6_con_Porcentaje_de_Mujeres["clae6"].tolist()
vals  = Clae6_con_Porcentaje_de_Mujeres["Porcentaje_mujeres"].tolist()

# función para cortar texto cada N caracteres
def cortar_lineas(texto, ancho):
    palabras = texto.split()
    lineas = []
    linea_actual =  ""
    for p in palabras:
        if p == "(Incluye":
            lineas.append(linea_actual)
            res = ""
            for j in lineas:
                res += j +'\n'
            return res
        else:
            
            if len(linea_actual) + len(p) + 1 <= ancho:
                if linea_actual != "":
                    p = " " + p
                linea_actual += p
            else:
                lineas.append(linea_actual)
                linea_actual = p
    if linea_actual != "":
        lineas.append(linea_actual)
    res = ""
    for j in lineas:
        res += j +'\n'
    return res

labels_multilinea =[]
for t in descs:
    labels_multilinea.append(cortar_lineas(t, 18))

#graficamos
fig, ax = plt.subplots(figsize=(15, 7))
barras = ax.bar(range(len(vals)),
       vals,
       width=0.55,
       edgecolor="white")
ax.set_xticks(range(len(vals)))
ax.set_xticklabels(labels_multilinea)
ax.set_xlabel("Rubro")
ax.set_ylabel("Porcentaje de mujeres")
plt.title('Porcentaje de mujeres en actividades productivas')
plt.subplots_adjust(bottom=0.35)  
plt.xticks(fontsize=7, ha="center")  
ax.bar_label(barras, fmt='%.1f%%', fontsize=8)
ax.axvline((len(vals)/2)-0.5, color='gray', linestyle='--', linewidth=1)
ax.axhline(y=promedio, color='red', linestyle='--', linewidth=1.5, label=f'Promedio ({promedio:.1f}%)')
ax.legend()
plt.show()

#%%
#calculo la poblacion en cada provincia
poblacionPorProvconId="""
    SELECT provincia_id, SUM(poblacion) AS poblacion
    FROM dfPoblacionXDepto
    LEFT OUTER JOIN dfDepartamento
    ON dfPoblacionXDepto.departamento_id = dfDepartamento.departamento_id
    GROUP BY provincia_id
"""

dfpoblacionPorProvconId = dd.query(poblacionPorProvconId).df()
#paso la tabla anterior para tener la provincia por nombre
poblacionPorProv = """
    SELECT provincia, poblacion
    FROM dfpoblacionPorProvconId 
    LEFT OUTER JOIN dfProvincia
    ON dfpoblacionPorProvconId.provincia_id = dfProvincia.provincia_id
    ORDER BY provincia
"""

dfpoblacionPorProv = dd.query(poblacionPorProv).df()
#calculo la cantidad de establecimientos educativos por provincia
EEPorProv = """
SELECT provincia, SUM("Total Establecimientos Educativos") AS EE
FROM dfEEPorDepartamentoyProvincia
GROUP BY provincia
ORDER BY provincia
"""
dfEEPorProv = dd.query(EEPorProv).df()
#tomo la tabla empleados por provincia y la ordeno alfabeticamente por provincia asi tengo las 3 tablas en el mismo orden
empleadosPorProvOrdenado = """
SELECT *
FROM dfEmpleadosXProvincia
ORDER BY provincia
"""
dfempleadosPorProvOrdenado = dd.query(empleadosPorProvOrdenado).df()

#creo una tabla con 3 columnas, la provincia (solo la usamos para tomar nota de los datos), la cantidad de establecimientos
#educativos cada 1000 habitantes y la cantidad de empleados cada 1000 habitantes
dfEEYEmpleadosCada1000PorProv = pd.DataFrame()
dfEEYEmpleadosCada1000PorProv['provincia'] = dfpoblacionPorProv['provincia']
dfEEYEmpleadosCada1000PorProv['EE_por_1000hab'] = (dfEEPorProv['EE'] / dfpoblacionPorProv['poblacion']) * 1000
dfEEYEmpleadosCada1000PorProv['Empleados_por_1000hab'] = (dfempleadosPorProvOrdenado['cant_empleos'] / dfpoblacionPorProv['poblacion']) * 1000

# Finalmente graficamos:
plt.figure(figsize=(8,6))
plt.scatter(dfEEYEmpleadosCada1000PorProv["EE_por_1000hab"], dfEEYEmpleadosCada1000PorProv["Empleados_por_1000hab"], color='red', alpha=0.7, s=25)

plt.xlabel("Establecimientos Educativos cada 1000 habitantes")
plt.ylabel("Empleados cada 1000 habitantes")
plt.title("Relación entre EE y trabajadores por cada 1000 habitantes (por provincia)")

plt.grid(True)
plt.show()