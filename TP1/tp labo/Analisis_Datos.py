import pandas as pd
import duckdb as dd

dfDepartamento = pd.read_csv("datasets/Finales/df_Departamento.csv")
dfEE = pd.read_csv("datasets/Finales/df_EE.csv")
dfEP = pd.read_csv("datasets/Finales/df_EP.csv")
dfPoblacion = pd.read_csv("datasets/Finales/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/Finales/EP_con_desc.csv")



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





#Ejercicio 1)

# i)

# Contamos los Establecimientos Educativos en cada departamento.

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

# Calculamos las poblaciones de cada rango de edad según nivel educativo por departamento:
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
        d.provincia, 
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
    INNER JOIN dfCantEE AS ee ON d.departamento_id = ee.departamento_id
    INNER JOIN dfPoblacionXJardinEnDpto AS pj ON d.departamento_id = pj.departamento_id
    INNER JOIN dfPoblacionXPrimarioEnDpto AS pp ON d.departamento_id = pp.departamento_id
    INNER JOIN dfPoblacionXSecundarioEnDpto AS ps ON d.departamento_id = ps.departamento_id
    INNER JOIN dfPoblacionXSNUEnDpto AS pa ON d.departamento_id = pa.departamento_id
    """
dfi = dd.query(i).df()


#%%


# ii)
trabajadoresXDepartamento = """
    SELECT 
        departamento_id,
        SUM(varones + mujeres) AS total
    FROM dfEP
    GROUP BY departamento_id
"""
dftrabajadoresXDepartamento = dd.query(trabajadoresXDepartamento).df()

ii = """
    SELECT provincia, departamento, total AS Cantidad_total_de_empleados_en_2022
    FROM dfDepartamento, dftrabajadoresXDepartamento
    WHERE dfDepartamento.departamento_id = dftrabajadoresXDepartamento.departamento_id
    ORDER BY provincia ASC, total DESC
"""
dfii = dd.query(ii).df()

#%%
#iii)

# iv)

iv = """
SELECT
  d.provincia,
  d.departamento,
  CASE
    WHEN t.clae3 < 10  THEN '00' || CAST(t.clae3 AS VARCHAR)
    WHEN t.clae3 < 100 THEN '0'  || CAST(t.clae3 AS VARCHAR)
    ELSE CAST(t.clae3 AS VARCHAR)
  END AS clae6_3digitos,
  t.empleo_clae3 AS empleos_en_rubro
FROM (
  -- empleo por depto y por “CLAE3” (primeros 3 dígitos via clae6/1000)
  SELECT
    e.departamento_id,
    CAST(e.clae6 / 1000 AS INTEGER) AS clae3,
    SUM(e.varones + e.mujeres) AS empleo_clae3
  FROM dfEP AS e
  GROUP BY
    e.departamento_id,
    CAST(e.clae6 / 1000 AS INTEGER)
) AS t
JOIN (
  -- rubro líder por depto (máximo empleo_clae3)
  SELECT
    y.departamento_id,
    MAX(y.empleo_clae3) AS max_empleo_clae3
  FROM (
    SELECT
      e.departamento_id,
      CAST(e.clae6 / 1000 AS INTEGER) AS clae3,
      SUM(e.varones + e.mujeres) AS empleo_clae3
    FROM dfEP AS e
    GROUP BY
      e.departamento_id,
      CAST(e.clae6 / 1000 AS INTEGER)
  ) AS y
  GROUP BY y.departamento_id
) AS m
  ON t.departamento_id = m.departamento_id
 AND t.empleo_clae3   = m.max_empleo_clae3
JOIN dfDepartamento AS d
  ON d.departamento_id = t.departamento_id

-- usar tu subconsulta dep_que_cumplen como filtro por JOIN
JOIN (
  SELECT
    t.departamento_id
  FROM (
    SELECT d.provincia_id,
           e.departamento_id,
           SUM(e.varones + e.mujeres) AS empleo_total
    FROM dfEP AS e
    INNER JOIN dfDepartamento AS d
      ON e.departamento_id = d.departamento_id
    GROUP BY d.provincia_id, e.departamento_id
  ) AS t
  INNER JOIN (
    SELECT provincia_id,
           AVG(empleo_total) AS promedio_prov
    FROM (
      SELECT d.provincia_id,
             e.departamento_id,
             SUM(e.varones + e.mujeres) AS empleo_total
      FROM dfEP AS e
      INNER JOIN dfDepartamento AS d
        ON e.departamento_id = d.departamento_id
      GROUP BY d.provincia_id, e.departamento_id
    ) AS x
    GROUP BY provincia_id
  ) AS p
    ON t.provincia_id = p.provincia_id
  WHERE t.empleo_total > p.promedio_prov
) AS dep_que_cumplen
  ON dep_que_cumplen.departamento_id = t.departamento_id

ORDER BY d.provincia, empleos_en_rubro DESC;
"""



df_iv = dd.query(iv).df()
print(df_iv)


q_iv_prov = """
SELECT 
  d.provincia,
  AVG(x.total_dep) AS promedio_empleo_total_por_dpto
FROM (
  SELECT e.departamento_id, SUM(e.mujeres+e.varones) AS total_dep
  FROM dfEP e
  GROUP BY e.departamento_id
) x
JOIN dfDepartamento d ON d.departamento_id = x.departamento_id
GROUP BY d.provincia
ORDER BY promedio_empleo_total_por_dpto DESC;

"""
df_iv_prov = dd.query(q_iv_prov).df()


dep_que_cumplen = """
SELECT
  d.provincia,
  d.departamento,
  t.departamento_id,
  t.empleo_total,
  p.promedio_prov,
  t.empleo_total - p.promedio_prov AS diferencia,
  (t.empleo_total * 1.0) / p.promedio_prov AS ratio
FROM (
  SELECT 
    d.provincia_id,
    e.departamento_id,
    SUM(e.varones + e.mujeres) AS empleo_total
  FROM dfEP AS e
  INNER JOIN dfDepartamento AS d
    ON e.departamento_id = d.departamento_id
  GROUP BY d.provincia_id, e.departamento_id
) AS t
INNER JOIN (
  SELECT 
    provincia_id,
    AVG(empleo_total) AS promedio_prov
  FROM (
    SELECT 
      d.provincia_id,
      e.departamento_id,
      SUM(e.varones + e.mujeres) AS empleo_total
    FROM dfEP AS e
    INNER JOIN dfDepartamento AS d
      ON e.departamento_id = d.departamento_id
    GROUP BY d.provincia_id, e.departamento_id
  ) AS x
  GROUP BY provincia_id
) AS p
  ON t.provincia_id = p.provincia_id
INNER JOIN dfDepartamento AS d
  ON d.departamento_id = t.departamento_id
WHERE t.empleo_total > p.promedio_prov
ORDER BY d.provincia, ratio DESC;


"""
df_dep_que_cumplen = dd.query(dep_que_cumplen).df()






# %%
