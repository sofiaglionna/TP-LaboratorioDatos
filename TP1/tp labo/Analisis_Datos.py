import pandas as pd
import duckdb as dd

dfDepartamento = pd.read_csv("datasets/Finales/df_Departamento.csv")
dfEE = pd.read_csv("datasets/Finales/df_EE.csv")
dfEP = pd.read_csv("datasets/Finales/df_EP.csv")
dfPoblacion = pd.read_csv("datasets/Finales/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/Finales/EP_con_desc.csv")



#Ejercicio 1)

# i)
#Tomamos de dfdepartamento la provincia y departamento 
#con el (id_departamento) asociado a departamento, contamos la cant de establecimientos educativos de dfEE por nivel (jardin, primario, secundario...)
#y relacionamos cada nivel con la población asociada a su rango de edad de dfPoblación.
#  El orden del reporte debe ser alfabético por provincia y dentro de las provincias descendente por cantidad de escuelas primarias.

# i) Provincia, Departamento, EE por nivel (Jardines/Primarias/Secundarias) y
#    población por grupo etario (3–5, 6–11, 12–17). Orden: provincia ASC, primarias DESC.

i = """

"""


jardines = """
SELECT 
 departamento_id,
 COUNT(*) AS cantidad_jardines
 FROM dfEE
 WHERE "Nivel inicial - Jardín maternal" > 0
   OR "Nivel inicial - Jardín de infantes" > 0
 GROUP BY departamento_id
"""

dfJardines = dd.query(jardines).df()
print(dfJardines)


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



