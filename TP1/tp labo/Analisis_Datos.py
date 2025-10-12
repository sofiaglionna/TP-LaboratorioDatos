from TP_laboDeDatos import dfEP_con_desc, dfPoblacion, dfEE, dfEP, dfDepartamento
import pandas as pd
import duckdb as dd

#Ejercicio 1)

# i)
#Tomamos de dfdepartamento la provincia y departamento 
#con el (id_departamento) asociado a departamento, contamos la cant de establecimientos educativos de dfEE por nivel (jardin, primario, secundario...)
#y relacionamos cada nivel con la población asociada a su rango de edad de dfPoblación.
#  El orden del reporte debe ser alfabético por provincia y dentro de las provincias descendente por cantidad de escuelas primarias.

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



