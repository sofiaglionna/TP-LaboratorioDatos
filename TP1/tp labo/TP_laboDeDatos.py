# -*- coding: utf-8 -*-
"""
Created on Sun Sep 28 11:10:17 2025

@author: Usuario
"""

import pandas as pd
import duckdb as dd

ArchivoEP = pd.read_csv("Datos_por_departamento_actividad_y_sexo.csv")
ArchivoEE = pd.read_csv("2022_padron_oficial_establecimientos_educativos.csv", header =6)
ArchivoPoblacion = pd.read_csv("padron_poblacion.csv", header= 11)
ArchivoActividadesEstablecimientos = pd.read_csv("actividades_establecimientos")

Departamento = """
                SELECT DISTINCT in_departamentos AS departamento_id,departamento,provincia_id,provincia
                FROM ArchivoEP
                """
dfDepartamento = dd.query(Departamento).df()


#cantidad de varones que emplea cada rubro por departamento en 2022
VaronesEmpleados = """
                    SELECT clae6,in_departamentos AS departamento_id, empleo AS varones
                    FROM ArchivoEP
                    WHERE  anio = 2022 AND genero = 'Varones'
                    """
dfVaronesEmpleados=dd.query(VaronesEmpleados).df()       

#cantidad de mujeres que emplea cada rubro por departamento en 2022
MujeresEmpleados = """ 
                    SELECT clae6,in_departamentos AS departamento_id, empleo AS mujeres
                    FROM ArchivoEP
                    WHERE anio = 2022 AND genero = 'Mujeres'
                    """
dfMujeresEmpleadas=dd.query(MujeresEmpleados).df()

#junto ambas tablas de varones y mujeres 
EmpleadosPorSexo = """
                    SELECT dfVaronesEmpleados.clae6, dfVaronesEmpleados.departamento_id, varones, mujeres
                    FROM dfVaronesEmpleados
                    INNER JOIN dfMujeresEmpleadas 
                    ON dfVaronesEmpleados.clae6 = dfMujeresEmpleadas.clae6 AND dfVaronesEmpleados.departamento_id = dfMujeresEmpleadas .departamento_id
                    """
dfEmpleadosPorSexo = dd.query(EmpleadosPorSexo).df()


"""
uso SELECT DISTINCT porque se me van a crear 2 filas iguales para cada clae6 y departamento, una por 2021 y otra por 2022
ambas son iguales por el INNER JOIN, a ambas les pongo los varones y mujeres de 2022 (ya que solo tengo esos 
en empleadosPorSexo) Entonces me quedan 2 filas con los datos de 2022 pero una de 2022 y otra que marca el año 2021
pero pise con los datos de 2022. Al hacer SELECT DISTINCT elimino la repetida

"""
EP = """
        SELECT DISTINCT ArchivoEP.clae6, ArchivoEP.in_departamentos AS departamento_id, varones,mujeres
        FROM ArchivoEP
        INNER JOIN dfEmpleadosPorSexo
        ON ArchivoEP.clae6=dfEmpleadosPorSexo.clae6 AND ArchivoEP.in_departamentos = dfEmpleadosPorSexo.departamento_id
    """
dfEP = dd.query(EP).df()


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

#corregir población
ArchivoPoblacionAC = """
                       SELECT "  de Edad" AS departamento_id, "Unnamed: 1" AS Edad, "Unnamed: 2" AS Casos,"Unnamed: 3" AS "%"
                        FROM ArchivoPoblacion
                        """
                        
                        
dfArchivoPoblacionAC=dd.query(ArchivoPoblacionAC).df()

dfArchivoPoblacionAC.iloc[56585]['Casos'] = "Nacional"

dfArchivoPoblacionAC.dropna(subset=['Edad'], inplace=True)
dfArchivoPoblacionAC.reset_index(drop=True, inplace=True)
departamento_id = 0
for i, row in dfArchivoPoblacionAC.iterrows():
    dfArchivoPoblacionAC.loc[i, 'departamento_id'] = departamento_id
    if row['Casos'] == 'Casos':
        departamento_id= dfArchivoPoblacionAC.iloc[i-1]['Casos']
for i, row in dfArchivoPoblacionAC.iterrows():
    if row['Casos'] == "Casos":
        dfArchivoPoblacionAC.drop(i,inplace=True)
        dfArchivoPoblacionAC.drop(i-1,inplace=True)
dfArchivoPoblacionAC.reset_index(drop=True, inplace=True)

"""
OBTENGO LA TABLA DE ACTIVIDADES ESTABLECIMIENTO 
"""

EP_con_desc = """
                SELECT 
                    ArchivoEP.clae6,
                    ArchivoActividadesEstablecimientos.clae6_desc
                FROM ArchivoEP
                LEFT JOIN ArchivoActividadesEstablecimientos
                ON ArchivoEP.clae6 = ArchivoActividadesEstablecimientos.clae6
              """
dfEP_con_desc = dd.query(EP_con_desc).df()
print(dfEP_con_desc)