# -*- coding: utf-8 -*-
"""
Created on Sun Sep 28 11:10:17 2025

@author: Usuario
"""

import pandas as pd
import duckdb as dd

ArchivoEP = pd.read_csv("Datos_por_departamento_actividad_y_sexo.csv")
ArchivoEE = pd.read_csv("2022_padron_oficial_establecimientos_educativos.csv", header =6)
ArchivoPoblacion = pd.read_csv("padron_poblacion.csv")

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