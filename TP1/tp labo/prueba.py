import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import textwrap


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

import matplotlib.pyplot as plt

# --- Datos ---
descs = df_mujeres_por_clae6_con_desc["clae6_desc"].tolist()
vals  = df_mujeres_por_clae6_con_desc["mujeres"].tolist()

# función casera para cortar texto cada N caracteres sin importar librerías
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

