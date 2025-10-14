import pandas as pd
import matplotlib.pyplot as plt

dfDepartamento = pd.read_csv("datasets/Finales/df_Departamento.csv")
dfEE = pd.read_csv("datasets/Finales/df_EE.csv")
dfEP = pd.read_csv("datasets/Finales/df_EP.csv")
dfPoblacion = pd.read_csv("datasets/Finales/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/Finales/EP_con_desc.csv")


# i)

df_EP_completo = pd.merge(dfEP, dfDepartamento[["departamento_id", "provincia", "departamento"]], on="departamento_id", how="left")
df_EP_completo["total"] = df_EP_completo["varones"] + df_EP_completo["mujeres"]

df_total_provincia = (
    df_EP_completo.groupby("provincia", as_index=False)["total"].sum().sort_values("total", ascending=False)  
)


plt.figure(figsize=(10,6))  # tamaño del gráfico
plt.barh(df_total_provincia["provincia"], df_total_provincia["total"])
plt.xlabel("Total de empleo (millones)")
plt.ylabel("Provincia")
plt.title("Total de empleo por provincia (2022)")
plt.gca().invert_yaxis()  # para que la más grande quede arriba
plt.tight_layout()
plt.xticks([0, 250000, 500000, 750000, 1000000, 1250000, 1500000, 1750000, 2000000, 2250000, 2500000,2750000,3000000])

#plt.show()

# v)

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

# Graficamos

plt.figure(figsize=(10,8))
plt.barh(df_mujeres_por_clae6_con_desc["clae6_desc"], df_mujeres_por_clae6_con_desc["mujeres"],height=0.8)

# Línea vertical con el promedio nacional de participación femenina (%)
plt.axvline(promedio_mujeres_sobre_total, color="red", linestyle="--", label=f"Promedio nacional {promedio_mujeres_sobre_total:.1f}%")


plt.title("Participación femenina por actividad económica")
plt.xlabel("Cantidad de empleos femeninos")
plt.ylabel("Actividad económica (CLAE6)")
plt.legend()
plt.tight_layout()
plt.show()





