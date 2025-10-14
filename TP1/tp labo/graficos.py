import pandas as pd
import matplotlib as m

dfDepartamento = pd.read_csv("datasets/Finales/df_Departamento.csv")
dfEE = pd.read_csv("datasets/Finales/df_EE.csv")
dfEP = pd.read_csv("datasets/Finales/df_EP.csv")
dfPoblacion = pd.read_csv("datasets/Finales/df_Poblacion.csv")
dfEP_con_desc = pd.read_csv("datasets/Finales/EP_con_desc.csv")

# i)

df_EP_completo = pd.merge(dfEP, dfDepartamento[["departamento_id", "provincia", "departamento"]], on="departamento_id", how="left")
print(df_EP_completo)
