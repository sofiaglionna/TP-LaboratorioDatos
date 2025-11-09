"""
===============================================================================
                    Trabajo Práctico 02 - Laboratorio de Datos
===============================================================================
2do. Cuatrimestre - 2025

Integrantes del grupo:
---------------------
- Felix Soriano
- Sofia Glionna
- Ramiro Gantman

Descripción:
-------------
En este archivo se encuentran el análisis exploratorio del dataset Kuzushiji-MNIST,
una clasificación binaria de solo las clases 4 y 5 y por último una clasificación
multiclase de las 10 clases del dataset.
===============================================================================
"""

##########################################################
# Imports
##########################################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import KNeighborsClassifier
import duckdb as dd
from sklearn.model_selection import train_test_split, KFold
from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
from sklearn.metrics import accuracy_score, confusion_matrix, ConfusionMatrixDisplay,recall_score

########################
#%% Leer el archivo
########################

kuzushiji = pd.read_csv("datos\originales\kuzushiji_full.csv")

##########################################################
#%% 1. Análisis exploratorio de los datos
##########################################################

# Veamos cuántas columnas tiene el archivo:
#print(kuzushiji.shape)
# (70000,785)
# El archivo kuzushiji cuenta con 70000 imagenes de caracteres.
# 785 columnas, de las cuales 784 pixeles (imágenes de 28x28) y 1 columna "label" con el caracter al que corresponde. 

#%% ¿Cuántas imágenes hay de cada caracter?
caracteres_distintos = """
                          SELECT label, COUNT(*) AS Cantidad
                          FROM kuzushiji
                          GROUP BY Label
                          ORDER BY Cantidad DESC
                    """
dfcaracteres_distintos = dd.query(caracteres_distintos).df()
    
# Hay exactamente 7000 imagenes por cada clase.
# 7000 imagenes x 10 clases = 70000 (numero de filas)

#=========================
#%% Gráfico de caracteres
#=========================

# Sleccionamos solo una clase
kuzushiji_clase = """
        SELECT *
        FROM kuzushiji
        WHERE "label" = 6 
"""
dfkuzushiji_clase = dd.query(kuzushiji_clase).df()
#print(dfkuzushiji_clase.shape)
XC = dfkuzushiji_clase.drop('label', axis=1)

#%% Viasualizamos varias imagenes para tener una idea de que aparece en cada clase:

for i in range(100,110):
    img = np.array(XC.iloc[i]).reshape((28,28))
    plt.imshow(img, cmap='gray')
    plt.show()

#=========================
#%% Gráfico promedio de píxeles
#=========================

# Se calculan los promedios de atributo de todo el dataset original
caracter_prom = kuzushiji.drop("label",axis=1).mean(axis=0)
#Redondeo el promedio para el grafico
caracter_prom_redondeado = caracter_prom.apply(np.floor).astype(int)
#Los reordeno en una matriz 28x28
img = np.array(caracter_prom_redondeado.iloc[0:784]).reshape(28,28)
# Proyección de la imagen promedio
plt.plot()
plt.imshow(img, cmap="gray")
plt.title("Imagen promedio - Todas las clases")
plt.axis("on")
plt.show()

##########################################################
#%% 2. Clasificación Binaria
##########################################################

#%% 2.a)
# Creamos un DF con clases 4 y 5:
clases4y5 = """
        SELECT *
        FROM kuzushiji
        WHERE "label" = 4 OR "label" = 5
"""
dfclases4y5 = dd.query(clases4y5).df()

#print para ver la cantidad de filas (caracteres). Tambien se puede ver en el tamaño del dataFrame
#print(dfclases4y5.shape) # Total = 14000 imágenes.
# Veamos de esas 14000 imágenes cuántas pertenecen a las clase 4.
clase4 = """
        SELECT *
        FROM dfclases4y5
        WHERE "label" = 4
"""
dfclase4 = dd.query(clase4).df()

#print para ver la cantidad de filas (caracteres). Tambien se puede ver en el tamaño del dataFrame
#print(dfclase4.shape) # Total = 7000 imágenes.
# En dfclases4y5 hay 7000 caracteres de la clase 4; exactamente la mitad.
# Por lo tanto el dfclase4y5 está perfectamente balanceado entre las clases 4 y 5.

# =====================
#%% 2.b)
# =====================
# Separamos datos en conjuntos de train y test de las clases 4 y 5.
X45 = dfclases4y5.drop('label', axis=1)
y45 = dfclases4y5["label"]

X45_train, X45_test, y45_train, y45_test = train_test_split(
    X45, y45, test_size = 0.2, random_state=42
)
# =====================
#%% 2.c y d)
# =====================
# Vamos a entrenar un modelo KNN para distintas cantidades reducidas de atributos para determinar la mejor.
# Para esto vamos a buscar los atributos mas distintivos entre ambas clases. Para hacer esto calculamos en promedio
# cuánto se pinta cada pixel en cada clase y tomamos los que mayor diferencia tengan entre ambas clases.

clase5 = """
        SELECT *
        FROM dfclases4y5
        WHERE "label" = 5
"""
dfclase5 = dd.query(clase5).df()

# Ya tenemos la clase 4 sola del punto 2.b. Le sacamos la columna label a ambas.
dfclase4.drop('label', axis=1,inplace = True)
dfclase5.drop('label', axis=1,inplace = True)


# =====================
#%% Relevancia de atributos
# =====================
# Calculamos el promedio por columna con .mean() de pandas. Esto nos dara cuanto se pinta cada pixel (columna) en promedio
prom4 = dfclase4.mean()
prom5 = dfclase5.mean()
# Pasamos los indices a int ya que toma los nombres de las columnas como indices, estos son strings
prom4.index = prom4.index.astype(int)
prom5.index = prom5.index.astype(int)
promedios = pd.DataFrame({"promedio4": prom4,
                          "promedio5": prom5})

# Agregamos una columna que sea la diferencia entre los 2 promedios.
promedios["diferencia"] = abs(promedios["promedio5"] - promedios["promedio4"])
# Reordenamos por diferencia para tener los de amyor diferencia mas arriba.
promedios = promedios.sort_values(by = "diferencia", ascending=False)
# Reseteamos los indices, ahora el de menor indice es el de mayor diferencia pero conservamos los indices viejos
# como una columna. Asi sabemos a que atributo hace referencia cada fila
promedios.reset_index(inplace=True)

#%%
"""
Ahora que tenemos los promedios ordenados por mayor diferencia creamos una funcion que tome los i atributos con mayor diferencia
separados por n atributos entre ellos en el grafico. Es decir, si el siguiente de mayor diferencia esta en un "radio"
menor o igual a n, se prueba con el siguiente en terminos de diferencia.
Con radio nos referimos a que si tenemos el pixel en la posicion (x1,y1) en la lista res, no aceptamos pixeles cuya
coordenada x este en el rango [x1-n,x1+n] ni su coordenada y en [y1-n,y1+n]. (Ver figura 4 del informe!).
Hay que tener en cuenta que cada fila mide 28 pixeles por lo que el pixel n esta encima del pixel n+28.
"""

def enRango  (res,indice,n):
    # lo piensamos como un grafico cartesiano. le asignamos coordenadas en x e y tomando como (0,0) el punto superior izquierdo
    # creciendo hacia abajo y a la derecha (la division entera // redondea hacia abajo)
    y = indice // 28
    x = indice%28
    for i in res:
        yi = i//28
        xi = i%28
        # si la distancia es menor a n en ambos ejes entonces devuelvemos True
        if (abs(x-xi) <= n) and (abs(y-yi) <=n):
            return True
    return False

def maximosConSeparacion (df,n,i):
    res = []
    # indice lo vamos a usar para ir recorriendo df
    indice = 0
    while len(res) < i:
        #Casos donde es imposible elegir esa cantidad de datos con esa separacion. Devuelve la lista incompleta que luego es descartada
        if indice >= 784:
            #prints de control:
            #print("//////////////////////////////////////////////////////////////////////////////")
            #print("fuera de rango, separacion o cantidad de atributos muy alto")
            break
        # si esta dentro del radio n entonces no lo agregamos y pasamos al siguiente con mayor diferencia
        elif enRango(res,df.loc[indice,"index"],n):
            indice +=1
        # si no esta dentro del radio lo agregamos y pasamos al siguiente con mayor diferencia
        else:
            res.append(df.loc[indice,"index"])
            indice+=1
    return res

# ============================================
#%% Modelos KNN con distintos hiperparámetros
# ============================================

"""
Hiperparámetros a probar:
 -K = 3, 10, 15
 -Cantidad de atributos = 3, 10, 20, 41, 50, 71, 100
 -Separación = 0, 1, 2, 3
"""

# Creamos un dataframe con unicamente las columnas vacias para ir rellenando
ModelosPorAccuracy = pd.DataFrame({"Valor de K": [] ,"cant_atributos": [], "Separación": [],"Accuracy": [],"Recall clase 4": [], "Recall clase 5": []})


valoresDeK = [3,10,15]
for k in valoresDeK:
    # Separacion 0 es sin separacion, se toma el inmediatamente siguiente si este fuera el mas grande
    separaciones = [0,1,2,3]
    for n in separaciones:
        # Nos quedamos con los i atributos con mayor diferencia entre ellos. Variando ese i probamos
        # con distintas cantidades de atributos. Para cada uno medimos el accuracy.
        cant_atributos = [3,10,20,41,50,71,100] 
        for i in cant_atributos:
            # Se toman los i atributos con mayor diferencia y una separacion n:
            indices = maximosConSeparacion(promedios, n,i)
            # En caso de que no se hayan podido juntar i atributos con separacion n no devuelve nada, se pasa a la siguiente iteración:
            if len(indices) != i:
                continue
            # lo pasamos a una lista de str porque esta en tipo int64 de numpy
            atributos = []
            for j in indices:
                atributos.append(str(int(j)))
            # Creamos X45acotado, una tabla que para cada fila de X_train conserva solo los valores de los atributos de interés.
            X45_train_acotado = X45_train[atributos]
            # También vamos a acotar X45_test a solo estos atributos
            X45_test_acotado = X45_test[atributos]
            # Hacemos el modelo de KNN para 5 vecinos (despues lo voy a cambiar)
            clasificador = KNeighborsClassifier(n_neighbors=k)
            clasificador.fit(X45_train_acotado, y45_train)
            # Calculamos predicciones
            y_pred = clasificador.predict(X45_test_acotado)
            # Accuracy comparando y con y_pred
            accuracyscore = accuracy_score(y45_test, y_pred)
            
            #Usamos recall. Al tener, dentro de cada clase, grupos de dibujos similares entre si pero distintos al resto
            #de su clase nos podria pasar que alguno de estos dibujos en particular sea mas suseptible a ser mal
            #categorizado. recall nos dice que porcentaje de cada clase fue categorizada correctamente (en su clase)
            #si el recall de una clase es notoriamente mas bajo que el de la otra podria estar ocurriendo que uno de los
            #grupos de dibujos de la clase con menor recall este siendo categorizado en la otra clase

            recall = recall_score(y45_test, y_pred, average=None)
            # Agregamos los valores al dataframe
            nuevaFila = pd.DataFrame({"Valor de K": [k] ,"cant_atributos": [i], "Separación": [n],"Accuracy": [accuracyscore],"Recall clase 4": [recall[0]], "Recall clase 5": [recall[1]]})
            ModelosPorAccuracy = pd.concat([ModelosPorAccuracy,nuevaFila])
            
            # Prints de control:
            #print("//////////////////////////////////////////////////////////////////////////////")
            #print("Valor de K: " + str(k))
            #print("Separación: " + str(n))
            #print(str(i)+ " atributos"  +":")
            #print("cantidad de atributos tomados realmente:" + str(len(indices)))
            #print("Accuracy: " + str(accuracyscore))
            #print("Recall clase 4: " + str(recall[0]) + "\n" + "recall clase 5: " + str(recall[1]))
            

# Ordenamos el DataFrame para cada valor de k y cantidad de atributos pongo el de mayor accuracy mas arriba
# por lo que el conjunto de atributos (valor de separacion) con mejor accuracy queda arriba.
ModelosPorAccuracyaux = """
            SELECT *
            FROM ModelosPorAccuracy
            ORDER BY "Valor de K", "cant_atributos", "Accuracy" DESC
"""
ModelosPorAccuracyOrdenado = dd.query(ModelosPorAccuracyaux).df()

# =====================
#%% Tabla con 10 modelos con mayor accuracy (figura 5):
# =====================
n10ModelosPorAccuracy = """
SELECT 
    "Valor de K",
    cant_atributos AS "n atributos",
    Separación,
    ROUND(Accuracy, 3) AS Accuracy,
    ROUND("Recall clase 4", 3) AS "Recall clase 4",
    ROUND("Recall clase 5", 3) AS "Recall clase 5"
FROM ModelosPorAccuracy
ORDER BY Accuracy DESC
LIMIT 10
"""
dftop10 = dd.query(n10ModelosPorAccuracy).df()

# Graficamos la tabla:
fig, ax = plt.subplots(figsize=(10, 3))
ax.axis('off') 

tablaDeAccuracy = ax.table(
    cellText=dftop10.values,
    colLabels=dftop10.columns,
    cellLoc='center',
    loc='center')
plt.title("Top 10 modelos con mayor Accuracy", fontsize=12, weight='bold')
plt.tight_layout()

##########################################################
# 3. Clasificación Multiclase
##########################################################

# =====================
#%% Se separan atributos relevantes
# =====================

# Uso la tabla con el promedio de cuanto se pinta cada pixel que calculamos antes. Para luego poder ver cuales son
# los pixeles más utilizados.

#Hago reset_index para obtener una columna con los indices (pixel con su promedio)
df_pixelesPorProm = caracter_prom.reset_index()
df_pixelesPorProm.columns = ['pixel', 'promedio']
#%% Creo una lista con los 350 pixeles más relevantes
Los350_pixeles_mas_pintados = """
                SELECT *
                FROM df_pixelesPorProm
                ORDER BY promedio DESC
                LIMIT 350
"""
dfLos350_pixeles_mas_pintados = dd.query(Los350_pixeles_mas_pintados).df()

# Obtenemos la lista de los 350 pixeles más pintados
pixeles_top350 = dfLos350_pixeles_mas_pintados['pixel'].tolist()

#Tambien voy a crear una para los 100 pixeles más relevantes
Los100_pixeles_mas_pintados = """
                SELECT *
                FROM df_pixelesPorProm
                ORDER BY promedio DESC
                LIMIT 100
"""
dfLos100_pixeles_mas_pintados = dd.query(Los100_pixeles_mas_pintados).df()
pixeles_top100 = dfLos100_pixeles_mas_pintados['pixel'].tolist()
# =====================
#%% 3.a
# =====================

# Separamos el dataset en dev = 80% y held-out = 20%
y = kuzushiji['label']
x = kuzushiji.drop("label", axis=1)

X_dev, X_held, y_dev, y_held = train_test_split(x, y,test_size=0.2,stratify=y,random_state=42)

# Usamos solo los 350 atributos que seleccionamos.
X_dev350 = X_dev[pixeles_top350]
X_held350 = X_held[pixeles_top350]

# Usamos solo los 100 atributos que seleccionamos
X_dev100 = X_dev[pixeles_top100]
X_held100 = X_held[pixeles_top100]

# =====================
#%% 3.b
# =====================
# Ahora dejamos de lado el conjunto held-out y separamos los datos de desarrollo (80/20)
X_entrenamiento, X_evaluacion, y_entrenamiento, y_evaluacion = train_test_split(X_dev, y_dev,test_size=0.2,stratify=y_dev,random_state=42)
X_entrenamiento_350, X_evaluacion_350, y_entrenamiento, y_evaluacion = train_test_split(X_dev350, y_dev,test_size=0.2,stratify=y_dev,random_state=42)
X_entrenamiento_100, X_evaluacion_100, y_entrenamiento, y_evaluacion = train_test_split(X_dev100, y_dev,test_size=0.2,stratify=y_dev,random_state=42)
alturas = [1,2,3,4,5,6,7,8,9,10]
# para el gráfico:
scores = [] 
scores350 = []
scores100 = []
#%% Creamos árbol para los 100 atributos
for i in alturas: 
    arbol = DecisionTreeClassifier(max_depth=i,criterion="entropy", random_state = 42) 
    arbol.fit(X_entrenamiento_100, y_entrenamiento)

    prediction = arbol.predict(X_evaluacion_100) 
    score100 = accuracy_score(y_evaluacion, prediction)
    scores100.append(score100)
    #printeamos los scores para analizar:
    #print("score del arbol (100 atributos) con altura",i,"=",score100)
#%%Creo arbol para los 350 atributos
for i in alturas: 
    arbol = DecisionTreeClassifier(max_depth=i,criterion="entropy", random_state = 42) 
    arbol.fit(X_entrenamiento_350, y_entrenamiento)

    prediction = arbol.predict(X_evaluacion_350) 
    score350 = accuracy_score(y_evaluacion, prediction)
    scores350.append(score350)
    #printeamos los scores para analizar:
    #print("score del arbol (350 atributos) con altura",i,"=",score350)
#%%Creo arbol para todos los atributos
for i in alturas: 
    arbol = DecisionTreeClassifier(max_depth=i,criterion="entropy", random_state = 42) 
    arbol.fit(X_entrenamiento, y_entrenamiento)

    prediction = arbol.predict(X_evaluacion) 
    score = accuracy_score(y_evaluacion, prediction)
    scores.append(score)
    #printeamos los scores para analizar:
    #print("score del arbol (todos los atributos) con altura",i,"=",score)

#%% 
#grafico (100 vs 784)
plt.subplots(figsize=(5, 4))
plt.plot(alturas, scores, marker='o', label = "784 atributos",  color = "blue")
plt.plot(alturas, scores100, marker='o',label = "100 atributos", color = "green")
plt.legend()
plt.xlabel("Altura Árbol")
plt.ylabel("accuracy en evaluación")
plt.title("Entropy 100 vs 784 atributos - altura vs accuracy")
plt.xlim(1, 10)
plt.ylim(0, 1)  
plt.grid(True)
plt.show()

#%%
#Gráfico (350 vs 784):
plt.subplots(figsize=(5, 4))
plt.plot(alturas, scores, marker='o', label = "784 atributos",  color = "blue")
plt.plot(alturas, scores350, marker='o',label = "350 atributos", color = "red")
plt.legend()
plt.xlabel("Altura Árbol")
plt.ylabel("accuracy en evaluación")
plt.title("Entropy 350 vs 784 atributos - altura vs accuracy")
plt.xlim(1, 10)
plt.ylim(0, 1)  
plt.grid(True)
plt.show()

# =====================
#%% 3.c
# =====================
"""
Hiperparámetros en árbol:
 -Criterio de elección de atributos -> (Gini, entropy)
 -Profundidad -> (1,...,10)
 -Estrategia de poda (no se si esto seria el k-fold?)
"""

# Hacemos el K-fold
alturas = [1,2,3,4,5,6,7,8,9,10] # (mismas que inciso anterior)
nsplits = 5
kf = KFold(n_splits=nsplits)

# =====================
#%% ENTROPY CON KFOLDING
# =====================
# DataFrame con los resultados
resultadosENTROPY = np.zeros((nsplits, len(alturas))) # una fila por cada fold, una columna por cada modelo

for i, (train_index, test_index) in enumerate(kf.split(X_dev350)):

    kf_X_train, kf_X_test = X_dev350.iloc[train_index], X_dev350.iloc[test_index]
    kf_y_train, kf_y_test = y_dev.iloc[train_index], y_dev.iloc[test_index]
    
    for j, hmax in enumerate(alturas):
        
        arbol = tree.DecisionTreeClassifier(max_depth = hmax, criterion="entropy", random_state = 42) 
        arbol.fit(kf_X_train, kf_y_train)
        pred = arbol.predict(kf_X_test)
        score = accuracy_score(kf_y_test,pred)
        
        resultadosENTROPY[i, j] = score
# promedio scores sobre los folds
scores_promedio_ENTROPY = resultadosENTROPY.mean(axis = 0)
#%%
#printeamos los scores para facilitar el analisis:
#for i,e in enumerate(alturas):
    #print(f'Score promedio del modelo con criterio ENTROPY con hmax = {e}: {scores_promedio_ENTROPY[i]:.4f}')

# =====================
#%% GINI CON KFOLDING
# =====================
# DataFrame con los resultados
resultadosGINI = np.zeros((nsplits, len(alturas))) # una fila por cada fold, una columna por cada modelo

for i, (train_index, test_index) in enumerate(kf.split(X_dev350)):

    kf_X_train, kf_X_test = X_dev350.iloc[train_index], X_dev350.iloc[test_index]
    kf_y_train, kf_y_test = y_dev.iloc[train_index], y_dev.iloc[test_index]
    
    for j, hmax in enumerate(alturas):
        
        arbol = tree.DecisionTreeClassifier(max_depth = hmax, criterion="gini", random_state = 42) 
        arbol.fit(kf_X_train, kf_y_train)
        pred = arbol.predict(kf_X_test)
        score = accuracy_score(kf_y_test,pred)
        
        resultadosGINI[i, j] = score
# promedio scores sobre los folds
scores_promedio_GINY = resultadosGINI.mean(axis = 0)
#%%
#printeamos los scores para facilitar el analisis:
#for i,e in enumerate(alturas):
    #print(f'Score promedio del modelo con criterio GINI con hmax = {e}: {scores_promedio_GINY[i]:.4f}')

# =====================
#%% (Figura 7) Grafico de Gini y Entropy superpuestos:
# =====================
plt.subplots(figsize=(5, 4))
plt.plot(alturas, scores_promedio_ENTROPY, marker='o', label='Entropy', color='blue')
plt.plot(alturas, scores_promedio_GINY, marker='o', label='Gini', color='green')
plt.xlabel("Altura Árbol")
plt.ylabel("accuracy promedio (k-folding)")
plt.title("Comparación Entropy vs Gini - altura vs accuracy")
plt.xlim(1, 10)
plt.ylim(0, 1)  
plt.legend()
plt.grid(True)
plt.show()

# =====================
#%% 3.d
# =====================
# Usamos Entropy con profundidad 10 ya que fue el mejor modelo, dando un score del 71.53%.
# Gini con profundidad 10 no estuvo muy lejos ya que dio un score del 70.00%.

# Entrenamos el modelo ahora en todo el conjunto de desarrollo
mejor_modelo = DecisionTreeClassifier(criterion="entropy", max_depth=10, random_state=42)
mejor_modelo.fit(X_dev350, y_dev)

# Predecimos sobre el held-out
y_pred = mejor_modelo.predict(X_held350)

# Calculamos accuracy del modelo
acc_final = accuracy_score(y_held, y_pred)
print(f"Accuracy final en held-out: {acc_final:.4f}")

#%% Generamos la matriz de confusión del modelo
# Matriz de confusión:
cm = confusion_matrix(y_held, y_pred)
#Printeamos la matriz para facilitar su visualizacion y analisis
#print("Matriz de confusión:\n", cm)

# Gráfico de matriz de confusión para el informe:
plt.figure()
ConfusionMatrixDisplay(confusion_matrix=cm).plot(cmap="Blues", values_format='d')
plt.xlabel("Clase predecida")
plt.ylabel("Clase real")
plt.title("Matriz de Confusión - Held-Out (Entropy, profundidad=10)")
plt.tight_layout()
plt.show()

#%%