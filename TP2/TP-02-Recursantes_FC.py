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
print(kuzushiji.shape)
# (70000,785)
# El archivo kuzushiji cuenta con 70000 imagenes de caracteres.
# 785 columnas, de las cuales 784 pixeles (imágenes de 28x28) y 1 columna "label" con el caracter al que corresponde. 

##########################
#%% Grafico de caracteres
##########################

# Sleccionamos solo una clase
kuzushiji_clase = """
        SELECT *
        FROM kuzushiji
        WHERE "label" = 6
"""
dfkuzushiji_clase = dd.query(kuzushiji_clase).df()
print(dfkuzushiji_clase.shape)
XC = dfkuzushiji_clase.drop('label', axis=1)

#%% Viasualizamos varias imagenes para tener una idea de que aparece en cada clase:

for i in range(100,110):
    img = np.array(XC.iloc[i]).reshape((28,28))
    plt.imshow(img, cmap='gray')
    plt.show()
#print(dfkuzushiji_clase.loc[i,"label"]) #para ver la clase

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

print(dfclases4y5.shape) # Total = 14000 imágenes.
# Veamos de esas 14000 imágenes cuántas pertenecen a las clase 4.
clase4 = """
        SELECT *
        FROM dfclases4y5
        WHERE "label" = 4
"""
dfclase4 = dd.query(clase4).df()

print(dfclase4.shape) # Total = 7000 imágenes.
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
# Vamos a entrenar un modelo KNN para distintas cantidades reducidas de atributos para determinar la mejor
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
# Ahora que tenemos los promedios ordenado por mayor diferencia creamos una funcion que tome los i atributos con mayor diferencia
# separados por n atributos entre ellos en el grafico. Es decir si el siguiente de mayor diferencia esta en un "radio"
# menor o igual a n se prueba con el siguiente en terminos de diferencia.
# Con radio nos referimos a que si tenemos el pixel en la posicion (x1,y1) en la lista res, no aceptamos pixeles cuya
# coordenada x este en el rango [x1-n,x1+n] ni su coordenada y en [y1-n,y1+n]. (ver figura 4 del informe)!!
# Hay que tener en cuenta que cada fila mide 28 pixeles por lo que el pixel n esta encima del pixel n+28

def enRango  (res,indice,n):
    #lo pienso como un grafico cartesiano. le asigno coordenadas en x e y tomando como (0,0) el punto superior izquierdo
    #creciendo hacia abajo y a la derecha (la division entera // redondea hacia abajo)
    y = indice // 28
    x = indice%28
    for i in res:
        yi = i//28
        xi = i%28
        #si la distancia es menor a n en ambos ejes entonces devuelvo True
        if (abs(x-xi) <= n) and (abs(y-yi) <=n):
            return True
    return False

def maximosConSeparacion (df,n,i):
    res = []
    #indice lo voy a usar para ir recorriendo df
    indice = 0
    while len(res) < i:
        if indice >= 784:
            print("//////////////////////////////////////////////////////////////////////////////")
            print("fuera de rango, separacion o cantidad de atributos muy alto")
            break
        #si esta dentro del radio n entonces no lo agrego y paso al siguiente con mayor diferencia
        elif enRango(res,df.loc[indice,"index"],n):
            indice +=1
        #si no esta dentro del radio lo agrego y paso al siguiente con mayor diferencia
        else:
            res.append(df.loc[indice,"index"])
            indice+=1
    return res

# ========================================
#%% Modelos con distintos hiperparámetros
# ========================================
# Creamos un dataframe con unicamente las columnas vacias para ir rellenando
ModelosPorAccuracy = pd.DataFrame({"Valor de K": [] ,"cant_atributos": [], "Separación": [],"Accuracy": [],"Recall clase 4": [], "Recall clase 5": []})
# Probamos para varios valores de k como pide el punto 2.d)
valoresDeK = [3,10,15]
for k in valoresDeK:
    #Vamos a probar con varias separaciones entre atributos ya que creemos que 2 pixeles que estan al lado seguramente
    #tengan una diferencia parecida. Lo que podría causar que los i atributos de mayor diferencia esten todos pegados en
    #el grafico y resulte redundante tomarlos.
    #Por lo que tomarlos de mayor diferencia pero separados podria mejorar el algoritmo. 
    #Separacion 0 es sin separacion, tomo el inmediatamente siguiente si este fuera el mas grande
    separaciones = [0,1,2,3]
    for n in separaciones:
        #voy a quedarme con los i atributos con mayor diferencia entre ellos. Variando ese i voy a probar irme quedando
        #con distintas cantidades de atributos. Para cada uno voy a medir el accuracy para quedarme con el que de maximo
        cant_atributos = [3,10,20,41,50,71,100] 
        for i in cant_atributos:
            #tomo los i atributos con mayor diferencia y una separacion n
            indices = maximosConSeparacion(promedios, n,i)
            #En caso de que no se hayan podido juntar i atributos con separacion n no devuelvo nada, paso a la siguiente iteración
            if len(indices) != i:
                continue
            #lo paso a una lista de str porque esta en tipo int64 de numpy
            atributos = []
            for j in indices:
                atributos.append(str(int(j)))
            #Creo X45acotado una tabla que para cada fila de X_train conserva solo los valores de los atributos de interes
            X45_train_acotado = X45_train[atributos]
            #tambien voy a acotar mi X45_test a solo estos atributos
            X45_test_acotado = X45_test[atributos]
            #hago el modelo de KNN para 5 vecinos (despues lo voy a cambiar)
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
            #agrego los valores al dataframe
            nuevaFila = pd.DataFrame({"Valor de K": [k] ,"cant_atributos": [i], "Separación": [n],"Accuracy": [accuracyscore],"Recall clase 4": [recall[0]], "Recall clase 5": [recall[1]]})
            ModelosPorAccuracy = pd.concat([ModelosPorAccuracy,nuevaFila])
            
            #printeamos para controlar
            print("//////////////////////////////////////////////////////////////////////////////")
            print("Valor de K: " + str(k))
            print("Separación: " + str(n))
            print(str(i)+ " atributos"  +":")
            print("cantidad de atributos tomados realmente:" + str(len(indices)))
            print("Accuracy: " + str(accuracyscore))
            print("Recall clase 4: " + str(recall[0]) + "\n" + "recall clase 5: " + str(recall[1]))
            

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

# A graficar la tabla:
fig, ax = plt.subplots(figsize=(10, 3))
ax.axis('off')  #para oclutar los ejes

tablaDeAccuracy = ax.table(
    cellText=dftop10.values,
    colLabels=dftop10.columns,
    cellLoc='center',
    loc='center')
plt.title("Top 10 modelos con mayor Accuracy", fontsize=12, weight='bold')
plt.tight_layout()

##########################################################
#%% 3. Clasificación Multiclase
##########################################################

# Se separa el dataset en dev = 80% y held-out = 20%
x = kuzushiji.drop(columns=['label'])
y = kuzushiji['label']

X_dev, X_held, y_dev, y_held = train_test_split(x, y,test_size=0.2,stratify=y,random_state=42)

# =====================
#%% 3.b
# =====================
# Ahora dejamos de lado el conjunto held-out y separamos los datos de desarrollo (80/20)
X_entrenamiento, X_evaluacion, y_entrenamiento, y_evaluacion = train_test_split(X_dev, y_dev,test_size=0.2,stratify=y_dev,random_state=42)

#%% Probamos con todas las profundidades entre 1 y 10

alturas = [1,2,3,4,5,6,7,8,9,10]
scores = [] # para el grafico

for i in alturas: 
    arbol = DecisionTreeClassifier(max_depth=i,criterion="entropy") 
    arbol.fit(X_entrenamiento, y_entrenamiento)

    prediction = arbol.predict(X_evaluacion) 
    score = accuracy_score(y_evaluacion, prediction)
    scores.append(score)
    print("score del arbol con altura",i,"=",score)

#%% Grafico:
plt.subplots(figsize=(5, 5))
plt.plot(alturas, scores, marker='o')
plt.xlabel("Altura Árbol")
plt.ylabel("accuracy en evaluación")
plt.title("Entropy (3.b) - altura vs accuracy")
plt.xlim(1, 10)
plt.ylim(0, 1)
plt.grid(True)
plt.tight_layout()
plt.show()

# =====================
#%% 3.c
# =====================
# HIPERPARAMETROS EN ARBOLES: (lo saqué de la clase)
# -Criterio de elección de atributos -> (Gini, entropy)
# -Profundidad -> (1,...,10)
# -Estrategia de poda (no se si esto seria el k-fold?)

# Hacemos el K-fold

alturas = [1,2,3,4,5,6,7,8,9,10] # (mismas que inciso anterior)
nsplits = 5
kf = KFold(n_splits=nsplits)

# =====================
#%% ENTROPY CON KFOLDING
# =====================
resultadosENTROPY = np.zeros((nsplits, len(alturas))) # una fila por cada fold, una columna por cada modelo

for i, (train_index, test_index) in enumerate(kf.split(X_dev)):

    kf_X_train, kf_X_test = X_dev.iloc[train_index], X_dev.iloc[test_index]
    kf_y_train, kf_y_test = y_dev.iloc[train_index], y_dev.iloc[test_index]
    
    for j, hmax in enumerate(alturas):
        
        arbol = tree.DecisionTreeClassifier(max_depth = hmax, criterion="entropy")
        arbol.fit(kf_X_train, kf_y_train)
        pred = arbol.predict(kf_X_test)
        score = accuracy_score(kf_y_test,pred)
        
        resultadosENTROPY[i, j] = score
# promedio scores sobre los folds
scores_promedio_ENTROPY = resultadosENTROPY.mean(axis = 0)
#%%
for i,e in enumerate(alturas):
    print(f'Score promedio del modelo con criterio ENTROPY con hmax = {e}: {scores_promedio_ENTROPY[i]:.4f}')

# =====================
#%% GINI CON KFOLDING
# =====================
resultadosGINI = np.zeros((nsplits, len(alturas))) # una fila por cada fold, una columna por cada modelo

for i, (train_index, test_index) in enumerate(kf.split(X_dev)):

    kf_X_train, kf_X_test = X_dev.iloc[train_index], X_dev.iloc[test_index]
    kf_y_train, kf_y_test = y_dev.iloc[train_index], y_dev.iloc[test_index]
    
    for j, hmax in enumerate(alturas):
        
        arbol = tree.DecisionTreeClassifier(max_depth = hmax, criterion="gini")
        arbol.fit(kf_X_train, kf_y_train)
        pred = arbol.predict(kf_X_test)
        score = accuracy_score(kf_y_test,pred)
        
        resultadosGINI[i, j] = score
# promedio scores sobre los folds
scores_promedio_GINY = resultadosGINI.mean(axis = 0)
#%%
for i,e in enumerate(alturas):
    print(f'Score promedio del modelo con criterio GINI con hmax = {e}: {scores_promedio_GINY[i]:.4f}')

# =====================
#%% Grafico de ambos (Gini y Entropy) superpuestos:
# =====================
plt.plot(alturas, scores_promedio_ENTROPY, marker='o', label='Entropy', color='blue')
plt.plot(alturas, scores_promedio_GINY, marker='o', label='Gini', color='green')
plt.xlabel("Altura Árbol")
plt.ylabel("accuracy promedio (k-folding)")
plt.title("Comparación Entropy vs Gini - altura vs accuracy")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# =====================
#%% 3.d
# =====================
# Usamos Entropy con profundidad 10 ya que fue el mejor modelo, dando un score del 71.53%.
# Gini con profundidad 10 no estuvo muy lejos ya que dio un score del 70.00%.

# Entrenamos el modelo ahora en todo el conjunto de desarrollo
mejor_modelo = DecisionTreeClassifier(criterion="entropy", max_depth=10)
mejor_modelo.fit(X_dev, y_dev)

# Predecimos sobre el held-out
y_pred = mejor_modelo.predict(X_held)

# Calculamos accuracy del modelo
acc_final = accuracy_score(y_held, y_pred)
print(f"Accuracy final en held-out: {acc_final:.4f}")

#%% Generamos la matriz de confusion del modelo
# Matriz de confusion:
cm = confusion_matrix(y_held, y_pred)
print("Matriz de confusión:\n", cm)

# Grafico de matriz de confusión para el informe:
plt.figure()
ConfusionMatrixDisplay(confusion_matrix=cm).plot(cmap="Blues", values_format='d')
plt.xlabel("Clase predecida")
plt.ylabel("Clase real")
plt.title("Matriz de Confusión – Held-Out (Entropy, depth=10)")
plt.tight_layout()
plt.show()




#%%

"""
##########################################################
# kNN with neighbors=4 benchmark for Kuzushiji-MNIST
# Acheives 92.10% test accuracy
##########################################################
def load(f):
    return np.load(f)['arr_0']

# Load the data
x_train = load('kmnist-train-imgs.npz')
x_test = load('kmnist-test-imgs.npz')
y_train = load('kmnist-train-labels.npz')
y_test = load('kmnist-test-labels.npz')

# Flatten images
x_train = x_train.reshape(-1, 784)
x_test = x_test.reshape(-1, 784)

clf = KNeighborsClassifier(n_neighbors=4, weights='distance', n_jobs=-1)
print('Fitting', clf)
clf.fit(x_train, y_train)
print('Evaluating', clf)

test_score = clf.score(x_test, y_test)
print('Test accuracy:', test_score)
"""
#%%