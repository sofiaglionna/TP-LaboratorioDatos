##########################################################
# Imports
##########################################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import KNeighborsClassifier
import duckdb as dd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, ConfusionMatrixDisplay, classification_report,recall_score
from sklearn.model_selection import StratifiedKFold, cross_val_score
import seaborn as sns #para graficos utilizados en el 2)c

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

#%% 2.b)
# Separamos datos en conjuntos de train y test de las clases 4 y 5.
X45 = dfclases4y5.drop('label', axis=1)
y45 = dfclases4y5["label"]

X45_train, X45_test, y45_train, y45_test = train_test_split(
    X45, y45, test_size = 0.2, random_state=42
)

#%% 2.c y d)
# Vamos a entrenar un modelo KNN para distintas cantidades reducidas de atributos para determinar la mejor
# Para esto vamos a buscar los atributos mas distintivos entre ambas clases. Para hacer esto calculamos en promedio
# cuánto se pinta cada pixel en cada clase y tomamos los que mayor diferencia tengan entre ambas clases.

# Clase 5 sola:
clase5 = """
        SELECT *
        FROM dfclases4y5
        WHERE "label" = 5
"""
dfclase5 = dd.query(clase5).df()

# Tenemos la clase 4 sola del punto 2.b. Le sacamos la columna label a ambas.
dfclase4.drop('label', axis=1,inplace = True)
dfclase5.drop('label', axis=1,inplace = True)
#%%

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

# Ahora que tenemos los promedios ordenado por mayor diferencia creamos una funcion que tome los i atributos con mayor diferencia
# separados por n atributos entre ellos en el grafico. Es decir si el siguiente de mayor diferencia esta en un "radio"
# menor o igual a n se prueba con el siguiente en terminos de diferencia.
# Con radio nos referimos a que si tenemos el pixel en la posicion (x1,y1) en la lista res, no aceptamos pixeles cuya
# coordenada x este en el rango [x1-n,x1+n] ni su coordenada y en [y1-n,y1+n]. (ver figura 4 del informe)
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
        cant_atributos = [3,10,20,50,75,100] #[3,5,7,10,15,20,25]
        for i in cant_atributos:
            #tomo los i atributos con mayor diferencia y una separacion n
            indices = maximosConSeparacion(promedios, n,i)
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

#%% Vamos a graficar una tabla con los 10 valores con mas accuracy para el informe:

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

# Para empezar, nos aseguramos de que en ambos modelos las veces que cada modelo recibe x letra sea la misma entre las letras
# dev = 80%, held-out = 20%

x = kuzushiji.drop(columns=['label'])
y = kuzushiji['label']

X_dev, X_held, y_dev, y_held = train_test_split(x, y,test_size=0.2,stratify=y,random_state=42)

# 3.b
# Volvemos a separar, esta vez en el conjunto de datos de desarrollo (80/20 )

X_entrenamiento, X_evaluacion, y_entrenamiento, y_evaluacion = train_test_split(X_dev, y_dev,test_size=0.2,stratify=y_dev,random_state=42)


# Probamos con todas las profundidades entre 1 y 10
resultados_entropy = []
for i in range(1,11):
 clf = DecisionTreeClassifier( max_depth=i,criterion="entropy")
 clf.fit(X_entrenamiento, y_entrenamiento)

 acc_entrenamiento = accuracy_score(y_entrenamiento, clf.predict(X_entrenamiento))
 acc_evaluacion = accuracy_score(y_evaluacion, clf.predict(X_evaluacion))

 resultados_entropy.append({"max_depth": i,"entrenamiento_acc": acc_entrenamiento,"evaluacion_acc": acc_evaluacion})

#Creamos un df con los resultados 

dataset_resultados_entropy = pd.DataFrame(resultados_entropy).sort_values("evaluacion_acc", ascending=False).reset_index(drop=True)

"""Graficamos los valores de entropy

plt.figure()
plt.scatter(dataset_resultados_entropy["max_depth"], dataset_resultados_entropy["evaluacion_acc"])
plt.xlabel("max_depth")
plt.ylabel("accuracy en evaluación")
plt.title("Entropy (3.b) - depth vs accuracy")
plt.tight_layout()
plt.show()"""   


# 3.c
# En este punto nos queda probar con el criterio de Gini y hacerlo con validacion cruzada (K-fold)

# Hacemos el K-fold

k_fold = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)


def gini():
 resultados_gini = []
 for i in range(1,11):
  clf = DecisionTreeClassifier( max_depth=i,criterion="gini", random_state=42)
  acc = cross_val_score(clf, X_dev, y_dev, cv=k_fold, scoring="accuracy", n_jobs=-1)
  resultados_gini.append({"max_depth": i,"cv_mean": acc.mean(),"cv_std": acc.std()})
  
 dataset_resultados_gini = pd.DataFrame(resultados_gini).sort_values("cv_mean", ascending=False).reset_index(drop=True)
 return dataset_resultados_gini

def entropy():
 resultados_entropy = []
 for i in range(1,11):
  clf = DecisionTreeClassifier( max_depth=i,criterion="entropy", random_state=42)
  acc = cross_val_score(clf, X_dev, y_dev, cv=k_fold, scoring="accuracy", n_jobs=-1)
  resultados_entropy.append({"max_depth": i,"cv_mean": acc.mean(),"cv_std": acc.std()})
  
 dataset_resultados_entropy = pd.DataFrame(resultados_entropy).sort_values("cv_mean", ascending=False).reset_index(drop=True)
 return dataset_resultados_entropy

df_gini = gini()
df_entropy = entropy()

#Graficamos el cv mean de cada uno

"""plt.figure()
plt.scatter(df_gini["max_depth"], df_gini["cv_mean"])
plt.scatter(df_entropy["max_depth"], df_entropy["cv_mean"])
plt.xlabel("max_depth")
plt.ylabel("accuracy (cv_mean)")
plt.title("Comparación - Gini vs Entropy")
plt.legend(["Gini","Entropy"])
plt.tight_layout()
plt.show()"""

# 3.d
# Usamos entropy ya que fue el mejor modelo

#Entrenamos el modelo
mejor_modelo = DecisionTreeClassifier(criterion="entropy", max_depth=10, random_state=42)
mejor_modelo.fit(X_dev, y_dev)

# Predecimos sobre held-out
y_pred = mejor_modelo.predict(X_held)

# Calculamos accuracy
acc_final = accuracy_score(y_held, y_pred)
#print(f"Accuracy final en held-out: {acc_final:.4f}")

"""
# Generamos la matriz de confusion del modelo y sus metricas

# Matriz de confusión 
cm = confusion_matrix(y_held, y_pred)
print("Matriz de confusión:\n", cm)

# Gráfico de matriz de confusión
plt.figure()
ConfusionMatrixDisplay(confusion_matrix=cm).plot(cmap="Blues", values_format='d')
plt.title("Matriz de Confusión – Held-Out (Entropy, depth=10)")
plt.tight_layout()
plt.show()

# Metricas
print("Reporte por clase:\n")
print(classification_report(y_held, y_pred, digits=4))"""










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