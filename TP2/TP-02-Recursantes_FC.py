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
from sklearn.metrics import accuracy_score, confusion_matrix, ConfusionMatrixDisplay, classification_report
from sklearn.model_selection import StratifiedKFold, cross_val_score
import random


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
    X45, y45, test_size = 0.2
)

#%% 2.c)
#voy a entrenar un modelo KNN para distintas cantidades reducidas de atributos para determinar la mejor
#Para esto voy a buscar los atributos mas distintivos entre las clases. Voy a ver los pixeles que nunca se pintan en ambas
#Para hacer esto voy a sumar cada columna de ambas clases y quedarme las que den 0

#clase 5 sola
clase5 = """
        SELECT *
        FROM dfclases4y5
        WHERE "label" = 5
"""
dfclase5 = dd.query(clase5).df()

#Tengo la clase 4 sola del punto 2.b. Le saco la columna label a ambas
dfclase4.drop('label', axis=1,inplace = True)
dfclase5.drop('label', axis=1,inplace = True)
#%%
#calculo el promedio por columna con .mean() de pandas
prom4 = dfclase4.mean()
prom5 = dfclase5.mean()
#paso los indices a int ya que toma los nombres de las columnas como indices, estos son strings
prom4.index = prom4.index.astype(int)
prom5.index = prom5.index.astype(int)
promedios = pd.DataFrame({"promedio4": prom4,
                          "promedio5": prom5})
#agrego una columna que sea la diferencia entre los 2 promedios.
promedios["diferencia"] = abs(promedios["promedio5"] - promedios["promedio4"])
#voy a quedarme con los i atributos con mayor diferencia entre ellos. Variando ese i voy a probar irme quedando
#con distintas cantidades de atributos. Para cada uno voy a medir el accuracy para quedarme con el que de maximo
cant_atributos = [3,5,7,10,15,20,25,50,100,150,200,250,300,400,500,600,700,750,1000]
for i in cant_atributos:
    #tomo los i atributos con mayor diferencia con un comando de pandas
    conjunto = promedios["diferencia"].nlargest(i)
    #tomo sus indices (nombre del atributo) y lo paso a tipo lista
    indices = conjunto.index.tolist()
    #creo una lista con el nombre de esos atributos
    atributos= []
    for j in indices:
        atributos.append(str(j))
    #Creo X45acotado una tabla que para cada fila de X_train conserva solo los valores de los atributos de interes
    X45_train_acotado = X45_train[atributos]
    #tambien voy a acotar mi X45_test a solo estos atributos
    X45_test_acotado = X45_test[atributos]
    #hago el modelo de KNN para 5 vecinos (despues lo voy a cambiar)
    clasificador = KNeighborsClassifier(n_neighbors=5)
    clasificador.fit(X45_train_acotado, y45_train)
    # Calculamos predicciones
    y_pred = clasificador.predict(X45_test_acotado)
    print(str(i) +":")
    # Accuracy comparando y con y_pred
    accuracyscore = accuracy_score(y45_test, y_pred)
    print(accuracyscore)

#%%

#%%
# Matriz de confusión comparando y con y_pred
matrizconfusion = confusion_matrix(y45_test, y_pred)



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