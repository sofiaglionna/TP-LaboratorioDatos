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
from sklearn.metrics import accuracy_score
from sklearn.model_selection import StratifiedKFold, cross_val_score

########################
#%% Leer el archivo
########################

kuzushiji = pd.read_csv("datos\originales\kuzushiji_full.csv")

##########################################################
#%% 1. Análisis exploratorio de los datos
##########################################################

# Ver cuántas columnas tiene el archivo
print(kuzushiji.shape)
# (70000,785)
# El archivo kuzushiji cuenta con 70000 imagenes de caracteres
# 785 columnas, de las cuales 784 pixeles y 1 columna "label" con el caracter al que corresponde 

########################
#%% Graficar caracteres
########################

X = kuzushiji.drop(columns=["label"])

#Solo clase 8
kuzushiji8 = """
        SELECT *
        FROM kuzushiji
        WHERE "label" = 8
"""
dfkuzushiji8 = dd.query(kuzushiji8).df()

X8 = dfkuzushiji8.drop(columns=["label"])

########################
#%% Plot imagen
########################

print(dfkuzushiji8.loc[50,"label"])

"""img = np.array(X8.iloc[50]).reshape((28,28))
plt.imshow(img, cmap='gray')
plt.show()"""

##########################################################
#%% 2. Clasificación Binaria
##########################################################

# DF con clases 4 y 5
clases4y5 = """
        SELECT *
        FROM kuzushiji
        WHERE "label" = 4 OR "label" = 5
"""
dfclases4y5 = dd.query(clases4y5).df()

# Separamos datos en conjuntos de train y test.
y = kuzushiji["label"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size = 0.1
)

# Ajustamos un modelo de KNN sobre los datos de entrenamiento.
clasificador = KNeighborsClassifier(n_neighbors=10) # construimos el modelo.
clasificador.fit(X_train, y_train) # lo entrenamos.

# Ejercicio 3 

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

plt.figure()
plt.scatter(df_gini["max_depth"], df_gini["cv_mean"])
plt.scatter(df_entropy["max_depth"], df_entropy["cv_mean"])
plt.xlabel("max_depth")
plt.ylabel("accuracy (cv_mean)")
plt.title("Comparación - Gini vs Entropy")
plt.legend(["Gini","Entropy"])
plt.tight_layout()
plt.show()

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