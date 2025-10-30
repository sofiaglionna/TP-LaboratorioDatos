##########################################################
# Imports
##########################################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import KNeighborsClassifier
import duckdb as dd
from sklearn.model_selection import train_test_split

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

img = np.array(X8.iloc[50]).reshape((28,28))
plt.imshow(img, cmap='gray')
plt.show()

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