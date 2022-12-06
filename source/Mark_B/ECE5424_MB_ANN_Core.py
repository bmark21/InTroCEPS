#Mark Bright - Fall 2022 - ECE5424 Final - InTroCEPS
#Main ANN Core Script for InTroCEPS ML Approach A

import pandas as pd
import numpy as np
import csv
import sys
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
import sklearn.metrics as metrics
import matplotlib.pyplot as plt

########## Load ##########
pathName = "C:\\Users\\Main\\Desktop\\ECE5424\\Final\\InTroCEPS\\datasets\\"
fileName = 'stitchedStormDataFrame.csv'

#Load the data
processedStormdataFrame = pd.read_csv(pathName + fileName)

#print("The variable, processedStormdataFrame is of type: ", type(processedStormdataFrame))
#print(processedStormdataFrame)

processedStormdataFrame = processedStormdataFrame.iloc[: , 2:]
processedStormdataFrame = processedStormdataFrame.drop(['WMO_WIND_0hr','WMO_PRES_0hr','WMO_WIND_12hr','WMO_PRES_12hr','WMO_WIND_24hr','WMO_PRES_24hr','WMO_WIND_36hr','WMO_PRES_36hr','WMO_WIND_48hr','WMO_PRES_48hr','WMO_WIND_60hr','WMO_PRES_60hr'], axis=1)
processedStormdataFrame = processedStormdataFrame.dropna()

print(processedStormdataFrame)

processedStormdataFrame["EP"] = 0
processedStormdataFrame["NI"] = 0
processedStormdataFrame["SA"] = 0
processedStormdataFrame["SI"] = 0
processedStormdataFrame["SP"] = 0
processedStormdataFrame["WP"] = 0

for indexB, SRCrow in processedStormdataFrame.iterrows():
    match SRCrow["BASIN"]:
        case "EP":
            processedStormdataFrame.at[indexB,'EP'] = 1
        case "NI":
            processedStormdataFrame.at[indexB,'NI'] = 1
        case "SA":
            processedStormdataFrame.at[indexB,'SA'] = 1
        case "SI":
            processedStormdataFrame.at[indexB,'SI'] = 1
        case "SP":
            processedStormdataFrame.at[indexB,'SP'] = 1
        case "WP":
            processedStormdataFrame.at[indexB,'WP'] = 1


pd.set_option("display.max_rows", 2300)
pd.set_option("display.expand_frame_repr", True)
pd.set_option('display.width', 300)        
     
#Drop the pop and assets columns. Drop total_economic_cost since we want to estimate total_impacted_pop first
X = processedStormdataFrame.drop(["DAY", "34kn_assets", "64kn_pop", "64kn_assets", "96kn_pop", "96kn_assets", "BASIN", "LAT_12hr","LONG_12hr","STORM_SPEED_12hr","STORM_DIR_12hr","LAT_24hr","LONG_24hr","STORM_SPEED_24hr","STORM_DIR_24hr","LAT_36hr","LONG_36hr","STORM_SPEED_36hr","STORM_DIR_36hr","LAT_48hr","LONG_48hr","STORM_SPEED_48hr","STORM_DIR_48hr"], axis=1).to_numpy()

#processedStormdataFrame.to_csv("processedStormdataFrame.csv")

trainXScaler = MinMaxScaler()
trainXScaler.fit(X)
X = trainXScaler.transform(X)

Y = processedStormdataFrame["34kn_pop"].to_numpy()

trainX, testX, trainY, testY = train_test_split(X, Y, test_size=0.3, random_state=19350)

training_Loss = []
validation_Loss = []
scores_train = []
scores_test = []

hidden_Layers = (50, 50, 50)
model = MLPRegressor(hidden_layer_sizes=hidden_Layers, activation='identity', solver='adam', max_iter=300, alpha=0.001, random_state=19350)

for epoch in range(3000):
    model.partial_fit(trainX,trainY)
    training_Loss.append(1-model.score(trainX, trainY))
    validation_Loss.append(1-model.score(testX, testY))
    scores_train.append(model.score(trainX, trainY))
    scores_test.append(model.score(testX, testY))

annPredY = model.predict(testX)

#Compute MSE
print("\n\rANN: MSE = %f" % metrics.mean_squared_error(testY, annPredY))

#Create training/validation loss plot
xlabel = "Epochs (Hidden Layers=" + str(hidden_Layers) + ")"
fig,ax = plt.subplots()
ax.plot(training_Loss, color="blue")
ax.set_xlabel(xlabel,fontsize=10)
ax.set_ylabel("loss",color="blue",fontsize=10)
ax.plot(validation_Loss,color="red")
ax.set_yscale('log')
plt.show()

#Create accuracy plot
plt.plot(scores_train, color='blue', alpha=0.8, label='Train')
plt.plot(scores_test, color='red', alpha=0.8, label='Test')
plt.title("Accuracy over epochs", fontsize=14)
plt.xlabel('Epochs')
plt.legend(loc='upper left')
plt.show()