##Mark Bright - Fall 2022 - ECE5424 Final - InTroCEPS
#This is the main python code for using ANN to predict impacted population and economic damage from tropical cyclone weather data

import pandas as pd
import numpy as np

pathName = "C:\\Users\\Main\\Desktop\\ECE5424\\Final\\InTroCEPS\\datasets\\"
fileName = ''

#Build pandas dataFrame from all 8 IBTrACS parts
for i in range(8):
    fileName = "data_part_" + str(i + 1) + ".csv"
    dataFramePart = pd.read_csv(pathName + fileName)
    print(dataFramePart)