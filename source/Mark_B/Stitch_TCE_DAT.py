##Mark Bright - Fall 2022 - ECE5424 Final - InTroCEPS
#This stitched the IBTrACS data with the TCE-DAT data
import pandas as pd
import numpy as np
import csv
import sys

########## Load ##########
pathName = "C:\\Users\\Main\\Desktop\\ECE5424\\Final\\InTroCEPS\\datasets\\"
fileNamePSD = 'processedStormDataFrame_with_WMOWINDPRES.csv'
fileNameTCD = 'TCE-DAT_2015-exposure_1950-2015.csv'
#Load the data
processedStormdataFrame = pd.read_csv(pathName + fileNamePSD)
TCEDATdataFrame = pd.read_csv(pathName + fileNameTCD)

print("The variable, processedStormdataFrame is of type: ", type(processedStormdataFrame))
print(processedStormdataFrame)

print("The variable, TCEDATdataFrame is of type: ", type(TCEDATdataFrame))
print(TCEDATdataFrame)


iter = 0
stitchedStormDataFrame = pd.DataFrame()
for indexT, SDFrow in TCEDATdataFrame.iterrows():
    iter += 1
    key = SDFrow['IBTrACS_ID']
    
    rowFrameBuilder34kn_pop = [SDFrow['34kn_pop']]
    rowFrameBuilder34kn_assets = [SDFrow['34kn_assets']]
    rowFrameBuilder64kn_pop = [SDFrow['64kn_pop']]
    rowFrameBuilder64kn_assets = [SDFrow['64kn_assets']]
    rowFrameBuilder96kn_pop = [SDFrow['96kn_pop']]
    rowFrameBuilder96kn_assets = [SDFrow['96kn_assets']]
    stormRow = pd.DataFrame()
    print(len(processedStormdataFrame.index))
    for indexB, SRCrow in processedStormdataFrame.iterrows():
        if (key == SRCrow ['SID']):
            print(indexB)
            rowFrameBuilderSID = [SRCrow['SID']]
            rowFrameBuilderBASIN = [SRCrow['BASIN']]
            rowFrameBuilderMONTH = [SRCrow['MONTH']]
            rowFrameBuilderDAY = [SRCrow['DAY']]
            rowFrameBuilderLAT0 = [SRCrow['LAT_0hr']]
            rowFrameBuilderLONG0 = [SRCrow['LONG_0hr']]
            rowFrameBuilderWMO_WIND0 = [SRCrow['WMO_WIND_0hr']]
            rowFrameBuilderWMO_PRES0 = [SRCrow['WMO_PRES_0hr']]
            rowFrameBuilderSTORM_SPEED0 = [SRCrow['STORM_SPEED_0hr']]
            rowFrameBuilderSTORM_DIR0 = [SRCrow['STORM_DIR_0hr']]
            rowFrameBuilderLAT12 = [SRCrow['LAT_12hr']]
            rowFrameBuilderLONG12 = [SRCrow['LONG_12hr']]
            rowFrameBuilderWMO_WIND12 = [SRCrow['WMO_WIND_12hr']]
            rowFrameBuilderWMO_PRES12 = [SRCrow['WMO_PRES_12hr']]
            rowFrameBuilderSTORM_SPEED12 = [SRCrow['STORM_SPEED_12hr']]
            rowFrameBuilderSTORM_DIR12 = [SRCrow['STORM_DIR_12hr']]
            rowFrameBuilderLAT24 = [SRCrow['LAT_24hr']]
            rowFrameBuilderLONG24 = [SRCrow['LONG_24hr']]
            rowFrameBuilderWMO_WIND24 = [SRCrow['WMO_WIND_24hr']]
            rowFrameBuilderWMO_PRES24 = [SRCrow['WMO_PRES_24hr']]
            rowFrameBuilderSTORM_SPEED24 = [SRCrow['STORM_SPEED_24hr']]
            rowFrameBuilderSTORM_DIR24 = [SRCrow['STORM_DIR_24hr']]
            rowFrameBuilderLAT36 = [SRCrow['LAT_36hr']]
            rowFrameBuilderLONG36 = [SRCrow['LONG_36hr']]
            rowFrameBuilderWMO_WIND36 = [SRCrow['WMO_WIND_36hr']]
            rowFrameBuilderWMO_PRES36 = [SRCrow['WMO_PRES_36hr']]
            rowFrameBuilderSTORM_SPEED36 = [SRCrow['STORM_SPEED_36hr']]
            rowFrameBuilderSTORM_DIR36 = [SRCrow['STORM_DIR_36hr']]
            rowFrameBuilderLAT48 = [SRCrow['LAT_48hr']]
            rowFrameBuilderLONG48 = [SRCrow['LONG_48hr']]
            rowFrameBuilderWMO_WIND48 = [SRCrow['WMO_WIND_48hr']]
            rowFrameBuilderWMO_PRES48 = [SRCrow['WMO_PRES_48hr']]
            rowFrameBuilderSTORM_SPEED48 = [SRCrow['STORM_SPEED_48hr']]
            rowFrameBuilderSTORM_DIR48 = [SRCrow['STORM_DIR_48hr']]
            rowFrameBuilderLAT60 = [SRCrow['LAT_60hr']]
            rowFrameBuilderLONG60 = [SRCrow['LONG_60hr']]
            rowFrameBuilderWMO_WIND60 = [SRCrow['WMO_WIND_60hr']]
            rowFrameBuilderWMO_PRES60 = [SRCrow['WMO_PRES_60hr']]
            rowFrameBuilderSTORM_SPEED60 = [SRCrow['STORM_SPEED_60hr']]
            rowFrameBuilderSTORM_DIR60 = [SRCrow['STORM_DIR_60hr']]
            stormRow['SID'] = rowFrameBuilderSID
            stormRow['BASIN'] = rowFrameBuilderBASIN
            stormRow['MONTH'] = rowFrameBuilderMONTH
            stormRow['DAY'] = rowFrameBuilderDAY
            stormRow['LAT_0hr'] = rowFrameBuilderLAT0
            stormRow['LONG_0hr'] = rowFrameBuilderLONG0
            stormRow['WMO_WIND_0hr'] = rowFrameBuilderWMO_WIND0
            stormRow['WMO_PRES_0hr'] = rowFrameBuilderWMO_PRES0
            stormRow['STORM_SPEED_0hr'] = rowFrameBuilderSTORM_SPEED0
            stormRow['STORM_DIR_0hr'] = rowFrameBuilderSTORM_DIR0
            stormRow['LAT_12hr'] = rowFrameBuilderLAT12
            stormRow['LONG_12hr'] = rowFrameBuilderLONG12
            stormRow['WMO_WIND_12hr'] = rowFrameBuilderWMO_WIND12
            stormRow['WMO_PRES_12hr'] = rowFrameBuilderWMO_PRES12
            stormRow['STORM_SPEED_12hr'] = rowFrameBuilderSTORM_SPEED12
            stormRow['STORM_DIR_12hr'] = rowFrameBuilderSTORM_DIR12
            stormRow['LAT_24hr'] = rowFrameBuilderLAT24
            stormRow['LONG_24hr'] = rowFrameBuilderLONG24
            stormRow['WMO_WIND_24hr'] = rowFrameBuilderWMO_WIND24
            stormRow['WMO_PRES_24hr'] = rowFrameBuilderWMO_PRES24
            stormRow['STORM_SPEED_24hr'] = rowFrameBuilderSTORM_SPEED24
            stormRow['STORM_DIR_24hr'] = rowFrameBuilderSTORM_DIR24
            stormRow['LAT_36hr'] = rowFrameBuilderLAT36
            stormRow['LONG_36hr'] = rowFrameBuilderLONG36
            stormRow['WMO_WIND_36hr'] = rowFrameBuilderWMO_WIND36
            stormRow['WMO_PRES_36hr'] = rowFrameBuilderWMO_PRES36
            stormRow['STORM_SPEED_36hr'] = rowFrameBuilderSTORM_SPEED36
            stormRow['STORM_DIR_36hr'] = rowFrameBuilderSTORM_DIR36
            stormRow['LAT_48hr'] = rowFrameBuilderLAT48
            stormRow['LONG_48hr'] = rowFrameBuilderLONG48
            stormRow['WMO_WIND_48hr'] = rowFrameBuilderWMO_WIND48
            stormRow['WMO_PRES_48hr'] = rowFrameBuilderWMO_PRES48
            stormRow['STORM_SPEED_48hr'] = rowFrameBuilderSTORM_SPEED48
            stormRow['STORM_DIR_48hr'] = rowFrameBuilderSTORM_DIR48
            stormRow['LAT_60hr'] = rowFrameBuilderLAT60
            stormRow['LONG_60hr'] = rowFrameBuilderLONG60
            stormRow['WMO_WIND_60hr'] = rowFrameBuilderWMO_WIND60
            stormRow['WMO_PRES_60hr'] = rowFrameBuilderWMO_PRES60
            stormRow['STORM_SPEED_60hr'] = rowFrameBuilderSTORM_SPEED60
            stormRow['STORM_DIR_60hr'] = rowFrameBuilderSTORM_DIR60
            stormRow['34kn_pop'] = rowFrameBuilder34kn_pop
            stormRow['34kn_assets'] = rowFrameBuilder34kn_assets
            stormRow['64kn_pop'] = rowFrameBuilder64kn_pop
            stormRow['64kn_assets'] = rowFrameBuilder64kn_assets
            stormRow['96kn_pop'] = rowFrameBuilder96kn_pop
            stormRow['96kn_assets'] = rowFrameBuilder96kn_assets
            stitchedStormDataFrame = stitchedStormDataFrame.append(stormRow)
            processedStormdataFrame = processedStormdataFrame.drop(indexB)
            break
    #if (iter > 3):
        #break
        
print(stitchedStormDataFrame)
stitchedStormDataFrame.to_csv("stitchedStormDataFrame.csv")   