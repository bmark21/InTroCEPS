##Mark Bright - Fall 2022 - ECE5424 Final - InTroCEPS
#This is the main python code for processing and preparing the IBTrACS and TCE-DAT datasets for use in the multiple ML approaches
import pandas as pd
import numpy as np
from collections import defaultdict
import csv
from datetime import datetime
import sys

pathName = "C:\\Users\\Main\\Desktop\\ECE5424\\Final\\InTroCEPS\\datasets\\"
fileName = ''

#Build pandas dataFrame from all 8 IBTrACS parts
for i in range(8):
    fileName = "data_part_" + str(i + 1) + ".csv"
    dataFramePart = pd.read_csv(pathName + fileName)
    if (i == 0):
        dataFrameBuild = dataFramePart
    else:
        dataFrameBuild = dataFrameBuild.append(dataFramePart)
    #print(dataFramePart)
    
    
dataFrameBuild = dataFrameBuild.drop(['SEASON', 'NUMBER',"SUBBASIN","NAME","NATURE","WMO_AGENCY","TRACK_TYPE","DIST2LAND","LANDFALL","IFLAG","USA_AGENCY","USA_ATCF_ID","USA_LAT","USA_LON","USA_RECORD","USA_STATUS","USA_WIND","USA_PRES","USA_SSHS","USA_R34_NE","USA_R34_SE","USA_R34_SW","USA_R34_NW","USA_R50_NE","USA_R50_SE","USA_R50_SW","USA_R50_NW","USA_R64_NE","USA_R64_SE","USA_R64_SW","USA_R64_NW","USA_POCI","USA_ROCI","USA_RMW","USA_EYE","TOKYO_LAT","TOKYO_LON","TOKYO_GRADE","TOKYO_WIND","TOKYO_PRES","TOKYO_R50_DIR","TOKYO_R50_LONG","TOKYO_R50_SHORT","TOKYO_R30_DIR","TOKYO_R30_LONG","TOKYO_R30_SHORT","TOKYO_LAND","CMA_LAT","CMA_LON","CMA_CAT","CMA_WIND","CMA_PRES","HKO_LAT","HKO_LON","HKO_CAT","HKO_WIND","HKO_PRES","NEWDELHI_LAT","NEWDELHI_LON","NEWDELHI_GRADE","NEWDELHI_WIND","NEWDELHI_PRES","NEWDELHI_CI","NEWDELHI_DP","NEWDELHI_POCI","REUNION_LAT","REUNION_LON","REUNION_TYPE","REUNION_WIND","REUNION_PRES","REUNION_TNUM","REUNION_CI","REUNION_RMW","REUNION_R34_NE","REUNION_R34_SE","REUNION_R34_SW","REUNION_R34_NW","REUNION_R50_NE","REUNION_R50_SE","REUNION_R50_SW","REUNION_R50_NW","REUNION_R64_NE","REUNION_R64_SE","REUNION_R64_SW","REUNION_R64_NW","BOM_LAT","BOM_LON","BOM_TYPE","BOM_WIND","BOM_PRES","BOM_TNUM","BOM_CI","BOM_RMW","BOM_R34_NE","BOM_R34_SE","BOM_R34_SW","BOM_R34_NW","BOM_R50_NE","BOM_R50_SE","BOM_R50_SW","BOM_R50_NW","BOM_R64_NE","BOM_R64_SE","BOM_R64_SW","BOM_R64_NW","BOM_ROCI","BOM_POCI","BOM_EYE","BOM_POS_METHOD","BOM_PRES_METHOD","NADI_LAT","NADI_LON","NADI_CAT","NADI_WIND","NADI_PRES","WELLINGTON_LAT","WELLINGTON_LON","WELLINGTON_WIND","WELLINGTON_PRES","DS824_LAT","DS824_LON","DS824_STAGE","DS824_WIND","DS824_PRES","TD9636_LAT","TD9636_LON","TD9636_STAGE","TD9636_WIND","TD9636_PRES","TD9635_LAT","TD9635_LON","TD9635_WIND","TD9635_PRES","TD9635_ROCI","NEUMANN_LAT","NEUMANN_LON","NEUMANN_CLASS","NEUMANN_WIND","NEUMANN_PRES","MLC_LAT","MLC_LON","MLC_CLASS","MLC_WIND","MLC_PRES","USA_GUST","BOM_GUST","BOM_GUST_PER","REUNION_GUST","REUNION_GUST_PER","USA_SEAHGT","USA_SEARAD_NE","USA_SEARAD_SE","USA_SEARAD_SW","USA_SEARAD_NW"], axis=1)

print("Final constructed dataFrame")
print(dataFrameBuild)
#dataFrameBuild.to_csv("dataFrameBuild.csv")
#sys.exit("export csv test")

#The following block is used for constructing an array of integers representing the number of tracking points per storm. i.e. [5 7 4]
#contains three storms and indicates storm 1 had 5 tracking points, storm 2 had 7 tracking points, and storm 3 had 4 tracking points
#This was output to a CSV called storm_trackCount_Distribution for analysis
#storm_trackCount_Dictionary = {}
#for index, row in dataFrameBuild.iterrows():
#   key = row['SID']
#    if key in storm_trackCount_Dictionary:
#        storm_trackCount_Dictionary[key] += 1
#    else:
#        storm_trackCount_Dictionary[key] = 1
        
#print(storm_trackCount_Dictionary)
#storm_trackCount_Distribution = storm_trackCount_Dictionary.values()
#print(storm_trackCount_Distribution)

#with open('storm_trackCount_Distribution.csv', 'w') as output_file:  
#   CSVwriter = csv.writer(output_file)
#   for key, value in storm_trackCount_Dictionary.items():
#      CSVwriter.writerow([key, value])


#We iterate through the tropical storms and collect the first 17 tracking points for them.
#Tracking points 1, 5, 9, 13, 17, 21, and 25 are utilized (5 points) and the storm information at
#these points are converted to features. Therefore, each storm will have its Basin, Time,
#Lat, Long, Wind, Press, Storm Speed, and Storm Dir presented at 0 hours (storm origin),
#12 hours, 24 hours, 36 hours, 48 hours, 60 hours, and 72 hours.
#This process filters out all storms that survived less than 3 days.
storm_trackCount_Dictionary = {}
local_StormTrackCount = 0
local_StormSID = ""
processIter = 0
validStorm = 0 #This is set to 1 if the storm is considered 'valid', i.e. it has at least 2.5 days of tracking data. Otherwise, the storm is discarded
processedStormDataFrame = pd.DataFrame()
for index, row in dataFrameBuild.iterrows():
    if (processIter != 0):
        #print("Processing record: " + row["SID"])
        #print(row["SID"])
        #print("Stored SID is: " + local_StormSID)
        if (local_StormSID == ""):
            local_StormSID = row['SID']
            #print("SID should be set: " + local_StormSID)
            print("SID start")
        if (local_StormSID == row['SID']):
            #print("Still on same storm")
            key = row['SID']
            if key in storm_trackCount_Dictionary:
                storm_trackCount_Dictionary[key] += 1    
            else:
                storm_trackCount_Dictionary[key] = 1
                local_StormFrame = pd.DataFrame()
            local_StormFrame = local_StormFrame.append(row)
        if (local_StormSID != row['SID']): #Occurs when we've encountered a row that belongs to a new storm. In this case, process the previous storm DataFrame
            print("Now on new storm. Process old storm.")
            stormRow = pd.DataFrame()
            rowFrameBuilderIter = 0
            if (len(local_StormFrame.index) >= 21):
                print("Valid storm")
                #print(local_StormFrame)
                for SRCindex,SRCrow, in local_StormFrame.iterrows():
                    if (rowFrameBuilderIter == 0): #rowFrameBuilderIter == 0 dicates we include SID
                        rowFrameBuilderSID = [SRCrow['SID']]
                        rowFrameBuilderBASIN = [SRCrow['BASIN']]
                        rowFrameBuilderISO_TIME = [SRCrow['ISO_TIME']]
                        rowFrameBuilderLAT = [SRCrow['LAT']]
                        rowFrameBuilderLONG = [SRCrow['LON']]
                        rowFrameBuilderWMO_WIND = [SRCrow['WMO_WIND']]
                        rowFrameBuilderWMO_PRES = [SRCrow['WMO_PRES']]
                        rowFrameBuilderSTORM_SPEED = [SRCrow['STORM_SPEED']]
                        rowFrameBuilderSTORM_DIR = [SRCrow['STORM_DIR']]
                        stormRow['SID'] = rowFrameBuilderSID
                        stormRow['BASIN'] = rowFrameBuilderBASIN
                        #try to process the time
                        
                        try:
                            stringFix = ""
                            dt_Extract = datetime.strptime(stringFix.join(rowFrameBuilderISO_TIME), '%Y-%m-%d %H:%M:%S')
                        except:
                            print("Time type 1 extract error. Attempting time type 2 extract")
                            try:
                                stringFix = ""
                                dt_Extract = datetime.strptime(stringFix.join(rowFrameBuilderISO_TIME), "%m/%d/%Y %H:%M")
                            except:
                                sys.exit("Time type 1/2 extract error. Fatal Error at " + SRCrow["SID"])
                        stormRow['MONTH'] = dt_Extract.month
                        stormRow['DAY'] = dt_Extract.day
                        stormRow['LAT_0hr'] = rowFrameBuilderLAT
                        stormRow['LONG_0hr'] = rowFrameBuilderLONG
                        #stormRow['WMO_WIND_0hr'] = rowFrameBuilderWMO_WIND
                        #stormRow['WMO_PRES_0hr'] = rowFrameBuilderWMO_PRES
                        stormRow['STORM_SPEED_0hr'] = rowFrameBuilderSTORM_SPEED
                        stormRow['STORM_DIR_0hr'] = rowFrameBuilderSTORM_DIR                    
                    if (rowFrameBuilderIter > 0):
                        match rowFrameBuilderIter:
                            case 4:
                                rowFrameBuilderLAT = [SRCrow['LAT']]
                                rowFrameBuilderLONG = [SRCrow['LON']]
                                rowFrameBuilderWMO_WIND = [SRCrow['WMO_WIND']]
                                rowFrameBuilderWMO_PRES = [SRCrow['WMO_PRES']]
                                rowFrameBuilderSTORM_SPEED = [SRCrow['STORM_SPEED']]
                                rowFrameBuilderSTORM_DIR = [SRCrow['STORM_DIR']]
                                stormRow['LAT_12hr'] = rowFrameBuilderLAT
                                stormRow['LONG_12hr'] = rowFrameBuilderLONG
                                #stormRow['WMO_WIND_12hr'] = rowFrameBuilderWMO_WIND
                                #stormRow['WMO_PRES_12hr'] = rowFrameBuilderWMO_PRES
                                stormRow['STORM_SPEED_12hr'] = rowFrameBuilderSTORM_SPEED
                                stormRow['STORM_DIR_12hr'] = rowFrameBuilderSTORM_DIR
                            case 8:
                                rowFrameBuilderLAT = [SRCrow['LAT']]
                                rowFrameBuilderLONG = [SRCrow['LON']]
                                rowFrameBuilderWMO_WIND = [SRCrow['WMO_WIND']]
                                rowFrameBuilderWMO_PRES = [SRCrow['WMO_PRES']]
                                rowFrameBuilderSTORM_SPEED = [SRCrow['STORM_SPEED']]
                                rowFrameBuilderSTORM_DIR = [SRCrow['STORM_DIR']]
                                stormRow['LAT_24hr'] = rowFrameBuilderLAT
                                stormRow['LONG_24hr'] = rowFrameBuilderLONG
                                #stormRow['WMO_WIND_24hr'] = rowFrameBuilderWMO_WIND
                                #stormRow['WMO_PRES_24hr'] = rowFrameBuilderWMO_PRES
                                stormRow['STORM_SPEED_24hr'] = rowFrameBuilderSTORM_SPEED
                                stormRow['STORM_DIR_24hr'] = rowFrameBuilderSTORM_DIR
                            case 12:
                                rowFrameBuilderLAT = [SRCrow['LAT']]
                                rowFrameBuilderLONG = [SRCrow['LON']]
                                rowFrameBuilderWMO_WIND = [SRCrow['WMO_WIND']]
                                rowFrameBuilderWMO_PRES = [SRCrow['WMO_PRES']]
                                rowFrameBuilderSTORM_SPEED = [SRCrow['STORM_SPEED']]
                                rowFrameBuilderSTORM_DIR = [SRCrow['STORM_DIR']]
                                stormRow['LAT_36hr'] = rowFrameBuilderLAT
                                stormRow['LONG_36hr'] = rowFrameBuilderLONG
                                #stormRow['WMO_WIND_36hr'] = rowFrameBuilderWMO_WIND
                                #stormRow['WMO_PRES_36hr'] = rowFrameBuilderWMO_PRES
                                stormRow['STORM_SPEED_36hr'] = rowFrameBuilderSTORM_SPEED
                                stormRow['STORM_DIR_36hr'] = rowFrameBuilderSTORM_DIR
                            case 16:
                                rowFrameBuilderLAT = [SRCrow['LAT']]
                                rowFrameBuilderLONG = [SRCrow['LON']]
                                rowFrameBuilderWMO_WIND = [SRCrow['WMO_WIND']]
                                rowFrameBuilderWMO_PRES = [SRCrow['WMO_PRES']]
                                rowFrameBuilderSTORM_SPEED = [SRCrow['STORM_SPEED']]
                                rowFrameBuilderSTORM_DIR = [SRCrow['STORM_DIR']]
                                stormRow['LAT_48hr'] = rowFrameBuilderLAT
                                stormRow['LONG_48hr'] = rowFrameBuilderLONG
                                #stormRow['WMO_WIND_48hr'] = rowFrameBuilderWMO_WIND
                                #stormRow['WMO_PRES_48hr'] = rowFrameBuilderWMO_PRES
                                stormRow['STORM_SPEED_48hr'] = rowFrameBuilderSTORM_SPEED
                                stormRow['STORM_DIR_48hr'] = rowFrameBuilderSTORM_DIR
                            case 20:
                                rowFrameBuilderLAT = [SRCrow['LAT']]
                                rowFrameBuilderLONG = [SRCrow['LON']]
                                rowFrameBuilderWMO_WIND = [SRCrow['WMO_WIND']]
                                rowFrameBuilderWMO_PRES = [SRCrow['WMO_PRES']]
                                rowFrameBuilderSTORM_SPEED = [SRCrow['STORM_SPEED']]
                                rowFrameBuilderSTORM_DIR = [SRCrow['STORM_DIR']]
                                stormRow['LAT_60hr'] = rowFrameBuilderLAT
                                stormRow['LONG_60hr'] = rowFrameBuilderLONG
                                #stormRow['WMO_WIND_60hr'] = rowFrameBuilderWMO_WIND
                                #stormRow['WMO_PRES_60hr'] = rowFrameBuilderWMO_PRES
                                stormRow['STORM_SPEED_60hr'] = rowFrameBuilderSTORM_SPEED
                                stormRow['STORM_DIR_60hr'] = rowFrameBuilderSTORM_DIR
                                #print("Storm row assembled")
                                #print(stormRow)
                                processedStormDataFrame = processedStormDataFrame.append(stormRow)
                                break
                    rowFrameBuilderIter += 1
            #else:
                #print("Invalid storm")
                
            #print("Setup for new storm")
            key = row['SID']
            storm_trackCount_Dictionary[key] = 1
            local_StormFrame = pd.DataFrame()
            local_StormSID = key
            
            
            
    processIter += 1 
    #Enable for first 300 line debugging
    #if (processIter > 300):
        #break
        
        
print(processedStormDataFrame)
processedStormDataFrame.to_csv("processedStormDataFrame.csv")