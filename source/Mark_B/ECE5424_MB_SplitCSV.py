#Mark Bright - ECE5424 Fall 2022 - CSV Splitter
#This script takes a CSV file and splits it according to the defined chunk size
#
#Reference: (https://mungingdata.com/python/split-csv-write-chunk-pandas/)

dataPerPart = 90000

def writePart(part, data):
    with open('../data_part_'+ str(part) +'.csv', 'w') as outputWriter:
        outputWriter.write(header)
        outputWriter.writelines(data)
        
with open(r"C:\Users\Main\Desktop\ECE5424\Final\InTroCEPS\datasets\ibtracs.ALL.list.v04r00.csv", "r") as file:
    count = 0
    header = file.readline()
    data = []
    for line in file:
        count += 1
        data.append(line)
        if count % dataPerPart == 0:
            writePart(count // dataPerPart, data)
            data = []
    if len(data) > 0:
        writePart((count // dataPerPart) + 1, data)