#Mark Bright - ECE5424 Fall 2022 - CSV Splitter
#This script takes a CSV file and splits it according to the defined chunk size
#
#Reference: (https://mungingdata.com/python/split-csv-write-chunk-pandas/)

chunk_size = 90000


def write_chunk(part, lines):
    with open('../data_part_'+ str(part) +'.csv', 'w') as f_out:
        f_out.write(header)
        f_out.writelines(lines)
        
with open(r"C:\Users\Main\Desktop\ECE5424\Final\InTroCEPS\datasets\ibtracs.ALL.list.v04r00.csv", "r") as f:
    count = 0
    header = f.readline()
    lines = []
    for line in f:
        count += 1
        lines.append(line)
        if count % chunk_size == 0:
            write_chunk(count // chunk_size, lines)
            lines = []
    # write remainder
    if len(lines) > 0:
        write_chunk((count // chunk_size) + 1, lines)