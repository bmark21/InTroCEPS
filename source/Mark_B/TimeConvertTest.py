from datetime import datetime
import sys

rowFrameBuilderISO_TIME = "1842-10-26 00:00:00"
try:
    dt_Extract = datetime.strptime(rowFrameBuilderISO_TIME, '%Y-%m-%d %H:%M:%S')
except:
    print("Time type 1 extract error. Attempting time type 2 extract")
    try:
        dt_Extract = datetime.strptime(rowFrameBuilderISO_TIME, "%m/%d/%Y %H:%M")
    except:
        sys.exit("Time type 2 extract error. Fatal Error")
print(dt_Extract.year)
print(dt_Extract.month)
print(dt_Extract.day)
print(dt_Extract.hour)