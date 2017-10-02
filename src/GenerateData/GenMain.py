import json
import random as rd
import string
import numpy as np
import copy as cp
import GenClasses


NumCars = 10
Inc = 1000 # time increment in msec <= 1000
MID = [x for x in range(0,50)] # Taxi Service Member IDs
VID = [] # Vehicle IDs
for _ in range(NumCars):
    VID.append(''.join(rd.choice(string.ascii_uppercase + string.digits)
                            for _ in range(10)))
Fleet = GenClasses.Cars(MID,VID,inc=Inc)


data = []
Days = 3 # number of days t simulate
FileCount = 0 # File id (1 file per day)
TotalCount = 0 # total record count
RecordCount = TotalCount # file record count

while FileCount<Days:
    with open('data'+str(FileCount)+'.txt', 'w') as outfile:
        Fleet.SetDay(FileCount + 1)
        while RecordCount < TotalCount + 1000/Inc*60*60*1:
	    Director = rd.random()
            if Director < 0.99:
                data = Fleet.GenerateSensorData1()
                for dat in data:
                    json.dump(dat, outfile)
                    outfile.write('\n')
                RecordCount += 1
            elif Director < 0.995:
                data = Fleet.GenerateSensorData2()
                for dat in data:
                    json.dump(dat, outfile)
                    outfile.write('\n')
            else:
                data = Fleet.GenerateBooking()
                for dat in data:
                    json.dump(dat, outfile)
                    outfile.write('\n')

    TotalCount = RecordCount
    FileCount+=1
