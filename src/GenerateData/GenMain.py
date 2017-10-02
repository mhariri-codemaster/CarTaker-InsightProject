"""
Copyright 2017 Mohamed Hariri Nokob 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This piece of code is used to generate fictonal sensor, location and booking
data for a fleet of cars used in data analysis.

"""

import json
import random as rd
import GenClasses

# Change these values
NumCars = 100
Inc = 10 # time increment in msec <= 1000
Days = 3 # number of days t simulate


MID = [x for x in range(0,500)] # Taxi Service Member IDs
VID = [] # Vehicle IDs
for _ in range(NumCars):
    VID.append(''.join(rd.choice(string.ascii_uppercase + string.digits)
                            for _ in range(10)))
Fleet = GenClasses.Cars(MID,VID,inc=Inc)

data = []
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
