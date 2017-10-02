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
import random as rd
import string
import numpy as np
import copy as cp

class DateTime():
    def __init__(self, day=1, inc=1000):
        self.year = 2018
        self.month = 1
        self.day = day
        self.hour = 0
        self.min = 0
        self.sec = 0
        self.msec = 0
        self.inc = inc

    def SetDay(self,day):
        self.day = day

    def IncrementTime(self):
        self.msec+=self.inc
        if self.msec >= 1000:
            self.msec = self.msec - 1000
            self.sec+=1
            if self.sec >= 60:
                self.sec = self.sec - 60
                self.min+=1
                if self.min == self.min - 60:
                    self.min = 0
                    self.hour+=1
    def GetSec(self):
        return self.sec
    def GetMin(self):
        return self.min
    def GetHour(self):
        return self.hour

    def ReturnDateTime(self):
        date = str(self.year)+'-'+str(self.month)+'-'+str(self.day)
        time = str(self.hour)+':'+str(self.min)+':'+str(self.sec)+'.'+str(self.msec)
        return date+' '+time

class Trip:
    def __init__(self,lng=0,lat=0,cid=0,bid=0,sdt=None):
        self.BID = bid
        self.CID = cid
        self.StartLat = lat
        self.StartLng = lng
        self.StartDateTime = cp.deepcopy(sdt)

    def SetEndDateTime(self,edt):
        self.EndDateTime = cp.deepcopy(edt)

    def SetDuration(self):
        sec = self.EndDateTime.GetSec() - self.StartDateTime.GetSec()
        min = self.EndDateTime.GetMin() - self.StartDateTime.GetMin()
        hour = self.EndDateTime.GetHour() - self.StartDateTime.GetHour()
        self.Duration = hour*3600+min*60+sec

    def SetPrice(self):
        self.EndLat = self.StartLat + self.Duration*(25-self.StartLat)* 1e-6 *rd.random()
        self.EndLng = self.StartLng + self.Duration*(45 - self.StartLng)* 1e-6 *rd.random()
        self.Distance = np.sqrt((self.StartLat - self.EndLat) ** 2 + (self.StartLng - self.EndLng) ** 2)
        self.Price = 5 + 500*self.Distance

class Cars:
    def __init__(self, mid, vid, day=1, inc=1000):
        self.NumCars = len(vid)
        self.MID = mid
        self.VID = vid
        self.DateTime = DateTime(day,inc)
        self.SID1 = {vid:0 for vid in self.VID} # sensor measurement id
        self.SID2 = {vid:0 for vid in self.VID}
        self.BID = {vid:0 for vid in self.VID} # booking id
        self.Occupied = {vid:0 for vid in self.VID} # is car occupied currently
        self.Lat = {vid:30.0-5.0*(rd.random()) for vid in self.VID} # initial latitude
        self.Lng = {vid:50.0-10.0*(rd.random()) for vid in self.VID} # initial longitude
        self.Trip = {vid:Trip() for vid in self.VID}

    def SetVelocity(self):
        self.LatV = {vid:(self.Lat[vid] - 27) * 1e-6 * (-0.1+rd.random()) for vid in self.VID}
        self.LngV = {vid:(self.Lng[vid] - 45) * 1e-6 * (-0.1+rd.random()) for vid in self.VID}

    def GenerateSensorData1(self):
        data = []
        self.DateTime.IncrementTime()
        self.SetVelocity()
        for vid in self.VID:
            self.Lat[vid] = self.Lat[vid] + self.LatV[vid]*self.DateTime.inc # latitude
            self.Lng[vid] = self.Lng[vid] + self.LngV[vid]*self.DateTime.inc # longitude
            Rd = rd.random() # Radar proximity sensor
            Ld = rd.random() # Ladar proximity sensor
            US = rd.random() # Ultrasound proximity sensor
            data.append({'VID': vid, 'SID': 'S1'+str(self.SID1[vid]),
                         'latv': self.LatV[vid], 'lngv': self.LngV[vid],
                         'lat': self.Lat[vid], 'lng': self.Lng[vid],
                         'Rd': Rd, 'Ld': Ld, 'US': US, 'Date-Time':self.DateTime.ReturnDateTime()})
            self.SID1[vid]+=1
        return data

    def GenerateSensorData2(self):
        data = []
        for vid in self.VID:
            gas = rd.random() # gas meter
            temperature = 95 - 45 * (rd.random()) # inside temperature
            pressure = rd.random() # tire pressure
            tire_temp = 150 - 100 * (rd.random())  # tire temperature
            engine_temp = 350 - 100 * (rd.random())  # engine temperature
            data.append({'VID': vid, 'SID': 'S2'+str(self.SID2[vid]),
                         'gas': gas, 'temp': temperature, 'pr': pressure,
                         'ttemp': tire_temp, 'etemp': engine_temp,
                         'Date-Time':self.DateTime.ReturnDateTime()})
            self.SID2[vid]+=1
        return data

    def GenerateBooking(self):
        vid = self.VID[int((self.NumCars)*rd.random())]
        if not self.Occupied[vid]:
            if int(round(rd.random())) == 1:
                CustomerID = self.MID[int(round((len(self.MID)-1)*rd.random()))]
            else:
                CustomerID = -1
            self.Trip[vid] = Trip(self.Lat[vid],self.Lng[vid],CustomerID,self.BID[vid],self.DateTime)
            data = [{'VID': vid, 'BID': 'BS' + str(self.Trip[vid].BID),
                    'StLat': self.Trip[vid].StartLat, 'StLng': self.Trip[vid].StartLng,
                    'CustomerID': CustomerID, 'Date-Time': self.DateTime.ReturnDateTime()}]
            self.BID[vid]+=1
            self.Occupied[vid] = 1
        else:
            self.Trip[vid].SetEndDateTime(self.DateTime)
            self.Trip[vid].SetDuration()
            self.Trip[vid].SetPrice()
            data = [{'VID': vid, 'BID': 'BE' + str(self.Trip[vid].BID), 'CustomerID': self.Trip[vid].CID,
                    'StLat': self.Trip[vid].StartLat, 'StLng': self.Trip[vid].StartLng,
                    'EndLat': self.Trip[vid].EndLat, 'EndLng': self.Trip[vid].EndLng,
                    'Price': self.Trip[vid].Price, 'Date-Time': self.DateTime.ReturnDateTime()}]
            DurationLeft = self.Trip[vid].Duration
            if int(round(rd.random())) == 1:
                NumMovies = int(4*rd.random()+1)
                for mov in range(NumMovies):
                    Duration = rd.random()*DurationLeft
                    DurationLeft-=Duration
                    Ent = {'VID': vid, 'BID': 'BM' + str(self.Trip[vid].BID),
                           'MovieID':int(15*rd.random()), 'Duration':Duration,
                           'Date-Time': self.DateTime.ReturnDateTime()}
                    data.append(Ent)
            self.Occupied[vid] = 0
            self.CurrentLat = self.Trip[vid].EndLat
            self.CurrentLng = self.Trip[vid].EndLng
        return data

    def SetDay(self, day=1):
        self.DateTime.SetDay(day)
