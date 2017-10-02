from app import app
from flask import render_template
from flask import request
from cassandra.cluster import Cluster
import ConfigFile

# Setting up connections to cassandra
cluster = Cluster(Cassandra_Nodes)
session = cluster.connect()

@app.route('/')
@app.route('/CarTaker')
def CarTaker():
 return render_template("CarTaker.html")

@app.route("/CarTaker", methods=['POST'])
def CarTaker_post():
  VID = request.form["VID"]
  stmt = "SELECT VID FROM CarTaker.s1_table PER PARTITION LIMIT 1;"
  response = session.execute(stmt)
  response_list = []
  for val in response:
    response_list.append(val.vid)
  if VID not in response_list:
    VID = response_list[0]

  stmt = "SELECT * FROM CarTaker.s1_table WHERE VID=%s ORDER BY DateTime DESC LIMIT 10;"
  response = session.execute(stmt, parameters=[VID])
  response_list = []
  for val in response:
    response_list.append(val)
  jsonresponse = [{"VID": x.vid, "Date": "{:%m/%d/%Y}".format(x.datetime), 
                   "Time": "{:%H:%M:%S}".format(x.datetime),
                   "Latitude": x.lat, "Longitude": x.lng,
                   "Latitude_Speed": x.latv, "Longitude_Speed": x.lngv, "Radar": x.radar, "Ladar": x.ladar,
                   "Ultrasound": x.ultrasound} for x in response_list]
  return render_template("CarTakerOP.html", output=jsonresponse)

@app.route("/map")
def CarTaker_Map():
 stmt = "SELECT vid,datetime,lat,lng FROM CarTaker.s1_table PER PARTITION LIMIT 1;"
 response = session.execute(stmt)
 response_list = []
 for val in response:
   response_list.append(val)
   jsonresponse = [{"VID": x.vid, "Date": "{:%m/%d/%Y}".format(x.datetime), 
                  "Time": "{:%H:%M:%S}".format(x.datetime),
                  "Latitude": 80*(x.lat-24.5), "Longitude": 40*(x.lng-37)} for x in response_list]
 return render_template("CarTakerMap.html", output=jsonresponse)

@app.route("/Blocks")
def CarTaker_Blocks():
 stmt = "SELECT block,datetime,count FROM CarTaker.booking_end_table PER PARTITION LIMIT 1;"
 response = session.execute(stmt)
 response_list = []
 for val in response:
   response_list.append(val)
   jsonresponse = [{"Block": x.block, "Date": "{:%m/%d/%Y}".format(x.datetime), 
                  "Time": "{:%H:%M:%S}".format(x.datetime),
                  "Count": x.count} for x in response_list]
 return render_template("CarTakerBlocks.html", output=jsonresponse)

