from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson.json_util import dumps,loads
from flask.ext.cors import CORS

app = Flask(__name__)
CORS(app)

def getconnection():
    client = MongoClient('mongodb://<User>:<Password>@candidate.2.mongolayer.com:10358,candidate.3.mongolayer.com:10315/solaire-217?replicaSet=set-5581c0387aaf5eb1cc000d93')
    db = client.get_database('solaire-217')
    return db


def getdata():
    db = getconnection()

    equipment = db.gym.aggregate([{"$match": {"thing": {"$nin": ["SolaireHub"]}}},
                                  {"$group": {"_id": {
                                                       "weekday": {"$dayOfWeek": [{"$subtract": ["$created", 14400000]}]},
                                                       "hourOfday": {"$hour": [{"$subtract": ["$created", 14400000]}]},
                                                      },
                                                       "Activity": {"$sum": "$payload.pr"}}
                                  },
                                  {"$sort": {"Activity": 1}},
                                  {"$project": {
                                      "_id": 0,
                                      "day": "$_id.weekday",
                                      "hour": "$_id.hourOfday",
                                      "value": {"$divide": ["$Activity", 10]}
                                  }
                                  },
                                  {
                                      "$sort": {
                                          "WD": 1,
                                          "HD": 1,
                                          "MD": 1,

                                      }
                                  }])
    return equipment

def gethubdata():
    db = getconnection()
    hubparameters = db.gym.aggregate([{"$match": {"thing":  "SolaireHub"}},
                                       {"$group": {"_id": {"monthcut": {"$month": [{"$subtract": ["$created", 14400000]}]},
                                                           "weekday": {"$dayOfWeek": [{"$subtract": ["$created", 14400000]}]},
                                                           "hourOfday": {"$hour": [{"$subtract": ["$created", 14400000]}]},
                                        },
                                       "AvgTemperature": {"$avg": "$payload.hubTemp"},
                                       "MaxTemperature": {"$max": "$payload.hubTemp"},
                                       "MinTemperature": {"$min": "$payload.hubTemp"},
                                       "AvgHumidity": {"$avg": "$payload.hubHum"},
                                       "MaxHumidity": {"$max": "$payload.hubHum"},
                                       "MinHumidity": {"$min": "$payload.hubHum"}
                                }},
                                       {"$sort": {"AvgTemperature": 1}},
                                        {"$project": {"_id": 0,
                                              "Mcut": "$_id.monthcut",
                                                "WD": "$_id.weekday",
                                                "HD": "$_id.hourOfday",
                                           "MinTemp": "$MinTemperature",
                                           "AvgTemp": "$AvgTemperature",
                                           "MaxTemp": "$MaxTemperature",
                                            "MinHum": "$MinHumidity",
                                            "AvgHum": "$AvgHumidity",
                                            "MaxHum": "$MaxHumidity"}
                                         },
                                         {"$match": {"Mcut": 4}},
                                          {"$sort": {"Mcut": 1, "WD": 1, "HD": 1}}
                                ])
    return hubparameters


@app.route('/api/v1.0/sensors', methods=['GET'])

def getEquipment():

     equipment = dumps(getdata())
     return (equipment)

@app.route('/api/v1.0/hub',methods=['GET'])

def gethuball():

     hubparam = dumps(gethubdata())
     return (hubparam)


if __name__ == '__main__':
    app.run()
