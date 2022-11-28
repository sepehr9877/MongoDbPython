import json
import os.path
from pprint import pprint

from pymongo import MongoClient

password="sepehr12345"
client=MongoClient(f"mongodb+srv://sepehr:{password}@sepehr.ypt1dzn.mongodb.net/?retryWrites=true&w=majority")
MyDatabase=client["NewDataBase"]
BoxOfficeDb=client["BoxOffice"]
MovieCollection=MyDatabase["Movies"]
BoxOffice_Collection=BoxOfficeDb["BoxOfficeDataSet"]
def inserting_moviefile():
    with open('tv-shows.json') as file:
        file_data=json.load(file)
    if isinstance(file_data,list):
        MovieCollection.insert_many(file_data)
def inserting_boxofficefile():
    with open('boxoffice.json') as newfile:
        file_data=json.load(newfile)
    if isinstance(file_data,list):
        BoxOffice_Collection.insert_many(file_data)
###Search All Movies that Have a rating higher than 9.2 and runtime lower than 100 minutes
def first():
    find=BoxOffice_Collection.find({"meta.rating":{"$gt":9.2},"meta.runtime":{"$lt":100}})
    for doc in find:
        pprint(doc)
def second():
    find=BoxOffice_Collection.find({"$or":[{"genre":"drama"},{"genre":"action"}]})
    for doc in find:
        pprint(doc)
#number of vistors exceeds the Excpected Vistors
def three():
    find=BoxOffice_Collection.find({"$expr":{"$gt":["$visitors","$expectedVisitors"]}})
    for doc in find:
        pprint(doc)
#Subtract the Number of Visitors and ExpectedVistors if Vistors Exceed otherwise give visotrs, Where Genre is Action
def four(new_item):
        for item in new_item:
            BoxOffice_Collection.update_many({},
                                             {
                                                 "$set":{"Differences":f"{item['Differences']}",
                                                         "Range":f"{item['Range']}"}
                                             })
##aggregation
def five():
    items=BoxOffice_Collection.aggregate(
        [
            {"$set":{"Differences":{"$subtract":["$expectedVisitors","$visitors"]}}},
            {"$set":{"Range":{
                "$cond":{
                "if":{"$gt":["$visitors","$expectedVisitors"]}
                                       ,
                "then":"Negative",
                "else":"Positive"

                              }
                     }
             }
            }
        ]
    )
    dic_new_item={}
    list_new_item=[]
    for item in items:
        dic_new_item["Range"]=item["Range"]
        dic_new_item["Differences"]=item["Differences"]
        list_new_item.append(dic_new_item)
    four(list_new_item)

#$match and $and Differences
def insert_embededArrays():
    dataitemone={"cast":[
        {
            "actor":"brad ",
            "Salery":10000,
            "gender":"Male"
        },
        {
            "actor": "Matt ",
            "Salery": 20000,
            "gender": "Male"
        },
        {
            "actor": "Tom",
            "Salery": 30000,
            "gender": "Male"
        }
    ]
     }
    dataitemtwo={
        "cast":[{
            "actor":"brad ",
            "Salery":10000,
            "gender":"Male"
        },
        {
            "actor": "Matt ",
            "Salery": 20000,
            "gender": "Male"
        },
        {
            "actor": "Jolie",
            "Salery": 130000,
            "gender": "Female"
        }
    ]}
    MovieCollection.update_one({"id":1},
                               {"$set":{"Cast":dataitemone['cast']}})
    MovieCollection.update_one({"id":2},
                               {"$set":{"Cast":dataitemtwo['cast']}})
def six():
    newitems=MovieCollection.find({"$and": [
        {"Cast.gender": "Male"}, {"Cast.Salery": {"$gt": 20000}}]})
    new_eleMatch=MovieCollection.find(
        {"Cast":{
            "$elemMatch":{"Salery":{"$gte":120000},"gender":"Female"}}
         }
    )
    for item in new_eleMatch:
        pprint(item)
#project operator
def eight():
    newel=MovieCollection.find({},
        {
                "Cast.Salery":1
        }
    )
    agg_col=MovieCollection.aggregate([
        {
            "$project":{
                "Cast.Salery":1,
                "name":1
            }
        }
    ])
    for i in agg_col:
        pprint(i)
