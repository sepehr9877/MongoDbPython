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
UserDb=client["User"]
User_Collection=UserDb["UserCollection"]
def insert_file_to_Db():
     with open('users.json') as file:
         file_data=json.load(file)
     if isinstance(file_data,list):
        User_Collection.insert_many(file_data)
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
#Using Upsert
def nine():
    creat_sport_collection=UserDb["Sport"]
    creat_sport_collection.update_many({},{"$set":{"title":"Soccer","requiresTeam":True}},upsert=True)
    creat_sport_collection.update_many({"title":"FootBall"},{"$set":{"requiresTeam":True}},upsert=True)
    creat_sport_collection.update_many({"title":"HorseRiding"},{"$set":{"requiresTeam":False}},upsert=True)
    creat_sport_collection.update_many({"title":"Running"},{"$set":{"requiresTeam":False}},upsert=True)
    creat_sport_collection.update_many({"requiresTeam":True},{"$set":{"minPlayer":11}},upsert=True)
    creat_sport_collection.update_many({"requiresTeam":True},{"$inc":{"minPlayer":10}})
#Modifying an Embeded Array
def ten():

    selected_ele=User_Collection.find({
        "hobbies":
            {"$elemMatch":{"title":"Sports","frequency":{"$gte":3}}}
    })
    updated_element=User_Collection.update_many({
       "hobbies":{"$elemMatch":{"title":"Sports","frequency":{"$gte":3}}}
       },
           {"$set":{"hobbies.$.highFrequency":True}}
       )
    for item in selected_ele:
        pprint(item)
def eleven():
    add_new_element=User_Collection.update_many(
        {"hobbies":{"$elemMatch":{"title":"Yoga","frequency":3}}},
        {"$set":{"hobbies.$[el].Coach":"Sepehr"}},
        array_filters=[{"el.title":"Sports"}]
    )

#Add new Array into Hobbies
def towelve():
    insert_one_el=User_Collection.update_many(
        {"hobbies":{"$elemMatch":{"title":"Cooking","frequency":5}}},
        {"$push":{"hobbies":{"title":"Meeting With Friends","frequency":2}}}
    )
#insert Multiple Array
def thirteen():
    insert_multilple_ar=User_Collection.update_many(
        {"hobbies.Coach":"Sepehr"},
        {"$push": {"hobbies":{"$each":[{"title":"Drinking Wine","frequency":1},
                                       {"title":"Hiking","frequency":3}
                                       ],
                              "$sort":{"frequency":-1}
                              }
                   }
         }
    )
def fourteen():
    insert_el=User_Collection.update_many(
        {"hobbies":{"$elemMatch":{"title":"Cooking","frequency":5}}},
        {"$addToSet":{"hobbies":{"title":"Meeting With Friends","frequency":2}}}
    )
    #this Wont be inserted
def fiftheen():
    aggregate_el=User_Collection.aggregate([
        {"$unwind":"$hobbies"},
        {"$group":{"_id":{"name":"$name"},"Allhobbies":{"$push":"$hobbies"}}}
    ])
    add_toset=User_Collection.aggregate([
        {"$unwind":"$hobbies"},
        {"$group":{"_id":{"name":"$name"},"AllHobbies":{"$addToSet":"$hobbies"}}}
    ])
    #remove Duplicate Element
    for item in add_toset:
        pprint(item)

def sixteen():
    #You Have To Use accumulator in Group
    agg_el=BoxOffice_Collection.aggregate(
       [
           {"$unwind":"$meta"},
           {"$project":{"_id":1,"title":1,"meta":"$meta.rating","genre":{"$arrayElemAt":["$genre",1]}}},
        {"$sort":{"rating":1}},
        {"$group":{
            "_id":"$_id",
            "Movie_title":{"$first":"$title"},
            "IMDB_rating":{"$max":"$meta"},
            "Genre":{"$first":"$genre"}}
        },
        {"$sort":{"IMDB_rating":-1}}
        ]
    )
    for item in agg_el:
        pprint(item)




