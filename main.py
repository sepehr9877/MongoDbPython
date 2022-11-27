import json
import os.path
from pprint import pprint

from pymongo import MongoClient

password="sepehr12345"
client=MongoClient(f"mongodb+srv://sepehr:{password}@sepehr.ypt1dzn.mongodb.net/?retryWrites=true&w=majority")
MyDatabase=client["NewDataBase"]
BoxOfficeDb=client["BoxOffice"]
MyCollection=MyDatabase["Movies"]
BoxOffice_Collection=BoxOfficeDb["BoxOfficeDataSet"]
def inserting_moviefile():
    with open('tv-shows.json') as file:
        file_data=json.load(file)
    if isinstance(file_data,list):
        MyCollection.insert_many(file_data)
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
def four():
    find=BoxOffice_Collection.update_many({},
        {"$set":{"Differences":
            {"$expr":
                 {"$subtract":["$visitors","$expectedVisitors"]
        }}}}
    )
four()
# BoxOffice_Collection.update_many({},
#                                  {
#                                      "$unset":{"Differences":1}
#                                  })