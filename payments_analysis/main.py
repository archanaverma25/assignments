from read_data import ReadCSV
from queries import DBQuery
from pprint import pprint
from pymongo import MongoClient

database_name = "business_data"
collection_name = "resturant_data"
collection_cursor = MongoClient()[database_name][collection_name]
file_path = ["dinning_places_open_hrs_1.csv","dinning_places_open_hrs_2.csv"]

collection_cursor.drop()

data = ReadCSV()
query = DBQuery(collection_cursor)

def main():
    for file in file_path:
        data.read_resturant_data(file,collection_cursor)
    name = ['Sudachi','Hanuri','Bamboo Restaurant',"Naan 'N' Curry",'Viva Pizza Restaurant']
    pprint(query.query1("Mon"))
    pprint(query.query2("Mon","11:00AM"))
    pprint(query.query3(name))



if __name__ == '__main__':
    main()
