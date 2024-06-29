import datetime
from dateutil.parser import parse

class DBQuery:
    def __init__(self, cursor):
        self.cursor = cursor
        self.default_date = datetime.datetime(2022, 1, 1)

    def get_business_hours(self, resturant_name):
        """
        Module that fetches data against resturant name
        """
        result = []
        query = self.cursor.find({'resturant_name': resturant_name}).sort([('day_number', 1), ('opening_hour', 1)]) #use timelimt or expand timelimit
        temp = None
        for idx, entry in enumerate(query):
            week_day = entry["day_of_week"]
            opening_hour = entry["opening_hour"].time()
            closing_hour = entry['closing_hour'].time()
            overflow = entry['overflow']
            if idx == 0 and overflow:
                temp = closing_hour.strftime("%I:%M %p")
            elif overflow and result:
                result[-1][2] = closing_hour.strftime("%I:%M %p")
            else:
                result.append([week_day, opening_hour.strftime("%I:%M %p"), closing_hour.strftime("%I:%M %p")])

        query.close() # closing find cursor.

        if temp: # Handle case in which Sunday overflow to Monday
            result[-1][2] = temp

        return result

    def query1(self, day:str) -> dict:
        #Query1: Given a day of the week, all places open that day.
        # Idea is to query for the week day and for which overflow is False
        # Follow-up how will be include national holidays??
        result = self.cursor.distinct('resturant_name', {'$and':[{'day_of_week': day}, {'overflow': False}]})
        return {"result": result}

    def query2(self, day:str, _time:str) -> dict:
        #Query2: Given a day and time, find all the resturant open in that day and hour
        parsed_time = parse(_time, default=self.default_date)

        result = self.cursor.distinct('resturant_name', {'$and': [\
                                                        {'day_of_week': day}, \
                                                        {'opening_hour': {'$lte': parsed_time}}, \
                                                            {'closing_hour': {'$gt': parsed_time}}
                                                        ]})
        return {'result': result}

    def query3(self, name: list) -> dict:
        #Query3: Given a resturant name or list of names return the day and opening hours.
        name = name if isinstance(name, list) else [name]
        result = {}
        for resturant_name in name:
            result[resturant_name] = self.get_business_hours(resturant_name)

        return result
