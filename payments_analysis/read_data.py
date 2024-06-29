import os
import csv
import re
from dateutil.parser import parse
import datetime
import pymongo
from pymongo import MongoClient
from pprint import pprint

#Optimization: Can keep opening and clossing hours in a list of seven elements
class ReadCSV:
    def __init__(self):
        self.default_date = datetime.datetime(2022, 1, 1)
        self.next_date = self.default_date + datetime.timedelta(days=1)

        self.day_to_number = {
            "Mon": 0,
            "Tue": 1, # Assumption: As Tue is not given in examples
            "Wed": 2,
            "Thu": 3,
            "Fri": 4,
            "Sat": 5,
            "Sun": 6
        }

        self.number_to_day = {value:key for key, value in self.day_to_number.items()}
        self.split_regex = re.compile(r', (Mon|Tue|Wed|Thu|Fri|Sat|Sun)( |-)', re.I|re.M)


    def get_first_digit_index(self, s:str) -> int:
        """
        Assumptions:
        1) String never start with an integer
        2) The generic format of the string will be days followed by timing
        """
        for idx, ele in enumerate(s):
            if ele in '0123456789':
                return idx
        return -1

    def get_opening_closing_hrs(self, s:str) -> tuple:
        """
        Input s<str>:
        An interval of time will be given seperated by Hypen '-'
        Sample: 11:30 am - 9 pm
        """
        opening_hour, closing_hour = s.split('-')

        # parsing the time and return datetime object
        opening_hour, closing_hour = parse(opening_hour, \
                                        default=self.default_date), \
                                    parse(closing_hour, \
                                        default=self.default_date)

        return opening_hour, closing_hour

    def get_opening_days(self, s:str) -> list:
        """
        Input s<str>:
        1) Continious days will be sepearted by Hypen having start and end day
        2) Extra similar days will be given with commma
        Sample: Mon-Thu, Sun
        """

        days = [0 for i in range(7)]
        for ele in s.split(','):
            if '-' in ele:
                start, end = ele.split('-')
                for i in range(self.day_to_number[start.strip()], self.day_to_number[end.strip()]+1):
                    days[i] = 1
            else:
                days[self.day_to_number[ele.strip()]] = 1

        return [self.number_to_day[idx] for idx, val in enumerate(days) if val]

    def parse_timings(self, timings:str) -> dict:
        """ Module that convert the timing information given as sentence into
        parsed and structured information, keeping the closing and opening
        timing intact.
        Eg: Mon-Thu 11 am - 11 pm  / Fri-Sat 11 am - 12:30 am  / Sun 10 am - 11 pm
        """
        different_timings = {}

        for common_timings in timings.split('/'):
            split_idx = self.get_first_digit_index(common_timings)
            days, timing = common_timings[:split_idx], common_timings[split_idx:]
            days = self.get_opening_days(days)
            timing = self.get_opening_closing_hrs(timing)
            for day in days:
                different_timings[day] = timing

        return different_timings


    def separate_resturant(self, entry:str) -> tuple:
        """
        Assumption:
        ',' is use to separate timing and resturant name
        """
        temp = re.search(self.split_regex, entry)
        split_idx = temp.span()[0]
        name, timing = entry[:split_idx], entry[split_idx:].strip(',| ')
        return name, timing

    def document_format(self, name:str, day_of_week:str, opening_hour:datetime.datetime, \
                     closing_hour:datetime.datetime, overflow:bool) -> None:
        """
        Format of individual document that will be inserted in database
        """
        return {
            "resturant_name": name,
            "day_of_week": day_of_week,
            "day_number": self.day_to_number[day_of_week], # to make sure that we can sort data based on it. # duplicate column
            "opening_hour": opening_hour, #.timestamp(),
            "closing_hour":  closing_hour, #.timestamp(),
            "overflow": overflow
        }

    def parse_row(self, name:str, timings:str) -> None:
        """
        timings = Mon-Thu 11 am - 11 pm  / Fri-Sat 11 am - 12:30 am  / Sun 10 am - 11 pm
        parsed_timings = "iso (11 am) , iso (11pm)
        """
        parsed_timings = self.parse_timings(timings)

        data_to_be_inserted = []
        for ele in parsed_timings:
            opening_hour = parsed_timings[ele][0]
            closing_hour = parsed_timings[ele][1]
            if opening_hour < closing_hour:
                data_to_be_inserted.append(self.document_format(name, ele, opening_hour, closing_hour, False))
            else: # case in which 00:00Hrs is included
                #print(f'Day change case involved in {name} {timings}') #\n {ele} - {opening_hour} - {closing_hour}')
                data_to_be_inserted.append\
                (self.document_format(name, ele, opening_hour, parse("00:00 am", default=self.next_date), False))

                next_day = self.number_to_day[((self.day_to_number[ele] + 1) % 7)] # to handle overflow of time
                if parse("00:00 am", default=self.default_date) != closing_hour:
                    data_to_be_inserted.append\
                    (self.document_format(name, next_day, parse("00:00 am", default=self.default_date), closing_hour, True))

        return data_to_be_inserted

    def read_resturant_data(self, file_path:str, cursor:MongoClient) -> None:
        """
        Main function that read and correct resturant data in csv file
        """

        if not os.path.exists(file_path):
            print(f'File do not exits at {file_path}')
            return

        if not os.access(file_path, os.R_OK):
            print(f'File at {file_path} is not accessible. Please grant permission')
            return

        if not file_path.endswith('.csv'):
            print(f'File format should be CSV. Please provide a CSV file!')
            return

        correct = 0
        wrong = 0
        other = 0
        with open(file_path) as fp:
            for line_no, row in enumerate(csv.reader(fp)):
                if len(row) == 2:
                    correct += 1
                    data = self.parse_row(row[0], row[1])
                elif len(row) == 1:
                    try:
                        resturant, timings = self.separate_resturant(row[0])
                        data = self.parse_row(resturant, timings)
                        wrong += 1
                    except Exception as e:
                        print(f'Error {e} while correcting line no. {line_no}. Skipping for now!')
                        other += 1
                        continue
                else:
                    print(f'Check line no. {line_no}. Skipping for now!')
                    other += 1
                    continue

                if data:
                    cursor.insert_many(data)

        if correct:
            print(f'Successfully processed {correct} rows')

        if wrong:
            print(f'Successfully corrected {wrong} rows')

        if other:
            print(f'Skipped {other} rows. Check Log Files for line nos.') #Logger Implementation Pending
