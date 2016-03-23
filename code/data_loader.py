__author__ = 'jaime'

import json
import csv
import psycopg2, psycopg2.extras

''' DATA DECRIPTION ========================================================'''
# Source of my data: Bureau of Transportation Statistics
# http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236

# The data used consists of all flights that departed from San Francisco
# during 12 months (October 2014 to September 2015)
# The websites outputs a CSV file per month. The CSVs are merged in the
# terminal, and transformed into JSON format through the following
# script

# DSN location of the AWS - RDS instance
# ADD YOURS HERE:
DB_DSN = "host=XX.rds.amazonaws.com " \
        "dbname=XX user=XX password=XX"

INPUT_DATA_1 = 'merged.csv'
INPUT_DATA_2 = 'merged.json'
CARRIERS_LIST = 'L_UNIQUE_CARRIERS.csv'


''' CSV TO JSON FUNCTION =================================================='''
def csv_to_json(filename1, filename2):
    """ Converts csv file to json
    :param filename1 (csv), filename2 (json)
    :return:"""
    csvfile = open(filename1, 'r')
    jsonfile = open(filename2, 'w')
    reader = csv.DictReader(csvfile)
    for row in reader:
        json.dump(row, jsonfile)
        jsonfile.write('\n')
    csvfile.close()
    jsonfile.close()

''' DICT_FIELDS FUNCTION ==================================================='''
def dict_fields(flight, fields):
    """ Inserts from a dictionary to a new dictionary using a list of keys
    :param flight (a dictionary), fields (a list of keys)
    :return a dicitionary """
    dict_f = dict()
    for i in fields:
        dict_f[i] = flight[i]
    return dict_f

''' GET_CAUSE_DELAY FUNCTION================================================'''
def get_cause_delay(flight):
    """ Convert multiple variables into one with a string with the delay cause
    :param flight (a dictionary)
    :return a string with the cause of delay """
    fields = ['CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY','SECURITY_DELAY',
              'LATE_AIRCRAFT_DELAY']
    try:
        for i in fields:
            if flight[i] != '':
                if float(flight[i]) > 0:
                    return i
            else:
                return '-'
    except:
        return '-'

''' GET_CARRIER FUNCTION===================================================='''
def get_carrier(filename):
    """ Convert a csv with a list of carrier codes and names to a dictionary
    :param filename of CSV with list of unique carriers (name and string)
    :return a dictionary with code (key) and name (value) """
    carriers = dict()
    reader = csv.DictReader(open(filename))
    for row in reader:
        carriers[row['Code']] = row['Description']
    return carriers

''' CONVERT_NA FUNCTION===================================================='''
def convert_NA(input):
    """ Convert NA values into 0, and rest into integer
    :param input (a delay value)
    :return an integer """
    try:
        if input == '':
            return 0
        else:
            return int(float(input))
    except:
        return 0

''' JSON_TO_TUPLE FUNCTION =================================================='''
def json_to_tuple(filename, carriers):
    """ Convert from JSON to tuple to insert in the database
    :param name of the JSON file
    :return a list of tuples """
    data = []
    jsonfile = open(filename, 'r')
    fields_dt = ['YEAR', 'MONTH', 'DAY_OF_WEEK', 'FL_DATE', 'CRS_DEP_TIME']
    fields_route = ['ORIGIN_CITY_NAME', 'DEST_CITY_NAME', 'AIR_TIME', 'DISTANCE']
    fields_cancel = ['CANCELLED', 'CANCELLATION_CODE', 'DIVERTED']
    for row in jsonfile:
        row = json.loads(row)
        # If the departing city is San Francisco
        if row['ORIGIN_CITY_NAME'] == 'San Francisco, CA':
            # Build 3 dictionaries with dates, routes and cancel information
            fdates = dict_fields(row, fields_dt)
            froute = dict_fields(row, fields_route)
            fcancel = dict_fields(row, fields_cancel)
            # Lookup the carrier code and replace with its name
            try:
                carrier = carriers[row['UNIQUE_CARRIER']]
            except:
                carrier = 'NA'
            # Convert delay columns to one string
            delay_cause = get_cause_delay(row)
            # Gather all in a tuple with JSONs
            this_tuple = (row['FL_NUM'], carrier,
                json.dumps(fdates), json.dumps(froute), json.dumps(fcancel),
                convert_NA(row['DEP_DELAY_NEW']), convert_NA(row['ARR_DELAY_NEW']),
                          delay_cause)
            data.append(this_tuple)
    jsonfile.close()
    return data

''' DROP_TABLE FUNCTION ====================================================='''
def drop_table():
    """ drops the table 'flights' if it exists
    :return: """
    try:
        sql = "DROP TABLE IF EXISTS flights;"
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

''' CREATE_TABLE FUNCTION ==================================================='''
def create_table():
    """ creates a postgres table 'flights' with columns
        f_number INT,
        carrier TEXT,
        dates JSON,
        route JSON,
        cancel JSON,
        dep_delay FLOAT
        arr_delay FLOAT
        delay_cause TEXT
    :return: """
    try:
        sql = "CREATE TABLE flights (" \
            "f_number INT, carrier TEXT, dates JSON, " \
            "route JSON, cancel JSON, dep_delay FLOAT, arr_delay FLOAT, delay_cause TEXT);"
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

''' INSERT_DATA FUNCTION ===================================================='''
def insert_data(data):
    """ Inserts the data
    :param data: a list of tuples
    :return: """
    try:
        sql = "INSERT INTO flights VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()


''' MAIN SCRIPT ============================================================='''
if __name__ == '__main__':

    print "******* Preparing data **********"
    csv_to_json(INPUT_DATA_1, INPUT_DATA_2)
    dict_carriers = get_carrier(CARRIERS_LIST)
    data = json_to_tuple(INPUT_DATA_2, dict_carriers)

    print "******* Dropping table **********"
    drop_table()

    print "******* Creating table **********"
    create_table()

    print "******* Inserting data into table **********"
    insert_data(data)

''' ========================================================================'''
