__author__ = 'jaime'

from flask import Flask, jsonify
import psycopg2, psycopg2.extras

# DSN location of the AWS - RDS instance
# ADD YOURS HERE:
DB_DSN = "host=XX.rds.amazonaws.com " \
        "dbname=XX user=XX password=XX"

# IP - AWS EC2 instance
# ADD YOURS HERE:
IP = "XX.XX.XX.XX"

app = Flask(__name__)

''' HOME FUNCTION =========================================================='''
@app.route('/')
def default():
    """ Prints message in "home"
    :return: a dict with welcome message """
    output = dict()
    output['1. Message 1'] = 'Welcome to the test app!'
    output['2. Message 2'] = 'Analysis of domestic flights departing from San Francisco'
    output['3.1 Link 1'] = 'http://' + IP + ':5000/avg_delay/carrier'
    output['3.2 Link 2'] = 'http://' + IP + ':5000/avg_delay/destination'
    output['3.3 Link 3'] = 'http://' + IP + ':5000/avg_delay/dayofweek'
    output['3.4 Link 4'] = 'http://' + IP + ':5000/avg_delay/departure-hour'
    output['3.5 Link 5'] = 'http://' + IP + ':5000/perc_cancelled'
    output['3.6 Link 6'] = 'http://' + IP + ':5000/perc_delayed_carrier'
    output['3.7 Link 7'] = 'http://' + IP + ':5000/destination/Honolulu,%20HI'
    output['3.8 Link 8'] = 'http://' + IP + ':5000/problem_by_destination/Honolulu,%20HI'
    return jsonify(output)

''' GENERIC QUERY FUNCTIONS ================================================='''
def get_delay(sql, order):
    """ Generic function to query the database and return a dict with key =
    minutes
    :param: a string with a sql query, an order to store key and values
    :return: a dict of all key value pairs """
    output = dict()
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        for item in rs:
            output[round(item.values()[order[0]],2)] = item.values()[order[1]]
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
    return jsonify(output)

def get_percentage(sql, order):
    """ Generic function to query the database and return a dict with key =
    percentage
    :param: a string with a sql query, an order to store key and values
    :return: a dict of all key value pairs """
    output = dict()
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        for item in rs:
            output[str(round(100 * item.values()[order[0]],2)) + '%'] = item.values()[order[1]]
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
    return jsonify(output)

''' AVERAGE DELAY PER CARRIER ==============================================='''
@app.route('/avg_delay/carrier')
def get_delay_carrier():
    """ Gets average delay per carrier
    :return: a dict of all key value pairs with key = minutes and
    value = carrier """
    sql = '''SELECT carrier as airline, avg(arr_delay) as average_delay
             FROM flights GROUP BY 1 ORDER BY 2 DESC;'''
    return get_delay(sql, [0,1])

''' AVERAGE DELAY PER DESTINATION ==========================================='''
@app.route('/avg_delay/destination')
def get_delay_destination():
    """ Gets average delay per destination
    :return: a dict of all key value pairs with key = minutes and
     value = destination """
    sql = '''Select route->>'DEST_CITY_NAME', avg(arr_delay) FROM flights
            GROUP BY 1 ORDER BY 2 DESC;'''
    return get_delay(sql, [1,0])

''' AVERAGE DELAY PER DAY OF WEEK ==========================================='''
@app.route('/avg_delay/dayofweek')
def get_delay_day_of_week():
    """ Gets average delay per day of the week (1 - 7)
    :return: a dict of all key value pairs with key = minutes and
     value = day of the week """
    sql = '''Select dates->>'DAY_OF_WEEK', avg(arr_delay) FROM flights
            GROUP BY 1 ORDER BY 2 DESC;'''
    return get_delay(sql, [1,0])

''' AVERAGE DELAY PER HOUR OF DEPARTURE ====================================='''
@app.route('/avg_delay/departure-hour')
def get_delay_dep_hour():
    """ Gets average delay per hour of departure
    :return: a dict of all key value pairs with key = range of hours and
     value = average delay in minutes """
    sql = '''SELECT CASE WHEN dtime >=0 and dtime<400 then '0-4 AM'
        WHEN dtime >=400 and dtime<800 then '4-8 AM'
        WHEN dtime >=800 and dtime<1200 then '8-12 AM'
        WHEN dtime >=1200 and dtime<1600 then '12-4 PM'
        WHEN dtime >=1600 and dtime<2000 then '4-8 PM'
        ELSE '8-12 PM' END as time, avg(arr_delay) FROM
        (SELECT (dates->>'CRS_DEP_TIME')::int as dtime, arr_delay
        FROM flights) AS Q1 GROUP BY 1 ORDER BY 2 DESC;'''
    return get_delay(sql, [0,1])

''' PERCENTAGE OF FLIGHTS CANCELLED ========================================='''
@app.route('/perc_cancelled')
def get_perc_cancelled():
    """ Gets the percentage of flights cancelled by carrier
    :return: a dict of all key value pairs with key = percentage cancelled and
     value = carrier """
    sql = '''SELECT carrier, sum((cancel->>'CANCELLED')::float)::float /
    count(*) as perc_cancelled FROM flights GROUP BY 1 ORDER BY 2 DESC;'''
    return get_percentage(sql, [1,0])

''' PERCENTAGE OF FLIGHTS DELAYED WITH CARRIER CAUSE ========================'''
@app.route('/perc_delayed_carrier')
def get_perc_delayed_carrier():
    """ Gets the percentage of flights delayed more than 10 minutes by carrier
    when the delay cause is carrier-related
    :return: a dict of all key value pairs with key = percentage delayed and
     value = carrier """
    sql = '''SELECT carrier, sum(CASE WHEN arr_delay > 10 and
    (delay_cause = 'CARRIER_DELAY' OR delay_cause ='LATE_AIRCRAFT_DELAY')
    THEN 1 END)::float / count(*) FROM flights GROUP BY 1 ORDER BY 2 DESC;'''
    return get_percentage(sql, [0,1])

''' COMPARING AIRLINES BY DESTINATION ======================================='''
@app.route('/destination/<destination>')
def compare_carriers_by_destination(destination):
    """ Compares the airlines flying to the destination chose by the user
    :param: a string with the destination "city, ST" (eg. "New York, NY")
    :return: a dict of dicts where each dict, includes carrier and several
     metrics of the flights to the specific destination """
    out = dict()
    try:
        sql = '''SELECT carrier, count(*) as num_flights, avg(arr_delay) as
        avg_delay, max(arr_delay)::float/60 as max_delay, sum(CASE WHEN
        arr_delay > 10 and (delay_cause = 'CARRIER_DELAY' OR delay_cause
        ='LATE_AIRCRAFT_DELAY') THEN 1 END)::float / count(*) as
        perc_del_carrier, sum((cancel->>'CANCELLED')::float)::float / count(*)
        as perc_cancelled, sum((cancel->>'DIVERTED')::float)::float / count(*)
        as perc_diverted FROM flights
        WHERE route->>'DEST_CITY_NAME' = %s
        GROUP BY 1;'''
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (destination,))
        rs = cur.fetchall()
        for item in rs:
            print item
            temp = dict()
            temp['1.Total_flights                '] = item['num_flights']
            temp['2.Average_delay_minutes        '] = round(item['avg_delay'],2)
            temp['3.Maximum_delay_hours          '] = round(item['max_delay'],1)
            temp['4.Percentage_delayed_by_carrier'] = str(round(100 *
                item['perc_del_carrier'],2))+'%'
            temp['5.Percentage_cancelled         '] = str(round(100 *
                item['perc_cancelled'],2))+'%'
            temp['6.Percentage_diverted          '] = str(round(100 *
                item['perc_diverted'],2))+'%'
            out[item['carrier']] = temp
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
    return jsonify(out)

''' PERCENTAGE FLIGHTS WITH PROBLEMS BY DESTINATION ========================='''
@app.route('/problem_by_destination/<destination>')
def problem_by_destination(destination):
    """ Retrieves
    :param: a string with a destination "city, ST" (eg. "New York, NY")
    :return: a dict with key = percentage problematic and value = cause """
    out = dict()
    try:
        sql = '''SELECT distinct delay_cause, sum(CASE WHEN arr_delay > 10
        THEN 1 WHEN (cancel->>'CANCELLED')::float = 1 THEN 1
        WHEN (cancel->>'DIVERTED')::float = 1 THEN 1 END) OVER
        (PARTITION BY delay_cause)::float /  count(*) OVER () as perc_problem
        FROM flights WHERE route->>'DEST_CITY_NAME' = %s;'''
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (destination,))
        rs = cur.fetchall()
        format_cause = {'NAS_DELAY':'National Aviation System',
        'WEATHER_DELAY':'Weather', 'LATE_AIRCRAFT_DELAY':'Late Aircraft',
        'CARRIER_DELAY':'Carrier','SECURITY_DELAY':'Security','-':'Other'}
        for item in rs:
            out[str(round(100 * item['perc_problem'],2)) + '%'] = \
                format_cause[item['delay_cause']]
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
    return jsonify(out)


''' MAIN SCRIPT ============================================================='''
if __name__=='__main__':
    app.debug = True
    app.run(host = '0.0.0.0', port = 5000)

'''=========================================================================='''