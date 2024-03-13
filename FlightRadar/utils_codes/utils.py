
import os
import time
import json
import datetime

import numpy as np
import pandas as pd

from FlightRadar24 import FlightRadar24API



def print_flight_info(flight):
    print('id:',                        flight.id)
    print('icao_24bit:',                flight.icao_24bit)
    print('heading:',                   flight.heading)
    print('altitude:',                  flight.altitude)
    print('ground_speed:',              flight.ground_speed)
    print('squawk:',                    flight.squawk)
    print('aircraft_code',              flight.aircraft_code)
    print('registration:',              flight.registration)
    print('time:',                      flight.time)
    print('origin_airport_iata:',       flight.origin_airport_iata)
    print('destination_airport_iata:',  flight.destination_airport_iata)
    print('number:',                    flight.number)
    print('airline_iata:',              flight.airline_iata)
    print('on_ground:',                 flight.on_ground)
    print('vertical_speed:',            flight.vertical_speed)
    print('callsign:',                  flight.callsign)
    print('callsign:',                  flight.airline_icao)
# -----------------------------------------------------------
def access_flight_brief_info(flight):
    info = {
        'id'                        : flight.id,
        'icao_24bit'                : flight.icao_24bit,
        'heading'                   : flight.heading,
        'altitude'                  : flight.altitude,
        'ground_speed'              : flight.ground_speed,
        'squawk'                    : flight.squawk,
        'aircraft_code'             : flight.aircraft_code,
        'registration'              : flight.registration,
        'time'                      : flight.time,
        'origin_airport_iata'       : flight.origin_airport_iata,
        'destination_airport_iata'  : flight.destination_airport_iata,
        'number'                    : flight.number,
        'airline_iata'              : flight.airline_iata,
        'on_ground'                 : flight.on_ground,
        'vertical_speed'            : flight.vertical_speed,
        'callsign'                  : flight.callsign,
        'airline_icao'              : flight.airline_icao
    }

    return info
# -----------------------------------------------------------
def access_brief_flight_by_airline(airline_li, time_sleep=0.1):

    data = []

    for airline in airline_li:

        # access flights by airline ICAO code
        time.sleep(time_sleep)
        flight_li = FlightRadar24API().get_flights(airline)

        print('Airline ICAO code:', airline, '; Number of airlines:', len(flight_li))

        for flight in flight_li:
            flight_info = access_flight_brief_info(flight)

            data.append(flight_info)

    data = pd.DataFrame(data)

    data = data.sort_values(by=['airline_icao', 'callsign'])

    return data
# -----------------------------------------------------------
def save_brief_fight_data(data):

    dt = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    dt_day, dt_hour = dt.split(' ')
    print(dt_day, dt_hour)

    save_folder = os.path.join('data', dt_day, dt_hour[:2])
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    save_path = os.path.join(save_folder, 'airline_cn_{0}.csv'.format(dt))
    data.to_csv(save_path, index=False)
    return None
# -----------------------------------------------------------
def access_detail_flight_by_airline(airline_li, time_sleep=0.1):

    dt = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    dt_day, dt_hour = dt.split(' ')

    for airline in airline_li:

        # access flights by airline ICAO code
        time.sleep(time_sleep)
        fr_api = FlightRadar24API()
        flight_li = fr_api.get_flights(airline)

        print('Airline ICAO code:', airline, '; Number of airlines:', len(flight_li))

        for flight in flight_li:
            time.sleep(time_sleep)
            flight_info = fr_api.get_flight_details(flight)

            save_path = os.path.join('data', dt_day, dt_hour[:2],
                                     'airflight_{0}_{1}.json'.format(flight.callsign, datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')))
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(flight_info, f, ensure_ascii=False, indent=4)

    return None
# -----------------------------------------------------------