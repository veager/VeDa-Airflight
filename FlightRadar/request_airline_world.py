import json
import os
import time
import datetime

import numpy as np
import pandas as pd

import utils_codes.utils as uc

from apscheduler.schedulers.blocking import BlockingScheduler


path = 'airline_iaca_code.txt'
with open(path, 'r') as f:
    airline_iaca_code = f.read()
    airline_iaca_code = airline_iaca_code.split('\n')
# airline_iaca_code


def request_job():
	# Access flight data
	data = uc.access_brief_flight_by_airline(airline_iaca_code, time_sleep=0.2)
	# Save flight data
	uc.save_brief_fight_data(data, save_folder='data/airline_workwide')

	# uc.access_detail_flight_by_airline(airline_icao)


def scheduler_job():
    # BlockingScheduler
    scheduler = BlockingScheduler()
    # 1 hour
    scheduler.add_job(request_job, 'interval', hours=1, id='request_job')
    
    scheduler.start()



print('Start...')

request_job()
scheduler_job()
