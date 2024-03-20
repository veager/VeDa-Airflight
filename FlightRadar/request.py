import json
import os
import time
import datetime

import numpy as np
import pandas as pd

import utils_codes.utils as uc

from apscheduler.schedulers.blocking import BlockingScheduler


path = r'C:\zhouweifile\Github-Project\VeDa-Airport-Location\case01_extract_cn_airport\cn_airline.xlsx'
airline_cn = pd.read_excel(path, sheet_name='airline CN')
airline_icao = airline_cn['ICAO'].dropna().drop_duplicates().to_list()


def request_job():
	# Access flight data
	data = uc.access_brief_flight_by_airline(airline_icao)
	# Save flight data
	uc.save_brief_fight_data(data)

	# uc.access_detail_flight_by_airline(airline_icao)


def scheduler_job():
    # BlockingScheduler
    scheduler = BlockingScheduler()
    # 1 hour
    scheduler.add_job(request_job, 'interval', hours=1, id='request_job')
    
    scheduler.start()



print('Start...')

scheduler_job()
