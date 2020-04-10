from starlette.middleware.cors import CORSMiddleware
import json
import uvicorn
from pydantic import BaseModel
import secrets
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import sys
from db import *
from calculator import *
from datetime import datetime

app = FastAPI()
security = HTTPBasic()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ItemRequest(BaseModel):
    limit: int = 0
    offset: int = 0
    is_read: bool = False

class DeviceRequest(BaseModel):
    esn: str = ''

class RangeRequest(BaseModel):
    start_date: str = ''
    end_date: str = ''

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
	correct_username = False
	correct_password = False
	rows = get_userpass(credentials.username, credentials.password)
	if len(rows) > 0 :
		for row in rows :
			# (username, password, is_active, is_expired, expired_date)
			# ('sansapi', 'sansapi2020', True, False, None)
			correct_username = secrets.compare_digest(credentials.username, row[0])
			correct_password = secrets.compare_digest(credentials.password, row[1])
	if not (correct_username and correct_password):
		raise HTTPException(
			status_code=status.HTTP_401_UNAUTHORIZED,
			detail="Incorrect username or password",
			headers={"WWW-Authenticate": "Basic"},
		)
	return credentials.username

@app.get("/")
def read_root():
    return {"status": "ok","description": "Worker API is active"}

@app.put("/consume/last_position")
def calculate_position(item: DeviceRequest, username: str = Depends(get_current_username)):
	data = []
	if item.esn == '':
		for esn in get_esn_all():
			ls = get_coordinate_detail(esn[0], 0, 2)
			distance = countDistanceFromLatLon(ls[1][0], ls[1][1], ls[0][0], ls[0][1])
			# 2020-03-25 01:03:54.994591
			velocity = calcVelocity(distance, ls[1][2], ls[0][2])
			print(type(velocity))
			data.append({ 
				"esn" : esn[0], 
				"start_timestamp" : ls[1][2],
				"last_timestamp" : ls[0][2],
				"start_latitude" : ls[1][0],
				"start_longitude" : ls[1][1],
				"end_latitude" : ls[0][0],
				"end_longitude" : ls[0][1],
				"distance" : round(distance*1000,2),
				"velocity" : velocity
			})
	if item.esn != '' :
		ls = get_coordinate_detail(esn[0], 0, 2)
		distance = countDistanceFromLatLon(ls[1][0], ls[1][1], ls[0][0], ls[0][1])
		velocity = calcVelocity(distance, ls[1][2],ls[0][2])
		data.append({ 
			"esn" : esn[0], 
			"start_timestamp" : ls[1][2],
			"last_timestamp" : ls[0][2],
			"start_latitude" : ls[1][0],
			"start_longitude" : ls[1][1],
			"end_latitude" : ls[0][0],
			"end_longitude" : ls[0][1],
			"distance" : round(distance*1000,2),
			"velocity" : velocity
		})
	return { "data": data }

@app.put("/update/is_read")
def read_current_user(item: RangeRequest, username: str = Depends(get_current_username)):
	is_read = flag_is_read(item.start_date, item.end_date)
	description = "Writing data is failed"
	if is_read == True :
		description = "Writing data is success"
	return {"status": is_read,"description": description}

@app.put("/consume/all")
def read_current_user(item: ItemRequest, username: str = Depends(get_current_username)):
	records = get_stu_message_detail(item.offset, item.limit)
	if len(records) > 0 :
		data = []
		for row in records :
			data.append({
				'stu_id' : row[0],
				'latitude' : row[1],
				'longitude' : row[2],
				'msg_type_1' : row[3],
				'subtype' : row[4],
				'msg_type_2' : row[5],
				'message_type' : row[6],
				'umn' : row[7],
				'battery' : row[8],
				'gps_valid' : row[9],
				'miss_contact_1' : row[10],
				'miss_contact_2' : row[11],
				'gps_fail_count' : row[12],
				'battery_contact_status' : row[13],
				'motion' : row[14],
				'fix_confidence' : row[15],
				'tx_perburst' : row[16],
				'gps_fault' : row[17],
				'transmitter_fault' : row[18],
				'scheduller_fault' : row[19],
				'min_interval' : row[20],
				'max_interval' : row[21],
				'gps_mean_search_time' : row[22],
				'gps_fail_count_2' : row[23],
				'transmition_count' : row[24],
				'accumulate_contact_1' : row[25],
				'accumulate_contact_2' : row[26],
				'accumulate_vibration' : row[27],
				'contact_1_count' : row[28],
				'contact_2_count' : row[29],
				'esn' : row[30], 
				'unixTime' : row[31], 
				'gps' : row[32], 
				'payload' : row[33]
			})
	return {"data": data, "total_all_data": get_total_stu_message_detail() }

if __name__ == "__main__":
	uvicorn.run("restful_api:app",host="0.0.0.0", port=8081, reload=True, access_log=False)