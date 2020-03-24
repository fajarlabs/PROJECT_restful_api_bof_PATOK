from starlette.middleware.cors import CORSMiddleware
import json
import uvicorn
from pydantic import BaseModel
import secrets
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import psycopg2
import sys
import configparser

try :
	config = configparser.ConfigParser()
	config.read('setting.ini')
	# DATABASE ACCOUNT
	PG_USERNAME = config['DATABASE']['username']
	PG_PASSWORD = config['DATABASE']['password']
	PG_PORTNAME = config['DATABASE']['portname']
	PG_HOSTNAME = config['DATABASE']['hostname']
	PG_DATABASE = config['DATABASE']['database']
except Exception as e :
	print(e)
	sys.exit()

app = FastAPI()
security = HTTPBasic()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_userpass(username, password):
	stu_records = []
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('SELECT username, password, is_active, is_expired, expired_date FROM "SANS_API" WHERE \
			username = \'%s\' AND password = \'%s\' ' % (username, password))
		stu_records = cursor.fetchall() 
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_records

def get_stu_message_detail(offset=0, limit=10):
	stu_records = []
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('SELECT stu_id, latitude, longitude, msg_type_1, subtype, msg_type_2,\
			message_type, umn, battery, gps_valid, miss_contact_1, miss_contact_2,\
			gps_fail_count, battery_contact_status, motion, fix_confidence, tx_perburst,\
			gps_fault, transmitter_fault, scheduller_fault, min_interval, max_interval,\
			gps_mean_search_time, gps_fail_count_2, transmition_count, accumulate_contact_1,\
			accumulate_contact_2, accumulate_vibration, contact_1_count, contact_2_count, \
			esn, unixTime, gps, payload \
			FROM "SANS_STU_MESSAGE_DETAIL" INNER JOIN "SANS_STU_MESSAGE" ON id = stu_id \
			OFFSET %s LIMIT %s ' % (offset, limit))
		stu_records = cursor.fetchall() 
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_records

class ItemRequest(BaseModel):
    limit: int = 0
    offset: int = 0

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
	return {"data": data}

if __name__ == "__main__":
	uvicorn.run("restful_api:app",host="0.0.0.0", port=8081, reload=True, access_log=False)
