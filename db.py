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

def get_stu_message_detail(offset=0, limit=10, is_read=False):
	stu_records = []
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('SELECT t1.stu_id, t1.latitude, t1.longitude, t1.msg_type_1, t1.subtype, t1.msg_type_2,\
			t1.message_type, t1.umn, t1.battery, t1.gps_valid, t1.miss_contact_1, t1.miss_contact_2,\
			t1.gps_fail_count, t1.battery_contact_status, t1.motion, t1.fix_confidence, t1.tx_perburst,\
			t1.gps_fault, t1.transmitter_fault, t1.scheduller_fault, t1.min_interval, t1.max_interval,\
			t1.gps_mean_search_time, t1.gps_fail_count_2, t1.transmition_count, t1.accumulate_contact_1,\
			t1.accumulate_contact_2, t1.accumulate_vibration, t1.contact_1_count, t1.contact_2_count,\
			t2.esn, t2.unixTime, t2.gps, t2.payload, t2.is_read, t2.ctime \
			FROM "SANS_STU_MESSAGE_DETAIL" as t1 INNER JOIN "SANS_STU_MESSAGE" as t2 ON id = stu_id \
			WHERE t2.is_read = %s ORDER BY t2.ctime DESC OFFSET %s LIMIT %s ' % (is_read, offset, limit))
		stu_records = cursor.fetchall() 
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_records

def get_coordinate_detail(device='', offset=0, limit=10):
	stu_records = []
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('SELECT t1.latitude, t1.longitude, t2.ctime \
			FROM "SANS_STU_MESSAGE_DETAIL" as t1 INNER JOIN "SANS_STU_MESSAGE" as t2 ON id = stu_id \
			WHERE t2.esn = \'%s\' ORDER BY t2.ctime DESC OFFSET %s LIMIT %s ' % (device, offset, limit))
		stu_records = cursor.fetchall() 
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_records

def get_total_stu_message_detail():
	stu_record = None
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('SELECT COUNT(*) as total FROM "SANS_STU_MESSAGE_DETAIL" INNER JOIN "SANS_STU_MESSAGE" ON id = stu_id')
		stu_record = cursor.fetchone() 
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_record

def get_esn_all():
	stu_records = []
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('SELECT DISTINCT(esn) FROM "SANS_STU_MESSAGE"')
		stu_records = cursor.fetchall() 
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_records

def flag_is_read(date_start, date_end):
	stu_records = []
	try :
		conn = psycopg2.connect(host=PG_HOSTNAME,
		                        port=PG_PORTNAME,
		                        user=PG_USERNAME,
		                        password=PG_PASSWORD,
		                        database=PG_DATABASE)
		cursor = conn.cursor()
		cursor.execute('UPDATE "SANS_STU_MESSAGE" SET is_read = \'%s\' WHERE ctime >= \'%s\' AND ctime < \'%s\' ' % (True, date_start, date_end))
		conn.commit()
		count = cursor.rowcount
		print(count, "Record Updated successfully ")
		cursor.close()
		conn.close()
	except Exception as e :
		print(e)

	return stu_records