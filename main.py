from dbfread import DBF
import pandas as pd
import os
import sqlite3
import csv
import time


load_text="""
#########################################################################
#                                                                       #
#                                                                       #
#                           ..::DBF2SQL::..                             #
#      Easily Convert Multiple DBF Files To a single SQL Database       #
#                                                                       #
#########################################################################
"""
print(load_text)

print('Selecting y will rebuild the database from dbf files.\n')

print('Load Database?')
load = input('-->')

start_time = time.clock()

db_filename = 'new_database.db'
fileCheck = os.path.isfile(db_filename)
if fileCheck == True and load.lower() =='y':
	os.remove(db_filename)
conn = sqlite3.connect(db_filename)
conn.text_factory = str

def writeCsv(sql_file,file_name='temp',split_count=0):
	#This function will export a csv from the sqlite db
	#sql_file is for a query string 
	#file_name is the file name that will be output, don't add csv
	#split_count will split the file exporting by the integer passed
	#so if you pass it 10 for a 100 record file you will end up with 12 files
	#if total row count is less than split count only one file will be exported
	name      = file_name
	cursor    = conn.execute(sql_file)
	results   = cursor.fetchall() 
	names     = [description[0] for description in cursor.description]
	ret_count = len(results)

	if split_count>0 and split_count<ret_count:
		count      = 0
		file       = 1
		ret        = []
		over_count = 0
		for r in results:
			ret.append(r)
			count+=1
			over_count+=1
			if count==split_count or ret_count==over_count:
				if file in [1,2,3,4,5,6,7,8,9]:
					l_zero='0'
				else:
					l_zero=''
				split_filename = name+'_'+l_zero+str(file)
				print(f"{split_filename.capitalize()} created with {count} rows...\n")
				with open('\nexport/'+split_filename+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerow(names)
					writer.writerows(ret)
				count=0
				file+=1
				ret=[]

	if split_count==0 or split_count>=ret_count:
		print(f"\n{file_name.capitalize()} created with {ret_count} rows...\n")
		with open('export/'+name+'.csv', 'w') as f:
			writer = csv.writer(f)
			writer.writerow(names)
			writer.writerows(results)


def dbIndex(table_name,field):
	#creates indexes on single table for 1 or more column
	#table must be a string, field can be string or list
	if type(field) is not list: field = [field]
	i_query = ""
	for i in field:
	    idxQuery = "CREATE INDEX idx_"+i+"_"+table_name+" ON "+table_name+" ("+i+" ASC);"
	    i_query = i_query + idxQuery+'\n'
	i_query = i_query[:-1]
	conn.execute(i_query)
	return i_query


if load.lower()=='y':
	files =  os.listdir('./data')
	files = [x for x in files if not x.startswith('.')]
	files = [x for x in files if x.lower().endswith('.dbf')]
	files.sort()

	print(f"\nA total of {len(files)} will be loaded...\n")
	time.sleep(2)
	
	#loading tables into database
	for table in files:
		print(f'Loading {table}...')
		dbf = DBF(f'data/{table}')
		df = pd.DataFrame(iter(dbf))
		if not df.empty:
			df.to_sql(name=table[:-4], con=conn, if_exists = 'replace', index=False)
	
	#creating table that gives you an over view of the database
	table_overview_ct="""
	CREATE TABLE IF NOT EXISTS TABLE_OVERVIEW (
		TABLE_NAME	TEXT,
		COLUMN_NAME	TEXT,
		DATA_TYPE	TEXT,
		ROW_COUNT 	INT
	);
	"""
	conn.execute(table_overview_ct)
	
	for file in files:
		table =file[:-4]
		print(f'Creating Stats for {table}...')
		cur = conn.execute(f'PRAGMA TABLE_INFO({table})')
		data = cur.fetchall()
		
		for d in data:
			column_name = d[1]
			data_type = d[2]
			row_count_q = f"SELECT COUNT(*) FROM {table}"
			insert = f"""
						INSERT INTO TABLE_OVERVIEW(TABLE_NAME,COLUMN_NAME,DATA_TYPE,ROW_COUNT) 
						VALUES('{table}','{column_name}','{data_type}',({row_count_q}))
						"""
			conn.execute(insert)
	#example index
	#table name can be a single column in as string
	#or multiple columns in a list can be passed 		
	dbIndex('TABLE_OVERVIEW','TABLE_NAME')

#clear out previous export files
contents =  os.listdir('./export')
contents = [x for x in contents if not x.startswith('.')]
for file in contents:
	os.remove('export/'+file)

#example csv export
#Add ,split_count=50 to create multiple files at 50 lines per file,
#or what ever number you like
writeCsv("SELECT * FROM TABLE_OVERVIEW ORDER BY ROW_COUNT DESC",'TABLE_OVERVIEW')

time_took= str(round(time.clock() - start_time,3))
print(f"Seconds taken... {time_took}\n")

conn.commit()