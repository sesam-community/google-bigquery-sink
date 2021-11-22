from google.cloud import bigquery
import os

credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-6ad7d894bab7.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()
table_id = 'bigquery-microservice.tesset.testset-table'

rows_to_insert = [
    {'id':'1', 'value':1, 'comment':'Comments are optional'}
]

errors = client.insert_rows_json(table_id, rows_to_insert)

if errors == []:
    print('New rows have been added to table')
else:
    print(f'Errors occured while adding rows to table: {errors}')
