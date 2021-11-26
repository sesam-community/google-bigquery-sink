from google.cloud import bigquery
import google
import os

credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-8767565ff502.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()
table_id = 'bigquery-microservice.tesset.testset-table'

dml_statement = "CREATE TABLE dmlCreatedTable (testId INTEGER, testValue STRING, testComment STRING);"

#dml_statement  = "INSERT INTO bigquery-microservice.tesset.testset-table\n(id, value, comment)\nVALUES ('2',25,'Another test value'),\n('3',14,'Yet another test value'"

query_job = client.query(dml_statement)
    #INSERT bigquery-microservice.tesset.testset-table (id, value, comment)
    #VALUES ('2',25,'Another test value'),
    #('3',14,'Yet another test value');
#)
#try:
query_job.result()
#except:
#    print('An error occured')

#rows_to_insert = [
#    {'id':'1', 'value':1, 'comment':'Comments are optional'}
#]

#errors = client.insert_rows_json(table_id, rows_to_insert)

#if errors == []:
#    print('New rows have been added to table')
#else:
#    print(f'Errors occured while adding rows to table: {errors}')
