from google.cloud import bigquery
import google
import os

credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-8767565ff502.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()
#table_id = 'bigquery-microservice.tesset.testset-table'

table_id = 'testsetTable'
cntr = 2

dataset_name = client.list_tables('bigquery-microservice.tesset')

for ele in dataset_name:
    if (table_id + str(cntr)) == ele.table_id:
        cntr += 1

new_table = table_id + str(cntr)

dml_statement = f"""CREATE TABLE bigquery-microservice.tesset.{new_table}
(id INTEGER, value INTEGER, comment STRING);
INSERT INTO bigquery-microservice.tesset.{new_table}
VALUES (1,2,'This is a comment'),
(2,2,'delete');
MERGE INTO bigquery-microservice.tesset.{table_id} T
USING bigquery-microservice.tesset.{new_table} S
ON T.id = S.id
WHEN NOT MATCHED THEN
    INSERT (id, value, comment)
    VALUES (id, value, comment)
WHEN MATCHED AND S.comment = 'delete' THEN
    DELETE;
DROP TABLE bigquery-microservice.tesset.{new_table}
"""


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
