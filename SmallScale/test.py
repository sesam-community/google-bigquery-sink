from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import json
import time

credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-8767565ff502.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()

def generate_schema(example_entity):
    schema = []
    field_type = ""
    field_types = []
    nullable = False
    for key, value in example_entity.properties:
        try:
            field_type = value["type"]
            if field_type == 'boolean':
                schema.append(bigquery.SchemaField(key, "BOOLEAN"))
            elif field_type == 'string':
                schema.append(bigquery.SchemaField(key, "STRING"))
            elif field_type == 'integer':
                schema.append(bigquery.SchemaField(key, "INTEGER"))
        except:
            field_types = value.anyOf
            for ele in field_types:
                if ele.type == 'null':
                    nullable = true
                else:
                    field_type = ele.type
            schema.append(bigquery.SchemaField(key,))

        if key in ['_previous','_updated']:
            schema.append(bigquery.SchemaField(key, "INTEGER"))
        elif isinstance(value, str):
            schema.append(bigquery.SchemaField(key, "STRING"))
        elif isinstance(value, bool):
            schema.append(bigquery.SchemaField(key, "BOOLEAN"))
        elif isinstance(value, int):
            schema.append(bigquery.SchemaField(key, "INTEGER"))

    return schema


def create_table(table_id, schema, replace=False):
    try:
        table = client.get_table(table_id)  # Make an API request.
        if replace:
            # Drop the table
            client.delete_table(table_id)
        else:
            return table
    except NotFound:
        pass

    table_obj = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table_obj)

    while True:
        try:
            _table = client.get_table(table_id)
            return _table
        except NotFound:
            pass

#Get testdata from file, and store as json.
with open("../testdata.json") as infile:
    test_data = json.loads(infile.read())

target_name = 'mikkel_test'
source_name = 'mikkel_test_temp'
dataset_name = 'bigquery-microservice.tesset'

#Get object from test_data and use it to create schema
example_entity = test_data[-2]
table_schema = generate_schema(example_entity)

# Create target table and temp tables
create_table(dataset_name + "." + target_name, table_schema, replace=True)
create_table(dataset_name + "." + source_name, table_schema, replace=True)

# Upload test data to temp table
#Due to data being uploaded asynchronisly, the program loops until upload is successful
timeout = 60
start_time = time.time()
while True:
    try:
        errors = client.insert_rows_json(dataset_name + "." + source_name, test_data)

        if errors == []:
           print('New rows have been added to table')
        else:
            print(f'Errors occured while adding rows to table: {errors}')

        break
    except NotFound as e:
        if time.time() - start_time > timeout:
            raise e
        time.sleep(5)

# Create MERGE query
#Make schema into array of strings
schema_arr = []
for ele in table_schema:
    schema_arr.append(ele.name)

#Merge temp table and target table
merge_query = f"""
MERGE {dataset_name}.{target_name} T
USING
(SELECT {", ".join(["t." + ele.name for ele in table_schema])}
FROM (
SELECT _id, MAX(_updated) AS _updated
FROM {dataset_name}.{source_name}
GROUP BY _id
)AS i JOIN {dataset_name}.{source_name} AS t ON t._id = i._id AND t._updated = i._updated) S
ON T._id = S._id
WHEN MATCHED AND S._deleted = true THEN
    DELETE
WHEN NOT MATCHED AND (S._deleted IS NULL OR S._deleted = false) THEN
    INSERT ({", ".join(schema_arr)})
    VALUES ({", ".join(["S." + ele.name for ele in table_schema])})
WHEN MATCHED AND (S._deleted IS NULL OR S._deleted = false) THEN
    UPDATE
    SET {", ".join([ele.name + " = S." + ele.name for ele in table_schema])};
"""

#Perform query and await result
query_job = client.query(merge_query)
query_job.result()