import sesamclient
from flask import Flask, request, Response
import cherrypy
import json
import time
import os
import logging
import paste.translogger
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-8767565ff502.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()

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

def generate_schema(entity_schema):
    schema = []
    for key, value in entity_schema["properties"].items():
            if "anyOf" in value:
                field_types = [v for v in value["anyOf"] if v["type"] != "null"]
                field_type = field_types[0]["type"]
            else:
                field_type = value["type"]

            # TODO: extend to other possible types in the entity schema from Sesam
            if field_type == "integer":
                schema.append(bigquery.SchemaField(key, "INTEGER"))
            elif field_type == 'boolean':
                schema.append(bigquery.SchemaField(key, "BOOLEAN"))
            elif field_type == 'string':
                schema.append(bigquery.SchemaField(key, "STRING"))
            elif field_type == 'integer':
                schema.append(bigquery.SchemaField(key, "INTEGER"))

    return schema

app = Flask(__name__)

logger = logging.getLogger("datasink-service")

jwt_token = os.environ.get("JWT_TOKEN")
node_url = os.environ.get("NODE_URL")
pipe_id = os.environ.get("PIPE_ID")
target_table = os.environ.get("TARGET_TABLE")

node_connection = sesamclient.Connection(node_url, jwt_auth_token=jwt_token)

pipe_schema_url = node_url + "/pipes/" + pipe_id + "/entity-types/sink"
r = node_connection.do_get_request(pipe_schema_url)
entity_schema = r.json()

print(entity_schema)

big_query_schema = generate_schema(entity_schema)


def insert_into_bigquery(entities, table_schema):
    source_table = target_table + '_temp'

    # Create target table and temp tables
    create_table(target_table, big_query_schema)
    create_table(source_table, big_query_schema, replace=True)

    # Upload test data to temp table
    # Due to data being uploaded asynchronisly, the program loops until upload is successful
    timeout = 60
    start_time = time.time()
    while True:
        try:
            errors = client.insert_rows_json(source_table, entities)

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
    # Make schema into array of strings
    schema_arr = []
    for ele in table_schema:
        schema_arr.append(ele.name)

    # Merge temp table and target table
    merge_query = f"""
    MERGE {target_table} T
    USING
    (SELECT {", ".join(["t." + ele.name for ele in table_schema])}
    FROM (
    SELECT _id, MAX(_updated) AS _updated
    FROM {source_table}
    GROUP BY _id
    )AS i JOIN {source_table} AS t ON t._id = i._id AND t._updated = i._updated) S
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

    # Perform query and await result
    query_job = client.query(merge_query)
    query_job.result()


@app.route('/', methods=['GET'])
def root():
    return Response(status=200, response="I am Groot!")


@app.route('/receiver', methods=['POST'])
def receiver():
    # get entities from request and write each of them to a file

    entities = request.json

    try:
        insert_into_bigquery(entities, big_query_schema)
    except BaseException as e:
        raise BadRequest(f"Something went wrong! {str(e)}")

    # create the response
    return Response("Thanks!", mimetype='text/plain')


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout, change to or add a (Rotating)FileHandler to log to a file
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    # Comment these two lines if you don't want access request logging
    app.wsgi_app = paste.translogger.TransLogger(app.wsgi_app, logger_name=logger.name,
                                                 setup_console_handler=False)
    app.logger.addHandler(stdout_handler)

    logger.propagate = False
    logger.setLevel(logging.INFO)

    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': 5001,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
