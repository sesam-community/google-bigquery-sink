import sesamclient
from flask import Flask, request, Response
import cherrypy
import json
import os
import logging
import paste.translogger
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest

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

# Convert to BigQuery schema
#
# def convert_schema(entity_schema):
#    ...
#
# bigquery_schema = convert_schema(entity_schema)


def insert_into_bigquery(entities):
    pass


@app.route('/', methods=['GET'])
def root():
    return Response(status=200, response="I am Groot!")


@app.route('/receiver', methods=['POST'])
def receiver():
    # get entities from request and write each of them to a file

    entities = request.json

    try:
        insert_into_bigquery(entities)
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
