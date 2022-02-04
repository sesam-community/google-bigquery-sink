import sesamclient
from flask import Flask, request, Response
import cherrypy
from more_itertools import sliced, chunked, collapse
import json
import time
from datetime import datetime, timezone, timedelta
import os
import logging
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import paste.translogger
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import GoogleAPICallError
from decimal import Decimal

EPOCH = datetime.utcfromtimestamp(0)  # NOTE: this is a datetime with tzinfo=None

if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    # Local dev env for mikkel
    credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-8767565ff502.json'

    if not os.path.exists(credentials_path):
        # Local dev env for tom
        credentials_path = '/home/tomb/Downloads/bigquery-microservice-70d791ad9009.json'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
else:
    # Dev env in the cloud
    credentials_content = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    with open("/tmp/bigquery-microservice-8767565ff502.json", "w") as outfile:
        outfile.write(credentials_content)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/tmp/bigquery-microservice-8767565ff502.json"

client = bigquery.Client()

cast_columns = []


def datetime_as_int(dt):
    # convert to naive UTC datetime
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)  # NOTE: same as dt = dt - dt.utcoffset()
        dt = dt.replace(tzinfo=None)

    time_delta = dt - EPOCH

    r = int(time_delta.total_seconds()) * 1000000000
    t = time_delta.microseconds * 1000

    if time_delta.days < 0 < t:
        t = -1000000000 + t

    return r + t


def datetime_parse(dt_str):
    # Parse a "nanoseconds" timestamp to int - it has up to 9 digits fractional seconds
    dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S")  # len('2015-11-24T07:58:53') == 19
    dt_str_digits = dt_str[19+1:-1]  # get number between . and Z
    dt_str_nanos = dt_str_digits.ljust(9, "0")
    return datetime_as_int(dt) + int(dt_str_nanos)


MIN_NANOS = datetime_parse("0001-01-01T00:00:00Z")
MAX_NANOS = datetime_parse("9999-12-31T23:59:59Z")


def datetime_format(dt_int):
    # Format a "nanoseconds" int to ISO format compatible with BQ (max 6 digit fractional seconds, aka microseconds)
    if MIN_NANOS <= dt_int <= MAX_NANOS:
        seconds = (dt_int//1000000000)
        nanoseconds = dt_int-(dt_int//1000000000)*1000000000
        microseconds_str = ("%06d" % nanoseconds).rstrip("0")
        dt = datetime.utcfromtimestamp(seconds)
        if len(microseconds_str) > 0:
            return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%S") + "." + microseconds_str + "Z"
        else:
            return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%SZ")
    else:
        raise ValueError("Integer %d is outside of valid datetime range" % (dt_int,))


def create_table(table_id, schema, replace=False):
    try:
        table = client.get_table(table_id)  # Make an API request.
        if replace:
            # Drop the table
            logger.info("Dropping table '%s'..." % table_id)
            client.delete_table(table_id)
        else:
            return table
    except NotFound:
        logger.info("Table '%s' didn't already exist" % table_id)
        pass

    table_obj = bigquery.Table(table_id, schema=schema)
    logger.info("Creating table '%s'..." % table_id)
    client.create_table(table_obj)

    timeout = 60
    start_time = time.time()
    logger.info("Waiting for table '%s' to appear..." % table_id)
    while True:
        try:
            table = client.get_table(table_id)
            logger.info("Table '%s' has appeared! Moving on.." % table_id)
            return table
        except NotFound:
            if time.time() - start_time > timeout:
                msg = f"Timed out while waiting for the table '{table_id}' to exist"
                logger.error(msg)
                raise AssertionError(msg)


def generate_schema(entity_schema):
    schema = []
    for key, value in entity_schema["properties"].items():
            if "anyOf" in value:
                field_types = [v for v in value["anyOf"] if v["type"] != "null"]
                if "subtype" not in field_types[0]:
                    field_type = field_types[0]["type"]
                else:
                    field_type = field_types[0]["subtype"]
            else:
                if "subtype" not in value:
                    field_type = value["type"]
                else:
                    field_type = value["subtype"]

            # TODO: extend to other possible types in the entity schema from Sesam
            if field_type == "integer":
                schema.append(bigquery.SchemaField(key, "INTEGER"))
            elif field_type == 'boolean':
                schema.append(bigquery.SchemaField(key, "BOOLEAN"))
            elif field_type == 'string':
                schema.append(bigquery.SchemaField(key, "STRING"))
            elif field_type == 'integer':
                schema.append(bigquery.SchemaField(key, "INTEGER"))
            elif field_type == "decimal":
                schema.append(bigquery.SchemaField(key, "BIGNUMERIC"))
                cast_columns.append(key)
            elif field_type == "nanoseconds":
                schema.append(bigquery.SchemaField(key, "DATETIME"))
                cast_columns.append(key)
            elif field_type in ['object', 'array', 'bytes', 'bytes', 'uuid', 'uri', 'ni']:
                schema.append(bigquery.SchemaField(key, "STRING"))
                cast_columns.append(key)
            else:
                logger.warning("Unknown field type '%s' - defaulting to 'STRING'" % field_type)

    return schema


app = Flask(__name__)

logger = logging.getLogger("datasink-service")

jwt_token = os.environ.get("JWT_TOKEN")
node_url = os.environ.get("NODE_URL")
pipe_id = os.environ.get("PIPE_ID")
target_table = os.environ.get("TARGET_TABLE")
use_multithreaded = os.environ.get("MULTITHREADED", False) in ["1", 1, "true", "True"]

node_connection = sesamclient.Connection(node_url, jwt_auth_token=jwt_token)

pipe_schema_url = node_url + "/pipes/" + pipe_id + "/entity-types/sink"
r = node_connection.do_get_request(pipe_schema_url)
entity_schema = r.json()

default_properties = {
    "_deleted": {"type": "boolean"},
    "_previous": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
    "_updated": {"type": "integer"}
}

entity_schema["properties"].update(default_properties)

big_query_schema = generate_schema(entity_schema)

#from pprint import pprint
#pprint(entity_schema, indent=2)

rows_seen = 0


def count_rows_in_table(table, retries=0, timeout=None, prefix=''):
    while True:
        try:
            logger.info("%scounting rows in table '%s'..." % (prefix, table))
            query_job = client.query(f'SELECT COUNT(*) FROM `{table}`', timeout=timeout)
            result = query_job.result(timeout=timeout)
            count = [item[0] for item in result][0]
            logger.info("%sRow count for table '%s' is: %s" % (prefix, table, count))
            return count
        except BaseException as e:
            retries -= 1

            msg = "%sFailed to get count from table '%s'" % (prefix, table)

            if retries >= 0:
                logger.warning("%s.. retrying" % msg)

                time.sleep(1)
            else:
                raise RuntimeError(msg)


def wait_for_rows_to_appear(entities, existing_count, table, timeout=60, prefix=''):
    start_time = time.time()
    while True:
        # Loop until count includes the new entities
        count = count_rows_in_table(table, retries=3, timeout=5, prefix=prefix)

        if (count - existing_count) == len(entities):
            logger.info("%sRow count for table '%s' after inserting all "
                        "entities in batch is: %s" % (prefix, table, count))
            break

        logger.info(f"{prefix}The last row count for table '{table}' was {count} - "
                    f"expected {existing_count + len(entities)} - retrying...")

        if time.time() - start_time > timeout:
            msg = f"{prefix}Timed out while waiting for row count for table '{table}' to increase to include " \
                  f"the inserted entities, the last row count was {count} - expected {existing_count + len(entities)}"
            logger.error(msg)
            raise AssertionError(msg)

        time.sleep(5)


def insert_entities_into_table(table, entities, wait_for_rows=True, prefix=''):
    existing_count = 0
    if wait_for_rows:
        try:
            existing_count = count_rows_in_table(table, retries=3, timeout=5, prefix=prefix)
        except BaseException as e:
            logger.warning("%sFailed to get row count from table '%s" % (prefix, table))

        logger.info("%sRow count before insert into table '%s' is: %s" % (prefix, table, existing_count))

    slices = len(entities)
    remaining_entities = list(sliced(entities, slices))
    notfound_retries = 10
    done = False

    while not done:
        last_ix = len(remaining_entities) - 1
        for ix, chunk in enumerate(remaining_entities):
            try:
                logger.info("%sInserting %s entities in table '%s'..." % (prefix, len(chunk), table))
                row_ids = [e["_updated"] for e in chunk]
                errors = client.insert_rows_json(table, chunk, row_ids=row_ids, timeout=30)
                notfound_retries = 3
                if not errors:
                    logger.info(f"{prefix}{len(chunk)} new rows have been added to table '%s'" % table)
                else:
                    raise AssertionError(f"{prefix}Failed to insert json for table '%s':  %s" % (table, errors))

                if ix == last_ix:
                    done = True
            except NotFound as e:
                notfound_retries -= 1
                if notfound_retries == 0:
                    raise AssertionError("%sGot too many NotFound errors in a row for table '%s', "
                                         "bailing out..." % (prefix, table))
                else:
                    logger.info("%sGot a NotFound error for table '%s', retrying..." % (prefix, table))

                time.sleep(1)

                # Skip the chunks we've already inserted
                remaining_entities = remaining_entities[ix:]

                # New loop
                break
            except GoogleAPICallError as ge:
                if ge.code == 413 and "Your client issued a request that was too large" in ge.message:
                    # We need to split the entities into smaller chunks and try again
                    logger.info("%sCurrent batch is too big for table '%s', slicing it up..." % (prefix, table))

                    # Gather and split the remaining entities one more level
                    slices = int(len(chunk) / 2)
                    tmp = []
                    for c in remaining_entities[ix:]:
                        tmp.extend(c)

                    remaining_entities = list(sliced(tmp, slices))
                    logger.info("%sTrying with new batch size for table '%s': %s for the %s "
                                "remaining entities" % (prefix, table, len(remaining_entities[0]), len(tmp)))
                    # New loop
                    break
                else:
                    # Fail for any other errors
                    raise ge
            except BaseException as e:
                logger.exception("%sinsert_rows_json('%s') failed for unknown reasons!" % (prefix, table))
                raise e

    if wait_for_rows:
        logger.info("%sVerifying number of rows inserted into '%s'..." % (prefix, table))
        wait_for_rows_to_appear(entities, existing_count, table, timeout=120, prefix=prefix)


def insert_entities_into_table_mt(table, entities):
    # Multithreaded version of the insert code
    existing_count = count_rows_in_table(table, retries=3, timeout=30)
    logger.info("Row count before insert into table '%s' is: %s" % (table, existing_count))

    partition_size = 1000
    workers = 50
    queue = Queue()
    futures = []

    # Fill up the queue with entity partitions
    for partition in sliced(entities, partition_size):
        queue.put(partition)

    running_threads = {}

    def insert_partition(prefix):
        while True:
            try:
                current_partition = queue.get(block=False)
                if current_partition is None:
                    return True
            except Empty:
                return True

            try:
                threading.current_thread().name = prefix

                running_threads[prefix] = True

                logger.info("%sinsert_partition(): inserting a partition of size "
                            "%s to table '%s'" % (prefix, len(current_partition), table))

                insert_entities_into_table(table, current_partition, wait_for_rows=False, prefix=prefix)

                logger.info("%sDone, terminating.." % prefix)
                queue.task_done()

                running_threads.pop(prefix)
            except BaseException as e:
                logger.exception("%sInsert failed! Terminating.." % prefix)
                raise e

    starttime = time.time()
    executor = ThreadPoolExecutor(max_workers=workers)
    for i in range(workers):
        futures.append(executor.submit(insert_partition, prefix="Thread %s: " % i))

    # Wait until all futures have either run successfully or failed
    failed = False
    while True:
        if not futures:
            break

        for future in futures[:]:
            if future.done():
                exc = future.exception()
                if exc is not None:
                    failed = True
                    logger.error(str(exc))
                futures.remove(future)

        logger.info("Still waiting for %s threads to finish" % len(futures))
        time.sleep(2)

    if not failed:
        # All futures completed successfully, wait until all the rows have appeared
        logger.info("Verifying row count for target table '%s'.." % table)
        wait_for_rows_to_appear(entities, existing_count, table, timeout=120)
        elapsed_time = time.time() - starttime
        num_entities = len(entities)
        logger.info("Finished inserting %s rows in %s secs (%1f rows/sec)" % (num_entities, elapsed_time,
                                                                              num_entities/elapsed_time))
    else:
        raise AssertionError("One or more threads failed to insert their partition, see the service log for details")


def insert_into_bigquery(entities, table_schema, is_full, is_first, request_id, sequence_id, multithreaded=False):
    # Remove _ts and _hash
    for entity in entities:
        entity.pop("_ts", None)
        entity.pop("_hash", None)

    if cast_columns:
        # Cast any arrays and objects to string
        for entity in entities:
            for key in cast_columns:
                value = entity.get(key)
                if value is not None:
                    if not isinstance(value, (list, dict)):
                        # Check if we need to transit decode this value
                        if isinstance(value, str) and len(value) > 1 and value[0] == "~":
                            prefix = value[:2]
                            if prefix in ["~r", "~u", "~:", "~b"]:
                                # URI, NI, UUID, bytes, Nanosecond: just cast it to string without the prefix
                                value = value[len(prefix):]
                            elif prefix in ["~d", "~f"]:
                                # Float or decimal
                                value = float(Decimal(value[len(prefix):]))
                            elif prefix == "̃~t":
                                # Truncate nanoseconds to microseconds to be compatible with BQ
                                value = value[len(prefix):]
                                dt_int = datetime_parse(value)
                                value = datetime_format(dt_int)
                            else:
                                # Unknown type, strip the prefix off
                                value = value[len(prefix):]

                            entity[key] = value
                    else:
                        entity[key] = json.dumps(value)

    # Check for is_full
    if is_full:
        if is_first:
            # Create target table
            create_table(target_table, big_query_schema, replace=True)

# Commented out for now, always upload via a temptable to avoid lost data and other async weirdness
#        # Upload test data to target_table
#        if multithreaded:
#            insert_entities_into_table_mt(target_table, entities)
#        else:
#            insert_entities_into_table(target_table, entities)
#    else:

        # Create a separate source table. The tables will be merged later.
        source_table = target_table + '_%s_%s_temp' % (sequence_id.replace("-", ""), request_id)

        # Create target table
        create_table(target_table, big_query_schema, replace=(is_full and is_first))

        try:
            # Create temp table
            create_table(source_table, big_query_schema, replace=True)

            existing_count = [item[0] for item in
                              client.query(f'SELECT COUNT(*) FROM `{target_table}`').result()][0]

            logger.info("Row count before merge to table '%s' was: %s" % (target_table, existing_count))

            # Upload test data to temp table
            if multithreaded:
                insert_entities_into_table_mt(source_table, entities)
            else:
                insert_entities_into_table(source_table, entities)

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

            existing_count = [item[0] for item in
                              client.query(f'SELECT COUNT(*) FROM `{target_table}`').result()][0]

            logger.info("Row count after merge to table '%s' is: %s" % (target_table, existing_count))
        except BaseException as e:
            logger.exception("Failed to process batch")
            raise e
        finally:
            try:
                logger.info("Deleting temp table '%s'" % source_table)
                client.delete_table(source_table)
            except BaseException as e:
                logger.exception("Failed to drop temp table '%s'" % source_table)


@app.route('/', methods=['GET'])
def root():
    return Response(status=200, response="I am Groot!")


@app.route('/receiver', methods=['POST'])
def receiver():
    # get entities from request and write each of them to a file
    global rows_seen

    entities = request.json

    is_full = request.args.get('is_full', "false")
    is_full = (is_full.lower() == "true" and True) or False

    is_first = request.args.get('is_first', "false")
    is_first = (is_first.lower() == "true" and True) or False

    is_last = request.args.get('is_last', "false")
    is_last = (is_last.lower() == "true" and True) or False

    sequence_id = request.args.get('sequence_id', 0)
    request_id = request.args.get('request_id', 0)

    try:
        if len(entities) > 0:
            insert_into_bigquery(entities, big_query_schema, is_full, is_first, request_id, sequence_id,
                                 multithreaded=use_multithreaded)

        if is_first:
            rows_seen = len(entities)
        else:
            rows_seen += len(entities)

        if is_last:
            logger.info("I saw %s rows during this run" % rows_seen)

    except BaseException as e:
        logger.exception("Something went wrong")
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

    if use_multithreaded:
        logger.info("Running in multithreaded mode")
    else:
        logger.info("Running in single threaded mode")

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
