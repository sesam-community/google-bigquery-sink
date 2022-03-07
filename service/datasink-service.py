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
from threading import RLock, Thread

version = "1.0.9"

PIPE_CONFIG_TEMPLATE = """
{
  "_id": "%(pipe_id)s",
  "type": "pipe",
  "source": {
    "type": "dataset",
    "dataset": "%(dataset_id)s"
  },
  "sink": {
    "type": "json",
    "system": "%(system_id)s",
    "batch_size": %(batch_size)s,
    "url": "receiver?pipe_id=%(dataset_id)s&target_table=%(table_prefix)s.%(dataset_id)s"
  },
  "pump": {
    "schedule_interval": %(interval)s,
    "fallback_to_single_entities_on_batch_fail": false
  },
  "metadata": {
    "$config-group": "%(config_group)s"
  },
  "batch_size": %(batch_size)s,
  "remove_namespaces": false
}
"""

SYSTEM_CONFIG_TEMPLATE = """
{
  "_id": "%(system_id)s",
  "type": "system:microservice",
  "metadata": {
    "$config-group": "%(config_group)s"
  },
  "docker": {
    "environment": {
      "GOOGLE_APPLICATION_CREDENTIALS": "$SECRET(bigquery-credentials)",
      "JWT_TOKEN": "$SECRET(bigquery-ms-jwt)",
      "MULTITHREADED": "true",
      "NODE_URL": "%(node_url)s",
      "BIGQUERY_TABLE_PREFIX": "%(table_prefix)s"
    },
    "image": "sesam/sesam-bigquery:latest",
    "memory": 1512,
    "port": 5000
  },
  "read_timeout": 7200,
  "use_https": false,
  "verify_ssl": false
}
"""

EPOCH = datetime.utcfromtimestamp(0)  # NOTE: this is a datetime with tzinfo=None

if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    # Local dev env for mikkel
    credentials_path = '/home/mikkel/Desktop/BigQueryMicroservice/BigQueryMicroservice/SmallScale/bigquery-microservice-8767565ff502.json'

    if not os.path.exists(credentials_path):
        # Local dev env for tom
        credentials_path = '/home/tomb/Downloads/bigquery-microservice-58c39f7392e7.json'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
else:
    # Dev env in the cloud
    credentials_content = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    with open("/tmp/bigquery-microservice-58c39f7392e7.json", "w") as outfile:
        outfile.write(credentials_content)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/tmp/bigquery-microservice-58c39f7392e7.json"

client = bigquery.Client()

app = Flask(__name__)

logger = logging.getLogger("datasink-service")

jwt_token = os.environ.get("JWT_TOKEN")
node_url = os.environ.get("NODE_URL")
config_pipe_id = os.environ.get("PIPE_ID")
bq_table_prefix = os.environ.get("BIGQUERY_TABLE_PREFIX")
config_target_table = os.environ.get("TARGET_TABLE")
bootstrap_pipes_recreate_pipes = os.environ.get("BOOTSTRAP_RECREATE_PIPES", False)
bootstrap_pipes = os.environ.get("BOOTSTRAP_PIPES", False)
bootstrap_single_system = os.environ.get("BOOTSTRAP_SINGLE_SYSTEM", False)
use_multithreaded = os.environ.get("MULTITHREADED", "true") in ["1", 1, "true", "True"]
bootstrap_config_group = os.environ.get("BOOTSTRAP_CONFIG_GROUP", "analytics")
bootstrap_interval = os.environ.get("BOOTSTRAP_INTERVAL", "24")
config_batch_size = 1000

node_connection = sesamclient.Connection(node_url, jwt_auth_token=jwt_token)

schema_cache = {}
client_locks_lock = RLock()
client_locks = {}


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
            return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%S UTC") + "." + microseconds_str
        else:
            return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%S UTC")
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

    table_obj = bigquery.Table(table_id, schema=schema.values())
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


class SchemaInfo:
    entity_schema = {}
    bigquery_schema = {}
    property_column_translation = {}
    cast_columns = {}
    rows_seen = 0
    seen_field_types = {}
    nonvalid_underscore_properties = []
    array_propery_filter_nulls = {}
    pipe_id = None

    valid_internal_properties = ["_id", "_updated", "_deleted"]

    def __init__(self, pipe_id):
        self.pipe_id = pipe_id
        self.pipe_schema_url = node_url + "/pipes/" + pipe_id + "/entity-types/sink"

        self.default_properties = {
            "_deleted": {"type": "boolean"},
            "_updated": {"type": "integer"}
        }

        self.update_schema()

    def update_schema(self):
        r = node_connection.do_get_request(self.pipe_schema_url)
        _entity_schema = r.json()
        _entity_schema["properties"].update(self.default_properties)

        _cast_columns = []
        _bigquery_schema = {}
        _property_column_translation = {}
        _seen_field_types = {}
        _nonvalid_underscore_properties = {}
        _array_propery_filter_nulls = {}

        for key, value in _entity_schema["properties"].items():
            if key[0] == "_" and key not in self.valid_internal_properties:
                # skip any non-internal underscore properties (which would be deleted in the dataset sink anyway)
                _nonvalid_underscore_properties[key] = 1
                continue

            mode = "NULLABLE"
            translated_key = key.lower().replace(":", "__")

            s = ""
            for c in translated_key:
                if ord(c) > 127:
                    if c == "å":
                        s += "a"
                    elif c == "æ":
                        s += "a"
                    elif c == "ø":
                        s += "o"
                    else:
                        s += "_"
                elif not (c.isalpha() or c.isdigit()):
                    s += "_"
                else:
                    s += c

            translated_key = s
            _property_column_translation[key] = translated_key

            if "anyOf" in value:
                field_types = [v for v in value["anyOf"] if v["type"] != "null"]
                if len(field_types) > 1:
                    # More than one type in entity schema -> cast to string in sql schema
                    field_type = "string"
                    _cast_columns.append(translated_key)
                else:
                    if "subtype" not in field_types[0]:
                        field_type = field_types[0]["type"]
                    else:
                        field_type = field_types[0]["subtype"]
            else:
                if "type" in value and value["type"] == "array":
                    if "type" in value["items"] and value["items"]["type"] != "anyOf":
                        # Array of single types -> mode:REPEATED, so the "real" data type resides in "items"
                        value = value["items"]
                        mode = "REPEATED"
                    elif "anyOf" in value["items"] and len(value["items"]["anyOf"]) == 2:
                        # Another possibly special case; an array of single type + NULL - here we can drop any NULL
                        # values when processing the entity
                        has_null = [(ix, el) for ix, el in enumerate(value["items"]["anyOf"])
                                    if el.get("type", "") == "null"]

                        if has_null:
                            if has_null[0][0] == 0:
                                value = value["items"]["anyOf"][1]
                            else:
                                value = value["items"]["anyOf"][0]

                            mode = "REPEATED"
                            _array_propery_filter_nulls[translated_key] = True

                if "subtype" not in value:
                    field_type = value["type"]
                else:
                    field_type = value["subtype"]

            if translated_key in _bigquery_schema:
                # We've seen this column already, so it exists in multiple cases probably. Check the type and decide
                # what to do
                if field_type == _seen_field_types[translated_key]:
                    # Same type so just skip it
                    continue
                else:
                    # Different type; we need to cast it to string
                    _bigquery_schema.pop(translated_key)
                    _seen_field_types.pop(translated_key)
                    field_type = "string"
                    _cast_columns.append(translated_key)

            # TODO: extend to other possible types in the entity schema from Sesam
            if field_type == "integer":
                bigquery_type = bigquery.SchemaField(translated_key, "INTEGER", mode=mode)
            elif field_type == 'boolean':
                bigquery_type = bigquery.SchemaField(translated_key, "BOOLEAN", mode=mode)
            elif field_type == 'string':
                bigquery_type = bigquery.SchemaField(translated_key, "STRING", mode=mode)
            elif field_type == 'integer':
                bigquery_type = bigquery.SchemaField(translated_key, "INTEGER", mode=mode)
            elif field_type == "decimal":
                bigquery_type = bigquery.SchemaField(translated_key, "BIGNUMERIC", mode=mode)
                _cast_columns.append(translated_key)
            elif field_type == "number":
                bigquery_type = bigquery.SchemaField(translated_key, "BIGNUMERIC", mode=mode)
                _cast_columns.append(translated_key)
            elif field_type == "nanoseconds":
                bigquery_type = bigquery.SchemaField(translated_key, "TIMESTAMP", mode=mode)
                _cast_columns.append(translated_key)
            elif field_type in ['object', 'array', 'bytes', 'bytes', 'uuid', 'uri', 'ni']:
                bigquery_type = bigquery.SchemaField(translated_key, "STRING", mode=mode)
                _cast_columns.append(translated_key)
            else:
                logger.warning("Unknown field type '%s' - defaulting to 'STRING'" % field_type)
                bigquery_type = bigquery.SchemaField(translated_key, "STRING", mode=mode)
                _cast_columns.append(translated_key)

            _bigquery_schema[translated_key] = bigquery_type
            _seen_field_types[translated_key] = field_type

        _cast_columns = sorted(list(set(_cast_columns)))

        self.entity_schema = _entity_schema
        self.cast_columns = _cast_columns
        self.bigquery_schema = _bigquery_schema
        self.property_column_translation = _property_column_translation
        self.seen_field_types = _seen_field_types
        self.nonvalid_underscore_properties = list(_nonvalid_underscore_properties.keys())
        self.array_propery_filter_nulls = _array_propery_filter_nulls


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
                    from pprint import pprint
                    logger.error("Example entity:\n%s" % pprint(chunk[0]))
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


def insert_entities_into_table_mt(table, entities, batch_size=1000):
    # Multithreaded version of the insert code
    existing_count = count_rows_in_table(table, retries=3, timeout=30)
    logger.info("Row count before insert into table '%s' is: %s" % (table, existing_count))

    workers = 50
    queue = Queue()
    futures = []

    # Fill up the queue with entity partitions
    for partition in sliced(entities, batch_size):
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


def insert_into_bigquery(target_table, entities, schema_info, request_id, sequence_id, multithreaded=False,
                         batch_size=1000):
    # Remove irrelevant properties and translate property names and, if needed, values
    for entity in entities:
        for key in list(entity.keys()):
            # Remove invalid underscore properties (unneeded internals and user-created ones that the dataset
            # sink would strip away in any case)
            if key[0] == "_" and key not in schema_info.valid_internal_properties:
                entity.pop(key, None)

        for key in schema_info.property_column_translation:
            # Translate properties->columns
            if key not in entity:
                continue

            value = entity.get(key)
            translated_key = schema_info.property_column_translation.get(key, key)

            def cast_value(_value):
                # Check if we need to transit decode this value
                if isinstance(_value, str) and len(_value) > 1 and _value[0] == "~":
                    prefix = _value[:2]
                    if prefix in ["~r", "~u", "~:", "~b"]:
                        # URI, NI, UUID, bytes, Nanosecond: just cast it to string without the prefix
                        _value = _value[len(prefix):]
                    elif prefix in ["~d", "~f"]:
                        # Float or decimal
                        _value = float(Decimal(_value[len(prefix):]))
                    elif prefix == "̃~t":
                        # Truncate nanoseconds to microseconds to be compatible with BQ
                        _value = _value[len(prefix):]
                        dt_int = datetime_parse(_value)
                        _value = datetime_format(dt_int)
                    else:
                        # Unknown type, strip the prefix off
                        _value = _value[len(prefix):]

                return str(_value)

            if value is not None and translated_key in schema_info.cast_columns:
                if isinstance(value, dict):
                    # Cast object values to string directly
                    # TODO: should we transit decode stuff recursively first?
                    value = json.dumps(value)
                elif isinstance(value, list):
                    if len(value) > 4096:
                        value = sorted(value)[:4096]

                    # Check if this is a supported list
                    if schema_info.bigquery_schema[translated_key].mode == "REPEATED":
                        result = []
                        # Truncate lists at 4096 to avoid a hanging insert, sort the list to make it deterministic
                        for litem in value:
                            if litem is None and translated_key in schema_info.array_propery_filter_nulls:
                                # Array of single-value + NULL, we have to drop the NULLs as BQ doesn't support them
                                continue
                            result.append(cast_value(litem))
                        value = result
                    else:
                        # Columns with mixed values we just serialize to json
                        value = json.dumps(value)
                else:
                    value = cast_value(value)

            if translated_key != key:
                entity.pop(key, None)

            entity[translated_key] = value

    # Create a separate source table. The tables will be merged later.
    source_table = target_table + '_%s_%s_temp' % (sequence_id.replace("-", ""), request_id)

    try:
        # Create temp table
        create_table(source_table, schema_info.bigquery_schema, replace=True)

        existing_count = [item[0] for item in
                          client.query(f'SELECT COUNT(*) FROM `{target_table}`').result()][0]

        logger.info("Row count before merge to table '%s' was: %s" % (target_table, existing_count))

        # Upload test data to temp table
        if multithreaded:
            insert_entities_into_table_mt(source_table, entities, batch_size=batch_size)
        else:
            insert_entities_into_table(source_table, entities)

        # Create MERGE query
        # Make schema into array of strings
        schema_arr = []
        for ele in schema_info.bigquery_schema.values():
            schema_arr.append("`" + ele.name + "`")

        # Merge temp table and target table
        merge_query = f"""
        MERGE `{target_table}` T
        USING
        (SELECT {", ".join(["t." + ele.name for ele in schema_info.bigquery_schema.values()])}
        FROM (
        SELECT _id, MAX(_updated) AS _updated
        FROM `{source_table}`
        GROUP BY _id
        )AS i JOIN `{source_table}` AS t ON t._id = i._id AND t._updated = i._updated) S
        ON T._id = S._id
        WHEN MATCHED AND S._deleted = true THEN
            DELETE
        WHEN NOT MATCHED AND (S._deleted IS NULL OR S._deleted = false) THEN
            INSERT ({", ".join(schema_arr)})
            VALUES ({", ".join(["S." + ele.name for ele in schema_info.bigquery_schema.values()])})
        WHEN MATCHED AND (S._deleted IS NULL OR S._deleted = false) THEN
            UPDATE
            SET {", ".join(
            ["`" + ele.name  + "` = S." + ele.name for ele in schema_info.bigquery_schema.values()])};
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
    entities = request.json

    is_full = request.args.get('is_full', "false")
    is_full = (is_full.lower() == "true" and True) or False

    is_first = request.args.get('is_first', "false")
    is_first = (is_first.lower() == "true" and True) or False

    is_last = request.args.get('is_last', "false")
    is_last = (is_last.lower() == "true" and True) or False

    sequence_id = request.args.get('sequence_id', 0)
    request_id = request.args.get('request_id', 0)

    batch_size = request.args.get('batch_size')
    try:
        batch_size = int(batch_size)
    except TypeError as e:
        logger.warning("The 'batch_size' parameter was '%s', which is not an integer" % batch_size)
        batch_size = config_batch_size

    request_pipe_id = request.args.get("pipe_id")
    if request_pipe_id is not None:
        # If the pipe id is given as a param, then the target table should be too. Raise a Badrequest if not
        request_target_table = request.args.get("target_table")
        if request_target_table is None:
            raise BadRequest("If pipe_id is used as a parameter, then target_table needs to be provided as a "
                             "parameter as well")
    else:
        request_pipe_id = config_pipe_id
        request_target_table = config_target_table

    with client_locks_lock:
        lock_key = "%s_%s" % (request_pipe_id, request_target_table)
        if lock_key not in client_locks:
            client_locks[lock_key] = RLock()

        # Don't allow more than one request per pipe/target table combo at a time
        if not client_locks[lock_key].acquire(blocking=False):
            raise BadRequest(f"Another request processing pipe '{request_pipe_id}' and "
                             f"target table '{request_target_table}' is already running")

    try:
        schema_info = schema_cache.get(request_pipe_id)
        if is_full:
            if is_first:
                # Update the schema info on full run, first batch + recreate the target table

                logger.info("Refreshing entity schema from pipe '%s'..." % request_pipe_id)
                schema_cache.pop(request_pipe_id, None)
                schema_info = SchemaInfo(request_pipe_id)
                schema_cache[request_pipe_id] = schema_info

                # Recreate the target table
                logger.info("Recreating target table '%s'..." % request_target_table)
                create_table(request_target_table, schema_info.bigquery_schema, replace=True)

            # Skip deleted entities if this is a full run - the target table will be empty so nothing will be deleted
            # anyway
            entities = [ent for ent in entities if ent.get("_deleted", False) is False]

        if len(entities) > 0:
            if schema_info is None:
                schema_info = SchemaInfo(request_pipe_id)
                schema_cache[request_pipe_id] = schema_info

            insert_into_bigquery(request_target_table, entities, schema_info, request_id, sequence_id,
                                 multithreaded=use_multithreaded, batch_size=batch_size)
        else:
            logger.info("Skipping empty batch...")

        if is_first:
            schema_info.rows_seen = len(entities)
        else:
            schema_info.rows_seen += len(entities)

        if is_last:
            logger.info("I saw %s rows during this run" % schema_info.rows_seen)
    except BaseException as e:
        logger.exception("Something went wrong")
        raise BadRequest(f"Something went wrong! {str(e)}")
    finally:
        with client_locks_lock:
            # Release the lock preventing other requests for the same pipe/target table
            client_locks[lock_key].release()

    # create the response
    return Response("Thanks!", mimetype='text/plain')


class GlobalBootstrapper:

    def __init__(self, connection):
        logger.info("Starting bootstrap thread...")
        self.connection = connection
        try:
            self.timeout_hours = int(bootstrap_interval)
        except ValueError:
            logger.error("The 'BOOTSTRAP_INTERVAL' value '%s' is convertable to an integer, "
                         "defaulting to 24 hours" % bootstrap_interval)
            self.timeout_hours = 24

        self._thread = Thread(target=self.do_update, daemon=True)
        self._thread.start()
        logger.info("Bootstrap thread started!")

    def do_update(self):
        while True:
            try:
                logger.info("Updating pipes...")
                self.update_pipes()
                logger.info("Pipes were updated!")
            except BaseException as e:
                logger.exception("Failed to update pipes!")

            logger.info("Bootstrapper now sleeping for %s hours.." % self.timeout_hours)
            time.sleep(self.timeout_hours * 24)

    def update_pipes(self):
        # Check that all globals have corresponding "share" pipes
        global_datasets = []
        pipes = {}
        for pipe in self.connection.get_pipes():
            pipes[pipe.id] = pipe
            if pipe.config["effective"].get("metadata", {}).get("global", False) is True:
                if "infer_pipe_entity_types" in pipe.config["original"] and \
                        pipe.config["original"]["infer_pipe_entity_types"] is False:
                    # Skip creating pipes for globals that have no schema info
                    logger.info("Skipping global '%s' because 'infer_pipe_entity_types' is set to false" % pipe.id)
                    continue
                logger.info("Found global '%s'.." % pipe.id)

                global_datasets.append(pipe.config.get("sink", {}).get("dataset", pipe.id))

        new_pipe_configs = {}
        new_system_configs = {}
        for dataset_id in global_datasets:
            bq_pipe_id = "bigquery-%s-share" % dataset_id
            if bootstrap_single_system:
                bq_system_id = "bigquery"
            else:
                bq_system_id = "bigquery-%s" % dataset_id

            if bq_pipe_id not in pipes or bootstrap_pipes_recreate_pipes is not False:
                logger.info("Found a new global '%s' or recreating an existing pipe - "
                            "generating pipe and/or system..." % dataset_id)

                pipe_config_params = {
                    "pipe_id": bq_pipe_id,
                    "system_id": bq_system_id,
                    "dataset_id": dataset_id,
                    "batch_size": 10000,
                    "table_prefix": bq_table_prefix,
                    "config_group": bootstrap_config_group,
                    "interval": 3600
                }

                pipe_config = json.loads(PIPE_CONFIG_TEMPLATE % pipe_config_params)

                new_pipe_configs[bq_pipe_id] = pipe_config
                if bq_system_id not in new_system_configs or bootstrap_pipes_recreate_pipes is not False:
                    system_params = {
                        "system_id": bq_system_id,
                        "config_group": bootstrap_config_group,
                        "node_url": node_url,
                        "table_prefix": bq_table_prefix
                    }

                    system_config = json.loads(SYSTEM_CONFIG_TEMPLATE % system_params)
                    new_system_configs[bq_system_id] = system_config

        new_configs = []
        for system_id, system_config in new_system_configs.items():
            system = self.connection.get_system(system_id)
            if system is not None:
                logger.info("Overwriting existing system '%s'.." % system_id)
                system.modify(system_config)
            else:
                logger.info("Adding new system '%s'.." % system_id)
                new_configs.append(system_config)

        if new_configs:
            self.connection.add_systems(new_configs)

        new_configs = []
        for pipe_id, pipe_config in new_pipe_configs.items():
            pipe = self.connection.get_pipe(pipe_id)
            if pipe is not None:
                logger.info("Overwriting existing pipe '%s'.." % pipe_id)
                pipe.modify(pipe_config)
            else:
                logger.info("Adding new pipe '%s'.." % pipe_id)
                new_configs.append(pipe_config)

        if new_configs:
            self.connection.add_pipes(new_configs)

        # Wait for pipes to be deployed, then run them
        for pipe_id in new_pipe_configs.keys():
            pipe = self.connection.get_pipe(pipe_id)

            logger.info("Waiting for pipe '%s' to be deployed..." % pipe_id)
            pipe.wait_for_pipe_to_be_deployed(timeout=60*15)
            logger.info("Pipe '%s' has been deployed." % pipe_id)


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

    logger.info("Starting BiqQuery sink version %s" % version)

    if use_multithreaded:
        logger.info("Running in multithreaded mode")
    else:
        logger.info("Running in single threaded mode")

    _batch_size = os.environ.get("BATCH_SIZE")
    try:
        config_batch_size = int(_batch_size)
        logger.info("Using BATCH_SIZE of %s" % config_batch_size)
    except TypeError as e:
        config_batch_size = 1000
        if _batch_size is None:
            logger.info("BATCH_SIZE was not set, falling back to default of 1000")
        else:
            logger.info("BATCH_SIZE '%s' is not an integer, falling back to 1000" % _batch_size)

    if bootstrap_pipes is not False:
        if bq_table_prefix is None:
            logger.error("Told to bootstrap globals, but missing required 'BIGQUERY_TABLE_PREFIX' environment variable")
        else:
            bootstrapper = GlobalBootstrapper(node_connection)

    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.max_request_body_size': 0,
        'server.socket_port': 5000,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
