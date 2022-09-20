import copy
import datetime

import pytest
import google.cloud.exceptions
import werkzeug.exceptions

from google.cloud import bigquery
from service.bq_sink import do_receiver_request, count_rows_in_table, SesamSchemaInfo, BQSchemaInfo
from service.bq_sink import filter_and_translate_entity

class TestableRequestReponse:

    def __init__(self, json_response):
        self.json_response = json_response

    def json(self):
        return self.json_response


class TestableSesamConnection:
    """ Class that simulates a node-connection and the methods needed by the sink """

    def __init__(self, node_config, entity_pipe_schemas=None, sesamapi_base_url="http://localhost:9042/api"):
        self.component_config_mapping = {}

        if not sesamapi_base_url.endswith("/"):
            sesamapi_base_url += "/"

        self.sesamapi_base_url = sesamapi_base_url

        for config in node_config:
            self.component_config_mapping[config["_id"]] = config

        self.entity_pipe_schemas = entity_pipe_schemas

    def add_config_elements(self, config):
        for config_element in config:
            config_id = config_element["_id"]
            self.component_config_mapping[config_id] = config_element

    def add_systems(self, systems):
        self.add_config_elements(systems)

    def add_pipes(self, pipes):
        self.add_config_elements(pipes)

    def get_pipe(self, pipe_id):
        if pipe_id in self.component_config_mapping:
            return copy.deepcopy(self.component_config_mapping[pipe_id])

        raise werkzeug.exceptions.NotFound("No pipe with id '%s' found" % pipe_id)

    def do_get_request(self, url):
        if url.endswith("/entity-types/sink"):
            s = url.replace("/entity-types/sink", "")
            pipe_id = s.split("/")[-1]
            if pipe_id in self.entity_pipe_schemas:
                return TestableRequestReponse(copy.deepcopy(self.entity_pipe_schemas[pipe_id]))
            else:
                raise werkzeug.exceptions.NotFound("No entity-type schema for pipe '%s' was found" % pipe_id)

        raise NotImplementedError("Don't know how to handle URL '%s'" % url)


class TestableQueryResult:

    def __init__(self, result):
        self._result = result

    def result(self, *args, **kwargs):
        return self._result


def assert_value(key, value, field_type):
    from service.bq_sink import cast_value
    if field_type in ['object', 'array', 'bytes', 'uuid', 'uri', 'ni', 'string']:
        if not isinstance(value, (list, dict, str)):
            raise AssertionError("The type of property '%s' does not "
                                 "match the schema (%s)" % (key, field_type))
    elif field_type in ["integer", "decimal", "number"]:
        try:
            _value = cast_value(value, stringify=False)
            if not type(_value) in (int, float):
                raise AssertionError("The type of property '%s' does not "
                                     "match the schema (%s)" % (key, field_type))
        except BaseException as e:
            raise AssertionError("The type of property '%s' does not "
                                 "match the schema (%s)" % (key, field_type))
    elif field_type == "boolean":
        if not isinstance(value, bool):
            raise AssertionError("The type of property '%s' does not "
                                 "match the schema (%s)" % (key, field_type))
    elif field_type == "nanoseconds":
        try:
            _value = cast_value(value, stringify=False)
            if not isinstance(_value, str):
                raise AssertionError("The type of property '%s' does not "
                                     "match the schema (%s)" % (key, field_type))
        except BaseException as e:
            raise AssertionError("The type of property '%s' does not "
                                 "match the schema (%s)" % (key, field_type))


class TestableBQClient:
    """ Class that simulates a BigQuery client and the methods needed to test the sink """

    def __init__(self, schema_info=None, **kwargs):
        self.table_rows = {}
        self.schema_info = schema_info
        self.table_row_indexes = {}

    def query(self, query, *args, **kwargs):
        if query.startswith("SELECT COUNT(*) FROM "):
            s = query.replace("SELECT COUNT(*) FROM ", "")
            table_id = s.replace("`", "")
            if table_id not in self.table_rows:
                google.cloud.exceptions.NotFound("No table '%s' was found" % table_id)

            return TestableQueryResult([[len(self.get_table_rows(table_id))]])
        elif query.find("MERGE") > -1:
            # Merge query - we have to cheat here and just extract the source and target tables
            # and implement the query with python. So if the merge query changes, this code needs to
            # change too

            ix = query.find("MERGE")
            ix2 = query.find("` T")
            target_table_id = query[ix+7:ix2]

            ix = query.find("FROM `")
            ix2 = query.find("GROUP BY _id")

            source_table_id = query[ix+6:ix2]
            source_table_id = source_table_id.replace("\n", "").replace("\t", "").replace("`", "").strip()

            source_table_rows = self.table_rows[source_table_id]
            source_table_indexes = self.table_row_indexes[source_table_id]

            target_table_rows = self.table_rows[target_table_id]
            target_table_indexes = self.table_row_indexes[target_table_id]

            def delete_target_row(target_row_id):
                if source_row_id in target_table_indexes:
                    for row_ix in target_table_indexes[source_row_id]["_id"]:
                        target_table_rows[row_ix] = None

                    target_table_indexes.pop(source_row_id, None)

            # Only keep the newest of the source table rows by deduplicating the source table rows and
            # always look up the latest of any versions we might have
            source_row_ids = list(source_table_indexes.keys())

            for source_row_id in source_row_ids:
                latest_source_row = None
                for source_row_ix in source_table_indexes[source_row_id]["_id"]:
                    source_row = source_table_rows[source_row_ix]
                    if latest_source_row is None:
                        latest_source_row = source_row
                    else:
                        if source_row["_updated"] > latest_source_row["_updated"]:
                            latest_source_row = source_row

                if latest_source_row.get("_deleted", False) is True:
                    # Deleted, remove row (and indexed columns) if it exists
                    delete_target_row(source_row_id)
                elif source_row_id in target_table_indexes:
                    # Row exists: update it by deleting any existing rows and inserting a new one
                    delete_target_row(source_row_id)
                    self.insert_rows_json(target_table_id, [latest_source_row])
                else:
                    # Row is not in table already: just insert it as a new row
                    self.insert_rows_json(target_table_id, [latest_source_row])

            return TestableQueryResult([])

        raise NotImplementedError("Don't know how to handle query '%s'" % query)

    def insert_rows_json(self, table_id, json_rows, row_ids=None, timeout=30):
        if table_id in self.table_rows:
            ix = len(self.table_rows[table_id])

            for row in json_rows:
                keys = sorted(list(row.keys()))
                if row.get("_deleted", False) is True:
                    if keys != ["_deleted", "_id", "_updated"]:
                        raise AssertionError("Deleted entities should only have _id, _deleted "
                                             "and _updated properties: %s" % keys)

                if self.schema_info:
                    # Verify that all entity properties we're about to insert are in the schema
                    translated_keys = list(self.schema_info.property_column_translation.values())
                    for key in keys:
                        translated_key = self.schema_info.translate_key(key)
                        if key not in self.schema_info.seen_field_types and \
                                key not in self.schema_info.valid_internal_properties:
                            raise AssertionError("Property '%s' is not in schema" % key)

                    # Check that the types match the schema
                    for key in keys:
                        if key in self.schema_info.valid_internal_properties:
                            continue

                        bq_field_type = self.schema_info.bigquery_schema[key]
                        field_type = self.schema_info.seen_field_types[key]

                        value = row.get(key)

                        from service.bq_sink import cast_value
                        if bq_field_type.mode == "REPEATED" and isinstance(value, list):
                            # We need to look at the individual items of the list
                            for subvalue in value:
                                assert_value(key, subvalue, bq_field_type)
                        else:
                            assert_value(key, value, field_type)

                self.table_rows[table_id].append(row)

                row_id = row["_id"]
                if row_id not in self.table_row_indexes[table_id]:
                    self.table_row_indexes[table_id][row_id] = {"_id": []}

                self.table_row_indexes[table_id][row_id]["_id"].append(ix)

                ix += 1
        else:
            raise google.cloud.exceptions.NotFound("No table '%s' was found" % table_id)

    def create_table(self, table_obj):
        if isinstance(table_obj, str):
            table_id = table_obj
        else:
            table_id = str(table_obj.reference)
        if table_id in self.table_rows:
            raise google.cloud.exceptions.Conflict("A table with id '%s' already exists" % table_id)

        self.table_rows[table_id] = []
        self.table_row_indexes[table_id] = {}

    def delete_table(self, table_id):
        if table_id not in self.table_rows:
            raise google.cloud.exceptions.NotFound("No table '%s' was found" % table_id)

        self.table_rows.pop(table_id, None)
        self.table_row_indexes.pop(table_id, None)

    def get_table(self, table_id):
        if table_id not in self.table_rows:
            raise google.cloud.exceptions.NotFound("No table '%s' was found" % table_id)

        # Not really used for anything
        if self.schema_info is not None:
            return bigquery.Table(table_id, schema=list(self.schema_info.bigquery_schema.values()))
        else:
            return bigquery.Table(table_id, schema=[])

    def get_table_rows(self, table_id):
        if table_id not in self.table_rows:
            raise google.cloud.exceptions.NotFound("No table '%s' was found" % table_id)

        # Not really used for anything

        return sorted([e for e in self.table_rows[table_id] if e is not None], key=lambda i: i['_updated'])


def test_merge_result():
    # Some simple tests to verify that the unit-test merge algo works as expected

    bq_client = TestableBQClient()

    entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "foo": "1"},
        {"_id": "2", "_updated": 1, "_deleted": False, "foo": "2"},
        {"_id": "3", "_updated": 2, "_deleted": False, "foo": "3"},
        {"_id": "1", "_updated": 3, "_deleted": False, "foo": "1.1"},
        {"_id": "1", "_updated": 4, "_deleted": False, "foo": "1.2"},
        {"_id": "3", "_updated": 5, "_deleted": True},
    ]

    source_table = "my-project.my-dataset.sourcetable"
    target_table = "my-project.my-dataset.targettable"
    source_table_obj = bigquery.Table(source_table)
    target_table_obj = bigquery.Table(target_table)

    bq_client.create_table(source_table_obj)
    bq_client.create_table(target_table_obj)

    bq_client.insert_rows_json(source_table, entities)

    # Fake query only including enough to parse the source and target table from
    merge_query = f"""
    MERGE `{target_table}` T
    FROM `{source_table}`
    GROUP BY _id
    """

    bq_client.query(merge_query)

    table_rows = bq_client.get_table_rows(target_table)

    expected_rows = [
        {"_id": "2", "_updated": 1, "_deleted": False, "foo": "2"},
        {"_id": "1", "_updated": 4, "_deleted": False, "foo": "1.2"},
    ]

    assert table_rows == expected_rows

    bq_client.delete_table(source_table)
    source_table_obj = bigquery.Table(source_table)
    bq_client.create_table(source_table_obj)

    entities = [
        {"_id": "1", "_updated": 6, "_deleted": False, "foo": "1.3"},
        {"_id": "2", "_updated": 7, "_deleted": True},
        {"_id": "3", "_updated": 8, "_deleted": False, "foo": "3.2"},
        {"_id": "3", "_updated": 9, "_deleted": False, "foo": "3.3"},
        {"_id": "3", "_updated": 10, "_deleted": True},
        {"_id": "3", "_updated": 11, "_deleted": False, "foo": "3.5"}
    ]

    bq_client.insert_rows_json(source_table, entities)

    bq_client.query(merge_query)

    expected_rows = [
        {"_id": "1", "_updated": 6, "_deleted": False, "foo": "1.3"},
        {"_id": "3", "_updated": 11, "_deleted": False, "foo": "3.5"}
    ]

    table_rows = bq_client.get_table_rows(target_table)

    assert table_rows == expected_rows


def test_happy_day_test():
    # A "happy day" test with a full and a delta run with a really simple schema
    node_config = [{
        "_id": "global-pipe",
        "metadata": {
            "global": True
        }
    }]

    node_entity_types = {}
    node_entity_types["global-pipe"] = {
        "$id": "/api/pipes/global-pipe/entity-types/sink",
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": True,
        "properties": {
            "$ids": {
                "items": {
                    "metadata": {
                        "namespaces": [
                            "global-pipe"
                        ]
                    },
                    "pattern": "^\\~:global\\-pipe:",
                    "subtype": "ni",
                    "type": "string"
                },
                "type": "array"
            },
            "$replaced": {
                "type": "boolean"
            },
            "code": {
                "type": "integer"
            },
            "global-pipe:FOO": {
                "anyOf": [
                    {
                        "subtype": "decimal",
                        "type": "string"
                    },
                    {
                        "items": {
                            "subtype": "decimal",
                            "type": "string"
                        },
                        "type": "array"
                    }
                ]
            },
        }
    }

    connection = TestableSesamConnection(node_config, entity_pipe_schemas=node_entity_types)

    sesam_schema = SesamSchemaInfo("global-pipe", connection)

    # We can use the sesam-schema for full runs
    bq_client = TestableBQClient(schema_info=sesam_schema)

    entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "code": 1, "global-pipe:FOO": "1"},
        {"_id": "2", "_updated": 1, "_deleted": False, "code": 2, "global-pipe:FOO": "2"},
        {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global-pipe:FOO": ["3", "3.1"]},
    ]

    target_table = "my-project.my-dataset.targettable"

    # Full first run
    params = {
        "pipe_id": "global-pipe",
        "target_table": target_table,
        "is_full": "true",
        "is_first": "true",
        "is_last": "true",
        "sequence_id": "0",
        "request_id": "0",
        "batch_size": 1000
    }

    do_receiver_request(entities, params, bq_client, connection)

    num_rows = count_rows_in_table(bq_client, target_table)
    assert num_rows == 3

    expected_entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "code": 1, "global_pipe__foo": ["1"]},
        {"_id": "2", "_updated": 1, "_deleted": False, "code": 2, "global_pipe__foo": ["2"]},
        {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global_pipe__foo": ["3", "3.1"]},
    ]

    table_entities = bq_client.get_table_rows(target_table)

    assert expected_entities == table_entities

    entities = [
        {"_id": "1", "_updated": 3, "_deleted": False, "code": 1, "global-pipe:FOO": "1.1"},
        {"_id": "2", "_updated": 4, "_deleted": True,  "code": 2, "global-pipe:FOO": "2.1"},
        {"_id": "3", "_updated": 5, "_deleted": False,  "code": 3, "global-pipe:FOO": "3.1"},
        {"_id": "3", "_updated": 6, "_deleted": False,  "code": 3, "global-pipe:FOO": "3.2"},
        {"_id": "3", "_updated": 7, "_deleted": True,  "code": 3, "global-pipe:FOO": "3.3"},
        {"_id": "3", "_updated": 8, "_deleted": False,  "code": 3, "global-pipe:FOO": "3.4"}
    ]

    # For incremental runs we need to simulate an existing BQ table by using a pre-existing bq schema
    existing_bq_schema = list(sesam_schema.bigquery_schema.values())
    bq_schema = BQSchemaInfo(existing_bq_schema, sesam_schema)
    bq_client = TestableBQClient(schema_info=bq_schema)

    bq_client.create_table(target_table)

    # Delta
    params = {
        "pipe_id": "global-pipe",
        "target_table": target_table,
        "is_full": "false",
        "is_first": "true",
        "is_last": "true",
        "sequence_id": "1",
        "request_id": "1",
        "batch_size": 1000
    }

    do_receiver_request(entities, params, bq_client, connection)

    num_rows = count_rows_in_table(bq_client, target_table)
    assert num_rows == 2

    expected_entities = [
        {"_id": "1", "_updated": 3, "_deleted": False, "code": 1, "global_pipe__foo": ["1.1"]},
        {"_id": "3", "_updated": 8, "_deleted": False, "code": 3, "global_pipe__foo": ["3.4"]},
    ]

    table_entities = bq_client.get_table_rows(target_table)

    assert expected_entities == table_entities


def test_insert_array_test():
    # Test inserting array fields
    node_config = [{
        "_id": "global-pipe",
        "metadata": {
            "global": True
        }
    }]

    node_entity_types = {}
    node_entity_types["global-pipe"] = {
        "$id": "/api/pipes/global-pipe/entity-types/sink",
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": True,
        "properties": {
            "$ids": {
                "items": {
                    "metadata": {
                        "namespaces": [
                            "global-pipe"
                        ]
                    },
                    "pattern": "^\\~:global\\-pipe:",
                    "subtype": "ni",
                    "type": "string"
                },
                "type": "array"
            },
            "global-pipe:FOO": {
                "items": {
                    "subtype": "decimal",
                    "type": "string"
                },
                "type": "array"
            },
        }
    }

    connection = TestableSesamConnection(node_config, entity_pipe_schemas=node_entity_types)
    sesam_schema = SesamSchemaInfo("global-pipe", connection)

    bq_client = TestableBQClient(schema_info=sesam_schema)

    entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "global-pipe:FOO": ["1", "1.1"]},
        {"_id": "2", "_updated": 1, "_deleted": False, "global-pipe:FOO": ["2", "2.1"]},
        {"_id": "3", "_updated": 2, "_deleted": False, "global-pipe:FOO": ["3", "3.1"]},
    ]

    target_table = "my-project.my-dataset.targettable"

    # Full first run
    params = {
        "pipe_id": "global-pipe",
        "target_table": target_table,
        "is_full": "true",
        "is_first": "true",
        "is_last": "true",
        "sequence_id": "0",
        "request_id": "0",
        "batch_size": 1000
    }

    do_receiver_request(entities, params, bq_client, connection)

    num_rows = count_rows_in_table(bq_client, target_table)
    assert num_rows == 3

    expected_entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "global_pipe__foo": ["1", "1.1"]},
        {"_id": "2", "_updated": 1, "_deleted": False, "global_pipe__foo": ["2", "2.1"]},
        {"_id": "3", "_updated": 2, "_deleted": False, "global_pipe__foo": ["3", "3.1"]},
    ]

    table_entities = bq_client.get_table_rows(target_table)

    assert expected_entities == table_entities

    entities = [
        {"_id": "1", "_updated": 3, "_deleted": False, "global-pipe:FOO": ["1", "1.2"]},
        {"_id": "2", "_updated": 4, "_deleted": True, "global-pipe:FOO": ["2", "2.2"]},
        {"_id": "3", "_updated": 5, "_deleted": False, "global-pipe:FOO": ["3", "3.2"]},
    ]

    # Delta
    params = {
        "pipe_id": "global-pipe",
        "target_table": target_table,
        "is_full": "false",
        "is_first": "true",
        "is_last": "true",
        "sequence_id": "1",
        "request_id": "1",
        "batch_size": 1000
    }

    # For incremental runs we need to simulate an existing BQ table by using a pre-existing bq schema
    existing_bq_schema = list(sesam_schema.bigquery_schema.values())
    bq_schema = BQSchemaInfo(existing_bq_schema, sesam_schema)
    bq_client = TestableBQClient(schema_info=bq_schema)
    bq_client.create_table(target_table)

    do_receiver_request(entities, params, bq_client, connection)

    num_rows = count_rows_in_table(bq_client, target_table)
    assert num_rows == 2

    expected_entities = [
        {"_id": "1", "_updated": 3, "_deleted": False, "global_pipe__foo": ["1", "1.2"]},
        {"_id": "3", "_updated": 5, "_deleted": False, "global_pipe__foo": ["3", "3.2"]},
    ]

    table_entities = bq_client.get_table_rows(target_table)

    assert expected_entities == table_entities


def test_schema_generation():
    # Test schema generation
    node_config = [{
        "_id": "global-pipe",
        "metadata": {
            "global": True
        }
    }]

    node_entity_types = {}
    node_entity_types["global-pipe"] = {
        "$id": "/api/pipes/global-pipe/entity-types/sink",
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": True,
        "properties": {
            "$ids": {
                "items": {
                    "metadata": {
                        "namespaces": [
                            "global-pipe"
                        ]
                    },
                    "pattern": "^\\~:global\\-pipe:",
                    "subtype": "ni",
                    "type": "string"
                },
                "type": "array"
            },
            "description": {
              "type": "string"
            },
            "global-pipe:bar-code": {
              "anyOf": [
                {
                  "subtype": "decimal",
                  "type": "string"
                },
                {
                  "items": {
                    "subtype": "decimal",
                    "type": "string"
                  },
                  "type": "array"
                }
              ]
            },
            "zendesk-user:user_fields": {
                "additionalProperties": True,
                "properties": {},
                "type": "object"
            },
            "global-user:organization_id-ni": {
              "metadata": {
                "namespaces": [
                  "global-organization"
                ]
              },
              "pattern": "^\\~:global\\-organization:",
              "subtype": "ni",
              "type": "string"
            },
            "global-pipe:some_flag": {
              "type": "boolean"
            },
            "global-person:department-name": {
              "anyOf": [
                {
                  "type": "null"
                },
                {
                  "type": "string"
                }
              ]
            },
            "global-pipe:FOO": {
                "items": {
                    "subtype": "decimal",
                    "type": "string"
                },
                "type": "array"
            },
        }
    }

    connection = TestableSesamConnection(node_config, entity_pipe_schemas=node_entity_types)

    schema = SesamSchemaInfo("global-pipe", connection)

    assert schema.pipe_schema_url == "http://localhost:9042/api/pipes/global-pipe/entity-types/sink"

    assert schema.cast_columns == ['_ids', 'global_pipe__bar_code', 'global_pipe__foo',
                                   'global_user__organization_id_ni', 'zendesk_user__user_fields']

    translated_properties = {
        '$ids': '_ids',
        '_deleted': '_deleted',
        '_id': '_id',
        '_updated': '_updated',
        'description': 'description',
        'global-pipe:bar-code': 'global_pipe__bar_code',
        'zendesk-user:user_fields': 'zendesk_user__user_fields',
        'global-user:organization_id-ni': 'global_user__organization_id_ni',
        'global-pipe:some_flag': 'global_pipe__some_flag',
        'global-person:department-name': 'global_person__department_name',
        'global-pipe:FOO': 'global_pipe__foo'
    }

    result = {}
    for key in translated_properties:
        result[key] = schema.translate_key(key)

    assert result == translated_properties

    assert schema.bigquery_schema["_ids"].mode == "REPEATED"
    assert schema.bigquery_schema["_ids"].is_nullable is False
    assert schema.bigquery_schema["_ids"].field_type == "STRING"

    assert schema.bigquery_schema["_deleted"].mode == "NULLABLE"
    assert schema.bigquery_schema["_deleted"].is_nullable is True
    assert schema.bigquery_schema["_deleted"].field_type == "BOOLEAN"

    assert schema.bigquery_schema["_id"].mode == "NULLABLE"
    assert schema.bigquery_schema["_id"].is_nullable is True
    assert schema.bigquery_schema["_id"].field_type == "STRING"

    assert schema.bigquery_schema["_updated"].mode == "NULLABLE"
    assert schema.bigquery_schema["_updated"].is_nullable is True
    assert schema.bigquery_schema["_updated"].field_type == "INTEGER"

    assert schema.bigquery_schema["description"].mode == "NULLABLE"
    assert schema.bigquery_schema["description"].is_nullable is True
    assert schema.bigquery_schema["description"].field_type == "STRING"

    assert schema.bigquery_schema["global_pipe__bar_code"].mode == "REPEATED"
    assert schema.bigquery_schema["global_pipe__bar_code"].is_nullable is False
    assert schema.bigquery_schema["global_pipe__bar_code"].field_type == "BIGNUMERIC"

    assert schema.bigquery_schema["zendesk_user__user_fields"].mode == "NULLABLE"
    assert schema.bigquery_schema["zendesk_user__user_fields"].is_nullable is True
    assert schema.bigquery_schema["zendesk_user__user_fields"].field_type == "STRING"

    assert schema.bigquery_schema["zendesk_user__user_fields"].mode == "NULLABLE"
    assert schema.bigquery_schema["zendesk_user__user_fields"].is_nullable is True
    assert schema.bigquery_schema["zendesk_user__user_fields"].field_type == "STRING"

    assert schema.bigquery_schema["global_user__organization_id_ni"].mode == "NULLABLE"
    assert schema.bigquery_schema["global_user__organization_id_ni"].is_nullable is True
    assert schema.bigquery_schema["global_user__organization_id_ni"].field_type == "STRING"

    assert schema.bigquery_schema["global_pipe__some_flag"].mode == "NULLABLE"
    assert schema.bigquery_schema["global_pipe__some_flag"].is_nullable is True
    assert schema.bigquery_schema["global_pipe__some_flag"].field_type == "BOOLEAN"

    assert schema.bigquery_schema["global_person__department_name"].mode == "NULLABLE"
    assert schema.bigquery_schema["global_person__department_name"].is_nullable is True
    assert schema.bigquery_schema["global_person__department_name"].field_type == "STRING"

    assert schema.bigquery_schema["global_pipe__foo"].mode == "REPEATED"
    assert schema.bigquery_schema["global_pipe__foo"].is_nullable is False
    assert schema.bigquery_schema["global_pipe__foo"].field_type == "BIGNUMERIC"


def test_entity_filtering():
    node_config = [{
        "_id": "global-pipe",
        "metadata": {
            "global": True
        }
    }]

    node_entity_types = {}
    node_entity_types["global-pipe"] = {
        "$id": "/api/pipes/global-pipe/entity-types/sink",
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": True,
        "properties": {
            "$ids": {
                "items": {
                    "metadata": {
                        "namespaces": [
                            "global-pipe"
                        ]
                    },
                    "pattern": "^\\~:global\\-pipe:",
                    "subtype": "ni",
                    "type": "string"
                },
                "type": "array"
            },
            "$replaced": {
                "type": "boolean"
            },
            "code": {
                "type": "integer"
            },
            "global-pipe:FOO": {
                "anyOf": [
                    {
                        "subtype": "decimal",
                        "type": "string"
                    },
                    {
                        "items": {
                            "subtype": "decimal",
                            "type": "string"
                        },
                        "type": "array"
                    }
                ]
            },
            "global-pipe:meh": {
                "items": {
                    "type": "string"
                },
                "type": "array"
            },
        }
    }

    connection = TestableSesamConnection(node_config, entity_pipe_schemas=node_entity_types)
    sesam_schema_info = SesamSchemaInfo("global-pipe", connection)

    bq_schema_fields = [
        bigquery.SchemaField('_id', 'STRING', 'NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', 'NULLABLE'),
        bigquery.SchemaField('_updated', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField('_ids', 'STRING', 'REPEATED'),
        bigquery.SchemaField('code', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField("global_pipe__foo", "BIGNUMERIC", mode="REPEATED")
    ]

    target_table = bigquery.Table("my-project.my-dataset.targettable", schema=bq_schema_fields)

    bq_schema_info = BQSchemaInfo(target_table.schema, sesam_schema_info)

    # Verify that the "normal" case with sesam schema == bq schema works as expected

    entity = {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global-pipe:FOO": ["~f3", "~f3.1"]}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=False)

    assert translated_entity == {"_id": "3", "_updated": 2, "_deleted": False,
                                 "code": 3, "global_pipe__foo": ["3.0", "3.1"]}

    # Verify that "extra" properties as passed through when not in lenient mode

    entity = {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global-pipe:FOO": ["~f3", "~f3.1"],
              "additional-property": "~f2"}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=False)

    assert translated_entity == {"_id": "3", "_updated": 2, "_deleted": False,
                                 "code": 3, "global_pipe__foo": ["3.0", "3.1"], "additional-property": "~f2"}

    # Verify that no attempt of rectifying a wrong type is done when not in lenient mode

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": True}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=False)

    assert translated_entity == {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global_pipe__foo": [
        'True']}

    # Check that making the bq and entity-type schema divert also makes "unknown" or wrong-type
    # properties pass through as-is in non-lenient mode
    bq_schema_fields = [
        bigquery.SchemaField('_id', 'STRING', 'NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', 'NULLABLE'),
        bigquery.SchemaField('_updated', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField('_ids', 'STRING', 'REPEATED'),
        # bigquery.SchemaField('code', 'INTEGER', 'NULLABLE'),     # Remove 'code' from the BQ schema
        bigquery.SchemaField("global_pipe__foo", "BIGNUMERIC", mode="REPEATED")
    ]

    target_table = bigquery.Table("my-project.my-dataset.targettable", schema=bq_schema_fields)
    bq_schema_info = BQSchemaInfo(target_table.schema, sesam_schema_info)

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": ["~f3", "~f3.1", True],
              "additional-property": "~f2"}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=False)

    assert translated_entity == {"_id": "3", "_updated": 2, "_deleted": "False",
                                 "code": 3, "global_pipe__foo": ["3.0", "3.1", "True"], "additional-property": "~f2"}

    # Verify that any "unknown" property types gets properly removed in lenient mode

    entity = {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global-pipe:FOO": ["~f3", "~f3.1"],
              "additional-property": "~f2"}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2, "_deleted": False, "global_pipe__foo": ["3.0", "3.1"]}

    # Verify that properties of the wrong type dropped in lenient mode

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": [True, "~f3.0"]}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2}

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": ["~f3.0"]}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2, "global_pipe__foo": ["3.0"]}

    bq_schema_fields = [
        bigquery.SchemaField('_id', 'STRING', 'NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', 'NULLABLE'),
        bigquery.SchemaField('_updated', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField('_ids', 'STRING', 'REPEATED'),
        bigquery.SchemaField('code', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField("global_pipe__foo", "BIGNUMERIC")  # No longer a list
    ]

    target_table = bigquery.Table("my-project.my-dataset.targettable", schema=bq_schema_fields)
    bq_schema_info = BQSchemaInfo(target_table.schema, sesam_schema_info)

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": ["~f3.0"],
              "global-pipe:meh":[1]}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2, "code": 3}

    # Check that casting mis-matching sesam types to a bq string works for the global-pipe:meh property (translated
    # to global_pipe__meh), which we here claim is of type STRING in the BQ table..
    bq_schema_fields = [
        bigquery.SchemaField('_id', 'STRING', 'NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', 'NULLABLE'),
        bigquery.SchemaField('_updated', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField('_ids', 'STRING', 'REPEATED'),
        bigquery.SchemaField('code', 'INTEGER', 'NULLABLE'),
        bigquery.SchemaField("global_pipe__foo", "BIGNUMERIC") , # No longer a list
        bigquery.SchemaField("global_pipe__meh", "STRING")  # String, but not repeated
    ]

    target_table = bigquery.Table("my-project.my-dataset.targettable", schema=bq_schema_fields)
    bq_schema_info = BQSchemaInfo(target_table.schema, sesam_schema_info)

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": ["~f3.0"],
              "global-pipe:meh": [1]}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2, "code": 3, "global_pipe__meh": "[1]"}

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": ["~f3.0"],
              "global-pipe:meh": True}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2, "code": 3, "global_pipe__meh": "true"}

    entity = {"_id": "3", "_updated": 2, "_deleted": "False", "code": 3, "global-pipe:FOO": ["~f3.0"],
              "global-pipe:meh": "~f1.0"}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=True)

    assert translated_entity == {"_id": "3", "_updated": 2, "code": 3, "global_pipe__meh": "1.0"}

    entity = {"_id": "3", "_updated": 2, "_deleted": "Nope", "code": 3, "global-pipe:FOO": ["~f3.0"],
              "global-pipe:meh": {"hm": "mm"}}

    translated_entity = filter_and_translate_entity(copy.deepcopy(entity), bq_schema_info, lenient_mode=False)

    # None-lenient mode will leave unknown properties be and cast any "complex" values to json, if the schema
    # doesn't match it'll likely fail the insert, for example the value of "global_pipe__foo" below is of type
    # BIGNUMERIC and not STRING, and the value of "_deleted" is a string that will be rejected by BQ.
    assert translated_entity == {"_id": "3",  '_deleted': 'Nope', "_updated": 2, "code": 3,
                                 "global_pipe__foo": '["~f3.0"]',
                                 "global_pipe__meh": '{"hm": "mm"}'}


def test_lenient_mode():
    node_config = [{
        "_id": "global-pipe",
        "metadata": {
            "global": True
        }
    }]

    node_entity_types = {}
    node_entity_types["global-pipe"] = {
        "$id": "/api/pipes/global-pipe/entity-types/sink",
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": True,
        "properties": {
            "$ids": {
                "items": {
                    "metadata": {
                        "namespaces": [
                            "global-pipe"
                        ]
                    },
                    "pattern": "^\\~:global\\-pipe:",
                    "subtype": "ni",
                    "type": "string"
                },
                "type": "array"
            },
            "$replaced": {
                "type": "boolean"
            },
            "code": {
                "type": "integer"
            },
            "global-pipe:FOO": {
                "anyOf": [
                    {
                        "subtype": "decimal",
                        "type": "string"
                    },
                    {
                        "items": {
                            "subtype": "decimal",
                            "type": "string"
                        },
                        "type": "array"
                    }
                ]
            },
        }
    }

    connection = TestableSesamConnection(node_config, entity_pipe_schemas=node_entity_types)

    schema_info = SesamSchemaInfo("global-pipe", connection)

    bq_client = TestableBQClient(schema_info=schema_info)

    # Verify that we don't accept properties not in the schema when not in lenient mode
    entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "code": 1, "global-pipe:FOO": "~f1", "additional-property": "~f2"},
        {"_id": "2", "_updated": 1, "_deleted": False, "code": 2, "global-pipe:FOO": "~f2"},
        {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global-pipe:FOO": ["~f3", "~f3.1"]},
    ]

    target_table = "my-project.my-dataset.targettable"

    # Full first run
    params = {
        "pipe_id": "global-pipe",
        "target_table": target_table,
        "is_full": "true",
        "is_first": "true",
        "is_last": "true",
        "sequence_id": "0",
        "request_id": "0",
        "lenient_mode": "false",
        "batch_size": 1000
    }

    with pytest.raises(werkzeug.exceptions.BadRequest) as exc:
        do_receiver_request(copy.deepcopy(entities), params, bq_client, connection)
        assert "not in schema" in str(exc.value)

    # Verify that we don't accept properties that doesn't match the type in the schema
    entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "code": "1", "global-pipe:FOO": "~d1"},
        {"_id": "2", "_updated": 1, "_deleted": False, "code": 2, "global-pipe:FOO": "~f2"},
        {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global-pipe:FOO": ["~d3", "~f3.1"]},
        {"_id": "4", "_updated": 4, "_deleted": False, "code": [4], "global-pipe:FOO": ["~d4", "~d4.1"]},
        {"_id": "5", "_updated": 5, "_deleted": False, "code": 5, "global-pipe:FOO": ["~f5", True]},
    ]

    target_table = "my-project.my-dataset.targettable"

    # Full first run
    params = {
        "pipe_id": "global-pipe",
        "target_table": target_table,
        "is_full": "true",
        "is_first": "true",
        "is_last": "true",
        "sequence_id": "0",
        "request_id": "0",
        "lenient_mode": "false",
        "batch_size": 1000
    }

    with pytest.raises(werkzeug.exceptions.BadRequest) as exc:
        do_receiver_request(copy.deepcopy(entities), params, bq_client, connection)

    assert "match the schema" in str(exc.value)

    # Turn on lenient mode
    params["lenient_mode"] = "true"

    # This time the property should just get dropped and so not trigger the test
    do_receiver_request(copy.deepcopy(entities), params, bq_client, connection)

    num_rows = count_rows_in_table(bq_client, target_table)
    assert num_rows == 5

    # In lenient mode, the properties that contains "misfits" vs the schema should be silently dropped:
    expected_entities = [
        {"_id": "1", "_updated": 0, "_deleted": False, "global_pipe__foo": ["1.0"]},
        {"_id": "2", "_updated": 1, "_deleted": False, "code": 2, "global_pipe__foo": ["2.0"]},
        {"_id": "3", "_updated": 2, "_deleted": False, "code": 3, "global_pipe__foo": ["3.0", "3.1"]},
        {"_id": "4", "_updated": 4, "_deleted": False, "global_pipe__foo": ["4.0", "4.1"]},
        {"_id": "5", "_updated": 5, "_deleted": False, "code": 5},
    ]

    table_entities = bq_client.get_table_rows(target_table)

    assert expected_entities == table_entities
