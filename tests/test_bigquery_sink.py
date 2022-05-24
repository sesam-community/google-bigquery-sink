
from service.datasink

class TestableSesamConnection:
    """ Class that simulates a node-connection and the methods needed by the sink """

    def __init__(self, node_config, entity_pipe_schemas=None):
        self.node_config = node_config
        self.entity_pipe_schemas = entity_pipe_schemas

    def add_systems(self, config):
        pass

    def add_pipes(self, pipes):
        pass

    def get_pipe(self, pipe_id):
        pass

    def do_get_request(self, url):
        pass


class TestableQueryResult:

    def __init__(self):
        pass

    def result(self, **kwargs):
        return []


class TestableBQClient:
    """ Class that simulates a BigQuery client and the methods needed to test the sink """

    def __init__(self, **kwargs):
        pass

    def query(self, query):
        pass

    def insert_rows_json(self, table_id, json_rows, row_ids=None, timeout=30):
        pass

    def create_table(self, table_obj):
        pass

    def delete_table(self, table_id):
        pass

    def get_table(self, table_id):
        pass


def test_happy_day_test():

    node_config = [{
        "_id": "global-pipe",
        "metadata": {
            "global": True
        }
    }]

    node_entity_types = {
        "global-pipe": {
            {
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
                    "_deleted": {
                        "type": "boolean"
                    },
                    "_id": {
                        "type": "string"
                    },
                    "_updated": {
                        "type": "integer"
                    },
                    "code": {
                        "type": "string"
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
        }
    }

    connection = TestableSesamConnection(node_config, entity_pipe_schemas=node_entity_types)
    bq_client = TestableBQClient()

    entities = []

