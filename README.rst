=====================
BigQuery Microservice
=====================

A python microservice for receiving a JSON entity stream from a Sesam service instance and inserting, updating or
deleting corresponding rows in a Google Bigquery table.

::

  $ python3 service/datasink-service.py
   * Running on http://0.0.0.0:5001/ (Press CTRL+C to quit)
   * Restarting with stat
   * Debugger is active!
   * Debugger pin code: 260-787-156

The service listens on port 5000.

JSON entities can be posted to 'http://localhost:5000/receiver'.

The receiver can take two parameters:

* pipe_id - the pipe to use as source for the bigquery schema
* target_table - the name of the table to write to
* batch_size - maximum number of rows to insert via the bigquery client per operation, default 1000. Reduce it if you experience limit related errors or hanging client connections.

Note that if 'pipe_id' is present in the URL then 'target_table' must be as well.

Environment variables
---------------------

GOOGLE_APPLICATION_CREDENTIALS - the google credentials to use (service account associated token in json form)

JWT_TOKEN - valid jwt token to the node the MS runs in. The role of the token needs permission to create pipes and systems if 'BOOTSTRAP_PIPES' is also set.'

NODE_URL - public url to the sesam service the MS runs within - it should end in '/api'.

BIGQUERY_TABLE_PREFIX - a path prefix to use for the biqquery tables, representing a bq project path

TARGET_TABLE - if not in the "receiver" endpoint URL during run time, you can set it in this variable

PIPE_ID - if not in the "receiver" endpoint URL during run time, you can set it in this variable

BOOTSTRAP_PIPES - if set to 'true' will bootstrap one endpoint pipe for each global present in the node

BOOTSTRAP_SINGLE_SYSTEM - if set to 'true' will use a single system with the id "bigquery" for all bootstrapped pipes - note that if changed after the node has already been bootstrapped, it will not remove any existing systems created

BOOTSTRAP_CONFIG_GROUP - the config group to generate pipes and systems in, if not set defaults to "analytics"

BOOTSTRAP_INTERVAL - how often (in hours) to rerun the bootstrapping process - the default is 24

BOOTSTRAP_RECREATE_PIPES - overwrite existing pipes and system when bootstrapping - mostly useful if the template has changed

BATCH_SIZE - maximum number of rows to insert via the bigquery client per operation, default 1000. Reduce it if you experience limit related errors or hanging client connections.

Note that if you set up the MS to bootstrap a node, you should name it in such a way that it will not be overwritten by the generated systems which all will start with "bigquery", for instance use the id "bootstrap-bigquery".

Example config:
---------------

::

    {
      "_id": "bigquery-foo",
      "type": "system:microservice",
      "docker": {
        "environment": {
          "BIGQUERY_TABLE_PREFIX": "my-sesam-project",
          "GOOGLE_APPLICATION_CREDENTIALS": "$SECRET(bigquery-credentials)",
          "JWT_TOKEN": "$SECRET(bigquery-ms-jwt)",
          "NODE_URL": "https://your-sesam-service/api"
        },
        "image": "sesamcommunity/sesam-bigquery:latest",
        "memory": 1512,
        "port": 5000
      },
      "read_timeout": 7200,
      "use_https": false,
      "verify_ssl": false
    }

Example usage:
--------------

::

    {
      "_id": "bigquery-global-category-share",
      "type": "pipe",
      "source": {
        "type": "dataset",
        "dataset": "global-foo"
      },
      "sink": {
        "type": "json",
        "system": "bigquery-foo",
        "batch_size": 10000,
        "url": "receiver?pipe_id=global-foo&target_table=global-foo"
      },
      "pump": {
        "fallback_to_single_entities_on_batch_fail": false,
        "schedule_interval": 3600
      },
      "batch_size": 10000,
      "remove_namespaces": false
    }
