==========================
BigQuery Sink Microservice
==========================

A python microservice for receiving a JSON entity stream from a Sesam service instance and inserting, updating or
deleting corresponding rows in a Google Bigquery table.

::

  $ python3 service/bq_sink.py
   * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
   * Restarting with stat
   * Debugger is active!
   * Debugger pin code: 260-787-156

The service listens on port 5000.

JSON entities can be posted to 'http://localhost:5000/receiver'.

The receiver understands these request (URL) parameters:

* ``pipe_id`` - the pipe to use as source for the bigquery schema
* ``target_table`` - the name of the table to write to
* ``batch_size`` - maximum number of rows to insert via the bigquery client per operation, default 1000. Reduce it if you experience limit related errors or hanging client connections.
* ``lenient_mode`` - set to ``true`` if you want the pipe to be lenient, i.e. skip unknown properties or properties that do not fit the schema (default is to fail)

Note that if ``pipe_id`` is present in the URL then ``target_table`` must be as well.

Environment variables
---------------------

The service takes a single environment variable named ``CONFIG``. This should be a string representing a JSON object
with the following properties:

``google_application_credentials`` - the google credentials to use (service account associated token in json form)

``jwt_token`` - valid jwt token to the node the MS runs in. The role of the token needs permission to create pipes and systems if ``bootstrap_pipes`` is also set.'

``node_url`` - public url to the sesam service the MS runs within - it should end in '/api'.

``bigquery_table_prefix`` - a path prefix to use for the biqquery tables, representing a bq project path - see https://cloud.google.com/bigquery/docs/tables#table_naming

``target_table`` - if not in the "receiver" endpoint URL during run time, you can set it in this variable

``pipe_id`` - if not in the "receiver" endpoint URL during run time, you can set it in this variable

``bootstrap_pipes`` - if set to 'true' will bootstrap one endpoint pipe for each global present in the node

``bootstrap_single_system`` - if set to 'true' will use a single system with the id "bigquery" for all bootstrapped pipes - note that if changed after the node has already been bootstrapped, it will not remove any existing systems created

``bootstrap_config_group`` - the config group to generate pipes and systems in, if not set defaults to "analytics"

``bootstrap_pipes_lenient_mode`` - boolean flag to control if all pipes should be created with ``lenient_mode`` to ``true`` (false by default)

``bootstrap_interval`` - how often (in hours) to rerun the bootstrapping process - the default is 24

``bootstrap_recreate_pipes`` - overwrite existing pipes and system when bootstrapping - mostly useful if the template has changed

``bootstrap_docker_image_name`` - customize the docker image name to use when bootrapping systems, defaults to "sesamcommunity/google-bigquery-sink:development"

``batch_size`` - maximum number of rows to insert via the bigquery client per operation, default 1000. Reduce it if you experience limit related errors or hanging client connections.

Note that if you set up the MS to bootstrap a node, you should name it in such a way that it will not be overwritten by the generated systems which all will start with "bigquery", for instance use the id "bootstrap-bigquery".

Global pipe metadata
--------------------

When bootstrapping pipes that read from the globals, you can override the generated target table name by setting the "bigquery-name"
property in the global pipe's "metadata". Make sure it follows the Bigquery table naming conventions: https://cloud.google.com/bigquery/docs/tables#table_naming

Example config:
---------------

::

    {
      "_id": "bigquery-foo",
      "type": "system:microservice",
      "docker": {
        "environment": {
          "CONFIG": {
            "bigquery_table_prefix": "my-sesam-project.my-dataset",
            "google_application_credentials": "$SECRET(bigquery-credentials)",
            "jwt_token": "$SECRET(bigquery-ms-jwt)",
            "node_url": "https://your-sesam-service/api"
          }
        },
        "image": "sesamcommunity/google-bigquery-sink:development",
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
        "url": "receiver?pipe_id=global-foo&target_table=your-project.your-dataset.global-foo"
      },
      "pump": {
        "fallback_to_single_entities_on_batch_fail": false,
        "schedule_interval": 3600
      },
      "batch_size": 10000,
      "remove_namespaces": false
    }
