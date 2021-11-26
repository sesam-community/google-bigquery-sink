# BigQueryMicroservice

Code that writes to big query is in SmallScale folder.
Run it with "python3 test.py". The json object pushed is currently hard coded to the program.

Currently using dml to create a new table. Put the path of your own bigquery authentication key 
in the credentials_path variable. The key needs the administrator role to create a new table.
