# gcp-flask-REST-API

Simple REST API to access a database on a GCP SQL instance (or locally for testing & debugging).

/query for queries, and /status for status.

404 error on / by design.

Rename app.yaml-dist to app.yaml and fill out DB information.

NOTE: requires simplejson!

### Reminder:

`gcloud app deploy app.yaml` to deploy

`gcloud app logs tail -s default` to tail the logs