import os

from flask import Flask, jsonify, request, Blueprint
from flask_cors import CORS

import pymysql
import simplejson as json


db_user = os.environ.get('CLOUD_SQL_USERNAME')
db_password = os.environ.get('CLOUD_SQL_PASSWORD')
db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
db_table_name = os.environ.get('CLOUD_SQL_DATABASE_TABLE_NAME')
db_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')

app = Flask(__name__)

# Enabling Cross Origin Resource Sharing (CORS), making cross-origin AJAX possible.
CORS(app)

###
#    Queries database for full row of data
#    Returns sql query text
def db_lookup(
    conn,
    indicator=None,
    indicator_description=None,
    country_iso_code=None,
    country_name=None,
    area_name=None,
    geographic_scope=None,
    year=None,
    sex=None,
    age=None,
    population_segment=None,
    population_sub_group=None,
    value=None,
    value_comment=None,
    unit_format=None,
    source_organization=None,
    source_database=None,
    source_year=None,
    notes=None,
    modality=None,
    modality_category=None,
    import_file=None,
    import_timestamp=None,
):
    sql = None  # Avoids warnings
    if indicator:
        sql = f"select * from `{db_table_name}` where `indicator`='{indicator}'"

        if indicator_description:
            sql += f" AND `indicator_description`='{indicator_description}'"
        if country_iso_code:
            sql += f" AND `country_iso_code`='{country_iso_code}'"
        if country_name:
            sql += f" AND `country_name`='{country_name}'"
        if area_name:
            sql += f" AND `area_name`='{area_name}'"
        if geographic_scope:
            sql += f" AND `geographic_scope`='{geographic_scope}'"
        if year:
            sql += f" AND `year`='{year}'"
        if sex:
            sql += f" AND `sex`='{sex}'"
        if age:
            sql += f" AND `age`='{age}'"
        if population_segment:
            sql += f" AND `population_segment`='{population_segment}'"
        if population_sub_group:
            sql += f" AND `population_sub_group`='{population_sub_group}'"
        if value:
            sql += f" AND `value`='{value}'"
        if value_comment:
            sql += f" AND `value_comment`='{value_comment}'"
        if unit_format:
            sql += f" AND `unit_format`='{unit_format}'"
        if source_organization:
            sql += f" AND `source_organization`='{source_organization}'"
        if source_database:
            sql += f" AND `source_database`='{source_database}'"
        if source_year:
            sql += f" AND `source_year`='{source_year}'"
        if notes:
            sql += f" AND `notes`='{notes}'"
        if modality:
            sql += f" AND `modality`='{modality}'"
        if modality_category:
            sql += f" AND `modality_category`='{modality_category}'"
        if import_file:
            sql += f" AND `import_file`='{import_file}'"
        if import_timestamp:
            sql += f" AND `import_timestamp`='{import_timestamp}'"

    elif country_name:
        sql = f"select * from `{db_table_name}` where `country_name`='{country_name}'"

    # At this point, we might have 'NULL' or 'null' values, and we need to remove the quotes and replace "="" with "IS"
    if "NULL" in sql.upper():
        sql = sql.replace("='null'", "is NULL")
        sql = sql.replace("='NULL'", "is NULL")

    # This is a minimum-overhead trick to force a database reconnect if conenction is broken, for example due to database server rebooting
    # conn.ping(reconnect=True)

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
        except Exception as e:
            print(f"ERROR: {e}")
            result = None
    conn.close

    return result


# Only used to get status
@app.route('/status')
def main():
    # When deployed to App Engine, the `GAE_ENV` environment variable will be
    # set to `standard`
    if os.environ.get('GAE_ENV') == 'standard':
        # If deployed, use the local socket interface for accessing Cloud SQL
        unix_socket = '/cloudsql/{}'.format(db_connection_name)
        cnx = pymysql.connect(user=db_user, password=db_password, unix_socket=unix_socket, db=db_name)
    else:
        # If running locally, use the TCP connections instead
        # Set up Cloud SQL Proxy (cloud.google.com/sql/docs/mysql/sql-proxy)
        # so that your application can use 127.0.0.1:3306 to connect to your
        # Cloud SQL instance
        host = '10.0.0.90'
        cnx = pymysql.connect(user=db_user, password=db_password, host=host, db=db_name)

    with cnx.cursor() as cursor:
        cursor.execute('SELECT NOW() as now;')
        result = cursor.fetchall()
        current_time = result[0][0]
    cnx.close()

    return str(f"{current_time} - database connection to {db_connection_name} successful")


# The main query route
@app.route("/query", methods=["GET"])
def get_entry():
    """Gets encoded string from URL, decodes and searches database.

    Arguments:
        slug {str} -- URL encoded string

    Returns:
        json string -- array of dicts
    """
    # When deployed to App Engine, the `GAE_ENV` environment variable will be
    # set to `standard`
    if os.environ.get('GAE_ENV') == 'standard':
        # If deployed, use the local socket interface for accessing Cloud SQL
        unix_socket = '/cloudsql/{}'.format(db_connection_name)
        cnx = pymysql.connect(user=db_user, password=db_password, unix_socket=unix_socket, db=db_name)
    else:
        # If running locally, use the TCP connections instead
        # Set up Cloud SQL Proxy (cloud.google.com/sql/docs/mysql/sql-proxy)
        # so that your application can use 127.0.0.1:3306 to connect to your
        # Cloud SQL instance
        host = '10.0.0.90'
        cnx = pymysql.connect(user=db_user, password=db_password, host=host, db=db_name)

    indicator = request.args.get('indicator')  # this notation raises exception if not found
    indicator_description = request.args.get('indicator_description')
    country_iso_code = request.args.get('country_iso_code')
    country_name = request.args.get('country_name')
    area_name = request.args.get('area_name')
    geographic_scope = request.args.get('geographic_scope')
    year = request.args.get('year')
    sex = request.args.get('sex')
    age = request.args.get('age')
    population_segment = request.args.get('population_segment')
    population_sub_group = request.args.get('population_sub_group')
    value = request.args.get('value')
    value_comment = request.args.get('value_comment')
    unit_format = request.args.get('unit_format')
    source_organization = request.args.get('source_organization')
    source_database = request.args.get('source_database')
    source_year = request.args.get('source_year')
    notes = request.args.get('notes')
    modality = request.args.get('modality')
    modality_category = request.args.get('modality_category')
    import_file = request.args.get('import_file')
    import_timestamp = request.args.get('import_timestamp')

    if indicator:
        result = db_lookup(
            cnx,
            indicator=indicator,
            indicator_description=indicator_description,
            country_iso_code=country_iso_code,
            country_name=country_name,
            area_name=area_name,
            geographic_scope=geographic_scope,
            year=year,
            sex=sex,
            age=age,
            population_segment=population_segment,
            population_sub_group=population_sub_group,
            value=value,
            value_comment=value_comment,
            unit_format=unit_format,
            source_organization=source_organization,
            source_database=source_database,
            source_year=source_year,
            notes=notes,
            modality=modality,
            modality_category=modality_category,
            import_file=import_file,
            import_timestamp=import_timestamp,
        )
        rows_returned = len(result)

        # Too many rows returned
        if rows_returned > 300:
            resp = jsonify({"error": f"too many rows ({rows_returned}) returned - limit is 300!"})
            resp.status_code = 400

        # No rows returned
        elif not rows_returned:  # Indicator name not found
            resp = jsonify({"error": "indicator not found"})
            resp.status_code = 401

        # Found rows, and less than 300 - SUCCESS!
        else:
            try:
                resp = jsonify(result)
                resp.status_code = 200
            except TypeError as e:
                print(f'ERROR: {e}')
                print(f'result: {result}')
                exit(1)

    else:  # no "indicator" key in query
        resp = jsonify({"error": "key 'indicator' is required"})
        resp.status_code = 400

    return resp


# For local testing - ignored in the cloud instance
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
