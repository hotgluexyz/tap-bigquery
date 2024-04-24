#!/usr/bin/env python3
import os
import time
import json
import singer
import datetime
import urllib3
import requests
import re
import pytz
from socket import error as SocketError
from socket import timeout as SocketTimeout
from ssl import SSLError as BaseSSLError
from datetime import timedelta, timezone
from singer import Transformer, utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from google.oauth2 import service_account
from google.api_core import retry
from google.cloud import bigquery

REQUIRED_CONFIG_KEYS = ['start_date', 'project', 'credentials_path']
LOGGER = singer.get_logger()

TYPE_MAP = {
    'STRING': { 'type': 'string'},
    'BOOLEAN':  { 'type': 'boolean' },
    'BOOL':  { 'type': 'boolean' },
    'INTEGER': { 'type': 'integer' },
    'INT64': { 'type': 'integer' },
    'FLOAT': { 'type': 'number', 'format': 'float' },
    'FLOAT64': { 'type': 'number', 'format': 'float' },
    'NUMERIC': { 'type': 'number', 'format': 'float' },
    'TIMESTAMP': { 'type': 'string', 'format': 'date-time' },
    'DATETIME': { 'type': 'string', 'format': 'date-time' },
    'DATE': { 'type': 'string', 'format': 'date' },
    'TIME': { 'type': 'string', 'format': 'time' },
    # TODO: 'BYTES' - I'm not sure how this comes, maybe a list of ints?
}

def convert_schemafield_to_jsonschema(schemafields):
    jsonschema = { 'type': 'object', 'properties': {} }

    for schemafield in schemafields:
        if schemafield.field_type in TYPE_MAP:
            jsonschema['properties'][schemafield.name] = TYPE_MAP[schemafield.field_type].copy()
        elif schemafield.field_type == 'RECORD' or schemafield.field_type == 'STRUCT' or schemafield.field_type == 'JSON':
            jsonschema['properties'][schemafield.name] = convert_schemafield_to_jsonschema(schemafield.fields)
        else:
            raise NotImplementedError(f"Field type not supported: {schemafield.field_type}")

        if schemafield.mode == 'NULLABLE':
            type = jsonschema['properties'][schemafield.name]['type']
            jsonschema['properties'][schemafield.name]['type'] = ['null', type]
        elif schemafield.mode == 'REPEATED':
            jsonschema['properties'][schemafield.name] = {
                'type': 'array',
                'items': jsonschema['properties'][schemafield.name]
            }
        jsonschema['properties'][schemafield.name]['description'] = schemafield.description
    return jsonschema


def discover(config, client):
    project = config["project"]
    streams = []

    for query in config.get("queries", []):
        # for each query define the schema of the query based on the fields in the query
        # and the type of the fields
        query = query.copy()
        query_name = query.pop("name")
        query_sql = query.pop("query")

        if query_sql.endswith(";"):
            query_sql = query_sql[:-1]

        mod_query_sql = f"{query_sql} LIMIT 1"

        if "{replication_key_condition}" in query_sql:
            replication_key = query.get("replication_key_field")
            if replication_key is None:
                raise ValueError("replication_key_field must be provided when using {replication_key_condition}")
            
            # infer type of rep_key
            # get project_dataset
            table_path = re.search(r'FROM `([^`]+)`', query_sql).group(1)
            table_path = table_path.split(".")
            project_dataset = '.'.join(table_path[:2])
            bq_table_name = table_path[-1]
            # query rep_key type
            rep_key_query = f"SELECT data_type FROM `{project_dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{bq_table_name}' AND column_name= '{replication_key}'"
            query_job = client.query(rep_key_query)
            results = query_job.result()
            for row in results:
                rep_key_type = row[0]     

            if rep_key_type == "DATETIME":
                start_date = config.get("start_date")
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%fZ')
                # format datetime as accepted by BQ
                start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
            mod_query_sql = mod_query_sql.replace("{replication_key_condition}", f"{replication_key} >=  {rep_key_type}('{start_date}')")

        try:
            results = client.query(mod_query_sql).result()
        except Exception as e:
            LOGGER.error(f"Error running query: {mod_query_sql}")
            LOGGER.error(f"Query: {query_sql}")
            raise

        results = list(results)
        if len(results) == 0:
            raise ValueError(f"Query {query_name} returned no results, so it's impossible to infer the schema. Please check the query.")

        original_row = {}

        schema = {}
        for row in results:
            original_row = dict(row)
            schema = {
                "type": "object", "properties": {
                    k: {
                        'type': ['string', 'null'],
                        "description": None
                    } for k in original_row.keys()
                }
            }

        schema = Schema.from_dict(schema)
        stream_metadata = [{
            'metadata': {
                'inclusion': 'available',
                'selected': True,
                'table-name': query_name,
                'query': query_sql
            },
            'breadcrumb': []
        }] + [
            {
                'breadcrumb': [
                    'properties',
                    f
                ],
                'metadata': {
                    'inclusion': 'available',
                    'selected-by-default': True,
                },
            } for f in original_row.keys()
        ]

        replication_method = "FULL_TABLE" if not "{replication_key_condition}" in query_sql else "INCREMENTAL"
        replication_key_field = query.get("replication_key_field") if replication_method == "INCREMENTAL" else None

        streams.append(
            CatalogEntry(
                tap_stream_id=query_name,
                stream=query_name,
                schema=schema,
                key_properties=[],
                metadata=stream_metadata,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=replication_method,
                replication_key=replication_key_field
            )
        )


    if config.get('only_discover_queries', False):
        return Catalog(streams)

    for dataset in client.list_datasets(project):
        for t in client.list_tables(dataset.dataset_id):
            full_table_id = f"{project}.{dataset.dataset_id}.{t.table_id}"

            # Load the full table details to get the schema
            table = client.get_table(full_table_id)
            partition_type = None
            partition_size = None
            replication_key = None

            if t.time_partitioning:
                replication_key = table.time_partitioning.field
                partition_type = table.time_partitioning.type_

                if partition_type == 'DAY':
                    partition_size = timedelta(days=1)
                elif partition_type == 'HOUR':
                    partition_size = timedelta(hour=1)
                else:
                    LOGGER.info(f"Skipping table {t.full_table_id}: Unsupported partition type: {partition_type}")
                    continue

            schema = Schema.from_dict(convert_schemafield_to_jsonschema(table.schema))

            # Try and guess required properties by looking for required fields that end in "id"
            # if this doesn't work, users can always specify their own key-properties with catalog
            key_properties = [s.name for s in table.schema if s.mode == 'REQUIRED' and s.name.lower().endswith("id")]
            additional_metadata = [
                {
                    'breadcrumb': [
                        'properties',
                        s.name
                    ],
                    'metadata': {
                        'inclusion': 'available',
                        'selected-by-default': True,
                    },
                } for s in table.schema
            ]
            metadata = {
                'inclusion': 'available',
                'selected': True,
                'table-name': full_table_id,
                'table-created-at': utils.strftime(table.created),
                'table-labels': table.labels,
            }

            if replication_key is not None:
                metadata['replication-key'] = replication_key

            if partition_size is not None:
                metadata['table-partition-size'] = partition_size.total_seconds(),

            stream_metadata = [{
                'metadata': metadata,
                'breadcrumb': []
            }] + additional_metadata

            stream_name = full_table_id.replace(".", "__").replace('-', '_')
            if replication_key is not None:
                streams.append(
                    CatalogEntry(
                        tap_stream_id=stream_name,
                        stream=f"{dataset.dataset_id}_{table.table_id}",
                        schema=schema,
                        key_properties=key_properties,
                        metadata=stream_metadata,
                        replication_key=replication_key,
                        is_view=None,
                        database=None,
                        table=None,
                        row_count=None,
                        stream_alias=None,
                        replication_method='INCREMENTAL',
                    )
                )
            else:
                streams.append(
                    CatalogEntry(
                        tap_stream_id=stream_name,
                        stream=f"{dataset.dataset_id}_{table.table_id}",
                        schema=schema,
                        key_properties=key_properties,
                        metadata=stream_metadata,
                        is_view=None,
                        database=None,
                        table=None,
                        row_count=None,
                        stream_alias=None,
                        replication_method='FULL_TABLE',
                    )
                )

    return Catalog(streams)

def localize_datetime(datetime):
    utc_timezone = pytz.timezone('UTC')
    if datetime.tzinfo is None:
        datetime = utc_timezone.localize(datetime)
    return datetime


def sync(config, state, catalog, client):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        rep_key_type = None
        schema = stream.schema.to_dict()
        stream_name = stream.table or stream.stream or stream.tap_stream_id
        singer.write_schema(
            stream_name=stream_name,
            schema=schema,
            key_properties=stream.key_properties,
        )

        start_date = utils.strptime_to_utc(state.get(stream.tap_stream_id, config['start_date']))
        base_metadata = metadata.to_map(stream.metadata)[()]
        table_name = base_metadata.get('table-name')
        original_query = base_metadata.get('query')
        fetch_data = True
        written_records = False

        # A. Logic for inc syncs streams
        if stream.replication_key is not None:
            LOGGER.info(f"Stream replication key: {stream.replication_key}")

            step = None
            if base_metadata.get('table-partition-size'):
                step = timedelta(seconds=base_metadata.get('table-partition-size'))

            with Transformer() as transformer:
                params = {
                    'table_name': table_name,
                    'replication_key': stream.replication_key,
                }

                # Check type of replication_key
                # get project.dataset
                table_path = re.search(r'FROM `([^`]+)`', original_query).group(1)
                table_path = table_path.split(".")
                project_dataset = '.'.join(table_path[:2])
                # get bigquery table name
                bq_table_name = table_path[-1]
                # add values to params to add them to the querys
                params["project_dataset"] = project_dataset
                params["bq_table_name"] = bq_table_name
                # get rep_key type from bigquery
                if not rep_key_type:
                    rep_key_query = "SELECT data_type FROM `{project_dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{bq_table_name}' AND column_name= '{replication_key}'".format(**params)
                    query_job = client.query(rep_key_query)
                    results = query_job.result()
                    for row in results:
                        rep_key_type = row[0]
                # add rep_key_type to params
                params["rep_key_type"] = rep_key_type 

                # Logic for time partitioned table streams
                if step:
                    now = datetime.datetime.now(timezone.utc)
                    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    while start_date < now and fetch_data:
                        # prepare params
                        end_date = start_date + step
                        params.update({
                            'start_date': start_date,
                            'end_date': end_date
                        })
 
                        if rep_key_type == "DATETIME":
                            params["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
                            params["end_date"] = end_date.strftime("%Y-%m-%d %H:%M:%S.%f")

                        # prepare query
                        replication_key_conditional = "{replication_key} > {rep_key_type}('{start_date}') AND {replication_key} <= {rep_key_type}('{end_date}')  ORDER BY {replication_key} ASC;"

                        if original_query is None:
                            query = ("""SELECT * FROM `{table_name}` WHERE""" + replication_key_conditional).format(**params)
                        else:
                            query = original_query.replace("{replication_key_condition}", replication_key_conditional).format(**params)

                        attempts = 0
                        while True:
                            try:
                                LOGGER.info("Running query:\n    %s" % query)
                                query_job = client.query(query)
                                results = query_job.result()

                                for row in query_job:
                                    record = { k: v for k, v in row.items() }
                                    record = deep_convert_datetimes(record)
                                    record = transformer.transform(record, schema)
                                    singer.write_record(stream_name, record)
                                break
                            except (TimeoutError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError, SocketTimeout, BaseSSLError, SocketError, OSError) as e:
                                LOGGER.warn(e)
                                attempts += 1
                                if attempts > 3:
                                    time.sleep(2**attempts)
                                    pass
                                raise e

                        # increase the start_date by the step (partition time)
                        start_date = round_to_partition(start_date + step, step)

                    state[stream.tap_stream_id] = start_date.isoformat()
                    singer.write_state(state)
        
                # Logic for non time partitioned tables with rep_key
                else:
                    #1. Get params for query
                    params.update({
                        'table_name': table_name,
                        'replication_key': stream.replication_key,
                        'start_date': start_date,
                    })

                    # greates_date will be used to write to the state, it starts beign the same as the start_date
                    greatest_date = start_date

                    #2. Fromat start_date if rep_key is datetime
                    if rep_key_type == "DATETIME":
                        params["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")

                    #3. Build rep_key conditional no limits, no iteration, this was faster than iterating by day
                    replication_key_conditional = "{replication_key} >= {rep_key_type}('{start_date}') ORDER BY {replication_key} ASC;"

                    if original_query is None:
                        query = ("""SELECT * FROM `{table_name}` WHERE""" + replication_key_conditional).format(**params)
                    else:
                        query = original_query.replace("{replication_key_condition}", replication_key_conditional).format(**params)
                
                    attempts = 0
                    while fetch_data:
                        try:
                            LOGGER.info("Running query:\n    %s" % query)
                            query_job = client.query(query)
                            results = query_job.result()

                            for row in query_job:
                                record = { k: v for k, v in row.items() }
                                record = deep_convert_datetimes(record)
                                record = transformer.transform(record, schema)
                                singer.write_record(stream_name, record)

                                greatest_date = localize_datetime(greatest_date)
                                if record.get(stream.replication_key) is not None:
                                    record_date = datetime.datetime.fromisoformat(record[stream.replication_key])
                                    record_date = localize_datetime(record_date)
                                    if greatest_date is None or record_date > greatest_date:
                                        greatest_date = record_date

                            break
                        except (TimeoutError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError, SocketTimeout, BaseSSLError, SocketError, OSError) as e:
                            LOGGER.warn(e)
                            attempts += 1
                            if attempts > 3:
                                time.sleep(2**attempts)
                                pass
                            raise e

                    state[stream.tap_stream_id] = greatest_date.isoformat()
                    singer.write_state(state)
                
        # B. Logic for fullsync streams
        else:
            with Transformer() as transformer:
                if original_query is not None:
                    query = original_query
                elif table_name is not None:
                    params = {
                        'table_name': table_name
                    }
                    query = """SELECT * FROM `{table_name}`""".format(**params)
                else:
                    raise ValueError("Either table-name or query must be provided in metadata")

                attempts = 0
                while True:
                    try:
                        LOGGER.info("Running query:\n    %s" % query)
                        query_job = client.query(query)

                        for row in query_job:
                            record = { k: v for k, v in row.items() }
                            record = deep_convert_datetimes(record)
                            record = transformer.transform(record, schema)

                            # adds only selected fields to record
                            selected_fields = [i["breadcrumb"][-1] for i in stream.metadata[1:] if i["metadata"]["selected"]]
                            record = {k: v for k, v in record.items() if k in selected_fields}

                            # writes the record
                            singer.write_record(stream_name, record)
                        break
                    except (TimeoutError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError, SocketTimeout, BaseSSLError, SocketError, OSError) as e:
                        LOGGER.warn(e)
                        attempts += 1
                        if attempts > 3:
                            time.sleep(2**attempts)
                            pass
                        raise e

                    state[stream.tap_stream_id] = start_date.isoformat()
                    singer.write_state(state)


def deep_convert_datetimes(value):
    if isinstance(value, list):
        return [deep_convert_datetimes(child) for child in value]
    elif isinstance(value, dict):
        return {k: deep_convert_datetimes(v) for k, v in value.items()}
    elif isinstance(value, datetime.date) or isinstance(value, datetime.datetime):
        return value.isoformat()
    return value

def round_to_partition(datetime, step):
    if step == timedelta(days=1):
        return datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    if step == timedelta(hours=1):
        return datetime.replace(minute=0, second=0, microsecond=0)
    if step == timedelta(seconds=1):
        return datetime + timedelta(seconds=1)
    else:
        raise NotImplementedError(f"Unsupported partition type: {step}")


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    credentials = service_account.Credentials.from_service_account_file(args.config['credentials_path'])
    client = bigquery.Client(project=args.config['project'], credentials=credentials)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(args.config, client)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(args.config, client)
        sync(args.config, args.state, catalog, client)


if __name__ == '__main__':
    main()
