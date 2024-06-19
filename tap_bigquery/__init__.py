#!/usr/bin/env python3
import os
import time
import json
import singer
import datetime
import urllib3
import requests
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
        elif schemafield.field_type == 'RECORD' or schemafield.field_type == 'STRUCT':
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
                elif partition_type == 'MONTH':
                    # TODO: Note this is only a guesstimate, months are not standard size
                    partition_size = timedelta(weeks=4)
                else:
                    LOGGER.info(f"Skipping table {t.full_table_id}: Unsupported partition type: {partition_type}")
                    continue

            try:
                schema = Schema.from_dict(convert_schemafield_to_jsonschema(table.schema))
            except:
                LOGGER.exception(f"Skipping table {t.full_table_id}: Error:")
                continue

            # Try and guess required properties by looking for required fields that end in "id"
            # if this doesn't work, users can always specify their own key-properties with catalog
            key_properties = [s.name for s in table.schema if s.mode == 'REQUIRED' and s.name.lower().endswith("id")]
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
            }]

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


def sync(config, state, catalog, client):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        schema = stream.schema.to_dict()
        stream_name = stream.table or stream.stream or stream.tap_stream_id
        singer.write_schema(
            stream_name=stream_name,
            schema=schema,
            key_properties=stream.key_properties,
        )

        start_date = utils.strptime_to_utc(state.get(stream.tap_stream_id, config['start_date']))
        base_metadata = metadata.to_map(stream.metadata)[()]
        table_name = base_metadata['table-name']

        LOGGER.info(stream.replication_key)

        if stream.replication_key is not None:
            step_sec = base_metadata['table-partition-size'][0] if type(base_metadata['table-partition-size']) == list else base_metadata['table-partition-size']
            step = timedelta(seconds=step_sec)

            with Transformer() as transformer:
                while start_date < datetime.datetime.now(timezone.utc):
                    params = {
                        'table_name': table_name,
                        'replication_key': stream.replication_key,
                        'start_date': start_date,
                        'end_date': start_date + step
                    }
                    query = """SELECT * FROM `{table_name}` WHERE {replication_key} >= timestamp '{start_date}' AND {replication_key} < timestamp '{end_date}' ORDER BY {replication_key}""".format(**params)
                    attempts = 0
                    while True:
                        try:
                            LOGGER.info("Running query:\n    %s" % query)
                            query_job = client.query(query)

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

                    state[stream.tap_stream_id] = start_date.isoformat()
                    singer.write_state(state)
                    start_date = round_to_partition(start_date + step, step)
        else:
            with Transformer() as transformer:
                params = {
                    'table_name': table_name
                }
                query = """SELECT * FROM `{table_name}`""".format(**params)
                attempts = 0
                while True:
                    try:
                        LOGGER.info("Running query:\n    %s" % query)
                        query_job = client.query(query)

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
    if step == timedelta(weeks=4):
        return datetime.replace(hour=0, minute=0, second=0, microsecond=0)
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
