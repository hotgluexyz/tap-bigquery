#!/usr/bin/env python3
import os
import secrets
import string
import time
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
from google.cloud import bigquery

import psutil
import pyarrow.parquet as pq
import pyarrow as pa

import gc
import json
import pathlib

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
                    # TODO: Note this is only a guesstimate but is roughly going to query every 6 months
                    partition_size = timedelta(weeks=(4*6))
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


def log_memory_usage(msg):
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB
    LOGGER.info(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg} - Memory usage: {memory_usage:.2f} MB")

def random_hash(length=6):
    ALPHANUM = string.ascii_letters + string.digits  # a-zA-Z0-9
    return ''.join(secrets.choice(ALPHANUM) for _ in range(length))

def sync(config, state, catalog, client, job_id, parquet_file_datetime):
    """
    Sync data from BigQuery tables to Parquet files.
    
    This function handles both incremental and full table syncs:
    - Incremental sync: Uses replication keys to sync only new/updated data
    - Full table sync: Syncs entire table when no replication key is available
    
    Args:
        config: Configuration dictionary containing sync settings
        state: Current sync state for tracking progress
        catalog: Stream catalog with table metadata
        client: BigQuery client instance
        job_id: Unique identifier for this sync job
        parquet_file_datetime: Timestamp for output file naming
    """
        
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        # Extract schema information and determine stream name
        schema = stream.schema.to_dict()
        stream_name = stream.table or stream.stream or stream.tap_stream_id
        singer.write_schema(
            stream_name=stream_name,
            schema=schema,
            key_properties=stream.key_properties,
        )

        # convert json schema to pyarrow schema
        pyarrow_schema = json_schema_to_pyarrow_schema(schema["properties"])

        # Get sync start date from state or use config default
        start_date = utils.strptime_to_utc(state.get(stream.tap_stream_id, config['start_date']))
        
        # Extract table metadata for configuration
        base_metadata = metadata.to_map(stream.metadata)[()]
        table_name = base_metadata['table-name']
        temp_table_name = f"{table_name}_temp_{random_hash()}"
        
        # Dynamically calculate optimal batch size based on table characteristics
        limit = estimate_limit(client, table_name)
        
        # Set up output directory and file path for Parquet files
        if job_id:
            output_dir = f"/home/hotglue/{job_id}/sync-output"
        else:
            # for local testing
            output_dir = "../.secrets/sync-output"
        file_path = os.path.join(output_dir, f"{stream_name}-{parquet_file_datetime}.parquet")

        LOGGER.info(stream.replication_key)

        # INCREMENTAL SYNC: Handle tables with replication keys (time-based partitioning)
        if stream.replication_key is not None:
            # Calculate time step for incremental sync based on table partitioning
            step_sec = base_metadata['table-partition-size'][0] if type(base_metadata['table-partition-size']) == list else base_metadata['table-partition-size']
            step = timedelta(seconds=step_sec)

            # Create temporary table with UUID for reliable pagination
            # This ensures we can resume from where we left off if sync is interrupted
            query = f"CREATE OR REPLACE TABLE {temp_table_name} AS SELECT GENERATE_UUID() AS row_id, * FROM `{table_name}`"
            client.query(query).result()

            # Initialize Parquet writer for output file
            writer = None

            # Process data in time-based chunks until we reach current time
            while start_date < datetime.datetime.now(timezone.utc):
                # Calculate time window for this batch
                end_date = start_date + step
                
                # Build WHERE clause for time-based filtering
                where_clause = f" WHERE {stream.replication_key} >= timestamp '{start_date}' AND {stream.replication_key} < timestamp '{end_date}'"
                last_seen_uuid = None
                if last_seen_uuid:
                    where_clause += f" AND row_id > '{last_seen_uuid}'"

                # Query data for current time window
                query = f"SELECT * FROM `{table_name}` {where_clause} ORDER BY {stream.replication_key} LIMIT {limit}"
                attempts = 0

                # Retry loop for handling network errors and API failures
                while True:
                    try:
                        LOGGER.info(f"Running query:\n    %s" % query)
                        log_memory_usage(f"Before querying")
                        
                        # Execute BigQuery query and convert to pandas DataFrame
                        df = client.query(query).result().to_dataframe(bqstorage_client=None)
                        log_memory_usage(f"After querying")

                        # Memory management: Keep reference to original DataFrame for cleanup
                        df_old = df
                        # Convert datetime objects to ISO strings for JSON compatibility
                        df = df.applymap(deep_convert_datetimes)

                        # If no data returned, we're done with this time window
                        if df.empty:
                            break

                        # Convert DataFrame to PyArrow table for efficient Parquet writing
                        table = pa.Table.from_pandas(df, schema=pyarrow_schema, preserve_index=False)
                        last_seen_uuid = df['row_id'].iloc[-1]
                            
                        LOGGER.info(f"Writing to parquet")
                        # Initialize Parquet writer on first batch
                        if writer is None:
                            writer = pq.ParquetWriter(file_path, table.schema)

                        # Write batch to Parquet file
                        writer.write_table(table)
                        update_job_metrics(stream.tap_stream_id, len(df), output_dir)
                        
                        # Clean up memory to prevent OOM issues
                        del df, table, df_old
                        gc.collect()
                        log_memory_usage(f"Finished writing batch {batch_num}")
                        batch_num += 1

                    except (TimeoutError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError, SocketTimeout, BaseSSLError, SocketError, OSError) as e:
                        # Handle network errors with exponential backoff
                        LOGGER.warn(e)
                        attempts += 1
                        if attempts > 3:
                            time.sleep(2**attempts)
                            pass
                        raise e

                # Update sync state and advance to next time window
                state[stream.tap_stream_id] = start_date.isoformat()
                singer.write_state(state)
                start_date = round_to_partition(start_date + step, step)
            
            # Clean up temporary table
            client.query(f"DROP TABLE `{temp_table_name}`").result()
            
            # Close Parquet writer and log completion
            if writer:
                writer.close()
                LOGGER.info(f"Finished writing {file_path}")
                break
        
        else:
            attempts = 0

            # create a temp table to generate uuid and paginate over it
            query = f"CREATE OR REPLACE TABLE {temp_table_name} AS SELECT GENERATE_UUID() AS row_id, * FROM `{table_name}`"
            client.query(query).result()
            last_seen_uuid = None
            batch_num = 0
            writer = None

            while True:
                try:
                    # READ DATAFRAME
                    while True:
                        where_clause = ""
                        if last_seen_uuid:
                            where_clause = f" WHERE row_id > '{last_seen_uuid}'"
                        
                        query = f"SELECT * FROM {temp_table_name} {where_clause} ORDER BY row_id ASC LIMIT {limit}"
                        
                        LOGGER.info(f"Running query:\n    %s" % query)
                        log_memory_usage(f"Before querying")

                        df = client.query(query).result().to_dataframe(bqstorage_client=None)
                        log_memory_usage(f"After querying")

                        if df.empty:
                            client.query(f"DROP TABLE `{temp_table_name}`").result()
                            break
                        
                        # keep a reference to the original dataframe to explicitly delete it and free memory
                        df_old = df 
                        # applymap creates a deep copy of the dataframe, so we can delete the original dataframe
                        df = df.applymap(deep_convert_datetimes)
                        table = pa.Table.from_pandas(df, schema=pyarrow_schema, preserve_index=False)
                        last_seen_uuid = df['row_id'].iloc[-1]
                            
                        LOGGER.info(f"Writing to parquet")
                        if writer is None:
                            writer = pq.ParquetWriter(file_path, table.schema)
                        
                        # clean up memory
                        writer.write_table(table)
                        update_job_metrics(stream.tap_stream_id, len(df), output_dir)
                        del df, table, df_old
                        gc.collect()
                        log_memory_usage(f"Finished writing batch {batch_num}")
                        batch_num += 1

                    if writer:
                        writer.close()
                        LOGGER.info(f"Finished writing {file_path}")
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
    if step == timedelta(weeks=(4*6)):
        return datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise NotImplementedError(f"Unsupported partition type: {step}")
    
def to_arrow_type(schema):
    """
    Convert a JSON schema type to a PyArrow data type.
    
    Args:
        schema (dict): A JSON schema dictionary containing type information
        
    Returns:
        pyarrow.DataType: The corresponding PyArrow data type
        
    Examples:
        >>> to_arrow_type({"type": "string"})
        StringType
        >>> to_arrow_type({"type": "integer"})
        Int64Type
        >>> to_arrow_type({"type": ["string", "null"]})
        StringType
    """
    json_type = schema.get("type")
    fmt = schema.get("format")

    # Handle unions
    if isinstance(json_type, list):
        non_null_types = [t for t in json_type if t != "null"]
        json_type = non_null_types[0] if non_null_types else "string"

    # Handle formats
    if fmt == "float":
        return pa.float64()

    if json_type == "string":
        return pa.string()
    elif json_type == "boolean":
        return pa.bool_()
    elif json_type == "integer":
        return pa.int64()
    elif json_type == "number":
        return pa.float64()
    elif json_type == "array":
        item_type = to_arrow_type(schema.get("items", {}))
        return pa.list_(item_type)
    elif json_type == "object":
        props = schema.get("properties", {})
        fields = [
            pa.field(name, to_arrow_type(subschema))
            for name, subschema in props.items()
        ]
        return pa.struct(fields)
    else:
        return pa.string()  # fallback

def json_schema_to_pyarrow_schema(properties: dict) -> pa.Schema:
    """
    Convert a JSON schema properties dictionary to a PyArrow schema.
    
    Args:
        properties (dict): A dictionary mapping field names to their JSON schemas
        
    Returns:
        pyarrow.Schema: A PyArrow schema object containing all the fields
        
    Examples:
        >>> properties = {
        ...     "name": {"type": "string"},
        ...     "age": {"type": "integer"},
        ...     "active": {"type": "boolean"}
        ... }
        >>> schema = json_schema_to_pyarrow_schema(properties)
        >>> print(schema)
        name: string
        age: int64
        active: bool
    """
    fields = []
    for name, schema in properties.items():
        arrow_type = to_arrow_type(schema)
        fields.append(pa.field(name, arrow_type))
    return pa.schema(fields)


def estimate_limit(client, table_id, target_batch_size_mb=1):
    """
    Estimate the number of rows to fetch to achieve a target batch size in megabytes.
    
    This function calculates the optimal row limit for BigQuery table queries based on
    the table's metadata (number of rows and total bytes) to achieve the desired batch size.
    
    Args:
        client: BigQuery client instance
        table_id (str): The ID of the BigQuery table
        target_batch_size_mb (int, optional): Target batch size in megabytes. Defaults to 1, the vytes size calculated by bigquery is compressed
        
    Returns:
        int: The estimated number of rows to fetch, with a minimum of 100 rows
        
    Examples:
        >>> limit = estimate_limit(client, "project.dataset.table", target_batch_size_mb=5)
        >>> print(f"Fetch {limit} rows for ~5MB batch")
    """
    table = client.get_table(table_id)
    if table.num_rows == 0 or table.num_bytes == 0:
        return 1_000  # Fallback if empty
    row_size_bytes = table.num_bytes / table.num_rows
    target_batch_bytes = target_batch_size_mb * 1024 * 1024
    limit =  max(int(target_batch_bytes / row_size_bytes), 100) 
    return limit


def update_job_metrics(stream_name: str, record_count: int, output_dir: str):
    """
    Update metrics for a running job by tracking record counts per stream.

    This function maintains a JSON file that keeps track of the number of records
    processed for each stream during a job execution. The metrics are stored in
    a 'job_metrics.json' file in the specified folder path.

    Args:
        stream_name (str): The name of the stream being processed
        record_count (int): Number of records processed in the current batch
        output_dir (str): Folder path to store the job metrics

    Examples:
        >>> update_job_metrics("customers", 1000, "job_123")
        # Updates job_metrics.json with:
        # {
        #   "recordCount": {
        #     "customers": 1000
        #   }
        # }
    """
    job_metrics_path = os.path.expanduser(os.path.join(output_dir, "job_metrics.json"))

    if not os.path.isfile(job_metrics_path):
        pathlib.Path(job_metrics_path).touch()

    with open(job_metrics_path, "r+") as f:
        content = dict()

        try:
            content = json.loads(f.read())
        except:
            pass

        if not content.get("recordCount"):
            content["recordCount"] = dict()

        content["recordCount"][stream_name] = (
            content["recordCount"].get(stream_name, 0) + record_count
        )

        f.seek(0)
        LOGGER.info(f"Updating job metrics for {stream_name} with {record_count} records")
        f.write(json.dumps(content))



@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    credentials = service_account.Credentials.from_service_account_file(args.config['credentials_path'])
    client = bigquery.Client(project=args.config['project'], credentials=credentials)

    job_id = os.environ.get("JOB_ID")
    parquet_file_datetime = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")

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
        sync(args.config, args.state, catalog, client, job_id, parquet_file_datetime)


if __name__ == '__main__':
    main()
