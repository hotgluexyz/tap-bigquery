# tap-bigquery

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

```bash
pip install git+https://github.com/fixdapp/tap-bigquery.git
```

This tap supports discovery and state. It currently only supports tables that are time-paritioned. However,
supporting other kinds of tables should be relatively straightforward and we will accept contributions. State is emitted per-partition, requiring only one read per partition to minimize charges for data access. It supports detecting tables and schemas via discovery, but it cannot reliably detect key_properties, so you will want to specify those in the catalog.

Selection is based on the full table id (`tap_stream_id`) with `__` replacing `.` and `_` replacing `-`, for example: `fooproject_12345__my_dataset__my_table`. Thus each table is independantly selectable. However, the stream name by default is the short table name. You can override this in the catalog by setting either `stream` or `table_name`.

Often you actually want to load the whole dataset into the same destination. You can accomplish this by setting multiple streams to having the `dataset_id` as the `stream` or `table_name`.

**TODO:** This tap queries the data, rather than using [export](https://cloud.google.com/bigquery/docs/exporting-data) for which I believe there is no charge.

## Config

You'll need to create a [service account that has permissions to read out of BigQuery](https://console.developers.google.com/start/api?id=bigquery-json.googleapis.com). Using the built-in
roles "BigQuery Data Viewer" and "BigQuery User" are sufficient. You'll need to generate a JSON
client secret and pass the path in to the config. You also need to supply the project id and the
start date.

```js
{
  // required:
  "project": "foobar-12345",
  "credentials_path": "./google-service-account-credentials.json",
  "start_date": "2020-08-14T00:00:00Z"
}
```

---

Copyright &copy; 2020 FIXD Automotive, Inc.
