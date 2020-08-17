# tap-bigquery

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

```bash
pip install git+https://github.com/fixdapp/tap-bigquery.git
```

This tap supports discovery and state. It currently only supports tables that are time-paritioned. However,
supporting other kinds of tables should be relatively straightforward and we will accept contributions. State is emitted per-partition, requiring only one read per partition. It supports detecting tables and schemas via discovery, but it cannot reliably detect key_properties, so you will want to specify those in the catalog.

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
