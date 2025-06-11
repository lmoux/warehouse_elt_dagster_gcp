# Data warehouse

## Description

We seek to jumpstart an electricity market data warehouse for
PJM FTRs with publicly available data to showcase
some specific tech pairings.

### Technological Requirements

* Python
    * Using UV, package manager
* Google Cloud storage
* Google BigQuery
* Using Dagster

#### Workflows

* A dagster sensor should be created: if a new file is available, materialize the asset
* The first asset of the dagster pipeline should download the file and upload it in Google Cloud Storage
* The second asset of the dagster pipeline should download the file from google cloud storage, parse it, and then load
  it in Google BigQuery.

#### Google Cloud

* Create a bucket(Cloud storage) in multi region, just keep the default options
* Create a dataset (BigQuery) in multi region, just keep the default options

#### Dagster

* No need Dagster resources or asset checks, just 2 assets, 1 job and 1 sensor.
* The job is for connect the sensor and the asset.
* No need to partition in Dagster.

## Running

```shell
dg dev
```

The first asset requires run time configuration. When materializing,
one needs to put `ops: ...` and the quickest most convenient way of doing it
seems to be to simply click on the scaffolding button upon receiving the
materialization warning and ensuring `ftr: 'https://www.pjm.com/markets-and-operations/ftr'`
is substituted for the placeholder.

Currently, these are hardcoded and need to be updated:

* GCP project id
* GCP storage bucket id
* GCP BigQuery dataset id

They are dependencies and need to exist before materializing.

The BigQuery tables are create automatically if necessary.

## Developing

```shell
uv sync
# After the sync, the `uv run` might no longer be necessary 
uv run ruff check --fix
uv run ruff format
uv run pytest
uv run dg dev 
```

I haven't been able to configure Dagster CLI (dg tool)'s import paths
correctly such that, currently, the `dg launch --assets gcp_file_processor` fails.