# Data warehouse

We seek to jumpstart an electricity market data warehouse for
PJM FTRs with publicly available data to showcase 
some specific tech pairings.

## Technological Requirements
 
* Python
  * Using UV, package manager
* Google Cloud storage
* Google BigQuery
* Using Dagster
 
### Workflows
 
* A dagster sensor should be created: if a new file is available, materialize the asset
* The first asset of the dagster pipeline should download the file and upload it in Google Cloud Storage
* The second asset of the dagster pipeline should download the file from google cloud storage, parse it, and then load it in Google BigQuery.
 
### Google Cloud
 
* Create a bucket(Cloud storage) in multi region, just keep the default options
* Create a dataset (BigQuery) in multi region, just keep the default options
 
### Dagster
 
* No need Dagster resources or asset checks, just 2 assets, 1 job and 1 sensor.
* The job is for connect the sensor and the asset.
* No need to partition in Dagster.