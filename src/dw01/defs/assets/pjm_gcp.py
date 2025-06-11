import os

import dagster as dg
import pandas as pd
from dagster import define_asset_job
from dagster_gcp import BigQueryResource
from dagster_gcp.gcs import GCSResource
from google.cloud import bigquery as bigqueryLib

import src.dw01.pjm
from src.dw01.pjm import PjmFtrScheduleFile, PjmFtrModelUpdateFile, PjmFileKind
from src.dw01.utils import download_file_locally

# For whatever reason the .env file wasn't being picked up... Maybe a dg vs dagster or tooling issue
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/luis/repos/sandbox/gcp_luis_srv_elect_dw01.json"
)
# TODO: turn into configuration parameters
big_query_dataset_id = "electricity-dw01.pjm_dataset"
gcs_bucket_id = "dw01_bucket"


@dg.asset(
    description="Uploads files to Google Cloud Storage as appropriate",
    group_name="ingestion",
    config_schema={"ftr_url": str},
)
def file_uploader_to_gcp(
        context: dg.AssetExecutionContext, gcs: GCSResource
) -> dg.MaterializeResult:
    log = context.log

    # Apparently this client doesn't need a `with` (IDisposable `using`)
    gcs_client = gcs.get_client()

    log.info(f"Connecting & retrieving files in GCP Storage: gs://{gcs_bucket_id}")
    bucket, blobs = retrieve_bucket_and_blobs(gcs_client, gcs_bucket_id)
    uploaded_files = set()
    ftr_url = context.op_config["ftr_url"]
    log.info(f"Identifying all sought files from {ftr_url}")
    files_to_download = src.dw01.pjm.get_urls_all_ftr(ftr_url)
    log.info(f"There were a total of {len(files_to_download)} prospect sought files")

    indexes_to_remove = []
    for ix in range(0, len(files_to_download)):
        # at the top level, we have raw files which have distinct, canonical names
        f = files_to_download[ix]
        if f.nice_filename is None or f.nice_filename in blobs:
            indexes_to_remove.append(ix)

    for ix in reversed(indexes_to_remove):
        files_to_download.pop(ix)

    log.info(
        f"There were a total of {len(files_to_download)} actual sought files we'll retrieve and upload"
    )

    for next_file in files_to_download:
        if next_file is None:
            continue
        blob = bucket.blob(next_file.nice_filename)

        log.info(f"Downloading file {next_file.nice_filename}")

        local_file = download_file_locally(
            next_file.url,
            context.op_config["ftr_url"],
            next_file.nice_filename,
            overwrite=False,
        )
        log.info(f"Uploading file {next_file.nice_filename} to GCS")
        blob.upload_from_filename(local_file)
        uploaded_files.add(local_file)
    return dg.MaterializeResult(
        metadata={
            "row_count": len(uploaded_files),
            "preview": dg.MetadataValue.md(
                pd.DataFrame(uploaded_files, columns=["Uploaded files"]).to_markdown()
            ),
        }
    )


def retrieve_bucket_and_blobs(gcs_client, bucket_name: str = "dw01_bucket"):
    # TODO: Add an asset execution context as configuration parameter to callers of this
    # TODO: probably should parametrize the search so that we retrieve a subset of the files
    #       although if architecturally we put a bucket per file type, then this need is bypassed
    bucket = gcs_client.bucket(bucket_name)
    blobs = set()
    for blob in bucket.list_blobs():
        blobs.add(blob.name)
    return bucket, blobs


pjm_table_schemas = {
    "ftr_auction_calendar_events": [
        bigqueryLib.SchemaField("version", "INTEGER", mode="REQUIRED"),
        bigqueryLib.SchemaField("market_name", "STRING", mode="REQUIRED"),
        bigqueryLib.SchemaField("product", "STRING", mode="REQUIRED"),
        bigqueryLib.SchemaField("period", "STRING", mode="REQUIRED"),
        bigqueryLib.SchemaField("auction_round", "string", mode="REQUIRED"),
        bigqueryLib.SchemaField("bidding_opening", "DATETIME", mode="REQUIRED"),
        bigqueryLib.SchemaField("bidding_closing", "DATETIME", mode="REQUIRED"),
        bigqueryLib.SchemaField("results_posted", "DATETIME", mode="REQUIRED"),
        bigqueryLib.SchemaField("contract_start", "DATE", mode="REQUIRED"),
        bigqueryLib.SchemaField("contract_end", "DATE", mode="REQUIRED"),
    ],
    "ftr_model_node_changes": [
        bigqueryLib.SchemaField("version", "DATE", mode="REQUIRED"),
        bigqueryLib.SchemaField("from_id", "INTEGER", mode="REQUIRED"),
        bigqueryLib.SchemaField("from_txt_zone", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("from_substation", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("from_voltage", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("from_equipment", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("from_name", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_id", "INTEGER", mode="REQUIRED"),
        bigqueryLib.SchemaField("to_txt_zone", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_substation", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_voltage", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_equipment", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_name", "STRING", mode="NULLABLE"),
    ],
}


@dg.asset(
    description="GCP file processor",
    group_name="transformation",
    deps=[file_uploader_to_gcp],  # comment out to debug faster
)
def gcp_file_processor(
        context: dg.AssetExecutionContext, gcs: GCSResource, bigquery: BigQueryResource
) -> dg.MaterializeResult:
    log = context.log

    gcs_client = gcs.get_client()
    log.info(f"Connecting & retrieving files in GCP Storage: gs://{gcs_bucket_id}")
    bucket, blobs = retrieve_bucket_and_blobs(gcs_client, gcs_bucket_id)

    processed_files = []

    # question: can I just send the name of the new files (ie uploaded_files from file_uploader_to_gcp) directly to this?
    #       otherwise, I probably need more artifacts than those prescribed to keep track of what I had
    #       processed already. Or we would be forced to less efficiently reprocess everything everytime and then maybe
    #       do a merge/upsert. I am assuming we want to keep the uploaded files, otherwise we could just delete.

    with bigquery.get_client() as bg_client:
        # First create tables as necessary

        log.info(
            f"Connecting & retrieving tables in BigQuery: bq://{big_query_dataset_id}"
        )
        tables_gcp = set(
            table.table_id for table in bg_client.list_tables(big_query_dataset_id)
        )

        missing_table_names = set(
            t for t in pjm_table_schemas.keys() if t not in tables_gcp
        )

        if len(missing_table_names) > 0:
            log.info(
                f"Identified missing tables in BigQuery: {','.join(missing_table_names)}"
            )

        for missing_table_name in missing_table_names:
            full_table_name = f"{big_query_dataset_id}.{missing_table_name}"
            big_table = bigqueryLib.Table(
                full_table_name,
                schema=pjm_table_schemas[missing_table_name],
            )
            bg_client.create_table(table=big_table)
            log.info(f"Created BigQuery table: {full_table_name}")

        should_merge_model_changes = False
        should_merge_auction_calendars = False

        for blob_name in blobs:
            if "cleaned/" in blob_name:
                continue

            blob = bucket.blob(blob_name)
            local_blob_name = os.path.join("downloads/", blob_name)
            new_name = os.path.basename(blob_name.split(".")[0] + ".csv")
            cleaned_file = bucket.blob("cleaned/" + new_name)
            if cleaned_file.exists():
                continue

            kind = PjmFileKind.FtrSchedule

            if not os.path.exists(local_blob_name):
                log.info(f"Downloading file: {blob_name}")
                blob.download_to_filename(local_blob_name)

            if "ftr-arr-market-schedule" in blob_name:
                kind = PjmFileKind.FtrSchedule
                log.info(f"Attempt at parsing PjmFtrScheduleFile: {blob_name}")
                parsed_result = PjmFtrScheduleFile(local_blob_name)
                cleaned_file.upload_from_string(
                    pd.DataFrame(parsed_result.auction_data_frame).to_csv(index=False),
                    "text/csv",
                )
                log.info(f"Merge new PjmFtrScheduleFile: {blob_name}")
                processed_files.append(blob_name)
                should_merge_auction_calendars = True
            elif blob_name.startswith("ftr-model-update") and blob_name.endswith(
                    ".csv"
            ):
                kind = PjmFileKind.FtrSchedule
                cleaned_file = bucket.blob("cleaned/" + blob_name)
                if cleaned_file.exists():
                    continue

                log.info(f"Downloading PjmFtrModelUpdateFile: {blob_name}")
                blob.download_to_filename(blob_name)
                log.info(f"Attempt at parsing PjmFtrModelUpdateFile: {blob_name}")
                parsed_result = PjmFtrModelUpdateFile(blob_name)
                cleaned_file.upload_from_string(
                    parsed_result.changes.to_csv(index=False), "text/csv"
                )
                log.info(f"Merge new PjmFtrModelUpdateFile: {blob_name}")
                processed_files.append(blob_name)
                should_merge_model_changes = True

        if should_merge_model_changes:
            bg_client.query("""
                merge `pjm_dataset.ftr_model_node_changes` as t
                using pjm_dataset.ftr_model_node_changes_from_files as s
                on t.version = s.version and t.from_id = s.from_id
                when not matched by target then
                  insert (
                      version,
                      from_id,from_txt_zone,from_substation,from_voltage,from_equipment,from_name,
                      to_id,to_txt_zone,to_substation,to_voltage, to_equipment, to_name)
                  values (
                      version,
                      from_id,from_txt_zone,from_substation,from_voltage,from_equipment,from_name,
                      to_id,to_txt_zone,to_substation,to_voltage, to_equipment, to_name);
            """)
            log.info("Synced the main modeled updates table")

        if should_merge_auction_calendars:
            bg_client.query("""
                merge `pjm_dataset.ftr_auction_calendar_events` as t
                using pjm_dataset.ftr_auction_calendar_events_from_files as s
                on t.version = s.version and t.market_name = s.market_name
                    and t.product = s.product
                    and t.auction_round = s.auction_round
                    and t.period = s.period
                when not matched by target then
                  insert (
                      version, market_name, product, period, auction_round,
                      bidding_opening, bidding_closing, results_posted,
                      contract_start, contract_end)
                  values (
                      version, market_name, product, period, auction_round,
                      bidding_opening, bidding_closing, results_posted,
                      contract_start, contract_end);
            """)
            log.info("Synced the main auction calendar events table")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(processed_files),
            "preview": dg.MetadataValue.md(
                pd.DataFrame(processed_files, columns=["Processed files"]).to_markdown()
            ),
        }
    )


defs = dg.Definitions(
    assets=[file_uploader_to_gcp, gcp_file_processor],
    resources={
        "gcs": GCSResource(project="electricity-dw01"),
        "bigquery": BigQueryResource(project="electricity-dw01"),
    },
    jobs=[
        define_asset_job(
            name="pjm_to_gcp", selection=[file_uploader_to_gcp, gcp_file_processor]
        )
    ],
)
