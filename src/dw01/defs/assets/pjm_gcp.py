import io
import os

import dagster as dg
import pandas as pd
from dagster import define_asset_job
from dagster_gcp import BigQueryResource
from dagster_gcp.gcs import GCSResource
from google.cloud import bigquery as bigqueryLib

import src.dw01.pjm
from src.dw01.pjm import PjmFtrScheduleFile, PjmFtrModelUpdateFile
from src.dw01.utils import download_file_locally

# sys.path.append("../../../")

# For whatever reason the .env file wasn't being picked up... Maybe a dg vs dagster or tooling issue
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/luis/repos/sandbox/gcp_luis_srv_elect_dw01.json"
)
# TODO: turn into configuration parameters
big_query_dataset_id = "electricity-dw01.pjm_dataset"


@dg.asset(
    description="Uploads files to Google Cloud Storage as appropriate",
    group_name="ingestion",
    config_schema={"ftr_url": str},
)
def file_uploader_to_gcp(
    context: dg.AssetExecutionContext, gcs: GCSResource
) -> dg.MaterializeResult:
    # Apparently this client doesn't need a `with` (IDisposable `using`)
    gcs_client = gcs.get_client()

    bucket, blobs = retrieve_bucket_and_blobs(gcs_client, "dw01_bucket")

    uploaded_files = set()

    for next_file in src.dw01.pjm.get_urls_all_ftr(context.op_config["ftr_url"]):
        if next_file is None:
            continue
        blob = bucket.blob(next_file.nice_filename)

        if blob.exists():
            # this seems inefficient; too many round trips, perhaps try to query for the X most recent files
            continue

        local_file = download_file_locally(
            next_file.url,
            context.op_config["ftr_url"],
            next_file.nice_filename,
            overwrite=False,
        )
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
        bigqueryLib.SchemaField("auction_round", "INTEGER", mode="REQUIRED"),
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
        bigqueryLib.SchemaField("from_name", "STRING", mode="REQUIRED"),
        bigqueryLib.SchemaField("to_id", "INTEGER", mode="REQUIRED"),
        bigqueryLib.SchemaField("to_txt_zone", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_substation", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_voltage", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_equipment", "STRING", mode="NULLABLE"),
        bigqueryLib.SchemaField("to_name", "STRING", mode="REQUIRED"),
    ],
}


@dg.asset(
    description="GCP file processor",
    group_name="transformation",
    # deps=[file_uploader_to_gcp], # comment out to debug faster
)
def gcp_file_processor(
    context: dg.AssetExecutionContext, gcs: GCSResource, bigquery: BigQueryResource
) -> dg.MaterializeResult:
    log = context.log

    gcs_client = gcs.get_client()
    bucket, blobs = retrieve_bucket_and_blobs(gcs_client, "dw01_bucket")

    processed_files = []

    # question: can I just send the name of the new files (ie uploaded_files from file_uploader_to_gcp) directly to this?
    #       otherwise, I probably need more artifacts than those prescribed to keep track of what I had
    #       processed already. Or we would be forced to less efficiently reprocess everything everytime and then maybe
    #       do a merge/upsert. I am assuming we want to keep the uploaded files, otherwise we could just delete.

    with bigquery.get_client() as bg_client:
        # First create tables as necessary

        # tables = bg_client.list_tables(big_query_dataset_id)
        # for table in tables:
        #     print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

        tables_gcp = set(
            table.table_id for table in bg_client.list_tables(big_query_dataset_id)
        )

        missing_table_names = set(
            t for t in pjm_table_schemas.keys() if t not in tables_gcp
        )

        for missing_table_name in missing_table_names:
            big_table = bigqueryLib.Table(
                f"{big_query_dataset_id}.{missing_table_name}",
                schema=pjm_table_schemas[missing_table_name],
            )
            bg_client.create_table(table=big_table)

        for blob_name in blobs:
            blob = bucket.blob(blob_name)
            # attempt1 []: download bytes... but in parsers I got info from filename
            #   data = blob.download_as_bytes()
            #   df = pd.read_csv(io.StringIO(data))
            # attempt2: maybe use gcsfs

            if "ftr-arr-market-schedule" in blob_name:
                log.info(f"Downloading PjmFtrScheduleFile: {blob_name}")
                data = io.BytesIO(blob.download_as_bytes())
                log.info(f"Attempt at parsing PjmFtrScheduleFile: {blob_name}")
                PjmFtrScheduleFile.parse_auctions(blob_name, data)
                # merge into the auction_schedule table
                bg_client.query("select 1")
                print(f"Merge new PjmFtrScheduleFile: {blob_name}")
                processed_files.append(blob_name)
            elif blob_name.startswith("ftr-model-update") and blob_name.endswith(
                ".csv"
            ):
                log.info(f"Downloading PjmFtrModelUpdateFile: {blob_name}")
                blob.download_to_filename(blob_name)
                log.info(f"Attempt at parsing PjmFtrModelUpdateFile: {blob_name}")
                PjmFtrModelUpdateFile(blob_name)
                print(f"Merge new PjmFtrModelUpdateFile: {blob_name}")
                processed_files.append(blob_name)

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
