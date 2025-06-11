import os

import dagster as dg
import pandas as pd
from dagster_gcp.gcs import GCSResource

import src.dw01.pjm
from src.dw01.utils import download_file_locally

# sys.path.append("../../../")

# For whatever reason the .env file wasn't being picked up... Maybe a dg vs dagster or tooling issue
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/luis/repos/sandbox/gcp_luis_srv_elect_dw01.json"
)


@dg.asset(
    description="Uploads files to Google Cloud Storage as appropriate",
    group_name="ingestion",
    config_schema={"ftr_url": str},
)
def file_uploader_to_gcp(
        context: dg.AssetExecutionContext, gcs: GCSResource
) -> dg.MaterializeResult:
    gcs_client = gcs.get_client()

    # TODO: Add to asset execution context as configuration parameter
    bucket = gcs_client.bucket("dw01_bucket")
    blobs = set()

    for blob in bucket.list_blobs():
        blobs.add(blob.name)

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


@dg.asset(description="GCP file processor", group_name="transformation")
def gcp_file_processor(gcs: GCSResource) -> dg.MaterializeResult:
    print("Look ma!")
    return dg.MaterializeResult()


defs = dg.Definitions(
    assets=[file_uploader_to_gcp],
    resources={"gcs": GCSResource(project="electricity-dw01")},
)
