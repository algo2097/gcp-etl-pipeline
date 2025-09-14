from datetime import datetime
from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator

GCP_PROJECT = Variable.get("gcp_project")
GCS_BUCKET  = Variable.get("gcs_bucket")
BQ_LOCATION = Variable.get("bq_location")
DBX_CLUSTER = Variable.get("dbx_cluster_id", default_var=None)
EXCLUDE_UNDERSCORE = r"'.*/_.*|^((?!\.parquet$).)*$'"
PARQUET_TRANSFORMED = f"gs://{GCS_BUCKET}/transformed/clean"
PARQUET_ENRICHED = f"gs://{GCS_BUCKET}/transformed"
PARQUET_CURATED = f"gs://{GCS_BUCKET}/curated"



SQL_GCS_PREFIX = f"gs://{GCS_BUCKET}/pipeline/sql/"
SQL_FILES = {
    "s1":  "SchemaCreation.sql",
    "s2":  "LoadRawExtractTables.sql",
    "s3":  "LoadRawTables.sql",
    "s5":  "LoadTransformedExtractTables.sql",
    "s6":  "LoadTransformedTables.sql",
    "s8":  "LoadEnrichedExtractTable.sql",
    "s9":  "LoadEnrichedTable.sql",
    "s11": "LoadCuratedExtractTable.sql",
    "s12": "LoadCuratedTable.sql",
}

def read_sql_from_gcs(uri: str) -> str:
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    bucket, key = uri.replace("gs://", "").split("/", 1)
    data = hook.download(bucket_name=bucket, object_name=key)  # returns bytes
    return data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else str(data)

with DAG(
    dag_id="JLR_Data_Enrichment_Pipeline",
    start_date=datetime(2025, 9, 4),
    schedule=None,            # on-demand only
    catchup=False,
    description="END to END Pipeline.",
    default_args={"owner": "etl"},
) as dag:

    # BQ tasks
    def bq_task(task_id, sql_key):
        sql_text = read_sql_from_gcs(SQL_GCS_PREFIX + SQL_FILES[sql_key])
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={"query": {"query": sql_text, "useLegacySql": False}},
            location=BQ_LOCATION,  # pass location via operator arg
            gcp_conn_id="google_cloud_default",
        )

    # ---- Step 1 - 2 - 3
    s1 = bq_task("schema_creation",          "s1")
    s2 = bq_task("load_raw_external",        "s2")
    s3 = bq_task("load_raw_tables",          "s3")
    
    # Databricks Job Step - 4
    eda_payload = {
        "run_name": "EDA_Raw_Tables",
        "tasks": [{
            "task_key": "eda",
            **({"existing_cluster_id": DBX_CLUSTER} if DBX_CLUSTER else {}),
            "notebook_task": {
                "notebook_path": "/Workspace/Users/identify.uid@gmail.com/Filestore/pipeline/EDA_Raw_Tables",
                "parameters": [
                    "project_id", GCP_PROJECT,
                    "bucket",     GCS_BUCKET,
                    "run_date",   "{{ ds_nodash }}",
                ],
            },
            "timeout_seconds": 3600,
        }],
    }
    s4 = DatabricksSubmitRunOperator(task_id="eda_raw_tables", databricks_conn_id="databricks_default", json=eda_payload)
    
    sync_base = BashOperator(
        task_id="sync_base_parquet_only",
        bash_command=(
            f"gsutil -m rsync -r -x {EXCLUDE_UNDERSCORE} "
            f"{PARQUET_TRANSFORMED}/base_data "
            f"{PARQUET_TRANSFORMED}/base_data_parquet"
            ),
    )
    sync_options = BashOperator(
        task_id="sync_options_parquet_only",
        bash_command=(
            f"gsutil -m rsync -r -x {EXCLUDE_UNDERSCORE} "
            f"{PARQUET_TRANSFORMED}/options_data "
            f"{PARQUET_TRANSFORMED}/options_data_parquet"
            ),
    )

    sync_vlm = BashOperator(
        task_id="sync_vlm_parquet_only",
        bash_command=(
            f"gsutil -m rsync -r -x {EXCLUDE_UNDERSCORE} "
            f"{PARQUET_TRANSFORMED}/vehicle_line_mapping "
            f"{PARQUET_TRANSFORMED}/vehicle_line_mapping_parquet"
            ),
    )    
    # ---- Step 5 - 6
    s5 = bq_task("load_transformed_external",        "s5")
    s6 = bq_task("load_transformed_tables",          "s6")
    
    # Databricks Job Step - 7
    eda_payload = {
        "run_name": "Data_Enrichment",
        "tasks": [{
            "task_key": "enrich",
            **({"existing_cluster_id": DBX_CLUSTER} if DBX_CLUSTER else {}),
            "notebook_task": {
                "notebook_path": "/Workspace/Users/identify.uid@gmail.com/Filestore/pipeline/Data_Enrichment",
                "parameters": [
                    "project_id", GCP_PROJECT,
                    "bucket",     GCS_BUCKET,
                    "run_date",   "{{ ds_nodash }}",
                ],
            },
            "timeout_seconds": 3600,
        }],
    }
    s7 = DatabricksSubmitRunOperator(task_id="enrichment", databricks_conn_id="databricks_default", json=eda_payload)
    
    sync_enrich = BashOperator(
    task_id="sync_enrich",
    bash_command=(
        f"gsutil -m rsync -r -x {EXCLUDE_UNDERSCORE} "
        f"{PARQUET_ENRICHED}/enriched  "
        f"{PARQUET_ENRICHED}/enriched_parquet"
        ),
    ) 
    
        # ---- Step 5 - 6
    s8 = bq_task("load_enriched_external",        "s8")
    s9 = bq_task("load_enriched_table",          "s9")
    
    # Databricks Job Step - 10
    eda_payload = {
        "run_name": "Data_Enrichment",
        "tasks": [{
            "task_key": "enrich",
            **({"existing_cluster_id": DBX_CLUSTER} if DBX_CLUSTER else {}),
            "notebook_task": {
                "notebook_path": "/Workspace/Users/identify.uid@gmail.com/Filestore/pipeline/Curated",
                "parameters": [
                    "project_id", GCP_PROJECT,
                    "bucket",     GCS_BUCKET,
                    "run_date",   "{{ ds_nodash }}",
                ],
            },
            "timeout_seconds": 3600,
        }],
    }
    s10 = DatabricksSubmitRunOperator(task_id="curated", databricks_conn_id="databricks_default", json=eda_payload)

    sync_curated = BashOperator(
    task_id="sync_curated",
    bash_command=(
        f"gsutil -m rsync -r -x {EXCLUDE_UNDERSCORE} "
        f"{PARQUET_CURATED}/data "
        f"{PARQUET_CURATED}/data_parquet"
        ),
    ) 
    

    s11 = bq_task("load_curated_external",        "s11")
    s12 = bq_task("load_curated_table",          "s12")
    
    s1 >> s2 >> s3 >> s4 >> sync_base >> sync_options >> sync_vlm >> s5 >> s6 >> s7 >> sync_enrich >> s8 >> s9 >> s10 >> sync_curated >> s11 >> s12
