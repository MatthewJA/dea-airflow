"""
# Sentinel-2_nrt update views automation

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""

from airflow import DAG
from datetime import datetime, timedelta

from airflow.kubernetes.secret import Secret
from airflow.operators.subdag_operator import SubDagOperator
from subdags.subdag_ows_views import ows_update_extent_subdag
from subdags.subdag_explorer_summary import (
    explorer_refresh_stats_subdag,
)
from infra.variables import (
    DB_DATABASE,
    DB_HOSTNAME,
    SECRET_AWS_NAME,
)

DAG_NAME = "sentinel_2_nrt_mv_update"

# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "Pin Jin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["pin.jin@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_DATABASE": DB_DATABASE,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "AWS_DEFAULT_REGION", SECRET_AWS_NAME, "AWS_DEFAULT_REGION"),
    ],
}


# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */6 * * *",  # every 6 hours
    catchup=False,
    tags=["k8s", "ows-update", "explorer-update"],
)


with dag:

    OWS_UPDATE_EXTENTS = SubDagOperator(
        task_id="run-ows-update-ranges",
        subdag=ows_update_extent_subdag(
            DAG_NAME, "run-ows-update-ranges", DEFAULT_ARGS
        ),
    )

    EXPLORER_SUMMARY = SubDagOperator(
        task_id="run-cubedash-gen-refresh-stat",
        subdag=explorer_refresh_stats_subdag(
            DAG_NAME, "run-cubedash-gen-refresh-stat", DEFAULT_ARGS
        ),
    )
