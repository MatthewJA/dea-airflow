"""
# Collection 3 NCI to AWS Scene Count Report

This DAG check scenes count on NCI and AWS. It:

 * Count scenes on NCI filesystem and DB.
 * Count scenes on AWS S3 and DB.
 * Check if scenes count matches on NCI and AWS.

In case of scenes count mismatch, the DAG fails with Airflow exception.

"""
import json
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG, AirflowException
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

from airflow.kubernetes.secret import Secret
from airflow.operators.python_operator import PythonOperator

from sensors.pbs_job_complete_sensor import maybe_decode_xcom

COLLECTION3_PRODUCTS = ["ga_ls5t_ard_3", "ga_ls7e_ard_3", "ga_ls8c_ard_3"]

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.9"

NCI_BASH_COMMAND = """
    # echo on and exit on fail
    set -eu

    # Load the latest stable DEA module
    module use /g/data/v10/public/modules/modulefiles
    module load dea

    # Be verbose and echo what we run
    set -x

    nci_db_count=$[$(datacube dataset search product={{ params.product }} -f csv | wc -l)-1]
    nci_fs_count=$(find /g/data/xu18/ga/{{ params.product }}/ -name *.odc-metadata.yaml | wc -l)
    echo '{"nci_db_count": "'${nci_db_count}'"\n, "nci_fs_count": "'${nci_fs_count}'"}'
"""

AWS_BASH_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
        aws_db_count=$[$(datacube dataset search product={{ params.product }} -f csv | wc -l)-1]
        aws_s3_count=$(s3-find --no-sign-request s3://dea-public-data/baseline/{{ params.product }}/**/*.stac-item.json | wc -l)
        echo '{"aws_db_count": "'${aws_db_count}'"\n, "aws_s3_count": "'${aws_s3_count}'"}' > /airflow/xcom/return.json
    """
    ),
]

default_args = {
    "owner": "Sachit Rajbhandari",
    "start_date": datetime(2020, 10, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": "sachit.rajbhandari@ga.gov.au",
    "ssh_conn_id": "sachit_gadi",
    "env_vars": {
        "DB_HOSTNAME": "ows_dev",
        "DB_DATABASE": "db_reader",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret(
            "env",
            "DB_USERNAME",
            "ows-db",
            "postgres-username",
        ),
        Secret(
            "env",
            "DB_PASSWORD",
            "ows-db",
            "postgres-password",
        ),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            "processing-landsat-3-aws-creds",
            "AWS_DEFAULT_REGION",
        ),
        Secret(
            "env",
            "AWS_ACCESS_KEY_ID",
            "processing-landsat-3-aws-creds",
            "AWS_ACCESS_KEY_ID",
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "processing-landsat-3-aws-creds",
            "AWS_SECRET_ACCESS_KEY",
        ),
    ],
}


def count_check(product_name, **kwargs):
    ti = kwargs["ti"]
    scene_count = json.loads(
        maybe_decode_xcom(ti.xcom_pull(task_ids=f"nci_count_{product_name}"))
    )
    scene_count.update(ti.xcom_pull(task_ids=f"aws_count_{product_name}"))
    print(scene_count)
    if (
            scene_count.get("nci_db_count") == scene_count.get("nci_fs_count")
            and scene_count.get("aws_db_count") == scene_count.get("aws_s3_count")
            and scene_count.get("nci_db_count") == scene_count.get("aws_db_count")
    ):
        print(
            f"There are {scene_count} scenes in ODC DB and filesystem/S3 of NCI and AWS"
        )
    else:
        raise AirflowException(
            "Scenes count doesn't match between ODC DB and filesystem/S3 of NCI and AWS"
        )


dag = DAG(
    "nci_c3_upload_s3_report",
    doc_md=__doc__,
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
    default_view="tree",
    tags=["nci", "landsat_c3"],
)

with dag:
    for product in COLLECTION3_PRODUCTS:
        # Count scenes on NCI
        count_nci = SSHOperator(
            task_id=f"nci_count_{product}",
            command=dedent(NCI_BASH_COMMAND),
            params={"product": product},
            do_xcom_push=True,
            timeout=90,  # For running SSH Commands
        )
        # Count scenes on AWS
        count_aws = KubernetesPodOperator(
            namespace="processing",
            image=INDEXER_IMAGE,
            image_pull_policy="Always",
            arguments=AWS_BASH_COMMAND,
            labels={"step": "count_aws"},
            name=f"aws_count_{product}",
            task_id=f"aws_count_{product}",
            params={"product": product},
            get_logs=True,
            do_xcom_push=True,
            is_delete_operator_pod=True,
        )
        # Report count check
        report_count_check = PythonOperator(
            task_id=f"report_count_check_{product}",
            python_callable=count_check,
            op_kwargs={"product_name": product},
            provide_context=True,
        )
        count_nci >> report_count_check
        count_aws >> report_count_check
