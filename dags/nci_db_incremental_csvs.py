"""
# NCI Database Backup and Upload to S3

"""
from textwrap import dedent

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.operators.ssh_operator import SSHOperator

from datetime import datetime, timedelta

import pendulum

local_tz = pendulum.timezone("Australia/Canberra")

default_args = {
    'owner': 'dayers',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 8, 26, 1, tzinfo=local_tz),
    'timeout': 60*60*2,  # For running SSH Commands
    'ssh_conn_id': 'lpgs_gadi',
    'remote_host': 'gadi-dm.nci.org.au',
    'email_on_failure': True,
    'email': 'damien.ayers@ga.gov.au',
}

with DAG('nci_incremental_csv_db_backup',
         default_args=default_args,
         catchup=False,
         schedule_interval="@daily",
         max_active_runs=1,
         tags=['nci'],
         ) as dag:

    COMMON = dedent('''
        set -e
        # Load dea module to ensure that pg_dump version and the server version
        # matches, when the cronjob is run from an ec2 instance
        module use /g/data/v10/public/modules/modulefiles
        module load dea/20200828

        cd /g/data/v10/agdc/backup/archive

        host=dea-db.nci.org.au
        datestring={{ ds_nodash }}
        datestring_psql={{ ds }}
        file_prefix="${host}-${datestring}"

    ''')

    aws_conn = AwsHook(aws_conn_id='aws_nci_db_backup')

    run_changes_csv_dump = SSHOperator(
        task_id='dump_table_changes_to_csv',
        command=COMMON + dedent("""
            set -euo pipefail
            IFS=$'\n\t'

            tables_with_added_updated_archived=(
                agdc.dataset

            )
            tables_with_added_updated=(
                agdc.dataset_type
                agdc.metadata_type
            )

            tables_with_added_archived=(
                agdc.dataset_location
            )

            # agdc.dataset_source doesn't have columns for added/updated

            output_dir=$TMPDIR/pg_change_csvs_${datestring}
            mkdir -p ${output_dir}
            cd ${output_dir}

            for table in agdc.dataset_type agdc.metadata_type; do
                echo Dumping changes from $table
                psql --quiet -c "\\copy (select * from $table where updated <@ tstzrange('{{ prev_ds }}', '{{ ds }}') or added <@ tstzrange('{{ prev_ds }}', '{{ ds }}')) to stdout with (format csv)" -h ${host} -d datacube | gzip -c - > $table_changes.csv.gz
            done

            table=agdc.dataset
            echo Dumping changes from $table
            psql --quiet -c "\\copy (select * from $table where updated <@ tstzrange('{{ prev_ds }}', '{{ ds }}') or archived <@ tstzrange('{{ prev_ds }}', '{{ ds }}') or added <@ tstzrange('{{ prev_ds }}', '{{ ds }}')) to stdout with (format csv)" -h ${host} -d datacube | gzip -c - > $table_changes.csv.gz

            table=agdc.dataset_location
            echo Dumping changes from $table
            psql --quiet -c "\\copy (select * from $table where updated <@ tstzrange('{{ prev_ds }}', '{{ ds }}') or archived <@ tstzrange('{{ prev_ds }}', '{{ ds }}')) to stdout with (format csv)" -h ${host} -d datacube | gzip -c - > agdc.dataset_location_changes.csv.gz

            table=agdc.dataset_source
            echo Dumping changes from $table
            psql --quiet -c "\\copy (select * from dataset_source where dataset_ref in (select id  from agdc.dataset where added <@ tstzrange('{{ prev_ds }}', '{{ ds }}')) to stdout with (format csv)" -h ${host} -d datacube | gzip -c - > agdc.dataset_source_changes.csv.gz

        """)
    )

    upload_change_csvs_to_s3 = SSHOperator(
        task_id='upload_change_csvs_to_s3',
        params={
            'aws_conn': aws_conn.get_credentials(),
        },
        command=COMMON + dedent('''
            export AWS_ACCESS_KEY_ID={{params.aws_conn.access_key}}
            export AWS_SECRET_ACCESS_KEY={{params.aws_conn.secret_key}}


            output_dir=$TMPDIR/pg_change_csvs_${datestring}
            cd ${output_dir}

            aws s3 sync ./ s3://nci-db-dump/csv-changes/${datestring}/ --content-encoding gzip --no-progress

            # Upload md5sums last, as a marker that it's complete.
            md5sum * > md5sums
            aws s3 cp md5sums s3://nci-db-dump/csv-changes/${datestring}/

            # Remove the CSV directory
            cd ..
            rm -rf ${output_dir}

        ''')

    )

    run_changes_csv_dump >> upload_change_csvs_to_s3
