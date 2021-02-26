"""
# Run a regular QA test on the NCI and report any errors
"""
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.email import send_email
from sensors.pbs_job_complete_sensor import PBSJobSensor

default_args = {
    "owner": "Damien Ayers",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 3, 4),  # probably ignored since schedule_interval is None
    "timeout": 90,  # For running SSH Commands
}


def check_file_for_output():
    with tempfile.TemporaryDirectory() as tmpdir:
        sftp = SFTPHook(ssh_conn_id='lpgs_gadi')
        local_file = Path(tmpdir) / 'logfile.txt'
        sftp.retrieve_file(
            remote_full_path='',
            local_full_path=str(local_file)
        )
        report_contents = local_file.read_text()

    send_email(
        self.to,
        self.subject,
        self.html_content,
        files=self.files,
        cc=self.cc,
        bcc=self.bcc,
        mime_subtype=self.mime_subtype,
        mime_charset=self.mime_charset,
    )


with DAG(
        "nci_qa_tester",
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        doc_md=__doc__,
) as dag:
    submit_pbs_job = SSHOperator(
        task_id=f"submit_pbs_job",
        ssh_conn_id="lpgs_gadi",
        command="""
          {% set work_dir = '~/airflow_testing/' -%}
          mkdir -p {{ work_dir }};
          cd {{ work_dir }};
          qsub \
          -q express \
          -W umask=33 \
          -l wd,walltime=0:10:00,mem=3GB -m abe \
          -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87 \
          -P {{ params.project }} -o {{ work_dir }} -e {{ work_dir }} \
          -- /bin/bash -l -c \
              "source $HOME/.bashrc; \
              module use /g/data/v10/public/modules/modulefiles/; \
              module load {{ params.module }}; \
              dea-coherence --check-locationless time in [2019-12-01, 2019-12-31] > coherence-out.txt"
        """,
        params={
            "project": "v10",
            "queue": "normal",
            "module": "dea/unstable",
            "year": "2019",
        },
        do_xcom_push=True,
    )
    wait_for_completion = PBSJobSensor(
        task_id="wait_for_completion",
        ssh_conn_id="lpgs_gadi",
        pbs_job_id="{{ ti.xcom_pull(task_ids='submit_pbs_job') }}",
    )

    wait_for_log_file = SFTPSensor(
        task_id='wait_for_log_file',
        sftp_conn_id='lpgs_gadi',
        path='expected_file_path'  # TODO
    )

    check_qa_file = PythonOperator(
        task_id='check_qa_file',
        python_callable=check_file_for_output
    )

    submit_pbs_job >> wait_for_completion >> wait_for_log_file >> check_qa_file
