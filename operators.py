"""
Custom Enterprise Governed Operators (custom_governance.operators)
Drop-in replacements for standard Airflow operators with automatic skip interception
and built-in database audit logging.
"""

import json
import logging
import os
from airflow.operators.python import PythonOperator as VanillaPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator as VanillaBigQueryInsertJobOperator

def _check_manifest_governance(task_id: str, dag_id: str):
    """Encapsulated central manifest lookup executed prior to compute execution."""
    short_id = task_id.split(".")[-1]
    
    manifest_path = os.path.join(os.path.dirname(__file__), f"{dag_id}_manifest.json")
    fuse_path = f"/home/airflow/gcs/data/manifests/{dag_id}_manifest.json"
    data = None
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f: data = json.load(f)
    elif os.path.exists(fuse_path):
        with open(fuse_path, "r") as f: data = json.load(f)
        
    if data:
        for j in data.get("jobs", []):
            if j.get("job_no") in (task_id, short_id) or j.get("job_nm") in (task_id, short_id):
                if str(j.get("ctn_run_ind", "Y")).upper() == "N":
                    logging.warning(f"Governance Policy: Task '{task_id}' disabled in control registry. Skipping compute.")
                    try:
                        from airflow.exceptions import AirflowSkipException
                    except ImportError:
                        class AirflowSkipException(Exception): pass
                    raise AirflowSkipException(f"Task '{task_id}' disabled via central control registry.")

def _audit_log(task_id: str, dag_id: str, status_cd: str, batch_id: str = "MANUAL"):
    """Encapsulated central audit logging into ETL_JOB_AUDIT_TABLE."""
    try:
        from airflow.models import Variable
        from google.cloud import bigquery
        project_id = Variable.get("gcp_project", "my-gcp-project-id")
        src_db = Variable.get("audit_db", "MY_AUDIT_DB")
        client = bigquery.Client(project=project_id)
        short_id = task_id.split(".")[-1]
        
        query = f"""
        INSERT INTO `{project_id}.{src_db}.ETL_JOB_AUDIT_TABLE`
        (ETL_INTF_CD, ETL_INTF_NBR, ETL_JOB_NO, ETL_PCS_ID, ETL_BATCH_SK, ETL_JOB_STRT_TS, ETL_JOB_END_TS, ETL_JOB_AUD_CTL_STS_CD, ETL_JOB_DTSTG_JOB_LOG_DSC)
        VALUES (
            'MY_INTERFACE_01',
            100,
            '{short_id}',
            'P0',
            1001,
            CURRENT_DATETIME("America/New_York"),
            CURRENT_DATETIME("America/New_York"),
            '{status_cd}',
            'AIRFLOW_SDK_AUDIT_EVENT'
        )
        """
        client.query(query).result()
        logging.info(f"Audit log recorded: Task '{task_id}' -> Status '{status_cd}'")
    except Exception as e:
        logging.warning(f"Audit log non-blocking observability event warning: {e}")

class BigQueryInsertJobOperator(VanillaBigQueryInsertJobOperator):
    """Governed BigQuery Operator: Automatically intercepts compute and writes central audit logs."""
    def execute(self, context):
        dag_id = context["dag"].dag_id if context.get("dag") else "UNKNOWN_DAG"
        batch_id = str(context.get("run_id", "MANUAL"))
        _check_manifest_governance(self.task_id, dag_id)
        _audit_log(self.task_id, dag_id, "I", batch_id)
        try:
            res = super().execute(context)
            _audit_log(self.task_id, dag_id, "S", batch_id)
            return res
        except Exception:
            _audit_log(self.task_id, dag_id, "F", batch_id)
            raise

class PythonOperator(VanillaPythonOperator):
    """Governed Python Operator: Automatically intercepts compute and writes central audit logs."""
    def execute(self, context):
        dag_id = context["dag"].dag_id if context.get("dag") else "UNKNOWN_DAG"
        batch_id = str(context.get("run_id", "MANUAL"))
        _check_manifest_governance(self.task_id, dag_id)
        _audit_log(self.task_id, dag_id, "I", batch_id)
        try:
            res = super().execute(context)
            _audit_log(self.task_id, dag_id, "S", batch_id)
            return res
        except Exception:
            _audit_log(self.task_id, dag_id, "F", batch_id)
            raise

try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator as VanillaGKEStartPodOperator
    class GKEStartPodOperator(VanillaGKEStartPodOperator):
        """Governed GKE Operator: Automatically intercepts compute and writes central audit logs."""
        def execute(self, context):
            dag_id = context["dag"].dag_id if context.get("dag") else "UNKNOWN_DAG"
            batch_id = str(context.get("run_id", "MANUAL"))
            _check_manifest_governance(self.task_id, dag_id)
            _audit_log(self.task_id, dag_id, "I", batch_id)
            try:
                res = super().execute(context)
                _audit_log(self.task_id, dag_id, "S", batch_id)
                return res
            except Exception:
                _audit_log(self.task_id, dag_id, "F", batch_id)
                raise
except ImportError:
    pass
