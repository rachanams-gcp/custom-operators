"""
Custom Enterprise Manifest Snapshot Operator (custom_governance.manifest)
"""

import json
import logging
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.models import Variable

class GovernanceManifestOperator(BaseOperator):
    """
    Standard Enterprise Operator: Phase 0 Manifest Sync.
    Queries central registry metadata tables and outputs local/GCS JSON snapshots.
    
    Usage:
        manifest_task = GovernanceManifestOperator(
            task_id="sync_governance_manifest",
            interface_cd="MY_INTERFACE_01",
            config_bucket="my-composer-config-bucket"
        )
    """
    template_fields = ("interface_cd", "config_bucket")

    def __init__(self, interface_cd: str, config_bucket: str = None, **kwargs):
        super().__init__(**kwargs)
        self.interface_cd = interface_cd
        self.config_bucket = config_bucket or Variable.get("gcs_config_bucket", "my-composer-config-bucket")

    def execute(self, context):
        dag_id = context["dag"].dag_id
        project_id = Variable.get("gcp_project", "my-gcp-project-id")
        src_db = Variable.get("control_db", "MY_CONTROL_DB")
        
        logging.info(f"Connecting to central control registry for Interface: {self.interface_cd}...")
        try:
            from google.cloud import bigquery, storage
            bq_client = bigquery.Client(project=project_id)
            query = f"""
            SELECT TRIM(ETL_JOB_NO) as job_no, TRIM(ETL_JOB_NM) as job_nm,
                   CASE 
                     WHEN TRIM(CTN_RUN_IND) = 'N' THEN 'N'
                     WHEN CAST(RW_EXP_DT AS STRING) < CAST(CURRENT_DATE() AS STRING) THEN 'N'
                     WHEN CAST(RW_EFF_DT AS STRING) > CAST(CURRENT_DATE() AS STRING) THEN 'N'
                     ELSE 'Y'
                   END as ctn_run_ind
            FROM `{project_id}.{src_db}.ETL_JOB_CONTROL_TABLE`
            WHERE UPPER(ETL_INTF_CD) = '{self.interface_cd.upper()}'
            """
            rows = [{"job_no": r.job_no, "job_nm": r.job_nm, "ctn_run_ind": r.ctn_run_ind} for r in bq_client.query(query).result()]
            manifest = {"interface": self.interface_cd, "exported_at": datetime.now().isoformat(), "jobs": rows}
            
            # Sync local task runner cache
            local_path = os.path.join(os.path.dirname(__file__), f"{dag_id}_manifest.json")
            with open(local_path, "w") as f:
                json.dump(manifest, f, indent=2)
                
            # Sync GCS Fuse shared mirror
            if os.path.exists("/home/airflow/gcs/data"):
                fuse_dir = "/home/airflow/gcs/data/manifests"
                os.makedirs(fuse_dir, exist_ok=True)
                with open(os.path.join(fuse_dir, f"{dag_id}_manifest.json"), "w") as f:
                    json.dump(manifest, f, indent=2)
                
            # Upload to GCS Cloud Storage
            storage.Client(project=project_id).bucket(self.config_bucket).blob(f"manifests/{dag_id}_manifest.json").upload_from_string(json.dumps(manifest))
            logging.info(f"Governance manifest successfully generated ({len(rows)} jobs registered).")
        except Exception as e:
            logging.error(f"CRITICAL: Failed to query central database registry ({e}). Aborting pipeline.")
            raise
