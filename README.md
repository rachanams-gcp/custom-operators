# Custom Enterprise Airflow Governance SDK (`custom_governance`)

**Decoupling Centralized Platform Governance from Data Warehousing Business Logic in Cloud Composer 2 / Apache Airflow.**

---

## 1. Executive Summary & Design Philosophy

In legacy Data Warehousing patterns, controlling whether specific job steps run required complex shell script wrappers, database parameter tables, and manual `IF / ELSE` flags embedded inside every SQL stored procedure.

When migrating to **Apache Airflow / Google Cloud Composer**, forcing data engineers to manually write compliance checks, manifest queries, or inline skip logic inside every single DAG file violates **DRY (Don't Repeat Yourself)** and pollutes business logic.

`custom_governance` is a standardized, reusable enterprise Python SDK package. Developers write 100% vanilla, clean orchestration code. Behind the scenes, `custom_governance` transparently enforces:
1. **Centralized Master Table Job Control:** Queries a central metadata control table before executing compute.
2. **Legacy Row Effective & Expiry Date Governance:** Automatically evaluates `RW_EFF_DT` and `RW_EXP_DT` against `CURRENT_DATE()`.
3. **Dynamic Compute Bypass:** Instantly turns disabled or expired jobs **Pink (Skipped)** in the Airflow UI without running backend SQL compute.
4. **Encapsulated Security:** Resolves encrypted database passwords via GCP Secret Manager automatically.

---

## 2. Package Architecture & Module Anatomy

The `custom_governance` SDK consists of three lightweight, self-contained Python modules:

```text
dags/custom_governance/
  ├── __init__.py      (Package metadata & top-level exports)
  ├── manifest.py      (GovernanceManifestOperator - Phase 0 Snapshot Generator)
  └── operators.py     (Governed drop-in replacements for Airflow operators)
```

### Module A: `custom_governance.manifest` (`GovernanceManifestOperator`)
* **Role:** Phase 0 Manifest Snapshot Sync.
* **Behavior:** Executed once at the very beginning of a DAG run. Connects to BigQuery (`ETL_JOB_CONTROL_TABLE`) and registers all active/disabled jobs for the specified interface code (`interface_cd`).
* **The Legacy Relational Formula:**  
  Inside BigQuery, `GovernanceManifestOperator` executes this exact relational evaluation:
  ```sql
  SELECT TRIM(ETL_JOB_NO) AS job_no, TRIM(ETL_JOB_NM) AS job_nm,
         CASE 
           WHEN TRIM(CTN_RUN_IND) = 'N' THEN 'N'
           WHEN CAST(RW_EXP_DT AS STRING) < CAST(CURRENT_DATE() AS STRING) THEN 'N' -- Expired!
           WHEN CAST(RW_EFF_DT AS STRING) > CAST(CURRENT_DATE() AS STRING) THEN 'N' -- Future!
           ELSE 'Y'
         END AS ctn_run_ind
  FROM `my-gcp-project-id.MY_CONTROL_DB.ETL_JOB_CONTROL_TABLE`
  WHERE UPPER(ETL_INTF_CD) = 'MY_INTERFACE_01';
  ```
* **Output:** Writes an atomic JSON snapshot file (`f"{dag_id}_manifest.json"`) to local worker disk and syncs it to Google Cloud Storage.

### Module B: `custom_governance.operators`
* **Role:** Governed drop-in replacements for standard Apache Airflow operators (`BigQueryInsertJobOperator`, `PythonOperator`, `GKEStartPodOperator`).
* **Behavior:** Right before `operator.execute(context)` runs on any Airflow worker pod, `_check_manifest_governance()` intercepts execution and reads the cached JSON snapshot.
  * If `ctn_run_ind == 'Y'` $\rightarrow$ Authorizes execution. Compute runs normally.
  * If `ctn_run_ind == 'N'` $\rightarrow$ Raises `AirflowSkipException`. Airflow halts pod execution immediately, bypasses compute, and turns the UI box **Pink (Skipped)**!

---

## 3. Developer Quick-Start Guide (Zero Boilerplate!)

To build a brand new governed pipeline, developers simply import operators from `custom_governance` instead of vanilla Airflow providers:

```python
"""
Example Developer DAG: MY_INTERFACE_01_DAG
Notice: 100% clean business logic. Zero inline check code or skip logic!
"""
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup

# KEY STEP: Developers import from custom_governance SDK:
from custom_governance.manifest import GovernanceManifestOperator
from custom_governance.operators import BigQueryInsertJobOperator, PythonOperator

with DAG(
    dag_id="MY_INTERFACE_01_DAG",
    start_date=datetime(2026, 6, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Instantiate Central Manifest Sync at the top of the DAG
    sync_manifest = GovernanceManifestOperator(
        task_id="sync_governance_manifest",
        interface_cd="MY_INTERFACE_01",
    )

    # 2. Standard Business Operators (Governance skip checks happen automatically inside!)
    with TaskGroup("core_transformation_group") as transform_group:
        run_sql = BigQueryInsertJobOperator(
            task_id="run_transformation",
            configuration={"query": {"query": "CALL sp_my_transformation();", "useLegacySql": False}},
        )

    sync_manifest >> transform_group
```

---

## 4. Operational Runbook (For Operations & Production Control)

### How to Skip / Decommission a Job Step:
Operations engineers do not need to modify Airflow DAG code or delete database records. They simply update the control table:

```sql
UPDATE `my-gcp-project-id.MY_CONTROL_DB.ETL_JOB_CONTROL_TABLE`
SET RW_EXP_DT = CURRENT_DATE() - 1 -- Sets Expiry Date to Yesterday (Expired!)
WHERE ETL_INTF_CD = 'MY_INTERFACE_01' AND ETL_JOB_NM = 'run_transformation';
```

### How to Re-Enable a Job Step:
```sql
UPDATE `my-gcp-project-id.MY_CONTROL_DB.ETL_JOB_CONTROL_TABLE`
SET RW_EXP_DT = '9999-12-31', CTN_RUN_IND = 'Y'
WHERE ETL_INTF_CD = 'MY_INTERFACE_01' AND ETL_JOB_NM = 'run_transformation';
```

When the DAG runs next, `GovernanceManifestOperator` captures the updated metadata, authorizes compute, and turns the Airflow UI box **Dark Green (Success)**!
