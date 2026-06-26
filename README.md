# 📘 IBX Enterprise Airflow Governance SDK (`ibx_governance`)

**Decoupling Centralized Platform Governance from Data Warehousing Business Logic in Cloud Composer 2.**

---

## 🏛️ 1. Executive Summary & Design Philosophy

In legacy Data Warehousing (Informatics / IBX standard pattern), controlling whether specific job steps run required complex shell script wrappers, database parameter tables, and manual `IF / ELSE` flags embedded inside every SQL stored procedure.

When migrating to **Google Cloud Composer 2 (Apache Airflow)**, forcing data engineers to manually write compliance checks, manifest queries, or `if skip: raise AirflowSkipException` inside every single DAG file violates **DRY (Don't Repeat Yourself)** and pollutes business logic.

`ibx_governance` is a standardized, reusable enterprise Python SDK package. Developers write 100% vanilla, clean orchestration code. Behind the scenes, `ibx_governance` transparently enforces:
1. **Centralized Master Table Job Control:** Queries `ETL_JOB_DTL_PROD_CP_GGLE` before executing compute.
2. **Legacy Row Effective & Expiry Date Governance:** Automatically evaluates `RW_EFF_DT` and `RW_EXP_DT` against `CURRENT_DATE()`.
3. **Dynamic Compute Bypass:** Instantly turns disabled or expired jobs **Pink (Skipped)** in the Airflow UI without running BigQuery SQL compute.
4. **Encapsulated Security:** Resolves encrypted database passwords via GCP Secret Manager automatically.

---

## ⚙️ 2. Package Architecture & Module Anatomy

The `ibx_governance` SDK consists of three lightweight, self-contained Python modules:

```text
dags/ibx_governance/
  ├── __init__.py      (Package metadata & top-level exports)
  ├── manifest.py      (GovernanceManifestOperator - Phase 0 Snapshot Generator)
  └── operators.py     (Governed drop-in replacements for Airflow operators)
```

### Module A: `ibx_governance.manifest` (`GovernanceManifestOperator`)
* **Role:** Phase 0 Manifest Snapshot Sync.
* **Behavior:** Executed once at the very beginning of a DAG run. Connects to BigQuery (`ETL_JOB_DTL_PROD_CP_GGLE`) and registers all active/disabled jobs for the specified interface code (`interface_cd`).
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
  FROM `ihg-dart-edw-test5.DB_SRCT5.ETL_JOB_DTL_PROD_CP_GGLE`
  WHERE UPPER(ETL_INTF_CD) = 'SRCPROV11172_TEST';
  ```
* **Output:** Writes an atomic JSON snapshot file (`f"{dag_id}_manifest.json"`) to local worker disk and syncs it to Google Cloud Storage.

### Module B: `ibx_governance.operators`
* **Role:** Governed drop-in replacements for standard Apache Airflow operators (`BigQueryInsertJobOperator`, `PythonOperator`, `GKEStartPodOperator`).
* **Behavior:** Right before `operator.execute(context)` runs on any Cloud Composer worker pod, `_check_manifest_governance()` intercepts execution and reads the cached JSON snapshot.
  * If `ctn_run_ind == 'Y'` $\rightarrow$ Authorizes execution. BigQuery SQL compute runs normally.
  * If `ctn_run_ind == 'N'` $\rightarrow$ Raises `AirflowSkipException`. Airflow halts pod execution immediately, bypasses database compute, and turns the UI box **Pink (Skipped)**!

---

## 🚀 3. Developer Quick-Start Guide (Zero Boilerplate!)

To build a brand new governed pipeline, developers simply import operators from `ibx_governance` instead of vanilla Airflow providers:

```python
"""
Example Developer DAG: SRCPROV11172_SHARED_SDK_TEST
Notice: 100% clean business logic. Zero inline check code or skip logic!
"""
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup

# 🌟 KEY STEP: Developers import from ibx_governance SDK:
from ibx_governance.manifest import GovernanceManifestOperator
from ibx_governance.operators import BigQueryInsertJobOperator, PythonOperator

with DAG(
    dag_id="SRCPROV11172_SHARED_SDK_TEST",
    start_date=datetime(2026, 6, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Instantiate Central Manifest Sync at the top of the DAG
    sync_manifest = GovernanceManifestOperator(
        task_id="sync_governance_manifest",
        interface_cd="SRCPROV11172_TEST",
    )

    # 2. Standard Business Operators (Governance skip checks happen automatically inside!)
    with TaskGroup("cdw_core_merge_P12") as p12_group:
        merge_core = BigQueryInsertJobOperator(
            task_id="merge_core",
            configuration={"query": {"query": "CALL sp_SRCPROV11172_aedw_load();", "useLegacySql": False}},
        )

    sync_manifest >> p12_group
```

---

## 📘 4. Operational Runbook (For Operations & Production Control)

### How to Skip / Decommission a Job Step Today:
Operations engineers do not need to modify Airflow DAG code or delete database records. They simply expire the row date in BigQuery:

```sql
UPDATE `ihg-dart-edw-test5.DB_SRCT5.ETL_JOB_DTL_PROD_CP_GGLE`
SET RW_EXP_DT = CURRENT_DATE() - 1 -- Sets Expiry Date to Yesterday (Expired!)
WHERE ETL_INTF_CD = 'SRCPROV11172_TEST' AND ETL_JOB_NM = 'merge_core';
```

### How to Re-Enable a Job Step:
```sql
UPDATE `ihg-dart-edw-test5.DB_SRCT5.ETL_JOB_DTL_PROD_CP_GGLE`
SET RW_EXP_DT = '9999-12-31', CTN_RUN_IND = 'Y'
WHERE ETL_INTF_CD = 'SRCPROV11172_TEST' AND ETL_JOB_NM = 'merge_core';
```

When the DAG runs next, `GovernanceManifestOperator` captures the updated metadata, authorizes compute, and turns the Airflow UI box **Dark Green (Success)**!
