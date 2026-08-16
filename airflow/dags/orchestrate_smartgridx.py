from airflow.sdk import dag, task, get_current_context
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import os
import time
import pendulum


DBT_PROJECT_DIR = "/opt/airflow/dbt"


def run_databricks_job(job_id_env_var: str, notebook_params: dict) -> str:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    job_id = int(os.environ[job_id_env_var])

    poll_interval = int(os.getenv("DATABRICKS_JOB_POLL_INTERVAL_SECONDS", "10"))
    max_wait = int(os.getenv("DATABRICKS_JOB_MAX_WAIT_SECONDS", "7200"))

    ws = WorkspaceClient(host=host, token=token)

    print(f"Triggering Databricks job_id={job_id}")
    print(f"Notebook params={notebook_params}")

    job_trigger = ws.jobs.run_now(
        job_id=job_id,
        notebook_params=notebook_params,
    )

    print(f"Databricks run_id={job_trigger.run_id}")
    print(f"Databricks run URL: {host}/#job/{job_id}/run/{job_trigger.run_id}")

    started = time.time()

    while True:
        job_run = ws.jobs.get_run(job_trigger.run_id)
        state = job_run.state

        print(
            "Databricks state: "
            f"life_cycle_state={state.life_cycle_state}, "
            f"result_state={state.result_state}, "
            f"message={state.state_message}"
        )

        if state.life_cycle_state in [
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
            RunLifeCycleState.INTERNAL_ERROR,
        ]:
            if state.result_state == RunResultState.SUCCESS:
                print("Databricks job completed successfully.")
                return f"Databricks job {job_id} completed successfully"

            raise Exception(
                f"Databricks job {job_id} failed. "
                f"life_cycle_state={state.life_cycle_state}, "
                f"result_state={state.result_state}, "
                f"message={state.state_message}"
            )

        if time.time() - started > max_wait:
            raise TimeoutError(
                f"Timed out waiting for Databricks job {job_id} after {max_wait} seconds"
            )

        time.sleep(poll_interval)


@dag(
    dag_id="smartgridx_daily_energy_pipeline",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["smartgridx", "energy", "databricks", "dbt"],
)
def orchestrate_smartgridx():

    @task
    def bronze_ingestion():
        context = get_current_context()
        return run_databricks_job(
            job_id_env_var="DATABRICKS_BRONZE_JOB_ID",
            notebook_params={
                "environment": os.getenv("SMARTGRIDX_ENVIRONMENT", "dev"),
                "run_id": context["run_id"],
            },
        )

    @task
    def silver_meter_readings():
        context = get_current_context()
        return run_databricks_job(
            job_id_env_var="DATABRICKS_SILVER_METER_READINGS_JOB_ID",
            notebook_params={
                "environment": os.getenv("SMARTGRIDX_ENVIRONMENT", "dev"),
                "catalog": os.getenv("SMARTGRIDX_CATALOG", "smartgridx_dev"),
                "run_id": context["run_id"],
            },
        )

    @task
    def silver_core_sources():
        context = get_current_context()
        return run_databricks_job(
            job_id_env_var="DATABRICKS_SILVER_CORE_SOURCES_JOB_ID",
            notebook_params={
                "environment": os.getenv("SMARTGRIDX_ENVIRONMENT", "dev"),
                "catalog": os.getenv("SMARTGRIDX_CATALOG", "smartgridx_dev"),
                "run_id": context["run_id"],
            },
        )

    clean_dbt_target = BashOperator(
        task_id="clean_dbt_target",
        cwd=DBT_PROJECT_DIR,
        bash_command="rm -rf target logs",
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt debug --profiles-dir .",
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt build --select staging --profiles-dir .",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt snapshot --profiles-dir .",
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt build --select marts --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt test --profiles-dir .",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt docs generate --profiles-dir .",
    )

    (
        bronze_ingestion()
        >> silver_meter_readings()
        >> silver_core_sources()
        >> clean_dbt_target
        >> dbt_debug
        >> dbt_build_staging
        >> dbt_snapshot
        >> dbt_build_marts
        >> dbt_test
        >> dbt_docs_generate
    )


orchestrate_smartgridx()
