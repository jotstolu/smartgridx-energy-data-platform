import argparse
import json
import os
import sys
import time

import requests


def run_databricks_job(job_id: int, notebook_params: dict) -> None:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "job_id": job_id,
        "notebook_params": notebook_params,
    }

    print(f"Triggering Databricks job_id={job_id}")
    print(f"Notebook params={json.dumps(notebook_params)}")

    run_now_response = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json=payload,
        timeout=60,
    )

    print("Run-now status code:", run_now_response.status_code)
    print("Run-now response:", run_now_response.text[:2000])

    if run_now_response.status_code >= 400:
        sys.exit(1)

    run_id = run_now_response.json()["run_id"]

    print(f"Databricks run_id={run_id}")
    print(f"Databricks run URL: {host}/#job/{job_id}/run/{run_id}")

    max_wait_seconds = int(os.getenv("DATABRICKS_JOB_MAX_WAIT_SECONDS", "7200"))
    poll_interval_seconds = int(os.getenv("DATABRICKS_JOB_POLL_INTERVAL_SECONDS", "30"))

    started = time.time()

    while True:
        run_response = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=60,
        )

        if run_response.status_code >= 400:
            print("Run status request failed:")
            print(run_response.text[:2000])
            sys.exit(1)

        run_data = run_response.json()
        state = run_data.get("state", {})

        life_cycle_state = state.get("life_cycle_state")
        result_state = state.get("result_state")
        state_message = state.get("state_message")

        print(
            f"Databricks state: life_cycle_state={life_cycle_state}, "
            f"result_state={result_state}, message={state_message}"
        )

        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result_state == "SUCCESS":
                print("Databricks job completed successfully.")
                return

            print("Databricks job failed.")
            print(json.dumps(state, indent=2))
            sys.exit(1)

        elapsed = time.time() - started

        if elapsed > max_wait_seconds:
            print(f"Timed out after {max_wait_seconds} seconds.")
            sys.exit(1)

        time.sleep(poll_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--notebook-params", required=True)

    args = parser.parse_args()

    notebook_params = json.loads(args.notebook_params)

    run_databricks_job(
        job_id=args.job_id,
        notebook_params=notebook_params,
    )


if __name__ == "__main__":
    main()