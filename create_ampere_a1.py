#!/usr/bin/env python3
"""Automate OCI Resource Manager stack Plan + Apply jobs with retry logic."""

import argparse
import logging
import os
import sys
import time

import oci

LOGFILE = "oracle_automation_v2.log"
DEFAULT_POLL_INTERVAL = 5
DEFAULT_RETRY_DELAY = 35
DEFAULT_MAX_RETRIES = 0  # 0 = unlimited

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGFILE, mode="a"),
    ],
)
log = logging.getLogger(__name__)


def wait_for_job(
    rm_client: oci.resource_manager.ResourceManagerClient,
    job_id: str,
    operation: str,
    poll_interval: int = DEFAULT_POLL_INTERVAL,
) -> oci.resource_manager.models.Job:
    """Poll a Resource Manager job until it reaches a terminal state."""
    previous_status = None
    while True:
        job = rm_client.get_job(job_id).data
        status = job.lifecycle_state

        if status != previous_status:
            log.info("%s job status: %s", operation, status)
            previous_status = status

        if status in TERMINAL_STATES:
            return job

        time.sleep(poll_interval)


def run_plan(
    rm_client: oci.resource_manager.ResourceManagerClient,
    stack_id: str,
) -> str:
    """Create and wait for a PLAN job. Returns the job ID on success, exits on failure."""
    log.info("Starting PLAN job...")

    job = rm_client.create_job(
        oci.resource_manager.models.CreateJobDetails(
            stack_id=stack_id,
            operation="PLAN",
        )
    ).data
    log.info("Created PLAN job: %s", job.id)

    job = wait_for_job(rm_client, job.id, "PLAN")

    if job.lifecycle_state == "SUCCEEDED":
        log.info("PLAN job succeeded.")
        return job.id

    log.error("PLAN job failed: %s", getattr(job, "failure_details", None))
    sys.exit(1)


def run_apply(
    rm_client: oci.resource_manager.ResourceManagerClient,
    stack_id: str,
) -> bool:
    """Create and wait for an APPLY job. Returns True on success, False on failure."""
    log.info("Starting APPLY job...")

    job = rm_client.create_job(
        oci.resource_manager.models.CreateJobDetails(
            stack_id=stack_id,
            operation="APPLY",
            apply_job_plan_resolution=oci.resource_manager.models.ApplyJobPlanResolution(
                is_auto_approved=True,
            ),
        )
    ).data
    log.info("Created APPLY job: %s", job.id)

    job = wait_for_job(rm_client, job.id, "APPLY")

    if job.lifecycle_state == "SUCCEEDED":
        log.info("APPLY job succeeded.")
        return True

    log.error("APPLY job failed: %s", getattr(job, "failure_details", None))
    try:
        logs = rm_client.get_job_logs_content(job.id).data
        error_lines = [line for line in logs.splitlines() if "Error:" in line]
        if error_lines:
            log.error("Error from logs:\n%s", "\n".join(error_lines))
    except oci.exceptions.ServiceError:
        log.warning("Could not retrieve job logs.")

    return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stack-id",
        default=os.environ.get("OCI_STACK_ID"),
        help="OCI Resource Manager Stack OCID (or set OCI_STACK_ID env var)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.environ.get("MAX_RETRIES", DEFAULT_MAX_RETRIES)),
        help="Max APPLY retries, 0 = unlimited (default: %(default)s)",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=DEFAULT_RETRY_DELAY,
        help="Seconds to wait between retries (default: %(default)s)",
    )
    args = parser.parse_args()

    if not args.stack_id:
        parser.error("--stack-id is required (or set OCI_STACK_ID)")

    os.environ["SUPPRESS_LABEL_WARNING"] = "True"
    config = oci.config.from_file()
    rm_client = oci.resource_manager.ResourceManagerClient(config)

    log.info("Using Stack ID: %s", args.stack_id)

    attempt = 0
    while True:
        attempt += 1
        run_plan(rm_client, args.stack_id)

        if run_apply(rm_client, args.stack_id):
            break

        if args.max_retries and attempt >= args.max_retries:
            log.error("Exhausted %d retries. Giving up.", args.max_retries)
            sys.exit(1)

        log.info("Retrying in %d seconds... (attempt %d)", args.retry_delay, attempt)
        time.sleep(args.retry_delay)


if __name__ == "__main__":
    main()
