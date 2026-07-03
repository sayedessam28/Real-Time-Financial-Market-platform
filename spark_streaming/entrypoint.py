import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JOBS = {
    "bronze": "spark_streaming.bronze.bronze_job",
    "silver": "spark_streaming.silver.silver_job",
    "alerts": "spark_streaming.alerts.alert_job",
}

if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else None

    if job not in JOBS:
        print(f"Usage: python -m spark_streaming.entrypoint [{'|'.join(JOBS)}]")
        sys.exit(1)

    logger.info(f"Starting job: {job}")
    __import__(JOBS[job])
