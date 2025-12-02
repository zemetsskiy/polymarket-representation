import sys
import logging
from datetime import datetime, timedelta
from src.config import setup_logging
from src.core import PolymarketSmartMoneyWorker

setup_logging()
logger = logging.getLogger(__name__)

JOB_CONFIGS = {
    'polymarket_smart_money_hourly': {
        'limit': 10000,
        'interval_minutes': 60,
        'description': 'Polymarket top 10k smart money (hourly)'
    },
    'polymarket_smart_money_daily': {
        'limit': 50000,
        'interval_minutes': 1440,
        'description': 'Polymarket full 50k smart money (daily)'
    }
}


def calculate_next_run(job_name: str) -> datetime:
    now = datetime.utcnow()
    if job_name == 'polymarket_smart_money_daily':
        next_run = now.replace(hour=1, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
    else:
        next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return next_run


def log_schedule_info(job_name: str, is_start: bool = True):
    now = datetime.utcnow()
    next_run = calculate_next_run(job_name)
    total_seconds = int((next_run - now).total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m {seconds}s"
    status = "STARTED" if is_start else "COMPLETED"
    config = JOB_CONFIGS.get(job_name, {})
    desc = config.get('description', job_name)
    logger.info(f'[{job_name}] {status} at {now.strftime("%H:%M:%S")} UTC | {desc} | Next: {next_run.strftime("%H:%M:%S")} UTC (in {time_str})')


def run_job(job_name: str) -> int:
    if job_name not in JOB_CONFIGS:
        logger.error(f"Unknown job: {job_name}")
        logger.info(f"Available jobs: {', '.join(JOB_CONFIGS.keys())}")
        return 1

    config = JOB_CONFIGS[job_name]
    log_schedule_info(job_name, is_start=True)

    try:
        worker = PolymarketSmartMoneyWorker()
        results = worker.run(limit=config['limit'])
        log_schedule_info(job_name, is_start=False)
        logger.info(f"Results: {results}")
        return 0
    except Exception as e:
        logger.error(f"Job {job_name} failed: {e}", exc_info=True)
        return 1


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python worker_scheduled.py <job_name>")
        print(f"Available jobs: {', '.join(JOB_CONFIGS.keys())}")
        return 1
    return run_job(sys.argv[1])


if __name__ == '__main__':
    sys.exit(main())
