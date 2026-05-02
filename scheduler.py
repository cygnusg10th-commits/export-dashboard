"""
매일 오전 9시 자동 파싱 스케줄러
별도 터미널에서 실행: python scheduler.py
"""
import logging
from pathlib import Path
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, str(Path(__file__).parent))
from parser import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def job():
    logger.info("=== 정기 파싱 시작 ===")
    run(force=False)
    logger.info("=== 정기 파싱 완료 ===")


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone="Asia/Seoul")
    scheduler.add_job(job, CronTrigger(hour=9, minute=0), id="daily_parse")
    logger.info("스케줄러 시작 (매일 09:00 자동 실행)")
    logger.info("Ctrl+C 로 종료")

    # 시작 즉시 1회 실행
    job()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("스케줄러 종료")
