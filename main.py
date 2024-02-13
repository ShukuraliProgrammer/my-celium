from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os, logging, warnings
from utils.discord import Discord
from drivers import DYDXFutures, DYDXFunding, CCXTDriverFunding, CCXTDriverOHLCV
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
warnings.simplefilter("ignore")

discord = Discord()

# the hour at which we execute
HOUR = 12
UPLOAD = True
UPLOAD_ONE_AT_A_TIME = True


def tick():
    print("Tick! The time is: %s" % datetime.now())
    return {"hello": "world"}


def updateBinance1hSpot():
    ccxt_ohlcv = CCXTDriverOHLCV(
        ccxt_exchange_id="binance",
        timeframe="1h",
        default_type="spot",
        coinapi_exchange_id="BINANCE",
        coinapi_symbol_type="SPOT",
    )
    ccxt_ohlcv.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def updateBinance1dSpot():
    ccxt_ohlcv = CCXTDriverOHLCV(
        ccxt_exchange_id="binance",
        timeframe="1d",
        default_type="spot",
        coinapi_exchange_id="BINANCE",
        coinapi_symbol_type="SPOT",
    )
    ccxt_ohlcv.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def updateBinance1hFuture():
    ccxt_ohlcv = CCXTDriverOHLCV(
        ccxt_exchange_id="binance",
        timeframe="1h",
        default_type="future",
        coinapi_exchange_id="BINANCEFTS",
        coinapi_symbol_type="PERPETUAL",
    )
    ccxt_ohlcv.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def updateBinance8hFuture():
    ccxt_ohlcv = CCXTDriverOHLCV(
        ccxt_exchange_id="binance",
        timeframe="8h",
        default_type="future",
        coinapi_exchange_id="BINANCEFTS",
        coinapi_symbol_type="PERPETUAL",
    )
    ccxt_ohlcv.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def updateBinanceFunding():
    ccxt_funding = CCXTDriverFunding(
        ccxt_exchange_id="binance",
        coinapi_exchange_id="BINANCEFTS",
        coinapi_symbol_type="PERPETUAL",
    )
    ccxt_funding.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def updateDYDXFunding():
    dydx = DYDXFunding()
    dydx.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def updateDYDX1hFuture():
    dydx_futures = DYDXFutures(timeframe="1HOUR")
    dydx_futures.fetch_data(upload=UPLOAD, upload_one_at_a_time=UPLOAD_ONE_AT_A_TIME)


def scheduler_callback(event):
    logger.info(f"Job: {event.job_id} completed, with exception:{event.exception}")
    discord.send_embed(job_id=event.job_id, exception=event.exception)


def schedule_all_jobs(start_scheduler: bool = True):
    scheduler = BlockingScheduler()
    scheduler.add_listener(scheduler_callback, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    # scheduler.add_executor('processpool')

    # adding one job per driver is a good idea since if one fatally fails the others won't be compromised

    scheduler.add_job(
        func=updateBinance1hSpot,
        trigger=CronTrigger(hour=HOUR),
        id="updateBinance1hSpot",
    )
    logger.debug("Added job 'updateBinance1hSpot'.")

    scheduler.add_job(
        func=updateBinance1dSpot,
        trigger=CronTrigger(hour=HOUR),
        id="updateBinance1dSpot",
    )
    logger.debug("Added job 'updateBinance1dSpot'.")

    scheduler.add_job(
        func=updateBinance1hFuture,
        trigger=CronTrigger(hour=HOUR),
        id="updateBinance1hFuture",
    )
    logger.debug("Added job 'updateBinance1hFuture'.")

    scheduler.add_job(
        func=updateBinance8hFuture,
        trigger=CronTrigger(hour=HOUR),
        id="updateBinance8hFuture",
    )
    logger.debug("Added job 'updateBinance8hFuture'.")

    scheduler.add_job(
        func=updateBinanceFunding,
        trigger=CronTrigger(hour=HOUR),
        id="updateBinanceFunding",
    )
    logger.debug("Added job 'updateBinanceFunding'.")

    scheduler.add_job(
        func=updateDYDXFunding,
        trigger=CronTrigger(hour=HOUR),
        id="updateDYDXFunding",
    )
    logger.debug("Added job 'updateDYDXFunding'.")

    scheduler.add_job(
        func=updateDYDX1hFuture,
        trigger=CronTrigger(hour=HOUR),
        id="updateDYDX1hFuture",
    )
    logger.debug("Added job 'updateDYDX1hFuture'.")

    # scheduler.add_job(
    #     func=tick,
    #     # trigger=CronTrigger(second="0"),
    #     trigger=CronTrigger(hour=HOUR),
    #     id="tick",
    # )
    # logger.debug("Added job 'tick'.")

    if os.getenv("DEBUG") == "True":
        jobs = scheduler.get_jobs()
        for job in jobs:
            job.trigger = DateTrigger()

    if start_scheduler:
        try:
            logger.debug("Starting scheduler...")
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("Stopping scheduler...")
            scheduler.shutdown()
            logger.info("Scheduler shut down successfully!")

    return scheduler


if __name__ == "__main__":
    schedule_all_jobs()
