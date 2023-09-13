"""Loading data from Postgresql to ElasticSearch"""
import logging
import os
from time import sleep

from auth_to_notification.config.settings import settings
from auth_to_notification.connections import (
    PostgresConnectionManager,
    PostgresConnector,
    RedisConnectionManager,
    RedisConnector,
)
from auth_to_notification.exceptions import Error
from auth_to_notification.loaders import PostgresLoader
from auth_to_notification.producers import PostgresProducer, Producer
from auth_to_notification.state import RedisStorage, State

logger = logging.getLogger()
logger.setLevel(os.environ.get("ETL_LOG_LEVEL", logging.INFO))


class ExtractionManager:
    producer: Producer
    loader: PostgresLoader
    index_name: str

    def execute_etl(self):
        logging.debug(f"Executing etl for {self.index_name}...")
        for table, changed in self.producer.scan(settings.tables_for_scan):
            logging.debug(f"Produced keys for table {table}")
            try:
                self.loader.load(table, changed)
            except Error as e:
                logging.error(e)
                continue
        logging.info(f"Finished etl for {self.index_name}")


class ExtractionUsersManager(ExtractionManager):
    def __init__(
        self, postgres: PostgresConnectionManager, redis: RedisConnectionManager
    ):
        self.postgres = postgres
        self.redis = redis
        self.storage = RedisStorage(self.redis)
        self.state = State(self.storage)
        self.producer = PostgresProducer(self.state, self.postgres)
        self.loader = PostgresLoader(self.postgres)


def transfer():
    """Основной метод загрузки данных из Postgres в ElasticSearch"""

    logging.debug("Beginning the extraction process...")
    logging.debug("Trying to establsh connection with db...")

    sentry_dsn = os.environ.get('SENTRY_DSN')

    if sentry_dsn:
        import sentry_sdk  # type: ignore

        sentry_sdk.init(dsn=sentry_dsn, traces_sample_rate=1.0)

    while True:
        try:
            postgres_manager = PostgresConnectionManager(PostgresConnector())
            redis_manager = RedisConnectionManager(RedisConnector())
            with (postgres_manager as postgres, redis_manager as redis):
                manager = ExtractionUsersManager(postgres, redis)
                manager.execute_etl()
                del manager

            logging.debug("Sleeping...")
            sleep(settings.wait_up_to)

        except Exception as e:
            logging.error(type(e))
            logging.error(e)
            logging.error("Exited scan...")
            return


if __name__ == "__main__":
    transfer()
