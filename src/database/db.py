import logging
from typing import List, Dict, Any, Optional
from uuid import uuid4
import clickhouse_connect
from ..config import Config

logger = logging.getLogger(__name__)


class ClickHouseClient:

    def __init__(self):
        self.client = None
        self._connect()

    def _connect(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=Config.CLICKHOUSE_HOST,
                port=Config.CLICKHOUSE_PORT,
                username=Config.CLICKHOUSE_USER,
                password=Config.CLICKHOUSE_PASSWORD,
                database=Config.CLICKHOUSE_DATABASE
            )
            logger.info(f'Connected to ClickHouse at {Config.CLICKHOUSE_HOST}:{Config.CLICKHOUSE_PORT}')
        except Exception as e:
            logger.error(f'Failed to connect to ClickHouse: {e}')
            raise

    def execute_query_dict(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        attempts = 2
        for attempt in range(attempts):
            try:
                logger.info('Executing query...')
                settings = {
                    'session_id': str(uuid4()),
                    'session_timeout': 900,
                    'max_execution_time': 900
                }
                result = self.client.query(query, parameters=parameters or {}, settings=settings)
                column_names = result.column_names
                dict_rows = [dict(zip(column_names, row)) for row in result.result_rows]
                logger.info(f'Query completed: {len(dict_rows):,} rows')
                return dict_rows
            except Exception as e:
                msg = str(e)
                if ('SESSION_IS_LOCKED' in msg or 'code: 373' in msg) and attempt < attempts - 1:
                    logger.warning('Session locked, reconnecting...')
                    self._connect()
                    continue
                logger.error(f'Query execution failed: {e}', exc_info=True)
                raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info('ClickHouse connection closed')


_db_client = None


def get_db_client() -> ClickHouseClient:
    global _db_client
    if _db_client is None:
        _db_client = ClickHouseClient()
    return _db_client