import logging
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from ..config import Config

logger = logging.getLogger(__name__)


class PostgresClient:

    TABLE_NAME = "smartmoney_polymarket"

    def __init__(self):
        self.conn = None
        self._connect()
        self._ensure_table()

    def _connect(self):
        try:
            if Config.POSTGRES_CONNECTION_STRING:
                self.conn = psycopg2.connect(Config.POSTGRES_CONNECTION_STRING)
            else:
                self.conn = psycopg2.connect(
                    host=Config.POSTGRES_HOST,
                    port=Config.POSTGRES_PORT,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                    database=Config.POSTGRES_DATABASE
                )
            self.conn.autocommit = False
            logger.info(f'Connected to PostgreSQL at {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}')
        except Exception as e:
            logger.error(f'Failed to connect to PostgreSQL: {e}')
            raise

    def _ensure_table(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            wallet_address VARCHAR(128) NOT NULL UNIQUE,
            positions_count INTEGER DEFAULT 0,
            markets_count INTEGER DEFAULT 0,
            avg_trades_per_position DOUBLE PRECISION DEFAULT 0,
            profit_usdc DOUBLE PRECISION DEFAULT 0,
            avg_roi DOUBLE PRECISION DEFAULT 0,
            total_returned_usdc DOUBLE PRECISION DEFAULT 0,
            total_invested_usdc DOUBLE PRECISION DEFAULT 0,
            portfolio_roi DOUBLE PRECISION DEFAULT 0,
            first_trade_at TIMESTAMP,
            last_trade_at TIMESTAMP,
            annual_avg_roi DOUBLE PRECISION DEFAULT 0,
            annual_portfolio_roi DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_wallet ON {self.TABLE_NAME} (wallet_address);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_profit ON {self.TABLE_NAME} (profit_usdc DESC);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_roi ON {self.TABLE_NAME} (portfolio_roi DESC);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_created ON {self.TABLE_NAME} (created_at DESC);
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_sql)
            self.conn.commit()
            logger.info(f'Ensured table {self.TABLE_NAME} exists')
        except Exception as e:
            self.conn.rollback()
            logger.error(f'Failed to create table: {e}')
            raise

    def refresh_smart_money(self, metrics: List[Dict[str, Any]]) -> int:
        if not metrics:
            logger.warning("No metrics to insert")
            return 0

        try:
            with self.conn.cursor() as cur:
                upsert_sql = f"""
                INSERT INTO {self.TABLE_NAME} (
                    wallet_address, positions_count, markets_count, avg_trades_per_position,
                    profit_usdc, avg_roi, total_returned_usdc, total_invested_usdc, portfolio_roi,
                    first_trade_at, last_trade_at, annual_avg_roi, annual_portfolio_roi
                ) VALUES %s
                ON CONFLICT (wallet_address) DO UPDATE SET
                    positions_count = EXCLUDED.positions_count,
                    markets_count = EXCLUDED.markets_count,
                    avg_trades_per_position = EXCLUDED.avg_trades_per_position,
                    profit_usdc = EXCLUDED.profit_usdc,
                    avg_roi = EXCLUDED.avg_roi,
                    total_returned_usdc = EXCLUDED.total_returned_usdc,
                    total_invested_usdc = EXCLUDED.total_invested_usdc,
                    portfolio_roi = EXCLUDED.portfolio_roi,
                    first_trade_at = EXCLUDED.first_trade_at,
                    last_trade_at = EXCLUDED.last_trade_at,
                    annual_avg_roi = EXCLUDED.annual_avg_roi,
                    annual_portfolio_roi = EXCLUDED.annual_portfolio_roi,
                    updated_at = NOW()
                """

                values = []
                for m in metrics:
                    wallet = m.get('wallet_address', '')
                    if isinstance(wallet, bytes):
                        wallet = wallet.decode('utf-8').rstrip('\x00')
                    values.append((
                        wallet,
                        int(m.get('positions_count', 0)),
                        int(m.get('markets_count', 0)),
                        float(m.get('avg_trades_per_position', 0)),
                        float(m.get('profit_usdc', 0)),
                        float(m.get('avg_roi', 0)),
                        float(m.get('total_returned_usdc', 0)),
                        float(m.get('total_invested_usdc', 0)),
                        float(m.get('portfolio_roi', 0)),
                        m.get('first_trade_at'),
                        m.get('last_trade_at'),
                        float(m.get('annual_avg_roi', 0) or 0),
                        float(m.get('annual_portfolio_roi', 0) or 0),
                    ))

                execute_values(
                    cur, upsert_sql, values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                logger.info(f'Upserted {len(metrics):,} smart money records')

            self.conn.commit()
            return len(metrics)

        except Exception as e:
            self.conn.rollback()
            logger.error(f'Failed to refresh smart money data: {e}')
            raise

    def get_user_count(self) -> int:
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f'Failed to get user count: {e}')
            return 0

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info('PostgreSQL connection closed')


_postgres_client = None


def get_postgres_client() -> PostgresClient:
    global _postgres_client
    if _postgres_client is None:
        _postgres_client = PostgresClient()
    return _postgres_client
