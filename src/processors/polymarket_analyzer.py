import logging
from typing import Dict, Any
from ..database import get_db_client, get_postgres_client

logger = logging.getLogger(__name__)


class PolymarketSmartMoneyAnalyzer:

    def __init__(self):
        self.db = get_db_client()
        self.postgres = get_postgres_client()

    def _build_smart_money_query(self, limit: int = 10000) -> str:
        query = """
        WITH market_tokens AS (
            SELECT
                question_id AS market_id,
                tupleElement(t, 1) AS token_id,
                tupleElement(t, 2) AS outcome_desc,
                winner,
                end_date_iso AS end_date
            FROM polymarket.markets
            ARRAY JOIN tokens AS t
            WHERE condition_id != ''
        ),
        user_trades AS (
            SELECT
                splitByChar('_', id)[1] as id_,
                timestamp,
                if(idx = 1, maker, taker) AS user_id,
                if(idx = 1, taker, maker) AS agent,
                if(idx = 1,
                   if(maker_asset_id = '0', maker_amount_filled, taker_amount_filled),
                   if(taker_asset_id = '0', taker_amount_filled, maker_amount_filled)
                ) AS usdc_amount,
                if(idx = 1,
                   if(maker_asset_id = '0', taker_amount_filled, maker_amount_filled),
                   if(taker_asset_id = '0', maker_amount_filled, taker_amount_filled)
                ) AS token_amount,
                if(idx = 1, maker_asset_id, taker_asset_id) AS asset_in,
                if(idx = 1, taker_asset_id, maker_asset_id) AS asset_out,
                if(asset_in = '0', asset_out, asset_in) AS token_id
            FROM polymarket.orders
            ARRAY JOIN [1, 2] AS idx
            WHERE is_deleted = 0
              AND (maker_asset_id = '0' OR taker_asset_id = '0')
        ),
        market_operations AS (
            SELECT
                ut.id_,
                ut.user_id,
                ut.agent,
                mt.market_id,
                ut.timestamp,
                ut.asset_in,
                ut.asset_out,
                CASE
                    WHEN ut.asset_in = '0' THEN 'BUY'
                    ELSE 'SELL'
                END AS operation_type,
                CASE
                    WHEN ut.asset_in = '0' THEN ut.token_amount
                    ELSE -ut.token_amount
                END AS token_balance_change,
                CASE
                    WHEN ut.asset_in = '0' THEN -ut.usdc_amount
                    ELSE ut.usdc_amount
                END AS usdc_balance_change,
                ut.token_id
            FROM user_trades ut
            INNER JOIN market_tokens mt
                ON ut.token_id = mt.token_id
        ),
        market_operations_correct as (
        SELECT
            id_,
            user_id,
            argMax(agent, priority) AS agent,
            argMax(market_id, priority) AS market_id,
            argMax(timestamp, priority) AS timestamp,
            argMax(asset_in, priority) AS asset_in,
            argMax(asset_out, priority) AS asset_out,
            argMax(operation_type, priority) AS operation_type,
            argMax(token_balance_change, priority) AS token_balance_change,
            argMax(usdc_balance_change, priority) AS usdc_balance_change,
            argMax(token_id, priority) AS token_id
        FROM (
            SELECT *,
                   multiIf(
                       agent IN ('0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e', '0xc5d563a36ae78145c45a50134d48a1215220f80a'), 2,
                       1
                   ) AS priority
            FROM market_operations
            where user_id not in ('0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e', '0xc5d563a36ae78145c45a50134d48a1215220f80a')
        )
        GROUP BY user_id, id_
        ),
        realized_profit AS (
            SELECT
                user_id,
                token_id,
                max(market_id) AS market_id,
                sumIf(usdc_balance_change, operation_type = 'SELL') AS realized_usdc,
                sumIf(-usdc_balance_change, operation_type = 'BUY') AS total_spent,
                count(*) AS operations_count,
                min(timestamp) as day_enter,
                max(timestamp) as day_exit
            FROM market_operations_correct
            GROUP BY user_id, token_id
        ),
        last_prices AS (
            SELECT
                token_id,
                toDecimal64(argMax(-usdc_balance_change / token_balance_change, timestamp), 2) AS last_price,
                MAX(market_id) as market_id
            FROM market_operations_correct
            GROUP BY token_id
        ),
        unrealized_profit AS (
            SELECT
                mt.user_id as user_id,
                mt.token_id as token_id,
                max(mt.market_id) as market_id,
                sum(
                    CASE
                        WHEN md.winner != '' THEN
                            if(md.winner = mt.token_id, toDecimal64(1, 2), toDecimal64(0, 2)) * toDecimal64(mt.token_balance, 2)
                        ELSE lp.last_price * toDecimal64(mt.token_balance, 2)
                    END
                ) AS unrealized_usdc,
                max(token_balance) as token_balance
            FROM (
                SELECT
                    user_id,
                    max(market_id) as market_id,
                    token_id,
                    sum(token_balance_change) AS token_balance
                FROM market_operations_correct
                GROUP BY user_id, token_id
            ) mt
            LEFT JOIN market_tokens md
                ON mt.token_id = md.token_id
            LEFT JOIN last_prices lp
                ON mt.token_id = lp.token_id
            GROUP BY mt.user_id, mt.token_id
        ),
        user_profits_by_tokens as (
        SELECT
            r.user_id,
            r.token_id,
            r.market_id,
            r.operations_count,
            r.realized_usdc + u.unrealized_usdc - r.total_spent AS absolute_profit,
            if(r.total_spent != 0,
                (r.realized_usdc + u.unrealized_usdc) / r.total_spent,
                0
            ) AS relative_profit,
            r.total_spent as total_spent,
            r.realized_usdc + u.unrealized_usdc as total_gained,
            r.realized_usdc as realized_usdc,
            u.unrealized_usdc as unrealized_usdc,
            r.day_enter as day_enter,
            r.day_exit as day_exit
        FROM realized_profit r
        LEFT JOIN unrealized_profit u
            USING (user_id, token_id)
        ORDER BY user_id, token_id
        ),
        user_grouped as (
        SELECT
            user_id AS wallet_address,
            COUNT(*) AS positions_count,
            COUNT(DISTINCT market_id) AS markets_count,
            AVG(operations_count) AS avg_trades_per_position,
            SUM(absolute_profit) / 1e6 AS profit_usdc,
            AVG(relative_profit) AS avg_roi,
            SUM(total_gained) / 1e6 AS total_returned_usdc,
            SUM(total_spent) / 1e6 AS total_invested_usdc,
            if(total_invested_usdc = 0, 0, total_returned_usdc / total_invested_usdc) AS portfolio_roi,
            MIN(day_enter) AS first_trade_at,
            MAX(day_exit) AS last_trade_at,
            pow(avg_roi, 365 / dateDiff('day', first_trade_at, last_trade_at)) AS annual_avg_roi,
            pow(portfolio_roi, 365 / dateDiff('day', first_trade_at, last_trade_at)) AS annual_portfolio_roi
        FROM user_profits_by_tokens
        GROUP BY user_id
        )
        SELECT * FROM user_grouped
        ORDER BY profit_usdc DESC
        LIMIT {limit}
        """
        return query.format(limit=limit)

    def analyze_smart_money(self, limit: int = 10000) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("POLYMARKET SMART MONEY ANALYSIS")
        logger.info("=" * 60)

        logger.info(f"Fetching top {limit:,} users by profit...")
        query = self._build_smart_money_query(limit=limit)

        try:
            metrics = self.db.execute_query_dict(query)
            logger.info(f"Retrieved {len(metrics):,} user metrics")
        except Exception as e:
            logger.error(f"Failed to fetch metrics: {e}")
            raise

        if not metrics:
            logger.warning("No metrics found")
            return {'users_processed': 0, 'users_stored': 0}

        try:
            stored_count = self.postgres.refresh_smart_money(metrics)
        except Exception as e:
            logger.error(f"Failed to refresh data: {e}")
            raise

        total_users = self.postgres.get_user_count()

        logger.info("=" * 60)
        logger.info(f"COMPLETE: {len(metrics):,} users, {total_users:,} in DB")
        logger.info("=" * 60)

        return {
            'users_processed': len(metrics),
            'users_stored': stored_count,
            'total_users_in_db': total_users
        }

    def close(self):
        self.db.close()
        self.postgres.close()
