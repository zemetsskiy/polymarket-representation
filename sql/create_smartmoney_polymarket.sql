CREATE TABLE IF NOT EXISTS smartmoney_polymarket (
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

CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_wallet ON smartmoney_polymarket (wallet_address);
CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_profit ON smartmoney_polymarket (profit_usdc DESC);
CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_roi ON smartmoney_polymarket (portfolio_roi DESC);
CREATE INDEX IF NOT EXISTS idx_smartmoney_pm_created ON smartmoney_polymarket (created_at DESC);