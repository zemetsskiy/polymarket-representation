#!/bin/bash

echo "============================================================"
echo "POLYMARKET SMART MONEY WORKER - CRON SCHEDULER"
echo "============================================================"
echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S') UTC"
echo ""
echo "Schedule:"
echo "  - polymarket_smart_money_hourly : every 3 hours at :00 (10k users)"
echo "  - polymarket_smart_money_daily  : daily at 01:00 UTC (50k users)"
echo ""
echo "Waiting for scheduled jobs..."
echo "============================================================"

printenv > /etc/environment
cron
tail -f /var/log/cron.log