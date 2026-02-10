#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  EGX AGENT SCHEDULER                                         ║
║  Automated periodic data collection + optional alerts        ║
╚══════════════════════════════════════════════════════════════╝

Usage:
  python scheduler.py                    # Run once now
  python scheduler.py --schedule daily   # Run daily at 4 PM Cairo time
  python scheduler.py --schedule weekly  # Run every Sunday

Can optionally send Telegram alerts when new undervalued stocks are found.
Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

logger = logging.getLogger("EGX_Scheduler")

# Import our collector
from collector import EGXDataAgent, EGX_TICKERS

# Optional: Telegram alerts
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def send_telegram_alert(message: str):
    """Send alert via Telegram bot"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        import requests
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        })
        logger.info("📱 Telegram alert sent")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")


def format_alert_message(results: list) -> str:
    """Format EVA results into a Telegram-friendly alert"""
    undervalued = [r for r in results if r.signal == "UNDERVALUED"]
    positive_eva = [r for r in results if r.eva > 0]

    msg = "🏦 <b>EGX EVA Analysis Update</b>\n"
    msg += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
    msg += f"📊 Analyzed: {len(results)} stocks\n\n"

    if undervalued:
        msg += "🎯 <b>Undervalued Opportunities:</b>\n"
        for s in sorted(undervalued, key=lambda x: x.intrinsic_premium, reverse=True)[:5]:
            msg += (f"  <code>{s.ticker}</code> — "
                    f"EVA: EGP {s.eva/1e6:,.0f}M | "
                    f"Upside: +{s.intrinsic_premium*100:.1f}%\n")
    else:
        msg += "No undervalued stocks found this run.\n"

    msg += f"\n📈 EVA+ Companies: {len(positive_eva)}/{len(results)}"
    return msg


def run_collection():
    """Run a single collection cycle"""
    logger.info("Starting scheduled collection run...")
    agent = EGXDataAgent()
    results = agent.run_full_collection(
        tickers=EGX_TICKERS,
        use_yahoo=True,
        use_web_scraper=False,
    )

    if results:
        alert_msg = format_alert_message(results)
        send_telegram_alert(alert_msg)

    return results


def run_with_schedule(mode: str = "daily"):
    """Run on a schedule using the schedule library"""
    try:
        import schedule
    except ImportError:
        print("Install schedule: pip install schedule")
        sys.exit(1)

    if mode == "daily":
        schedule.every().day.at("16:00").do(run_collection)  # 4 PM Cairo
        logger.info("Scheduled: daily at 16:00 Cairo time")
    elif mode == "weekly":
        schedule.every().sunday.at("10:00").do(run_collection)
        logger.info("Scheduled: every Sunday at 10:00")
    elif mode == "hourly":
        schedule.every().hour.do(run_collection)
        logger.info("Scheduled: every hour")

    logger.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="EGX Agent Scheduler")
    parser.add_argument("--schedule", choices=["daily", "weekly", "hourly"],
                        help="Run on schedule (default: run once now)")
    args = parser.parse_args()

    if args.schedule:
        run_with_schedule(args.schedule)
    else:
        run_collection()
