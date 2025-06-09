"""Utility script that schedules stock analysis tasks via the backend API.

This script is designed for local use. It periodically sends a list of stock tickers
to the backend service and stores simplified portfolio information between runs.

The backend API must be running (e.g. via `run_with_backend.py`). Results from
`/api/analysis/{run_id}/result` are used to update the local portfolio.
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, time as time_obj
from typing import Dict, List, Tuple

import requests
from src.tools.api import get_price_history


class Portfolio:
    """Minimal portfolio tracker using value-based positions."""

    def __init__(self, cash: float, positions: Dict[str, Dict[str, float]] | None = None):
        self.cash = cash
        # {ticker: {"shares": float, "value": float}}
        self.positions = positions or {}

    @classmethod
    def load(cls, path: str, default_cash: float) -> "Portfolio":
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return cls(
                data.get("cash", default_cash),
                data.get("positions", {})
            )
        return cls(default_cash, {})

    def save(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"cash": self.cash, "positions": self.positions}, f, indent=2)

    def apply_decision(self, ticker: str, decision: Dict, price: float) -> None:
        action = decision.get("action")
        qty = int(decision.get("quantity", 0))
        if ticker not in self.positions:
            self.positions[ticker] = {"shares": 0.0, "value": 0.0}
        pos = self.positions[ticker]
        shares = pos.get("shares", 0.0)

        if action == "buy":
            shares += qty
            self.cash -= qty * price
        elif action == "sell":
            shares -= qty
            self.cash += qty * price
            if shares <= 0:
                self.positions.pop(ticker, None)
                return

        pos["shares"] = shares
        pos["value"] = round(shares * price, 2)

    def update_price(self, ticker: str, price: float) -> None:
        if ticker in self.positions:
            shares = self.positions[ticker].get("shares", 0.0)
            self.positions[ticker]["value"] = round(shares * price, 2)


def get_latest_price(ticker: str) -> float:
    """Fetch the latest closing price for a ticker."""
    try:
        df = get_price_history(ticker)
        if df is not None and not df.empty:
            return float(df.iloc[-1]["close"])
    except Exception as e:
        print(f"Failed to fetch price for {ticker}: {e}")
    return 0.0


def start_analysis(base_url: str, ticker: str, cash: float, position: float) -> str:
    url = f"{base_url}/api/analysis/start"
    payload = {
        "ticker": ticker,
        "show_reasoning": True,
        "num_of_news": 5,
        "initial_capital": cash,
        "initial_position": position,
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    return data.get("run_id")


def wait_for_completion(base_url: str, run_id: str, poll_interval: int = 5) -> None:
    status_url = f"{base_url}/api/analysis/{run_id}/status"
    while True:
        time.sleep(poll_interval)
        resp = requests.get(status_url, timeout=10)
        resp.raise_for_status()
        info = resp.json().get("data", {})
        if info.get("is_complete"):
            break


def fetch_result(base_url: str, run_id: str) -> Dict:
    result_url = f"{base_url}/api/analysis/{run_id}/result"
    resp = requests.get(result_url, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {})


def run_once(base_url: str, tickers: List[str], portfolio: Portfolio) -> None:
    for ticker in tickers:
        price = get_latest_price(ticker)
        if ticker in portfolio.positions:
            portfolio.update_price(ticker, price)
            position = portfolio.positions[ticker]["shares"]
        else:
            position = 0.0
        cash = portfolio.cash
        run_id = start_analysis(base_url, ticker, cash, position)
        if not run_id:
            print(f"Failed to start analysis for {ticker}")
            continue
        print(f"Started analysis {run_id} for {ticker}")
        wait_for_completion(base_url, run_id)
        result = fetch_result(base_url, run_id)
        decision = result.get("final_decision")
        if isinstance(decision, str):
            try:
                decision = json.loads(decision)
            except Exception:
                decision = None
        if isinstance(decision, dict):
            portfolio.apply_decision(ticker, decision, price)
            print(f"{ticker} decision: {decision}")
        else:
            print(f"No usable decision for {ticker}: {decision}")
        portfolio.update_price(ticker, price)


def main() -> None:
    parser = argparse.ArgumentParser(description="Schedule stock analysis via backend API")
    parser.add_argument("--tickers", type=str, help="Comma separated list of tickers or path to file")
    parser.add_argument("--config", type=str, help="Path to JSON config file")
    parser.add_argument("--run-times", type=str, help="Comma separated HH:MM schedule times")
    parser.add_argument("--portfolio", type=str, default="portfolio.json", help="Path to portfolio file")
    parser.add_argument("--backend-url", type=str, default="http://localhost:8000", help="Backend base URL")
    parser.add_argument("--total-cash", type=float, default=100000.0, help="Total cash if portfolio file does not exist")

    args = parser.parse_args()

    config = {}
    if args.config:
        with open(args.config, "r", encoding="utf-8") as f:
            config = json.load(f)

    tickers_arg = args.tickers or config.get("tickers")
    if not tickers_arg:
        parser.error("--tickers or --config with 'tickers' required")

    if isinstance(tickers_arg, list):
        tickers = [t.strip() for t in tickers_arg if isinstance(t, str) and t.strip()]
    elif os.path.isfile(str(tickers_arg)):
        with open(str(tickers_arg), "r", encoding="utf-8") as f:
            tickers = [line.strip() for line in f if line.strip()]
    else:
        tickers = [t.strip() for t in str(tickers_arg).split(",") if t.strip()]

    run_times_arg = config.get("run_times") or args.run_times
    portfolio_path = args.portfolio
    backend_url = config.get("backend_url", args.backend_url)
    total_cash = config.get("total_cash", args.total_cash)

    if "portfolio" in config and not os.path.exists(portfolio_path):
        port_cfg = config["portfolio"]
        portfolio = Portfolio(port_cfg.get("cash", total_cash), port_cfg.get("positions", {}))
    else:
        portfolio = Portfolio.load(portfolio_path, total_cash)

    def parse_times(value: str | List[str]) -> List[time_obj]:
        if not value:
            return []
        if isinstance(value, str):
            parts = value.split(",")
        else:
            parts = value
        times = []
        for item in parts:
            try:
                times.append(datetime.strptime(item.strip(), "%H:%M").time())
            except ValueError:
                pass
        return sorted(times)

    run_times = parse_times(run_times_arg)
    if not run_times:
        parser.error("--run-times or config with 'run_times' required")

    def get_next_run(now: datetime) -> datetime:
        for t in run_times:
            candidate = datetime.combine(now.date(), t)
            if candidate > now and candidate.weekday() < 5:
                return candidate
        next_day = now.date() + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        return datetime.combine(next_day, run_times[0])

    next_run = get_next_run(datetime.now())
    while True:
        now = datetime.now()
        if now >= next_run:
            print(f"\n[{now.isoformat()}] Starting scheduled run")
            run_once(backend_url, tickers, portfolio)
            portfolio.save(portfolio_path)
            print("Portfolio updated", {k: v["value"] for k, v in portfolio.positions.items()})
            next_run = get_next_run(now + timedelta(seconds=1))
        time.sleep(30)


if __name__ == "__main__":
    main()
