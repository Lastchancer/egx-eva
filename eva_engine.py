"""
EGX EVA Analyzer — EVA Calculation Engine
Computes Economic Value Added for all stocks using collected financial data.
Includes data quality scoring and multi-source data merging.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from config import (
    RISK_FREE_RATE, EQUITY_RISK_PREMIUM, COST_OF_DEBT_PRETAX,
    CORPORATE_TAX_RATE, DEFAULT_BETA,
    UNDERVALUED_THRESHOLD, OVERVALUED_THRESHOLD,
)
import database as db

logger = logging.getLogger(__name__)


class EVAEngine:
    """Computes EVA analysis from collected financial data."""

    def __init__(self, conn):
        self.conn = conn

    def _merge_financials(self, ticker: str) -> Dict:
        """
        Merge financial data from multiple sources with priority:
        1. Yahoo Finance (most structured)
        2. StockAnalysis
        3. Mubasher
        4. EGX Official

        Uses the most recent and complete data available.
        """
        merged = {
            "ticker": ticker,
            "revenue": None,
            "ebit": None,
            "ebitda": None,
            "net_income": None,
            "interest_expense": None,
            "tax_expense": None,
            "total_assets": None,
            "current_assets": None,
            "current_liabilities": None,
            "total_debt": None,
            "total_equity": None,
            "long_term_debt": None,
            "cash_and_equivalents": None,
            "close_price": None,
            "market_cap": None,
            "shares_outstanding": None,
            "beta": None,
            "pe_ratio": None,
            "dividend_yield": None,
        }

        source_priority = ["yahoo_finance", "stockanalysis", "mubasher", "egx_official"]

        # ── Merge Income Statement ──
        for source in source_priority:
            row = self.conn.execute(
                """SELECT * FROM income_statements
                   WHERE ticker = ? AND source = ? AND period = 'annual'
                   ORDER BY period_end DESC LIMIT 1""",
                (ticker, source)
            ).fetchone()
            if row:
                row = dict(row)
                for field in ["revenue", "ebit", "ebitda", "net_income", "interest_expense", "tax_expense"]:
                    if merged[field] is None and row.get(field) is not None:
                        merged[field] = row[field]

        # ── Merge Balance Sheet ──
        for source in source_priority:
            row = self.conn.execute(
                """SELECT * FROM balance_sheets
                   WHERE ticker = ? AND source = ? AND period = 'annual'
                   ORDER BY period_end DESC LIMIT 1""",
                (ticker, source)
            ).fetchone()
            if row:
                row = dict(row)
                for field in ["total_assets", "current_assets", "current_liabilities",
                              "total_debt", "total_equity", "long_term_debt", "cash_and_equivalents"]:
                    if merged[field] is None and row.get(field) is not None:
                        merged[field] = row[field]

        # ── Merge Market Data ──
        for source in source_priority:
            row = self.conn.execute(
                """SELECT * FROM market_data
                   WHERE ticker = ? AND source = ?
                   ORDER BY date DESC LIMIT 1""",
                (ticker, source)
            ).fetchone()
            if row:
                row = dict(row)
                for field in ["close_price", "market_cap", "shares_outstanding", "beta",
                              "pe_ratio", "dividend_yield"]:
                    if merged[field] is None and row.get(field) is not None:
                        merged[field] = row[field]

        return merged

    def _compute_data_quality(self, data: Dict) -> float:
        """
        Score data quality from 0.0 to 1.0 based on completeness.
        EVA requires specific fields — score how many we have.
        """
        critical_fields = ["ebit", "total_assets", "current_liabilities", "total_equity",
                          "total_debt", "market_cap", "shares_outstanding"]
        important_fields = ["revenue", "net_income", "close_price", "beta"]
        nice_to_have = ["interest_expense", "tax_expense", "ebitda", "cash_and_equivalents"]

        score = 0
        total_weight = 0

        for f in critical_fields:
            total_weight += 3
            if data.get(f) is not None:
                score += 3

        for f in important_fields:
            total_weight += 2
            if data.get(f) is not None:
                score += 2

        for f in nice_to_have:
            total_weight += 1
            if data.get(f) is not None:
                score += 1

        return round(score / total_weight, 3) if total_weight > 0 else 0

    def _estimate_missing(self, data: Dict) -> Dict:
        """
        Estimate missing critical fields when possible.
        Uses reasonable approximations to maximize coverage.
        """
        # Estimate EBIT from net income if missing
        if data["ebit"] is None and data["net_income"] is not None:
            # Approximate: EBIT ≈ Net Income / (1 - tax_rate) + interest
            interest = data["interest_expense"] or 0
            data["ebit"] = data["net_income"] / (1 - CORPORATE_TAX_RATE) + abs(interest)
            logger.debug(f"  {data['ticker']}: Estimated EBIT from net income")

        # Estimate total_debt from long_term_debt
        if data["total_debt"] is None and data["long_term_debt"] is not None:
            data["total_debt"] = data["long_term_debt"] * 1.2  # Assume ~20% short-term
            logger.debug(f"  {data['ticker']}: Estimated total debt from long-term debt")

        # Estimate shares outstanding from market cap and price
        if data["shares_outstanding"] is None and data["market_cap"] and data["close_price"]:
            data["shares_outstanding"] = data["market_cap"] / data["close_price"]

        # Estimate market cap from shares and price
        if data["market_cap"] is None and data["shares_outstanding"] and data["close_price"]:
            data["market_cap"] = data["shares_outstanding"] * data["close_price"]

        # Default beta
        if data["beta"] is None:
            data["beta"] = DEFAULT_BETA

        return data

    def calculate_eva(self, ticker: str) -> Optional[Dict]:
        """
        Calculate EVA for a single stock.

        EVA = NOPAT - (WACC × Invested Capital)

        Where:
          NOPAT = EBIT × (1 - Tax Rate)
          Invested Capital = Total Assets - Current Liabilities
          WACC = (E/V × Ke) + (D/V × Kd × (1-T))
          Ke = Rf + β × (Rm - Rf)  ← CAPM
        """
        # Get merged financial data
        data = self._merge_financials(ticker)
        data = self._estimate_missing(data)
        quality = self._compute_data_quality(data)

        # Check minimum requirements
        if data["ebit"] is None:
            logger.warning(f"  {ticker}: Cannot compute EVA — missing EBIT")
            return None

        if data["total_assets"] is None or data["current_liabilities"] is None:
            logger.warning(f"  {ticker}: Cannot compute EVA — missing balance sheet data")
            return None

        if data["total_equity"] is None or data["total_debt"] is None:
            # Try to compute from what we have
            if data["total_equity"] is None and data["total_assets"] and data.get("total_liabilities"):
                data["total_equity"] = data["total_assets"] - data["total_liabilities"]
            if data["total_debt"] is None:
                data["total_debt"] = 0  # Assume no debt if unknown
                logger.debug(f"  {ticker}: Assuming zero debt (data unavailable)")

        # ── Calculate EVA Components ──

        # NOPAT
        effective_tax_rate = CORPORATE_TAX_RATE
        if data["tax_expense"] is not None and data.get("pretax_income"):
            actual_rate = abs(data["tax_expense"]) / abs(data["pretax_income"])
            if 0.05 < actual_rate < 0.5:  # Sanity check
                effective_tax_rate = actual_rate

        nopat = data["ebit"] * (1 - effective_tax_rate)

        # Invested Capital
        invested_capital = data["total_assets"] - data["current_liabilities"]
        if invested_capital <= 0:
            logger.warning(f"  {ticker}: Negative invested capital — skipping")
            return None

        # Capital Structure Weights
        equity = max(data["total_equity"] or 0, 1)  # Avoid division by zero
        debt = max(data["total_debt"] or 0, 0)
        total_capital = equity + debt

        equity_weight = equity / total_capital
        debt_weight = debt / total_capital

        # Cost of Equity (CAPM)
        beta = data["beta"] or DEFAULT_BETA
        cost_of_equity = RISK_FREE_RATE + beta * EQUITY_RISK_PREMIUM

        # Cost of Debt (after tax)
        cost_of_debt_after_tax = COST_OF_DEBT_PRETAX * (1 - CORPORATE_TAX_RATE)

        # If we have actual interest expense data, use implied cost of debt
        if data["interest_expense"] and debt > 0:
            implied_cost = abs(data["interest_expense"]) / debt
            if 0.05 < implied_cost < 0.5:  # Sanity check
                cost_of_debt_after_tax = implied_cost * (1 - CORPORATE_TAX_RATE)

        # WACC
        wacc = (equity_weight * cost_of_equity) + (debt_weight * cost_of_debt_after_tax)

        # EVA
        capital_charge = wacc * invested_capital
        eva = nopat - capital_charge

        # ROIC & Spread
        roic = nopat / invested_capital if invested_capital > 0 else 0
        eva_spread = roic - wacc

        # Per-share metrics
        shares = data["shares_outstanding"] or 1
        eva_per_share = eva / shares

        # Intrinsic Value (Gordon Growth simplified)
        # Intrinsic Value = Invested Capital + (EVA / WACC)
        intrinsic_value = invested_capital + (eva / wacc) if wacc > 0 else invested_capital
        market_cap = data["market_cap"] or (data["close_price"] * shares if data["close_price"] else None)

        intrinsic_premium = None
        signal = "INSUFFICIENT DATA"
        if market_cap and market_cap > 0:
            intrinsic_premium = (intrinsic_value - market_cap) / market_cap
            if intrinsic_premium > UNDERVALUED_THRESHOLD:
                signal = "UNDERVALUED"
            elif intrinsic_premium < OVERVALUED_THRESHOLD:
                signal = "OVERVALUED"
            else:
                signal = "FAIR VALUE"

        # ── Store Result ──
        result = {
            "ticker": ticker,
            "calculation_date": datetime.now().strftime("%Y-%m-%d"),
            "nopat": round(nopat, 2),
            "invested_capital": round(invested_capital, 2),
            "wacc": round(wacc, 6),
            "cost_of_equity": round(cost_of_equity, 6),
            "cost_of_debt_after_tax": round(cost_of_debt_after_tax, 6),
            "equity_weight": round(equity_weight, 4),
            "debt_weight": round(debt_weight, 4),
            "eva": round(eva, 2),
            "eva_spread": round(eva_spread, 6),
            "roic": round(roic, 6),
            "capital_charge": round(capital_charge, 2),
            "eva_per_share": round(eva_per_share, 2),
            "intrinsic_value": round(intrinsic_value, 2),
            "market_cap": round(market_cap, 2) if market_cap else None,
            "intrinsic_premium": round(intrinsic_premium, 4) if intrinsic_premium is not None else None,
            "signal": signal,
            "data_quality_score": quality,
        }

        db.upsert_eva_result(self.conn, result)
        return result

    def run_all(self, tickers: List[str] = None) -> List[Dict]:
        """Run EVA calculation for all tickers."""
        if not tickers:
            tickers = db.get_all_tickers(self.conn)

        if not tickers:
            logger.warning("No tickers found in database. Run data collection first.")
            return []

        results = []
        total = len(tickers)
        computed = 0
        skipped = 0

        logger.info(f"\n{'='*60}")
        logger.info(f"EVA ENGINE — Computing for {total} stocks")
        logger.info(f"{'='*60}")
        logger.info(f"Parameters: Rf={RISK_FREE_RATE:.1%}, ERP={EQUITY_RISK_PREMIUM:.1%}, "
                    f"Kd={COST_OF_DEBT_PRETAX:.1%}, T={CORPORATE_TAX_RATE:.1%}")

        for ticker in tickers:
            result = self.calculate_eva(ticker)
            if result:
                results.append(result)
                computed += 1
                signal = result["signal"]
                eva = result["eva"]
                logger.info(f"  {ticker}: EVA={eva:,.0f} | ROIC={result['roic']:.1%} | "
                           f"WACC={result['wacc']:.1%} | Signal={signal} | "
                           f"Quality={result['data_quality_score']:.0%}")
            else:
                skipped += 1

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"EVA RESULTS SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Computed: {computed} | Skipped: {skipped} | Total: {total}")

        if results:
            undervalued = [r for r in results if r["signal"] == "UNDERVALUED"]
            fair = [r for r in results if r["signal"] == "FAIR VALUE"]
            overvalued = [r for r in results if r["signal"] == "OVERVALUED"]
            positive_eva = [r for r in results if r["eva"] > 0]

            logger.info(f"EVA+ (value creators): {len(positive_eva)}")
            logger.info(f"UNDERVALUED: {len(undervalued)} | FAIR VALUE: {len(fair)} | OVERVALUED: {len(overvalued)}")

            if undervalued:
                logger.info(f"\n🎯 TOP UNDERVALUED PICKS:")
                for r in sorted(undervalued, key=lambda x: x["intrinsic_premium"] or 0, reverse=True)[:10]:
                    logger.info(f"  {r['ticker']}: EVA={r['eva']:,.0f} | Upside={r['intrinsic_premium']:.1%} | "
                               f"ROIC={r['roic']:.1%} | Quality={r['data_quality_score']:.0%}")

        return results
