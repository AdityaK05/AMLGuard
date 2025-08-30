"""
Feature Engineering for AML Risk Scoring
Extracts and transforms transaction features for ML models
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import structlog
import asyncio
import aiosqlite
from pathlib import Path

logger = structlog.get_logger()

class FeatureEngineeer:
    """Feature engineering for AML risk scoring"""
    
    def __init__(self):
        self.customer_profiles = {}
        self.geographic_risk_scores = self._load_geographic_risks()
        self.currency_risk_scores = self._load_currency_risks()
        
    def _load_geographic_risks(self) -> Dict[str, float]:
        """Load geographic risk scores for countries"""
        # Simplified risk scores (in production, use FATF lists, sanctions, etc.)
        return {
            "US": 0.1, "CA": 0.1, "GB": 0.1, "DE": 0.1, "FR": 0.1,
            "AU": 0.1, "JP": 0.1, "CH": 0.1, "NL": 0.1, "SE": 0.1,
            "CN": 0.3, "RU": 0.7, "IR": 0.9, "KP": 0.9, "AF": 0.8,
            "SY": 0.8, "VE": 0.6, "MY": 0.4, "PK": 0.5, "BD": 0.4
        }
    
    def _load_currency_risks(self) -> Dict[str, float]:
        """Load currency risk scores"""
        return {
            "USD": 0.0, "EUR": 0.0, "GBP": 0.0, "JPY": 0.0, "CHF": 0.0,
            "CAD": 0.0, "AUD": 0.0, "CNY": 0.2, "RUB": 0.6, "BTC": 0.8,
            "ETH": 0.7, "XMR": 0.9, "ZEC": 0.9
        }
    
    async def engineer_features(self, transaction_data) -> Dict[str, float]:
        """Engineer features for a transaction"""
        
        features = {}
        
        # Basic transaction features
        features.update(self._extract_basic_features(transaction_data))
        
        # Temporal features
        features.update(self._extract_temporal_features(transaction_data))
        
        # Geographic features
        features.update(self._extract_geographic_features(transaction_data))
        
        # Customer behavior features
        features.update(await self._extract_customer_features(transaction_data))
        
        # Amount-based features
        features.update(self._extract_amount_features(transaction_data))
        
        # Structuring detection features
        features.update(self._extract_structuring_features(transaction_data))
        
        return features
    
    def _extract_basic_features(self, transaction_data) -> Dict[str, float]:
        """Extract basic transaction features"""
        
        features = {}
        
        # Transaction amount (log-scaled)
        amount = float(transaction_data.amount)
        features["amount_log"] = np.log1p(amount)
        features["amount_raw"] = amount
        
        # Currency risk
        currency = transaction_data.currency
        features["currency_risk"] = self.currency_risk_scores.get(currency, 0.5)
        
        # Transaction type encoding
        txn_type = transaction_data.transaction_type.lower()
        features["is_wire_transfer"] = 1.0 if "wire" in txn_type else 0.0
        features["is_cash_transaction"] = 1.0 if "cash" in txn_type or "atm" in txn_type else 0.0
        features["is_online_transfer"] = 1.0 if "online" in txn_type else 0.0
        features["is_card_payment"] = 1.0 if "card" in txn_type else 0.0
        
        return features
    
    def _extract_temporal_features(self, transaction_data) -> Dict[str, float]:
        """Extract time-based features"""
        
        features = {}
        
        timestamp = transaction_data.timestamp or datetime.utcnow()
        
        # Hour of day (0-23)
        hour = timestamp.hour
        features["hour_of_day"] = hour
        
        # Day of week (0=Monday, 6=Sunday)
        features["day_of_week"] = timestamp.weekday()
        
        # Weekend indicator
        features["is_weekend"] = 1.0 if timestamp.weekday() >= 5 else 0.0
        
        # Business hours (9 AM - 5 PM weekdays)
        is_business_hours = (
            timestamp.weekday() < 5 and 
            9 <= hour <= 17
        )
        features["is_business_hours"] = 1.0 if is_business_hours else 0.0
        
        # Late night hours (10 PM - 6 AM)
        is_late_night = hour >= 22 or hour <= 6
        features["is_late_night"] = 1.0 if is_late_night else 0.0
        
        return features
    
    def _extract_geographic_features(self, transaction_data) -> Dict[str, float]:
        """Extract geography-based features"""
        
        features = {}
        
        location = transaction_data.location or {}
        country = location.get("country", "US")
        
        # Country risk score
        features["geographic_risk"] = self.geographic_risk_scores.get(country, 0.5)
        
        # High-risk country indicator
        features["is_high_risk_country"] = 1.0 if features["geographic_risk"] > 0.5 else 0.0
        
        # Sanctions country indicator (simplified)
        sanctioned_countries = {"IR", "KP", "SY", "RU"}
        features["is_sanctioned_country"] = 1.0 if country in sanctioned_countries else 0.0
        
        return features
    
    async def _extract_customer_features(self, transaction_data) -> Dict[str, float]:
        """Extract customer behavior features"""
        
        features = {}
        customer_id = transaction_data.customer_id
        
        # Get or create customer profile
        if customer_id not in self.customer_profiles:
            self.customer_profiles[customer_id] = await self._build_customer_profile(customer_id)
        
        profile = self.customer_profiles[customer_id]
        
        # Customer age (days since first transaction)
        features["customer_age_days"] = profile.get("age_days", 0)
        
        # Historical transaction statistics
        features["avg_transaction_amount"] = profile.get("avg_amount", 1000.0)
        features["transaction_frequency"] = profile.get("frequency", 1.0)
        features["total_transactions"] = profile.get("total_count", 1)
        
        # Account age (simplified)
        features["account_age_days"] = profile.get("account_age_days", 30)
        
        return features
    
    async def _build_customer_profile(self, customer_id: str) -> Dict[str, Any]:
        """Build customer profile from historical data"""
        
        # In a real implementation, this would query the database
        # For now, return synthetic profile data
        
        profile = {
            "age_days": np.random.randint(30, 1825),  # 1 month to 5 years
            "avg_amount": np.random.lognormal(6, 1),  # ~$400 average
            "frequency": np.random.poisson(5),        # ~5 transactions per month
            "total_count": np.random.randint(10, 500),
            "account_age_days": np.random.randint(30, 2000),
            "risk_score_history": np.random.beta(2, 8, 10).tolist()  # Historical risk scores
        }
        
        return profile
    
    def _extract_amount_features(self, transaction_data) -> Dict[str, float]:
        """Extract amount-based features"""
        
        features = {}
        amount = float(transaction_data.amount)
        customer_id = transaction_data.customer_id
        
        # Get customer profile for comparison
        profile = self.customer_profiles.get(customer_id, {"avg_amount": 1000.0})
        avg_amount = profile["avg_amount"]
        
        # Amount z-score (how unusual is this amount for this customer)
        amount_std = avg_amount * 0.5  # Assume 50% std dev
        features["amount_zscore"] = (amount - avg_amount) / max(amount_std, 1.0)
        
        # Large amount indicators
        features["is_large_amount"] = 1.0 if amount > 10000 else 0.0
        features["is_very_large_amount"] = 1.0 if amount > 50000 else 0.0
        
        # Round amount indicators (potential structuring)
        features["is_round_amount"] = 1.0 if amount % 100 == 0 else 0.0
        features["is_very_round_amount"] = 1.0 if amount % 1000 == 0 else 0.0
        
        return features
    
    def _extract_structuring_features(self, transaction_data) -> Dict[str, float]:
        """Extract features related to structuring detection"""
        
        features = {}
        amount = float(transaction_data.amount)
        
        # CTR threshold proximity (US: $10,000)
        ctr_threshold = 10000
        features["near_ctr_threshold"] = 1.0 if 9000 <= amount < ctr_threshold else 0.0
        
        # Multiple threshold proximity
        features["near_5k_threshold"] = 1.0 if 4500 <= amount < 5000 else 0.0
        features["near_3k_threshold"] = 1.0 if 2500 <= amount < 3000 else 0.0
        
        # Structuring amount patterns
        features["amount_structuring"] = self._calculate_structuring_score(amount)
        
        # Velocity features (would be calculated from recent transaction history)
        # For now, use simplified estimates
        features["velocity_1h"] = np.random.exponential(1.0)    # Txns in last hour
        features["velocity_24h"] = np.random.exponential(3.0)   # Txns in last 24h
        features["velocity_7d"] = np.random.exponential(10.0)   # Txns in last 7 days
        
        return features
    
    def _calculate_structuring_score(self, amount: float) -> float:
        """Calculate structuring risk score based on amount"""
        
        # Common structuring thresholds
        thresholds = [3000, 5000, 9000, 10000, 15000]
        
        structuring_score = 0.0
        
        for threshold in thresholds:
            # Higher score if amount is just below threshold
            if threshold * 0.9 <= amount < threshold:
                structuring_score += 0.3
            elif threshold * 0.8 <= amount < threshold * 0.9:
                structuring_score += 0.1
        
        # Bonus for being near multiple thresholds
        if structuring_score > 0.3:
            structuring_score *= 1.5
        
        return min(structuring_score, 1.0)
    
    async def update_customer_profile(self, customer_id: str, transaction_data):
        """Update customer profile with new transaction"""
        
        if customer_id not in self.customer_profiles:
            self.customer_profiles[customer_id] = await self._build_customer_profile(customer_id)
        
        profile = self.customer_profiles[customer_id]
        
        # Update running averages (simplified)
        current_avg = profile["avg_amount"]
        new_amount = float(transaction_data.amount)
        count = profile["total_count"]
        
        # Update average amount
        profile["avg_amount"] = (current_avg * count + new_amount) / (count + 1)
        profile["total_count"] = count + 1
        
        # Update frequency (transactions per day)
        profile["frequency"] = profile["total_count"] / max(profile["age_days"], 1)
        
        logger.debug("Updated customer profile", customer_id=customer_id, profile=profile)
