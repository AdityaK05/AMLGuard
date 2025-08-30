#!/usr/bin/env python3
"""
Synthetic Data Seeder for AML Testing
Generates realistic transaction streams with suspicious patterns for testing the AML platform
"""

import asyncio
import aiosqlite
import json
import structlog
from datetime import datetime, timedelta
from uuid import uuid4
from faker import Faker
import random
import numpy as np
from pathlib import Path

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()
fake = Faker()

DATABASE_PATH = "data/amlguard.db"

class SyntheticDataGenerator:
    """Generates synthetic AML test data with realistic suspicious patterns"""
    
    def __init__(self):
        self.transaction_types = [
            "Wire Transfer", "ACH Transfer", "Card Payment", "ATM Withdrawal",
            "Online Transfer", "Check Deposit", "Cash Deposit", "International Transfer",
            "Mobile Payment", "Cryptocurrency Exchange"
        ]
        
        self.high_risk_countries = [
            "IR", "KP", "AF", "SY", "VE", "MM", "BD", "GH", "KH", "PK", "UG", "YE"
        ]
        
        self.medium_risk_countries = [
            "CN", "RU", "MY", "TH", "LB", "UA", "TR", "EG", "NG", "IN", "BR"
        ]
        
        self.low_risk_countries = [
            "US", "CA", "GB", "DE", "FR", "AU", "JP", "CH", "NL", "SE", "DK", "NO"
        ]
        
        self.suspicious_patterns = [
            "structuring",
            "smurfing", 
            "layering",
            "placement",
            "integration",
            "velocity_anomaly",
            "geographic_anomaly",
            "amount_anomaly",
            "timing_anomaly",
            "round_amount_pattern"
        ]

    async def seed_data(self, num_customers: int = 50, num_transactions: int = 1000):
        """Seed the database with synthetic data"""
        
        logger.info("Starting synthetic data generation", 
                   customers=num_customers, transactions=num_transactions)
        
        async with aiosqlite.connect(DATABASE_PATH) as db:
            # Get existing customers and accounts
            existing_customers = await self._get_existing_customers(db)
            existing_accounts = await self._get_existing_accounts(db)
            
            # Generate additional customers if needed
            if len(existing_customers) < num_customers:
                new_customers = await self._generate_customers(
                    db, num_customers - len(existing_customers)
                )
                existing_customers.extend(new_customers)
            
            # Generate additional accounts if needed
            total_accounts_needed = num_customers * 2  # 2 accounts per customer on average
            if len(existing_accounts) < total_accounts_needed:
                new_accounts = await self._generate_accounts(
                    db, existing_customers, total_accounts_needed - len(existing_accounts)
                )
                existing_accounts.extend(new_accounts)
            
            # Generate transactions with suspicious patterns
            await self._generate_transactions(db, existing_accounts, num_transactions)
            
            await db.commit()
        
        logger.info("Synthetic data generation completed successfully")

    async def _get_existing_customers(self, db):
        """Get existing customers from database"""
        async with db.execute("SELECT * FROM customers") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def _get_existing_accounts(self, db):
        """Get existing accounts from database"""
        async with db.execute("SELECT * FROM accounts") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def _generate_customers(self, db, count: int):
        """Generate synthetic customers with various risk profiles"""
        
        customers = []
        
        for _ in range(count):
            # Determine risk level (80% low, 15% medium, 5% high)
            risk_weights = [0.8, 0.15, 0.05]
            risk_level = random.choices(["low", "medium", "high"], weights=risk_weights)[0]
            
            # Generate customer data
            first_name = fake.first_name()
            last_name = fake.last_name()
            
            customer = {
                "id": str(uuid4()),
                "first_name": first_name,
                "last_name": last_name,
                "email": fake.email(),
                "phone": fake.phone_number(),
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
                "address": json.dumps({
                    "street": fake.street_address(),
                    "city": fake.city(),
                    "state": fake.state_abbr(),
                    "zipCode": fake.zipcode(),
                    "country": random.choice(self.low_risk_countries)
                }),
                "risk_level": risk_level,
                "kyc_status": random.choices(
                    ["approved", "pending", "rejected"],
                    weights=[0.85, 0.10, 0.05]
                )[0],
                "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
                "updated_at": datetime.utcnow()
            }
            
            customers.append(customer)
            
            # Insert into database
            await db.execute("""
                INSERT INTO customers (id, first_name, last_name, email, phone, date_of_birth, 
                                     address, risk_level, kyc_status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                customer["id"], customer["first_name"], customer["last_name"],
                customer["email"], customer["phone"], customer["date_of_birth"],
                customer["address"], customer["risk_level"], customer["kyc_status"],
                customer["created_at"], customer["updated_at"]
            ))
        
        logger.info(f"Generated {len(customers)} synthetic customers")
        return customers

    async def _generate_accounts(self, db, customers, count: int):
        """Generate synthetic accounts for customers"""
        
        accounts = []
        account_types = ["checking", "savings", "business", "investment"]
        
        # Distribute accounts among customers
        customers_with_few_accounts = [c for c in customers if c.get("account_count", 0) < 3]
        
        for _ in range(count):
            if not customers_with_few_accounts:
                break
                
            customer = random.choice(customers_with_few_accounts)
            
            account = {
                "id": str(uuid4()),
                "customer_id": customer["id"],
                "account_number": f"{''.join([str(random.randint(0, 9)) for _ in range(12)])}",
                "account_type": random.choice(account_types),
                "balance": round(random.lognormal(8, 1.5), 2),  # ~$3000 median
                "currency": random.choices(
                    ["USD", "EUR", "GBP", "CAD"],
                    weights=[0.7, 0.15, 0.1, 0.05]
                )[0],
                "status": random.choices(
                    ["active", "suspended", "closed"],
                    weights=[0.9, 0.05, 0.05]
                )[0],
                "created_at": fake.date_time_between(
                    start_date=customer["created_at"], 
                    end_date="now"
                ),
                "updated_at": datetime.utcnow()
            }
            
            accounts.append(account)
            customer["account_count"] = customer.get("account_count", 0) + 1
            
            # Remove customer from list if they have enough accounts
            if customer["account_count"] >= 3:
                customers_with_few_accounts.remove(customer)
            
            # Insert into database
            await db.execute("""
                INSERT INTO accounts (id, customer_id, account_number, account_type, 
                                    balance, currency, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account["id"], account["customer_id"], account["account_number"],
                account["account_type"], account["balance"], account["currency"],
                account["status"], account["created_at"], account["updated_at"]
            ))
        
        logger.info(f"Generated {len(accounts)} synthetic accounts")
        return accounts

    async def _generate_transactions(self, db, accounts, count: int):
        """Generate synthetic transactions with suspicious patterns"""
        
        transactions = []
        alerts_created = 0
        
        # Active accounts only
        active_accounts = [acc for acc in accounts if acc["status"] == "active"]
        
        for i in range(count):
            account = random.choice(active_accounts)
            
            # Determine if this should be suspicious (20% of transactions)
            is_suspicious = random.random() < 0.2
            
            if is_suspicious:
                transaction, alert_created = await self._generate_suspicious_transaction(
                    db, account, i
                )
                if alert_created:
                    alerts_created += 1
            else:
                transaction = await self._generate_normal_transaction(account, i)
            
            transactions.append(transaction)
            
            # Insert transaction
            await db.execute("""
                INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency,
                                        transaction_type, description, location, risk_score,
                                        ml_prediction, rules_hit, status, processed_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transaction["id"], transaction["from_account_id"], transaction.get("to_account_id"),
                transaction["amount"], transaction["currency"], transaction["transaction_type"],
                transaction["description"], transaction["location"], transaction["risk_score"],
                transaction["ml_prediction"], transaction["rules_hit"], transaction["status"],
                transaction["processed_at"], transaction["created_at"]
            ))
        
        logger.info(f"Generated {len(transactions)} synthetic transactions")
        logger.info(f"Created {alerts_created} alerts for suspicious transactions")

    async def _generate_normal_transaction(self, account, sequence):
        """Generate a normal transaction"""
        
        # Normal amounts follow log-normal distribution
        amount = max(10.0, random.lognormal(5, 1.2))  # ~$150 median
        
        # Normal transaction types (weighted by frequency)
        transaction_type = random.choices(
            self.transaction_types,
            weights=[0.05, 0.25, 0.35, 0.15, 0.10, 0.03, 0.02, 0.02, 0.02, 0.01]
        )[0]
        
        # Normal timing (business hours preferred)
        created_at = self._generate_normal_timestamp()
        
        # Normal location
        location = {
            "country": random.choice(self.low_risk_countries),
            "city": fake.city(),
            "coordinates": {
                "lat": float(fake.latitude()),
                "lng": float(fake.longitude())
            }
        }
        
        # Low risk score
        risk_score = random.uniform(0.5, 3.0)
        
        return {
            "id": str(uuid4()),
            "from_account_id": account["id"],
            "to_account_id": None,
            "amount": round(amount, 2),
            "currency": account["currency"],
            "transaction_type": transaction_type,
            "description": f"{transaction_type} - {fake.company()}",
            "location": json.dumps(location),
            "risk_score": round(risk_score, 2),
            "ml_prediction": json.dumps({
                "score": risk_score,
                "confidence": random.uniform(0.7, 0.95),
                "features": {
                    "amount_zscore": random.uniform(-1, 1),
                    "velocity_1h": random.randint(0, 2),
                    "geographic_risk": random.uniform(0, 0.3)
                },
                "model_version": "v1.0"
            }),
            "rules_hit": json.dumps([]),
            "status": "clear",
            "processed_at": created_at,
            "created_at": created_at
        }

    async def _generate_suspicious_transaction(self, db, account, sequence):
        """Generate a suspicious transaction with specific AML patterns"""
        
        # Choose suspicious pattern
        pattern = random.choice(self.suspicious_patterns)
        
        transaction = {
            "id": str(uuid4()),
            "from_account_id": account["id"],
            "to_account_id": None,
            "currency": account["currency"],
            "pattern": pattern
        }
        
        alert_created = False
        
        if pattern == "structuring":
            # Amounts just below reporting thresholds
            thresholds = [3000, 5000, 9000, 10000, 15000]
            threshold = random.choice(thresholds)
            transaction.update({
                "amount": threshold - random.uniform(50, 500),
                "transaction_type": "Wire Transfer",
                "description": "Business payment - invoice settlement",
                "risk_score": random.uniform(7.0, 9.5),
                "rules_hit": ["structuring"]
            })
            
        elif pattern == "smurfing":
            # Multiple small transactions to avoid detection
            transaction.update({
                "amount": random.uniform(1000, 2999),
                "transaction_type": "Online Transfer",
                "description": f"Transfer batch #{random.randint(1, 20)}",
                "risk_score": random.uniform(6.0, 8.0),
                "rules_hit": ["velocity", "smurfing"]
            })
            
        elif pattern == "velocity_anomaly":
            # High velocity transactions
            transaction.update({
                "amount": random.uniform(5000, 15000),
                "transaction_type": "ACH Transfer",
                "description": "Rapid transfer sequence",
                "risk_score": random.uniform(6.5, 8.5),
                "rules_hit": ["velocity"]
            })
            
        elif pattern == "geographic_anomaly":
            # Transactions from high-risk countries
            high_risk_country = random.choice(self.high_risk_countries)
            transaction.update({
                "amount": random.uniform(10000, 50000),
                "transaction_type": "International Transfer",
                "description": "International business payment",
                "location": json.dumps({
                    "country": high_risk_country,
                    "city": fake.city(),
                    "coordinates": {
                        "lat": float(fake.latitude()),
                        "lng": float(fake.longitude())
                    }
                }),
                "risk_score": random.uniform(7.5, 9.0),
                "rules_hit": ["geographic", "high_risk_country"]
            })
            
        elif pattern == "round_amount_pattern":
            # Suspiciously round amounts
            round_amounts = [5000, 10000, 15000, 20000, 25000, 50000, 100000]
            transaction.update({
                "amount": random.choice(round_amounts),
                "transaction_type": "Wire Transfer",
                "description": "Large round amount transfer",
                "risk_score": random.uniform(5.5, 7.5),
                "rules_hit": ["round_amount", "large_amount"]
            })
            
        else:
            # General suspicious transaction
            transaction.update({
                "amount": random.uniform(10000, 100000),
                "transaction_type": random.choice(["Wire Transfer", "International Transfer"]),
                "description": "High-risk transaction",
                "risk_score": random.uniform(6.0, 9.0),
                "rules_hit": ["anomaly_detection"]
            })
        
        # Set defaults for missing fields
        if "location" not in transaction:
            transaction["location"] = json.dumps({
                "country": random.choice(self.medium_risk_countries),
                "city": fake.city(),
                "coordinates": {
                    "lat": float(fake.latitude()),
                    "lng": float(fake.longitude())
                }
            })
        
        # Common fields for suspicious transactions
        transaction.update({
            "ml_prediction": json.dumps({
                "score": transaction["risk_score"],
                "confidence": random.uniform(0.6, 0.9),
                "features": {
                    "amount_zscore": random.uniform(1.5, 3.0),
                    "velocity_1h": random.randint(1, 10),
                    "geographic_risk": random.uniform(0.3, 0.9)
                },
                "model_version": "v1.0"
            }),
            "rules_hit": json.dumps(transaction["rules_hit"]),
            "status": "flagged" if transaction["risk_score"] >= 7.0 else "review",
            "processed_at": datetime.utcnow() - timedelta(minutes=random.randint(1, 1440)),
            "created_at": datetime.utcnow() - timedelta(minutes=random.randint(1, 1440))
        })
        
        # Create alert for high-risk transactions
        if transaction["risk_score"] >= 7.0:
            alert_created = await self._create_alert_for_transaction(db, transaction, account)
        
        return transaction, alert_created

    async def _create_alert_for_transaction(self, db, transaction, account):
        """Create an alert for a suspicious transaction"""
        
        # Get customer for the account
        async with db.execute("SELECT customer_id FROM accounts WHERE id = ?", (account["id"],)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return False
            
            customer_id = row[0]
        
        # Determine alert details based on triggered rules
        rules_hit = json.loads(transaction["rules_hit"])
        
        if "structuring" in rules_hit:
            alert_type = "structuring"
            title = "Potential Structuring Pattern Detected"
        elif "velocity" in rules_hit:
            alert_type = "velocity"
            title = "High-Velocity Transaction Pattern"
        elif "geographic" in rules_hit:
            alert_type = "geographic"
            title = "High-Risk Geographic Activity"
        else:
            alert_type = "anomaly"
            title = "Suspicious Transaction Detected"
        
        # Determine severity
        if transaction["risk_score"] >= 8.5:
            severity = "critical"
        elif transaction["risk_score"] >= 7.0:
            severity = "high"
        else:
            severity = "medium"
        
        alert = {
            "id": str(uuid4()),
            "transaction_id": transaction["id"],
            "customer_id": customer_id,
            "alert_type": alert_type,
            "severity": severity,
            "title": title,
            "description": f"Transaction flagged with risk score {transaction['risk_score']:.1f}. Rules triggered: {', '.join(rules_hit)}",
            "risk_score": transaction["risk_score"],
            "assigned_to": None,
            "status": "open",
            "resolved_at": None,
            "created_at": transaction["created_at"],
            "updated_at": transaction["created_at"]
        }
        
        # Insert alert
        await db.execute("""
            INSERT INTO alerts (id, transaction_id, customer_id, alert_type, severity, title,
                              description, risk_score, assigned_to, status, resolved_at,
                              created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            alert["id"], alert["transaction_id"], alert["customer_id"], alert["alert_type"],
            alert["severity"], alert["title"], alert["description"], alert["risk_score"],
            alert["assigned_to"], alert["status"], alert["resolved_at"],
            alert["created_at"], alert["updated_at"]
        ))
        
        return True

    def _generate_normal_timestamp(self):
        """Generate timestamp for normal business activity"""
        
        now = datetime.utcnow()
        
        # 70% chance during business hours
        if random.random() < 0.7:
            # Business day, business hours
            days_back = random.randint(0, 30)
            business_day = now - timedelta(days=days_back)
            
            # Ensure weekday
            while business_day.weekday() >= 5:
                business_day -= timedelta(days=1)
            
            # Business hours (9 AM - 5 PM)
            business_day = business_day.replace(
                hour=random.randint(9, 17),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            return business_day
        else:
            # Random time
            days_back = random.randint(0, 30)
            return now - timedelta(
                days=days_back,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

async def main():
    """Main function to run synthetic data generation"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate synthetic AML test data")
    parser.add_argument("--customers", type=int, default=100, help="Number of customers to generate")
    parser.add_argument("--transactions", type=int, default=2000, help="Number of transactions to generate")
    parser.add_argument("--clear", action="store_true", help="Clear existing data before generating")
    
    args = parser.parse_args()
    
    # Ensure database exists
    db_path = Path(DATABASE_PATH)
    if not db_path.exists():
        logger.error("Database does not exist. Please run init_db.py first.")
        return
    
    # Clear existing data if requested
    if args.clear:
        logger.info("Clearing existing synthetic data")
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("DELETE FROM alerts")
            await db.execute("DELETE FROM transactions") 
            await db.execute("DELETE FROM accounts")
            await db.execute("DELETE FROM customers WHERE email LIKE '%@synthetic.test'")
            await db.commit()
        logger.info("Existing data cleared")
    
    # Generate synthetic data
    generator = SyntheticDataGenerator()
    await generator.seed_data(args.customers, args.transactions)
    
    logger.info("Synthetic data generation completed successfully")
    print(f"Generated {args.customers} customers and {args.transactions} transactions")

if __name__ == "__main__":
    asyncio.run(main())
