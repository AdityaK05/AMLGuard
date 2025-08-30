"""
Transaction Stream Producer
Generates synthetic transaction streams for testing and development
"""

import asyncio
import json
import structlog
from typing import Dict, Any, List
from datetime import datetime, timedelta
from uuid import uuid4
import random
from faker import Faker

from .consumer import transaction_consumer

logger = structlog.get_logger()
fake = Faker()

class TransactionProducer:
    """Synthetic transaction producer for testing"""
    
    def __init__(self):
        self.running = False
        self.generated_count = 0
        self.customers = []
        self.accounts = []
        self.transaction_types = [
            "Wire Transfer", "ACH Transfer", "Card Payment", "ATM Withdrawal",
            "Online Transfer", "Check Deposit", "Cash Deposit", "International Transfer"
        ]
        self.countries = [
            "US", "CA", "GB", "DE", "FR", "AU", "JP", "CH", "NL", "SE",
            "CN", "RU", "IR", "KP", "AF", "SY", "VE", "MY", "PK", "BD"
        ]
        
    async def start(self):
        """Start the transaction producer"""
        logger.info("Starting transaction producer")
        self.running = True
        
        # Initialize customer and account data
        await self._initialize_data()
        
        # Start generating transactions
        await self._generate_transaction_stream()
    
    async def stop(self):
        """Stop the transaction producer"""
        logger.info("Stopping transaction producer")
        self.running = False
    
    async def _initialize_data(self):
        """Initialize customer and account data"""
        logger.info("Initializing customer and account data")
        
        # Generate customers
        for _ in range(100):
            customer = {
                "id": str(uuid4()),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "risk_level": random.choice(["low", "medium", "high"]),
                "created_at": fake.date_between(start_date="-2y", end_date="today")
            }
            self.customers.append(customer)
        
        # Generate accounts for customers
        for customer in self.customers:
            num_accounts = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
            
            for _ in range(num_accounts):
                account = {
                    "id": str(uuid4()),
                    "customer_id": customer["id"],
                    "account_number": fake.iban(),
                    "account_type": random.choice(["checking", "savings", "business"]),
                    "balance": random.uniform(1000, 100000),
                    "currency": "USD"
                }
                self.accounts.append(account)
        
        logger.info(f"Initialized {len(self.customers)} customers and {len(self.accounts)} accounts")
    
    async def _generate_transaction_stream(self):
        """Generate continuous stream of transactions"""
        logger.info("Starting transaction stream generation")
        
        while self.running:
            try:
                # Generate batch of transactions
                batch_size = random.randint(1, 5)
                
                for _ in range(batch_size):
                    transaction = await self._generate_transaction()
                    await transaction_consumer.add_transaction(transaction)
                
                # Wait before next batch (simulate realistic timing)
                await asyncio.sleep(random.uniform(2, 10))
                
            except Exception as e:
                logger.error("Error generating transactions", error=str(e))
                await asyncio.sleep(5)
    
    async def _generate_transaction(self) -> Dict[str, Any]:
        """Generate a single transaction"""
        
        # Select random account
        account = random.choice(self.accounts)
        customer = next(c for c in self.customers if c["id"] == account["customer_id"])
        
        # Determine if this should be a suspicious transaction
        is_suspicious = self._should_generate_suspicious_transaction(customer)
        
        if is_suspicious:
            transaction = await self._generate_suspicious_transaction(account, customer)
        else:
            transaction = await self._generate_normal_transaction(account, customer)
        
        self.generated_count += 1
        return transaction
    
    def _should_generate_suspicious_transaction(self, customer: Dict[str, Any]) -> bool:
        """Determine if we should generate a suspicious transaction"""
        
        # Base probability
        base_prob = 0.05  # 5% suspicious transactions
        
        # Increase probability for high-risk customers
        if customer["risk_level"] == "high":
            base_prob = 0.15
        elif customer["risk_level"] == "medium":
            base_prob = 0.08
        
        return random.random() < base_prob
    
    async def _generate_normal_transaction(self, account: Dict[str, Any], customer: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a normal transaction"""
        
        # Normal transaction amounts (log-normal distribution)
        amount = max(10, random.lognormvariate(5, 1.5))  # ~$150 average
        
        # Normal transaction types (weighted)
        transaction_type = random.choices(
            self.transaction_types,
            weights=[0.1, 0.3, 0.4, 0.1, 0.05, 0.02, 0.02, 0.01]  # Card payments most common
        )[0]
        
        # Normal timing (business hours more likely)
        timestamp = self._generate_normal_timestamp()
        
        # Normal location (home country)
        location = {
            "country": "US",
            "city": fake.city(),
            "coordinates": {"lat": float(fake.latitude()), "lng": float(fake.longitude())}
        }
        
        return {
            "transaction_id": str(uuid4()),
            "customer_id": customer["id"],
            "from_account_id": account["id"],
            "to_account_id": None,
            "amount": round(amount, 2),
            "currency": "USD",
            "transaction_type": transaction_type,
            "description": f"{transaction_type} - {fake.company()}",
            "location": location,
            "timestamp": timestamp.isoformat(),
            "is_suspicious": False
        }
    
    async def _generate_suspicious_transaction(self, account: Dict[str, Any], customer: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a suspicious transaction with AML red flags"""
        
        # Choose type of suspicious pattern
        suspicious_patterns = [
            "structuring",
            "large_amount", 
            "high_velocity",
            "unusual_geography",
            "late_night",
            "round_amounts"
        ]
        
        pattern = random.choice(suspicious_patterns)
        
        transaction = {
            "transaction_id": str(uuid4()),
            "customer_id": customer["id"],
            "from_account_id": account["id"],
            "to_account_id": None,
            "currency": "USD",
            "is_suspicious": True,
            "suspicious_pattern": pattern
        }
        
        if pattern == "structuring":
            # Amounts just below reporting thresholds
            thresholds = [3000, 5000, 9000, 10000]
            threshold = random.choice(thresholds)
            transaction["amount"] = threshold - random.uniform(50, 500)
            transaction["transaction_type"] = "Wire Transfer"
            transaction["description"] = "Wire transfer - business payment"
            
        elif pattern == "large_amount":
            # Unusually large amounts
            transaction["amount"] = random.uniform(50000, 500000)
            transaction["transaction_type"] = random.choice(["Wire Transfer", "International Transfer"])
            transaction["description"] = "Large value transfer"
            
        elif pattern == "high_velocity":
            # Multiple rapid transactions
            transaction["amount"] = random.uniform(1000, 5000)
            transaction["transaction_type"] = "Online Transfer"
            transaction["description"] = f"Rapid transfer #{random.randint(1, 20)}"
            
        elif pattern == "unusual_geography":
            # High-risk countries
            high_risk_countries = ["IR", "KP", "AF", "SY", "VE"]
            country = random.choice(high_risk_countries)
            transaction["amount"] = random.uniform(5000, 25000)
            transaction["transaction_type"] = "International Transfer"
            transaction["description"] = "International wire transfer"
            transaction["location"] = {
                "country": country,
                "city": fake.city(),
                "coordinates": {"lat": float(fake.latitude()), "lng": float(fake.longitude())}
            }
            
        elif pattern == "late_night":
            # Late night transactions
            late_hour = random.choice([23, 0, 1, 2, 3, 4])
            timestamp = datetime.now().replace(hour=late_hour, minute=random.randint(0, 59))
            transaction["amount"] = random.uniform(10000, 30000)
            transaction["transaction_type"] = "ATM Withdrawal"
            transaction["timestamp"] = timestamp.isoformat()
            
        elif pattern == "round_amounts":
            # Suspiciously round amounts
            round_amounts = [5000, 10000, 15000, 20000, 25000, 50000, 100000]
            transaction["amount"] = random.choice(round_amounts)
            transaction["transaction_type"] = "Wire Transfer"
            transaction["description"] = "Business payment"
        
        # Set defaults for missing fields
        if "amount" not in transaction:
            transaction["amount"] = random.uniform(5000, 15000)
        if "transaction_type" not in transaction:
            transaction["transaction_type"] = "Wire Transfer"
        if "description" not in transaction:
            transaction["description"] = "Suspicious transaction"
        if "location" not in transaction:
            transaction["location"] = {
                "country": "US",
                "city": fake.city(),
                "coordinates": {"lat": float(fake.latitude()), "lng": float(fake.longitude())}
            }
        if "timestamp" not in transaction:
            transaction["timestamp"] = datetime.utcnow().isoformat()
        
        return transaction
    
    def _generate_normal_timestamp(self) -> datetime:
        """Generate timestamp for normal transaction (business hours more likely)"""
        
        now = datetime.utcnow()
        
        # 70% chance during business hours (9 AM - 5 PM weekdays)
        if random.random() < 0.7:
            # Business hours
            business_day = now.replace(
                hour=random.randint(9, 17),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            # Ensure it's a weekday
            while business_day.weekday() >= 5:
                business_day -= timedelta(days=1)
            return business_day
        else:
            # Random time
            return now.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
    
    def get_generated_count(self) -> int:
        """Get total generated transaction count"""
        return self.generated_count
