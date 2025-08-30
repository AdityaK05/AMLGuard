#!/usr/bin/env python3
"""
Database Initialization Script
Creates SQLite database and populates with sample data for AMLGuard platform
"""

import sqlite3
import asyncio
import aiosqlite
import json
import bcrypt
from datetime import datetime, timedelta
from uuid import uuid4
from pathlib import Path
import structlog

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

DATABASE_PATH = "data/amlguard.db"

async def init_database():
    """Initialize SQLite database with schema and sample data"""
    
    logger.info("Initializing AMLGuard database")
    
    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)
    
    # Create database and schema
    await create_schema()
    
    # Populate with sample data
    await populate_sample_data()
    
    logger.info("Database initialization completed")

async def create_schema():
    """Create database schema"""
    
    schema_sql = """
    -- Users table
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'analyst',
        permissions TEXT DEFAULT '[]',
        last_login TIMESTAMP,
        is_active BOOLEAN NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Customers table
    CREATE TABLE IF NOT EXISTS customers (
        id TEXT PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        date_of_birth TIMESTAMP,
        address TEXT,
        risk_level TEXT NOT NULL DEFAULT 'low',
        kyc_status TEXT NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Accounts table  
    CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        account_number TEXT UNIQUE NOT NULL,
        account_type TEXT NOT NULL,
        balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
        currency TEXT NOT NULL DEFAULT 'USD',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers (id)
    );

    -- Transactions table
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        from_account_id TEXT,
        to_account_id TEXT,
        amount DECIMAL(15,2) NOT NULL,
        currency TEXT NOT NULL DEFAULT 'USD',
        transaction_type TEXT NOT NULL,
        description TEXT,
        location TEXT,
        risk_score DECIMAL(4,2),
        ml_prediction TEXT,
        rules_hit TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        processed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (from_account_id) REFERENCES accounts (id),
        FOREIGN KEY (to_account_id) REFERENCES accounts (id)
    );

    -- Alerts table
    CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        transaction_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        severity TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        risk_score DECIMAL(4,2) NOT NULL,
        assigned_to TEXT,
        status TEXT NOT NULL DEFAULT 'open',
        resolved_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (transaction_id) REFERENCES transactions (id),
        FOREIGN KEY (customer_id) REFERENCES customers (id),
        FOREIGN KEY (assigned_to) REFERENCES users (id)
    );

    -- Cases table
    CREATE TABLE IF NOT EXISTS cases (
        id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        priority TEXT NOT NULL DEFAULT 'medium',
        status TEXT NOT NULL DEFAULT 'open',
        assigned_to TEXT,
        alert_ids TEXT DEFAULT '[]',
        findings TEXT,
        resolution TEXT,
        closed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers (id),
        FOREIGN KEY (assigned_to) REFERENCES users (id)
    );

    -- Audit logs table
    CREATE TABLE IF NOT EXISTS audit_logs (
        id TEXT PRIMARY KEY,
        user_id TEXT,
        action TEXT NOT NULL,
        resource TEXT NOT NULL,
        resource_id TEXT,
        details TEXT,
        ip_address TEXT,
        user_agent TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    );

    -- Model registry table
    CREATE TABLE IF NOT EXISTS model_registry (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        version TEXT NOT NULL,
        model_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'training',
        accuracy DECIMAL(5,4),
        precision DECIMAL(5,4),
        recall DECIMAL(5,4),
        f1_score DECIMAL(5,4),
        deployed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(from_account_id);
    CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at);
    CREATE INDEX IF NOT EXISTS idx_alerts_customer ON alerts(customer_id);
    CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
    CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at);
    """
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.executescript(schema_sql)
        await db.commit()
    
    logger.info("Database schema created successfully")

async def populate_sample_data():
    """Populate database with realistic sample data"""
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        
        # Create default users
        await create_default_users(db)
        
        # Create sample customers
        customers = await create_sample_customers(db)
        
        # Create accounts for customers
        accounts = await create_sample_accounts(db, customers)
        
        # Create sample transactions
        transactions = await create_sample_transactions(db, accounts)
        
        # Create sample alerts
        await create_sample_alerts(db, transactions, customers)
        
        # Create sample model registry entries
        await create_model_registry(db)
        
        await db.commit()
    
    logger.info("Sample data populated successfully")

async def create_default_users(db):
    """Create default system users"""
    
    users = [
        {
            "id": str(uuid4()),
            "username": "admin",
            "email": "admin@amlguard.com",
            "password": "admin123",
            "first_name": "Sarah",
            "last_name": "Chen",
            "role": "admin",
            "permissions": '["read", "write", "admin"]'
        },
        {
            "id": str(uuid4()),
            "username": "analyst1",
            "email": "analyst1@amlguard.com", 
            "password": "analyst123",
            "first_name": "Michael",
            "last_name": "Rodriguez",
            "role": "analyst",
            "permissions": '["read", "write"]'
        },
        {
            "id": str(uuid4()),
            "username": "reviewer",
            "email": "reviewer@amlguard.com",
            "password": "reviewer123", 
            "first_name": "Lisa",
            "last_name": "Wang",
            "role": "reviewer",
            "permissions": '["read"]'
        }
    ]
    
    for user in users:
        # Hash password
        hashed_password = bcrypt.hashpw(user["password"].encode(), bcrypt.gensalt()).decode()
        
        await db.execute("""
            INSERT INTO users (id, username, email, password, first_name, last_name, role, permissions, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user["id"], user["username"], user["email"], hashed_password,
            user["first_name"], user["last_name"], user["role"], user["permissions"],
            True, datetime.utcnow(), datetime.utcnow()
        ))
    
    logger.info(f"Created {len(users)} default users")

async def create_sample_customers(db):
    """Create sample customers with various risk profiles"""
    
    customers = [
        {
            "id": str(uuid4()),
            "first_name": "Marcus",
            "last_name": "Johnson",
            "email": "marcus.johnson@email.com",
            "phone": "+1-555-0123",
            "date_of_birth": "1985-03-15",
            "address": json.dumps({
                "street": "123 Main St",
                "city": "New York",
                "state": "NY", 
                "zipCode": "10001",
                "country": "US"
            }),
            "risk_level": "high",
            "kyc_status": "approved"
        },
        {
            "id": str(uuid4()),
            "first_name": "Lisa",
            "last_name": "Wang",
            "email": "lisa.wang@email.com",
            "phone": "+1-555-0456",
            "date_of_birth": "1990-07-22",
            "address": json.dumps({
                "street": "456 Oak Ave",
                "city": "San Francisco",
                "state": "CA",
                "zipCode": "94102", 
                "country": "US"
            }),
            "risk_level": "medium",
            "kyc_status": "approved"
        },
        {
            "id": str(uuid4()),
            "first_name": "Robert",
            "last_name": "Chen",
            "email": "robert.chen@email.com",
            "phone": "+1-555-0789",
            "date_of_birth": "1978-12-08",
            "address": json.dumps({
                "street": "789 Pine St",
                "city": "Seattle",
                "state": "WA",
                "zipCode": "98101",
                "country": "US"
            }),
            "risk_level": "low",
            "kyc_status": "approved"
        },
        {
            "id": str(uuid4()),
            "first_name": "Emma",
            "last_name": "Rodriguez",
            "email": "emma.rodriguez@email.com", 
            "phone": "+1-555-0321",
            "date_of_birth": "1992-05-14",
            "address": json.dumps({
                "street": "321 Elm Dr",
                "city": "Austin",
                "state": "TX",
                "zipCode": "73301",
                "country": "US"
            }),
            "risk_level": "low",
            "kyc_status": "approved"
        }
    ]
    
    for customer in customers:
        await db.execute("""
            INSERT INTO customers (id, first_name, last_name, email, phone, date_of_birth, address, risk_level, kyc_status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            customer["id"], customer["first_name"], customer["last_name"], customer["email"],
            customer["phone"], customer["date_of_birth"], customer["address"],
            customer["risk_level"], customer["kyc_status"], datetime.utcnow(), datetime.utcnow()
        ))
    
    logger.info(f"Created {len(customers)} sample customers")
    return customers

async def create_sample_accounts(db, customers):
    """Create sample accounts for customers"""
    
    accounts = []
    account_types = ["checking", "savings", "business"]
    
    for customer in customers:
        # Each customer gets 1-2 accounts
        num_accounts = 1 if customer["risk_level"] == "low" else 2
        
        for i in range(num_accounts):
            account = {
                "id": str(uuid4()),
                "customer_id": customer["id"],
                "account_number": f"****{str(uuid4())[:4]}",
                "account_type": account_types[i % len(account_types)],
                "balance": 25000.00 + (i * 15000),
                "currency": "USD",
                "status": "active"
            }
            accounts.append(account)
            
            await db.execute("""
                INSERT INTO accounts (id, customer_id, account_number, account_type, balance, currency, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account["id"], account["customer_id"], account["account_number"],
                account["account_type"], account["balance"], account["currency"],
                account["status"], datetime.utcnow(), datetime.utcnow()
            ))
    
    logger.info(f"Created {len(accounts)} sample accounts")
    return accounts

async def create_sample_transactions(db, accounts):
    """Create sample transactions with various risk patterns"""
    
    transactions = []
    transaction_types = ["Wire Transfer", "ACH Transfer", "Card Payment", "ATM Withdrawal", "Online Transfer"]
    
    # Create transactions for each account
    for account in accounts:
        # Create 3-8 transactions per account
        num_transactions = 5 if account["account_type"] == "checking" else 3
        
        for i in range(num_transactions):
            # Vary transaction characteristics based on account's customer risk
            base_time = datetime.utcnow() - timedelta(days=i, hours=i*2)
            
            if i == 0:  # First transaction is suspicious for high-risk customers
                # High-risk transaction
                transaction = {
                    "id": str(uuid4()),
                    "from_account_id": account["id"],
                    "to_account_id": None,
                    "amount": 9850.00,  # Near structuring threshold
                    "currency": "USD",
                    "transaction_type": "Wire Transfer",
                    "description": "International wire transfer",
                    "location": json.dumps({"country": "US", "city": "New York"}),
                    "risk_score": 8.7,
                    "ml_prediction": json.dumps({
                        "score": 8.7,
                        "features": {"amount_zscore": 2.1, "velocity_1h": 1, "geographic_risk": 0.1},
                        "model_version": "v1.0"
                    }),
                    "rules_hit": json.dumps(["structuring"]),
                    "status": "flagged",
                    "processed_at": base_time,
                    "created_at": base_time
                }
            elif i == 1:  # Second transaction is medium risk
                transaction = {
                    "id": str(uuid4()),
                    "from_account_id": account["id"],
                    "to_account_id": None,
                    "amount": 2450.00,
                    "currency": "USD", 
                    "transaction_type": "ATM Withdrawal",
                    "description": "ATM withdrawal",
                    "location": json.dumps({"country": "UK", "city": "London"}),
                    "risk_score": 6.2,
                    "ml_prediction": json.dumps({
                        "score": 6.2,
                        "features": {"amount_zscore": 1.1, "velocity_1h": 0, "geographic_risk": 0.3},
                        "model_version": "v1.0"
                    }),
                    "rules_hit": json.dumps(["geographic"]),
                    "status": "review",
                    "processed_at": base_time,
                    "created_at": base_time
                }
            else:  # Normal transactions
                transaction = {
                    "id": str(uuid4()),
                    "from_account_id": account["id"],
                    "to_account_id": None,
                    "amount": 125.00 + (i * 50),
                    "currency": "USD",
                    "transaction_type": transaction_types[i % len(transaction_types)],
                    "description": f"Regular transaction {i}",
                    "location": json.dumps({"country": "US", "city": "New York"}),
                    "risk_score": 1.2 + (i * 0.3),
                    "ml_prediction": json.dumps({
                        "score": 1.2 + (i * 0.3),
                        "features": {"amount_zscore": 0.1, "velocity_1h": 0, "geographic_risk": 0.1},
                        "model_version": "v1.0"
                    }),
                    "rules_hit": json.dumps([]),
                    "status": "clear",
                    "processed_at": base_time,
                    "created_at": base_time
                }
            
            transactions.append(transaction)
            
            await db.execute("""
                INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, transaction_type, description, location, risk_score, ml_prediction, rules_hit, status, processed_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transaction["id"], transaction["from_account_id"], transaction["to_account_id"],
                transaction["amount"], transaction["currency"], transaction["transaction_type"],
                transaction["description"], transaction["location"], transaction["risk_score"],
                transaction["ml_prediction"], transaction["rules_hit"], transaction["status"],
                transaction["processed_at"], transaction["created_at"]
            ))
    
    logger.info(f"Created {len(transactions)} sample transactions")
    return transactions

async def create_sample_alerts(db, transactions, customers):
    """Create sample alerts for high-risk transactions"""
    
    alerts = []
    
    # Create alerts for flagged transactions
    flagged_transactions = [t for t in transactions if t["status"] == "flagged"]
    
    for transaction in flagged_transactions:
        # Find customer for this transaction
        account_id = transaction["from_account_id"]
        customer = None
        
        # This is simplified - in real implementation we'd join tables
        for c in customers:
            if any(t["from_account_id"] == account_id for t in transactions):
                customer = c
                break
        
        if not customer:
            continue
        
        alert = {
            "id": str(uuid4()),
            "transaction_id": transaction["id"],
            "customer_id": customer["id"],
            "alert_type": "structuring",
            "severity": "critical",
            "title": "Potential Structuring Pattern Detected",
            "description": f"Transaction amount ${transaction['amount']} may be designed to avoid reporting requirements",
            "risk_score": transaction["risk_score"],
            "assigned_to": None,
            "status": "open",
            "resolved_at": None,
            "created_at": transaction["created_at"],
            "updated_at": transaction["created_at"]
        }
        
        alerts.append(alert)
        
        await db.execute("""
            INSERT INTO alerts (id, transaction_id, customer_id, alert_type, severity, title, description, risk_score, assigned_to, status, resolved_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            alert["id"], alert["transaction_id"], alert["customer_id"], alert["alert_type"],
            alert["severity"], alert["title"], alert["description"], alert["risk_score"],
            alert["assigned_to"], alert["status"], alert["resolved_at"],
            alert["created_at"], alert["updated_at"]
        ))
    
    logger.info(f"Created {len(alerts)} sample alerts")

async def create_model_registry(db):
    """Create model registry entries"""
    
    models = [
        {
            "id": str(uuid4()),
            "name": "XGBoost Risk Classifier",
            "version": "1.0.0",
            "model_type": "classification",
            "status": "deployed",
            "accuracy": 0.942,
            "precision": 0.897,
            "recall": 0.863,
            "f1_score": 0.880,
            "deployed_at": datetime.utcnow(),
            "created_at": datetime.utcnow()
        },
        {
            "id": str(uuid4()),
            "name": "Isolation Forest Anomaly Detector",
            "version": "1.0.0", 
            "model_type": "anomaly_detection",
            "status": "deployed",
            "accuracy": 0.889,
            "precision": 0.823,
            "recall": 0.791,
            "f1_score": 0.807,
            "deployed_at": datetime.utcnow(),
            "created_at": datetime.utcnow()
        }
    ]
    
    for model in models:
        await db.execute("""
            INSERT INTO model_registry (id, name, version, model_type, status, accuracy, precision, recall, f1_score, deployed_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            model["id"], model["name"], model["version"], model["model_type"],
            model["status"], model["accuracy"], model["precision"], model["recall"],
            model["f1_score"], model["deployed_at"], model["created_at"]
        ))
    
    logger.info(f"Created {len(models)} model registry entries")

if __name__ == "__main__":
    asyncio.run(init_database())
