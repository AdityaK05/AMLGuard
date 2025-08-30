"""
Database configuration and connection management
"""

import os
import sqlite3
import aiosqlite
from pathlib import Path
import structlog

logger = structlog.get_logger()

DATABASE_URL = os.getenv("DATABASE_URL", "data/amlguard.db")
DB_PATH = Path(DATABASE_URL.replace("sqlite:///", "").replace("sqlite://", ""))

async def init_db():
    """Initialize SQLite database with schema"""
    
    # Ensure data directory exists
    DB_PATH.parent.mkdir(exist_ok=True)
    
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
    """
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(schema_sql)
        await db.commit()
    
    logger.info("Database schema initialized", db_path=str(DB_PATH))

async def get_db():
    """Get database connection"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db
