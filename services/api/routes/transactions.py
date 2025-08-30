"""
Transaction management routes
"""


from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
import json
from uuid import uuid4
from datetime import datetime
import asyncpg
import os

from ..database import get_db, IS_POSTGRES
from ..models import TransactionCreate, TransactionResponse
from .auth import verify_token

router = APIRouter()


@router.post("/", response_model=TransactionResponse)
async def create_transaction(
    transaction: TransactionCreate,
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Create a new transaction and trigger ML analysis"""
    transaction_id = str(uuid4())
    now = datetime.utcnow()
    if IS_POSTGRES:
        await db.execute(
            """
            INSERT INTO transactions (
                id, from_account_id, to_account_id, amount, currency,
                transaction_type, description, location, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            transaction_id,
            transaction.from_account_id,
            transaction.to_account_id,
            float(transaction.amount),
            transaction.currency,
            transaction.transaction_type,
            transaction.description,
            json.dumps(transaction.location) if transaction.location else None,
            "pending",
            now
        )
        row = await db.fetchrow("SELECT * FROM transactions WHERE id = $1", transaction_id)
        return _transaction_from_row(dict(row))
    else:
        await db.execute(
            """
            INSERT INTO transactions (
                id, from_account_id, to_account_id, amount, currency,
                transaction_type, description, location, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transaction_id,
                transaction.from_account_id,
                transaction.to_account_id,
                float(transaction.amount),
                transaction.currency,
                transaction.transaction_type,
                transaction.description,
                json.dumps(transaction.location) if transaction.location else None,
                "pending",
                now
            )
        )
        await db.commit()
        async with db.execute(
            "SELECT * FROM transactions WHERE id = ?",
            (transaction_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return _transaction_from_row(dict(row))


@router.get("/recent", response_model=List[TransactionResponse])
async def get_recent_transactions(
    limit: int = Query(default=10, le=100),
    risk_level: Optional[str] = Query(default=None),
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Get recent transactions with optional filtering"""
    if IS_POSTGRES:
        query = "SELECT * FROM transactions"
        if risk_level == "high":
            query += " WHERE risk_score >= 7"
        elif risk_level == "medium":
            query += " WHERE risk_score >= 4 AND risk_score < 7"
        elif risk_level == "low":
            query += " WHERE risk_score < 4"
        query += " ORDER BY created_at DESC LIMIT $1"
        rows = await db.fetch(query, limit)
        return [_transaction_from_row(dict(row)) for row in rows]
    else:
        query = "SELECT * FROM transactions"
        params = []
        if risk_level == "high":
            query += " WHERE risk_score >= 7"
        elif risk_level == "medium":
            query += " WHERE risk_score >= 4 AND risk_score < 7"
        elif risk_level == "low":
            query += " WHERE risk_score < 4"
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [_transaction_from_row(dict(row)) for row in rows]


@router.get("/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(
    transaction_id: str,
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Get transaction by ID"""
    if IS_POSTGRES:
        row = await db.fetchrow("SELECT * FROM transactions WHERE id = $1", transaction_id)
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transaction not found"
            )
        return _transaction_from_row(dict(row))
    else:
        async with db.execute(
            "SELECT * FROM transactions WHERE id = ?",
            (transaction_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transaction not found"
            )
        return _transaction_from_row(dict(row))

def _transaction_from_row(row: dict) -> TransactionResponse:
    """Convert database row to TransactionResponse"""
    return TransactionResponse(
        id=row["id"],
        from_account_id=row["from_account_id"],
        to_account_id=row["to_account_id"],
        amount=row["amount"],
        currency=row["currency"],
        transaction_type=row["transaction_type"],
        description=row["description"],
        location=json.loads(row["location"]) if row.get("location") else None,
        risk_score=row.get("risk_score"),
        ml_prediction=json.loads(row["ml_prediction"]) if row.get("ml_prediction") else None,
        rules_hit=json.loads(row["rules_hit"]) if row.get("rules_hit") else None,
        status=row["status"],
        processed_at=row.get("processed_at"),
        created_at=row["created_at"]
    )
