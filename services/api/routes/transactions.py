"""
Transaction management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
import aiosqlite
import json
from uuid import uuid4
from datetime import datetime

from ..database import get_db
from ..models import TransactionCreate, TransactionResponse
from .auth import verify_token

router = APIRouter()

@router.post("/", response_model=TransactionResponse)
async def create_transaction(
    transaction: TransactionCreate,
    token_data: dict = Depends(verify_token),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Create a new transaction and trigger ML analysis"""
    
    transaction_id = str(uuid4())
    now = datetime.utcnow()
    
    # Insert transaction
    await db.execute("""
        INSERT INTO transactions (
            id, from_account_id, to_account_id, amount, currency,
            transaction_type, description, location, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
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
    ))
    await db.commit()
    
    # TODO: Send to ML service for risk scoring
    # TODO: Send to stream processor for real-time analysis
    
    # Return created transaction
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
    db: aiosqlite.Connection = Depends(get_db)
):
    """Get recent transactions with optional filtering"""
    
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
    db: aiosqlite.Connection = Depends(get_db)
):
    """Get transaction by ID"""
    
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
        location=json.loads(row["location"]) if row["location"] else None,
        risk_score=row["risk_score"],
        ml_prediction=json.loads(row["ml_prediction"]) if row["ml_prediction"] else None,
        rules_hit=json.loads(row["rules_hit"]) if row["rules_hit"] else None,
        status=row["status"],
        processed_at=row["processed_at"],
        created_at=row["created_at"]
    )
