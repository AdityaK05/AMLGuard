"""
Alert management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
import asyncpg
from datetime import datetime

from ..database import get_db, IS_POSTGRES
from ..models import AlertResponse, AlertUpdate
from .auth import verify_token

router = APIRouter()

@router.get("/recent", response_model=List[AlertResponse])
async def get_recent_alerts(
    limit: int = Query(default=10, le=100),
    severity: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Get recent alerts with optional filtering"""
    if IS_POSTGRES:
        query = "SELECT * FROM alerts WHERE 1=1"
        params = []
        if severity:
            query += f" AND severity = ${len(params)+1}"
            params.append(severity)
        if status_filter:
            query += f" AND status = ${len(params)+1}"
            params.append(status_filter)
        query += f" ORDER BY created_at DESC LIMIT ${len(params)+1}"
        params.append(limit)
        rows = await db.fetch(query, *params)
        return [_alert_from_row(dict(row)) for row in rows]
    else:
        query = "SELECT * FROM alerts WHERE 1=1"
        params = []
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        if status_filter:
            query += " AND status = ?"
            params.append(status_filter)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [_alert_from_row(dict(row)) for row in rows]

@router.get("/{alert_id}", response_model=AlertResponse)
async def get_alert(
    alert_id: str,
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Get alert by ID"""
    if IS_POSTGRES:
        row = await db.fetchrow("SELECT * FROM alerts WHERE id = $1", alert_id)
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
        return _alert_from_row(dict(row))
    else:
        async with db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
        return _alert_from_row(dict(row))

@router.patch("/{alert_id}", response_model=AlertResponse)
async def update_alert(
    alert_id: str,
    alert_update: AlertUpdate,
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Update alert status or assignment"""
    if IS_POSTGRES:
        row = await db.fetchrow("SELECT * FROM alerts WHERE id = $1", alert_id)
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
        updates = []
        params = []
        if alert_update.assigned_to is not None:
            updates.append(f"assigned_to = ${len(params)+1}")
            params.append(alert_update.assigned_to)
        if alert_update.status is not None:
            updates.append(f"status = ${len(params)+1}")
            params.append(alert_update.status)
            if alert_update.status == "resolved":
                updates.append(f"resolved_at = ${len(params)+1}")
                params.append(datetime.utcnow())
        if updates:
            updates.append(f"updated_at = ${len(params)+1}")
            params.append(datetime.utcnow())
            params.append(alert_id)
            query = f"UPDATE alerts SET {', '.join(updates)} WHERE id = ${len(params)}"
            await db.execute(query, *params)
        updated_row = await db.fetchrow("SELECT * FROM alerts WHERE id = $1", alert_id)
        return _alert_from_row(dict(updated_row))
    else:
        async with db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
        updates = []
        params = []
        if alert_update.assigned_to is not None:
            updates.append("assigned_to = ?")
            params.append(alert_update.assigned_to)
        if alert_update.status is not None:
            updates.append("status = ?")
            params.append(alert_update.status)
            if alert_update.status == "resolved":
                updates.append("resolved_at = ?")
                params.append(datetime.utcnow())
        if updates:
            updates.append("updated_at = ?")
            params.append(datetime.utcnow())
            params.append(alert_id)
            query = f"UPDATE alerts SET {', '.join(updates)} WHERE id = ?"
            await db.execute(query, params)
            await db.commit()
        async with db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)) as cursor:
            updated_row = await cursor.fetchone()
        return _alert_from_row(dict(updated_row))

@router.post("/{alert_id}/assign")
async def assign_alert(
    alert_id: str,
    assigned_to: str,
    token_data: dict = Depends(verify_token),
    db = Depends(get_db)
):
    """Assign alert to a user"""
    if IS_POSTGRES:
        await db.execute(
            "UPDATE alerts SET assigned_to = $1, updated_at = $2 WHERE id = $3",
            assigned_to, datetime.utcnow(), alert_id
        )
        return {"message": "Alert assigned successfully"}
    else:
        await db.execute(
            "UPDATE alerts SET assigned_to = ?, updated_at = ? WHERE id = ?",
            (assigned_to, datetime.utcnow(), alert_id)
        )
        await db.commit()
        return {"message": "Alert assigned successfully"}

def _alert_from_row(row: dict) -> AlertResponse:
    """Convert database row to AlertResponse"""
    return AlertResponse(
        id=row["id"],
        transaction_id=row["transaction_id"],
        customer_id=row["customer_id"],
        alert_type=row["alert_type"],
        severity=row["severity"],
        title=row["title"],
        description=row["description"],
        risk_score=row["risk_score"],
        assigned_to=row["assigned_to"],
        status=row["status"],
        resolved_at=row["resolved_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"]
    )
