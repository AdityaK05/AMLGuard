"""
Case management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
import aiosqlite
import json
from uuid import uuid4
from datetime import datetime

from ..database import get_db
from ..models import CaseCreate, CaseResponse, CaseUpdate
from .auth import verify_token

router = APIRouter()

@router.post("/", response_model=CaseResponse)
async def create_case(
    case: CaseCreate,
    token_data: dict = Depends(verify_token),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Create a new case"""
    
    case_id = str(uuid4())
    now = datetime.utcnow()
    
    await db.execute("""
        INSERT INTO cases (
            id, customer_id, title, description, priority,
            status, alert_ids, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        case_id,
        case.customer_id,
        case.title,
        case.description,
        case.priority,
        "open",
        json.dumps(case.alert_ids),
        now,
        now
    ))
    await db.commit()
    
    # Return created case
    async with db.execute(
        "SELECT * FROM cases WHERE id = ?",
        (case_id,)
    ) as cursor:
        row = await cursor.fetchone()
    
    return _case_from_row(dict(row))

@router.get("/", response_model=List[CaseResponse])
async def get_cases(
    limit: int = Query(default=50, le=100),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    priority: Optional[str] = Query(default=None),
    assigned_to: Optional[str] = Query(default=None),
    token_data: dict = Depends(verify_token),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Get cases with optional filtering"""
    
    query = "SELECT * FROM cases WHERE 1=1"
    params = []
    
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    
    if priority:
        query += " AND priority = ?"
        params.append(priority)
    
    if assigned_to:
        query += " AND assigned_to = ?"
        params.append(assigned_to)
    
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    
    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
    
    return [_case_from_row(dict(row)) for row in rows]

@router.get("/{case_id}", response_model=CaseResponse)
async def get_case(
    case_id: str,
    token_data: dict = Depends(verify_token),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Get case by ID"""
    
    async with db.execute(
        "SELECT * FROM cases WHERE id = ?",
        (case_id,)
    ) as cursor:
        row = await cursor.fetchone()
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )
    
    return _case_from_row(dict(row))

@router.patch("/{case_id}", response_model=CaseResponse)
async def update_case(
    case_id: str,
    case_update: CaseUpdate,
    token_data: dict = Depends(verify_token),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Update case"""
    
    # Check if case exists
    async with db.execute(
        "SELECT * FROM cases WHERE id = ?",
        (case_id,)
    ) as cursor:
        row = await cursor.fetchone()
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )
    
    # Build update query
    updates = []
    params = []
    
    update_fields = [
        "title", "description", "priority", "status", 
        "assigned_to", "findings", "resolution"
    ]
    
    for field in update_fields:
        value = getattr(case_update, field, None)
        if value is not None:
            updates.append(f"{field} = ?")
            params.append(value)
    
    # Set closed_at if status is closed
    if case_update.status == "closed":
        updates.append("closed_at = ?")
        params.append(datetime.utcnow())
    
    if updates:
        updates.append("updated_at = ?")
        params.append(datetime.utcnow())
        params.append(case_id)
        
        query = f"UPDATE cases SET {', '.join(updates)} WHERE id = ?"
        await db.execute(query, params)
        await db.commit()
    
    # Return updated case
    async with db.execute(
        "SELECT * FROM cases WHERE id = ?",
        (case_id,)
    ) as cursor:
        updated_row = await cursor.fetchone()
    
    return _case_from_row(dict(updated_row))

def _case_from_row(row: dict) -> CaseResponse:
    """Convert database row to CaseResponse"""
    return CaseResponse(
        id=row["id"],
        customer_id=row["customer_id"],
        title=row["title"],
        description=row["description"],
        priority=row["priority"],
        status=row["status"],
        assigned_to=row["assigned_to"],
        alert_ids=json.loads(row["alert_ids"]) if row["alert_ids"] else [],
        findings=row["findings"],
        resolution=row["resolution"],
        closed_at=row["closed_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"]
    )
