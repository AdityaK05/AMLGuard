"""
Pydantic models for API requests and responses
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal

# Authentication models
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    user: Dict[str, Any]
    token: str

class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    first_name: str
    last_name: str
    role: str
    permissions: List[str]
    last_login: Optional[datetime]
    is_active: bool
    created_at: datetime

# Transaction models
class TransactionCreate(BaseModel):
    from_account_id: Optional[str] = None
    to_account_id: Optional[str] = None
    amount: Decimal = Field(..., gt=0)
    currency: str = "USD"
    transaction_type: str
    description: Optional[str] = None
    location: Optional[Dict[str, Any]] = None

class TransactionResponse(BaseModel):
    id: str
    from_account_id: Optional[str]
    to_account_id: Optional[str]
    amount: Decimal
    currency: str
    transaction_type: str
    description: Optional[str]
    location: Optional[Dict[str, Any]]
    risk_score: Optional[Decimal]
    ml_prediction: Optional[Dict[str, Any]]
    rules_hit: Optional[List[str]]
    status: str
    processed_at: Optional[datetime]
    created_at: datetime

# Alert models
class AlertResponse(BaseModel):
    id: str
    transaction_id: str
    customer_id: str
    alert_type: str
    severity: str
    title: str
    description: str
    risk_score: Decimal
    assigned_to: Optional[str]
    status: str
    resolved_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

class AlertUpdate(BaseModel):
    assigned_to: Optional[str] = None
    status: Optional[str] = None
    resolution: Optional[str] = None

# Case models
class CaseCreate(BaseModel):
    customer_id: str
    title: str
    description: Optional[str] = None
    priority: str = "medium"
    alert_ids: List[str] = []

class CaseResponse(BaseModel):
    id: str
    customer_id: str
    title: str
    description: Optional[str]
    priority: str
    status: str
    assigned_to: Optional[str]
    alert_ids: List[str]
    findings: Optional[str]
    resolution: Optional[str]
    closed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

class CaseUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    findings: Optional[str] = None
    resolution: Optional[str] = None

# Metrics models
class DashboardMetrics(BaseModel):
    active_alerts: int
    daily_transactions: int
    avg_risk_score: float
    open_cases: int
    alerts_change: str
    transactions_change: str
    risk_score_change: str
    urgent_cases: int

class SystemStatus(BaseModel):
    ml_engine: str
    rules_engine: str
    stream_processing: str
    data_pipeline: str
    model_performance: Dict[str, float]
