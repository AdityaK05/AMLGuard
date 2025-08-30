"""
Authentication routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import aiosqlite
import bcrypt
import jwt
import json
from datetime import datetime, timedelta
from uuid import uuid4

from ..database import get_db
from ..models import LoginRequest, LoginResponse, UserResponse

router = APIRouter()
security = HTTPBearer()

JWT_SECRET = "dev-secret-key"  # In production, use environment variable
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify JWT token"""
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
        return payload
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

@router.post("/login", response_model=LoginResponse)
async def login(
    login_data: LoginRequest,
    db: aiosqlite.Connection = Depends(get_db)
):
    """Authenticate user and return JWT token"""
    
    # Get user from database
    async with db.execute(
        "SELECT * FROM users WHERE username = ?", 
        (login_data.username,)
    ) as cursor:
        user_row = await cursor.fetchone()
    
    if not user_row:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    user = dict(user_row)
    
    # Verify password
    if not bcrypt.checkpw(login_data.password.encode(), user["password"].encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    # Update last login
    await db.execute(
        "UPDATE users SET last_login = ? WHERE id = ?",
        (datetime.utcnow(), user["id"])
    )
    await db.commit()
    
    # Create JWT token
    token_data = {
        "sub": user["id"],
        "username": user["username"],
        "role": user["role"],
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    }
    token = jwt.encode(token_data, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    # Remove password from response
    user.pop("password")
    user["permissions"] = json.loads(user.get("permissions", "[]"))
    
    return LoginResponse(user=user, token=token)

@router.get("/me", response_model=UserResponse)
async def get_current_user(
    token_data: dict = Depends(verify_token),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Get current user information"""
    
    user_id = token_data["sub"]
    
    async with db.execute(
        "SELECT * FROM users WHERE id = ?", 
        (user_id,)
    ) as cursor:
        user_row = await cursor.fetchone()
    
    if not user_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user = dict(user_row)
    user.pop("password")  # Remove password
    user["permissions"] = json.loads(user.get("permissions", "[]"))
    
    return UserResponse(**user)

@router.post("/logout")
async def logout():
    """Logout user (client should remove token)"""
    return {"message": "Logged out successfully"}
