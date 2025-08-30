"""
AMLGuard API Service
Main FastAPI application for transaction processing and alert management
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import structlog
from contextlib import asynccontextmanager

from .database import init_db
from .routes import transactions, alerts, cases, auth

# Configure structured logging
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting AMLGuard API Service")
    
    # Initialize database
    await init_db()
    logger.info("Database initialized")
    
    yield
    
    logger.info("Shutting down AMLGuard API Service")

# Create FastAPI app
app = FastAPI(
    title="AMLGuard API",
    description="Anti-Money Laundering Compliance Platform API",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(transactions.router, prefix="/api/transactions", tags=["Transactions"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["Alerts"])
app.include_router(cases.router, prefix="/api/cases", tags=["Cases"])

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "AMLGuard API",
        "status": "operational",
        "version": "1.0.0"
    }

@app.get("/api/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "services": {
            "database": "operational",
            "ml_service": "operational",
            "stream_processor": "operational"
        }
    }

@app.get("/api/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    # TODO: Implement Prometheus metrics
    return {"message": "Metrics endpoint - to be implemented"}

@app.get("/api/metrics/dashboard")
async def get_dashboard_metrics():
    """Get dashboard metrics"""
    # Mock data for development
    return {
        "activeAlerts": 23,
        "dailyTransactions": 12847,
        "avgRiskScore": 2.3,
        "openCases": 7,
        "alertsChange": "+12% from yesterday",
        "transactionsChange": "+5.2% from yesterday",
        "riskScoreChange": "-0.1 from yesterday",
        "urgentCases": 2
    }

@app.get("/api/system/status")
async def get_system_status():
    """Get system status"""
    return {
        "mlEngine": "operational",
        "rulesEngine": "operational", 
        "streamProcessing": "operational",
        "dataPipeline": "degraded",
        "modelPerformance": {
            "accuracy": 0.942,
            "precision": 0.897
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_config=None  # Use structlog instead
    )
