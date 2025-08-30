"""
AMLGuard ML Service
Machine Learning service for risk scoring and anomaly detection
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import structlog
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import os

from .models.ensemble import EnsembleRiskModel
from .features.engineering import FeatureEngineeer

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

# Create FastAPI app
app = FastAPI(
    title="AMLGuard ML Service",
    description="Machine Learning service for AML risk scoring and anomaly detection",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Initialize models and feature engineer
ensemble_model = EnsembleRiskModel()
feature_engineer = FeatureEngineeer()

# Pydantic models
class TransactionData(BaseModel):
    transaction_id: str
    customer_id: str
    account_id: str
    amount: float = Field(..., gt=0)
    currency: str = "USD"
    transaction_type: str
    description: Optional[str] = None
    location: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None

class RiskPrediction(BaseModel):
    transaction_id: str
    risk_score: float = Field(..., ge=0, le=10)
    risk_level: str
    confidence: float = Field(..., ge=0, le=1)
    feature_importance: Dict[str, float]
    model_version: str
    prediction_timestamp: datetime
    explanation: List[str]

class ModelMetrics(BaseModel):
    model_name: str
    version: str
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    last_trained: datetime
    total_predictions: int

@app.on_event("startup")
async def startup_event():
    """Initialize ML models on startup"""
    logger.info("Starting AMLGuard ML Service")
    
    # Create model directory if it doesn't exist
    os.makedirs("models", exist_ok=True)
    
    # Train or load models
    await ensemble_model.initialize()
    logger.info("ML models initialized successfully")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "AMLGuard ML Service",
        "status": "operational",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    model_status = "operational" if ensemble_model.is_ready() else "initializing"
    
    return {
        "status": "healthy",
        "models": {
            "ensemble_model": model_status,
            "feature_engineer": "operational"
        },
        "version": ensemble_model.get_version()
    }

@app.post("/predict", response_model=RiskPrediction)
async def predict_risk(transaction: TransactionData):
    """Predict risk score for a transaction"""
    
    if not ensemble_model.is_ready():
        raise HTTPException(status_code=503, detail="ML models are not ready")
    
    try:
        # Engineer features
        features = await feature_engineer.engineer_features(transaction)
        
        # Get prediction from ensemble model
        prediction = await ensemble_model.predict(features)
        
        # Determine risk level
        risk_level = "low"
        if prediction["risk_score"] >= 7.0:
            risk_level = "critical"
        elif prediction["risk_score"] >= 4.0:
            risk_level = "medium"
        
        # Generate explanation
        explanation = _generate_explanation(prediction["feature_importance"], prediction["risk_score"])
        
        return RiskPrediction(
            transaction_id=transaction.transaction_id,
            risk_score=prediction["risk_score"],
            risk_level=risk_level,
            confidence=prediction["confidence"],
            feature_importance=prediction["feature_importance"],
            model_version=ensemble_model.get_version(),
            prediction_timestamp=datetime.utcnow(),
            explanation=explanation
        )
        
    except Exception as e:
        logger.error("Prediction failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

@app.get("/model/metrics", response_model=List[ModelMetrics])
async def get_model_metrics():
    """Get performance metrics for all models"""
    
    metrics = await ensemble_model.get_metrics()
    return [
        ModelMetrics(
            model_name=metric["name"],
            version=metric["version"],
            accuracy=metric["accuracy"],
            precision=metric["precision"],
            recall=metric["recall"],
            f1_score=metric["f1_score"],
            last_trained=metric["last_trained"],
            total_predictions=metric["total_predictions"]
        )
        for metric in metrics
    ]

@app.get("/model/version")
async def get_model_version():
    """Get current model version"""
    return {
        "version": ensemble_model.get_version(),
        "model_type": "ensemble",
        "components": ["xgboost", "isolation_forest"],
        "last_updated": ensemble_model.get_last_updated()
    }

@app.post("/model/retrain")
async def retrain_models():
    """Trigger model retraining (for production use)"""
    try:
        await ensemble_model.retrain()
        return {"message": "Model retraining initiated", "status": "success"}
    except Exception as e:
        logger.error("Model retraining failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Retraining failed: {str(e)}")

@app.get("/features/importance")
async def get_feature_importance():
    """Get global feature importance scores"""
    
    if not ensemble_model.is_ready():
        raise HTTPException(status_code=503, detail="ML models are not ready")
    
    importance = await ensemble_model.get_feature_importance()
    return {
        "feature_importance": importance,
        "model_version": ensemble_model.get_version(),
        "generated_at": datetime.utcnow()
    }

def _generate_explanation(feature_importance: Dict[str, float], risk_score: float) -> List[str]:
    """Generate human-readable explanation for risk score"""
    
    explanations = []
    
    # Sort features by importance
    sorted_features = sorted(feature_importance.items(), key=lambda x: abs(x[1]), reverse=True)
    
    # Take top 3 most important features
    for feature, importance in sorted_features[:3]:
        if abs(importance) < 0.01:  # Skip very low importance features
            continue
            
        if feature == "amount_zscore":
            if importance > 0:
                explanations.append("Transaction amount is significantly higher than customer's typical pattern")
            else:
                explanations.append("Transaction amount is within normal range for this customer")
                
        elif feature == "velocity_1h":
            if importance > 0:
                explanations.append("High transaction velocity detected in the last hour")
                
        elif feature == "geographic_risk":
            if importance > 0:
                explanations.append("Transaction from unusual geographic location")
                
        elif feature == "time_anomaly":
            if importance > 0:
                explanations.append("Transaction occurred at unusual time for this customer")
                
        elif feature == "amount_structuring":
            if importance > 0:
                explanations.append("Amount falls within potential structuring pattern")
    
    # Add overall risk assessment
    if risk_score >= 8.0:
        explanations.append("Transaction requires immediate investigation")
    elif risk_score >= 6.0:
        explanations.append("Transaction flagged for manual review")
    elif risk_score >= 3.0:
        explanations.append("Transaction shows moderate risk indicators")
    else:
        explanations.append("Transaction appears normal with low risk indicators")
    
    return explanations[:5]  # Limit to 5 explanations

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_config=None  # Use structlog instead
    )
