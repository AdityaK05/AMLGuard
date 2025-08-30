"""
Ensemble ML Model for AML Risk Scoring
Combines XGBoost and Isolation Forest for comprehensive risk assessment
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
import xgboost as xgb
import joblib
from datetime import datetime, timedelta
import structlog
from pathlib import Path
from typing import Dict, List, Any, Optional
import asyncio

logger = structlog.get_logger()

class EnsembleRiskModel:
    """Ensemble model combining XGBoost classifier and Isolation Forest for anomaly detection"""
    
    def __init__(self):
        self.xgb_model = None
        self.isolation_forest = None
        self.scaler = StandardScaler()
        self.feature_names = []
        self.version = "1.0.0"
        self.is_trained = False
        self.metrics = {}
        self.last_updated = None
        self.prediction_count = 0
        
    async def initialize(self):
        """Initialize and train models with synthetic data"""
        logger.info("Initializing ensemble models")
        
        # Try to load existing models
        if self._load_models():
            logger.info("Loaded existing models from disk")
            return
        
        # Generate training data and train new models
        await self._train_new_models()
        
    def _load_models(self) -> bool:
        """Load models from disk if they exist"""
        try:
            model_dir = Path("models")
            
            if (model_dir / "xgb_model.joblib").exists():
                self.xgb_model = joblib.load(model_dir / "xgb_model.joblib")
                self.isolation_forest = joblib.load(model_dir / "isolation_forest.joblib")
                self.scaler = joblib.load(model_dir / "scaler.joblib")
                
                # Load metadata
                metadata = joblib.load(model_dir / "metadata.joblib")
                self.feature_names = metadata["feature_names"]
                self.version = metadata["version"]
                self.metrics = metadata["metrics"]
                self.last_updated = metadata["last_updated"]
                
                self.is_trained = True
                return True
                
        except Exception as e:
            logger.warning("Failed to load existing models", error=str(e))
            
        return False
        
    def _save_models(self):
        """Save models to disk"""
        try:
            model_dir = Path("models")
            model_dir.mkdir(exist_ok=True)
            
            joblib.dump(self.xgb_model, model_dir / "xgb_model.joblib")
            joblib.dump(self.isolation_forest, model_dir / "isolation_forest.joblib")
            joblib.dump(self.scaler, model_dir / "scaler.joblib")
            
            # Save metadata
            metadata = {
                "feature_names": self.feature_names,
                "version": self.version,
                "metrics": self.metrics,
                "last_updated": self.last_updated
            }
            joblib.dump(metadata, model_dir / "metadata.joblib")
            
            logger.info("Models saved successfully")
            
        except Exception as e:
            logger.error("Failed to save models", error=str(e))
    
    async def _train_new_models(self):
        """Train new models with synthetic data"""
        logger.info("Training new ensemble models")
        
        # Generate synthetic training data
        X, y = self._generate_training_data()
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train XGBoost classifier
        self.xgb_model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            eval_metric='logloss'
        )
        self.xgb_model.fit(X_train_scaled, y_train)
        
        # Train Isolation Forest for anomaly detection
        self.isolation_forest = IsolationForest(
            contamination=0.1,
            random_state=42,
            n_estimators=100
        )
        self.isolation_forest.fit(X_train_scaled)
        
        # Evaluate models
        y_pred_xgb = self.xgb_model.predict(X_test_scaled)
        y_pred_proba_xgb = self.xgb_model.predict_proba(X_test_scaled)[:, 1]
        
        # Calculate metrics
        auc_score = roc_auc_score(y_test, y_pred_proba_xgb)
        
        self.metrics = {
            "xgboost": {
                "accuracy": (y_pred_xgb == y_test).mean(),
                "auc": auc_score,
                "precision": np.sum((y_pred_xgb == 1) & (y_test == 1)) / max(np.sum(y_pred_xgb == 1), 1),
                "recall": np.sum((y_pred_xgb == 1) & (y_test == 1)) / max(np.sum(y_test == 1), 1)
            }
        }
        
        self.is_trained = True
        self.last_updated = datetime.utcnow()
        
        # Save models
        self._save_models()
        
        logger.info("Model training completed", metrics=self.metrics)
    
    def _generate_training_data(self, n_samples: int = 10000) -> tuple:
        """Generate synthetic training data for AML scenarios"""
        
        np.random.seed(42)
        
        # Feature names
        self.feature_names = [
            "amount_zscore", "velocity_1h", "velocity_24h", "geographic_risk",
            "time_anomaly", "amount_structuring", "account_age_days",
            "avg_transaction_amount", "transaction_frequency", "risk_country"
        ]
        
        # Generate normal transactions (80%)
        n_normal = int(n_samples * 0.8)
        normal_data = np.random.normal(0, 1, (n_normal, len(self.feature_names)))
        
        # Adjust normal patterns
        normal_data[:, 0] = np.random.normal(0, 0.5, n_normal)  # amount_zscore
        normal_data[:, 1] = np.random.exponential(1, n_normal)  # velocity_1h
        normal_data[:, 2] = np.random.exponential(2, n_normal)  # velocity_24h
        normal_data[:, 3] = np.random.beta(2, 8, n_normal)      # geographic_risk
        normal_data[:, 4] = np.random.beta(2, 8, n_normal)      # time_anomaly
        normal_data[:, 5] = np.random.beta(1, 10, n_normal)     # amount_structuring
        normal_data[:, 6] = np.random.lognormal(4, 1, n_normal) # account_age_days
        normal_data[:, 7] = np.random.lognormal(6, 1, n_normal) # avg_transaction_amount
        normal_data[:, 8] = np.random.poisson(5, n_normal)      # transaction_frequency
        normal_data[:, 9] = np.random.beta(1, 9, n_normal)      # risk_country
        
        normal_labels = np.zeros(n_normal)
        
        # Generate suspicious transactions (20%)
        n_suspicious = n_samples - n_normal
        suspicious_data = np.random.normal(0, 1, (n_suspicious, len(self.feature_names)))
        
        # Create suspicious patterns
        suspicious_data[:, 0] = np.random.normal(2, 1, n_suspicious)    # High amount_zscore
        suspicious_data[:, 1] = np.random.exponential(5, n_suspicious)  # High velocity_1h
        suspicious_data[:, 2] = np.random.exponential(10, n_suspicious) # High velocity_24h
        suspicious_data[:, 3] = np.random.beta(8, 2, n_suspicious)      # High geographic_risk
        suspicious_data[:, 4] = np.random.beta(6, 4, n_suspicious)      # Higher time_anomaly
        suspicious_data[:, 5] = np.random.beta(7, 3, n_suspicious)      # Higher structuring risk
        suspicious_data[:, 6] = np.random.lognormal(3, 1, n_suspicious) # Newer accounts
        suspicious_data[:, 7] = np.random.lognormal(8, 1, n_suspicious) # Higher avg amounts
        suspicious_data[:, 8] = np.random.poisson(15, n_suspicious)     # Higher frequency
        suspicious_data[:, 9] = np.random.beta(6, 4, n_suspicious)      # Higher risk countries
        
        suspicious_labels = np.ones(n_suspicious)
        
        # Combine data
        X = np.vstack([normal_data, suspicious_data])
        y = np.hstack([normal_labels, suspicious_labels])
        
        # Shuffle
        indices = np.random.permutation(len(X))
        X = X[indices]
        y = y[indices]
        
        return X, y
    
    async def predict(self, features: Dict[str, float]) -> Dict[str, Any]:
        """Predict risk score for a transaction"""
        
        if not self.is_trained:
            raise ValueError("Model is not trained")
        
        # Convert features to array
        feature_array = np.array([features.get(name, 0.0) for name in self.feature_names]).reshape(1, -1)
        
        # Scale features
        feature_array_scaled = self.scaler.transform(feature_array)
        
        # Get XGBoost prediction
        xgb_proba = self.xgb_model.predict_proba(feature_array_scaled)[0, 1]
        
        # Get anomaly score from Isolation Forest
        anomaly_score = self.isolation_forest.decision_function(feature_array_scaled)[0]
        # Convert to 0-1 scale (higher = more anomalous)
        anomaly_score_normalized = max(0, min(1, (0.5 - anomaly_score) / 1.0))
        
        # Ensemble prediction (weighted average)
        ensemble_score = (0.7 * xgb_proba) + (0.3 * anomaly_score_normalized)
        
        # Convert to 0-10 risk score
        risk_score = ensemble_score * 10
        
        # Get feature importance
        feature_importance = dict(zip(
            self.feature_names,
            self.xgb_model.feature_importances_
        ))
        
        # Calculate confidence based on consistency between models
        confidence = 1.0 - abs(xgb_proba - anomaly_score_normalized)
        
        self.prediction_count += 1
        
        return {
            "risk_score": float(risk_score),
            "confidence": float(confidence),
            "feature_importance": feature_importance,
            "xgb_probability": float(xgb_proba),
            "anomaly_score": float(anomaly_score_normalized)
        }
    
    async def get_feature_importance(self) -> Dict[str, float]:
        """Get global feature importance"""
        if not self.is_trained:
            return {}
        
        return dict(zip(self.feature_names, self.xgb_model.feature_importances_))
    
    async def get_metrics(self) -> List[Dict[str, Any]]:
        """Get model performance metrics"""
        if not self.metrics:
            return []
        
        return [
            {
                "name": "XGBoost Classifier",
                "version": self.version,
                "accuracy": self.metrics.get("xgboost", {}).get("accuracy", 0.0),
                "precision": self.metrics.get("xgboost", {}).get("precision", 0.0),
                "recall": self.metrics.get("xgboost", {}).get("recall", 0.0),
                "f1_score": 2 * (self.metrics.get("xgboost", {}).get("precision", 0.0) * 
                               self.metrics.get("xgboost", {}).get("recall", 0.0)) / 
                          max((self.metrics.get("xgboost", {}).get("precision", 0.0) + 
                               self.metrics.get("xgboost", {}).get("recall", 0.0)), 0.001),
                "last_trained": self.last_updated or datetime.utcnow(),
                "total_predictions": self.prediction_count
            }
        ]
    
    async def retrain(self):
        """Retrain models with new data"""
        logger.info("Retraining ensemble models")
        self.is_trained = False
        await self._train_new_models()
        
    def is_ready(self) -> bool:
        """Check if models are ready for prediction"""
        return self.is_trained and self.xgb_model is not None and self.isolation_forest is not None
    
    def get_version(self) -> str:
        """Get model version"""
        return self.version
    
    def get_last_updated(self) -> Optional[datetime]:
        """Get last update timestamp"""
        return self.last_updated
