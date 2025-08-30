"""
AMLGuard Stream Processing Service
Asyncio-based stream processor for real-time transaction monitoring
"""

import asyncio
import json
import structlog
from datetime import datetime
from typing import Dict, Any, Optional
import aiofiles
import httpx
from pathlib import Path

from .consumer import TransactionConsumer
from .producer import TransactionProducer

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

class StreamProcessor:
    """Main stream processing coordinator"""
    
    def __init__(self):
        self.consumer = TransactionConsumer()
        self.producer = TransactionProducer()
        self.ml_service_url = "http://localhost:8001"
        self.api_service_url = "http://localhost:8000"
        self.rules_engine = None
        self.running = False
        self.processed_count = 0
        
    async def start(self):
        """Start the stream processor"""
        logger.info("Starting AMLGuard Stream Processor")
        
        # Initialize rules engine
        from ..rules.engine import RulesEngine
        self.rules_engine = RulesEngine()
        await self.rules_engine.load_rules()
        
        self.running = True
        
        # Start consumer and producer tasks
        tasks = [
            asyncio.create_task(self.consumer.start()),
            asyncio.create_task(self.producer.start()),
            asyncio.create_task(self._process_transactions())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the stream processor"""
        logger.info("Stopping stream processor")
        self.running = False
        
        await self.consumer.stop()
        await self.producer.stop()
        
        logger.info("Stream processor stopped", processed_count=self.processed_count)
    
    async def _process_transactions(self):
        """Main transaction processing loop"""
        logger.info("Starting transaction processing loop")
        
        while self.running:
            try:
                # Get next transaction from queue
                transaction = await self.consumer.get_transaction()
                
                if transaction is None:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process the transaction
                await self._process_single_transaction(transaction)
                self.processed_count += 1
                
            except Exception as e:
                logger.error("Error processing transaction", error=str(e))
                await asyncio.sleep(1)
    
    async def _process_single_transaction(self, transaction: Dict[str, Any]):
        """Process a single transaction through the AML pipeline"""
        
        transaction_id = transaction.get("transaction_id", "unknown")
        logger.info("Processing transaction", transaction_id=transaction_id)
        
        try:
            # Step 1: Get ML risk score
            risk_prediction = await self._get_ml_prediction(transaction)
            
            # Step 2: Apply rules engine
            rule_results = await self._apply_rules(transaction)
            
            # Step 3: Calculate final risk score
            final_risk_score = self._calculate_final_risk_score(risk_prediction, rule_results)
            
            # Step 4: Update transaction in database
            await self._update_transaction(transaction_id, risk_prediction, rule_results, final_risk_score)
            
            # Step 5: Create alert if necessary
            if final_risk_score >= 6.0:  # High risk threshold
                await self._create_alert(transaction, risk_prediction, rule_results, final_risk_score)
            
            logger.info(
                "Transaction processed successfully",
                transaction_id=transaction_id,
                risk_score=final_risk_score,
                rules_triggered=len(rule_results.get("triggered_rules", []))
            )
            
        except Exception as e:
            logger.error("Failed to process transaction", transaction_id=transaction_id, error=str(e))
    
    async def _get_ml_prediction(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get ML risk prediction for transaction"""
        
        try:
            # Prepare transaction data for ML service
            ml_request = {
                "transaction_id": transaction.get("transaction_id"),
                "customer_id": transaction.get("customer_id"),
                "account_id": transaction.get("from_account_id"),
                "amount": transaction.get("amount"),
                "currency": transaction.get("currency", "USD"),
                "transaction_type": transaction.get("transaction_type"),
                "description": transaction.get("description"),
                "location": transaction.get("location"),
                "timestamp": transaction.get("timestamp")
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.ml_service_url}/predict",
                    json=ml_request,
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(
                        "ML service request failed",
                        status_code=response.status_code,
                        response=response.text
                    )
                    return None
                    
        except Exception as e:
            logger.error("ML prediction failed", error=str(e))
            return None
    
    async def _apply_rules(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Apply rules engine to transaction"""
        
        if self.rules_engine is None:
            return {"triggered_rules": [], "rule_scores": {}}
        
        try:
            return await self.rules_engine.evaluate_transaction(transaction)
        except Exception as e:
            logger.error("Rules evaluation failed", error=str(e))
            return {"triggered_rules": [], "rule_scores": {}}
    
    def _calculate_final_risk_score(self, ml_prediction: Optional[Dict], rule_results: Dict) -> float:
        """Calculate final risk score combining ML and rules"""
        
        # Get ML risk score (0-10 scale)
        ml_score = 0.0
        if ml_prediction:
            ml_score = ml_prediction.get("risk_score", 0.0)
        
        # Get highest rule score
        rule_scores = rule_results.get("rule_scores", {})
        max_rule_score = max(rule_scores.values()) if rule_scores else 0.0
        
        # Combine scores (weighted average with boost for rule triggers)
        if rule_results.get("triggered_rules"):
            # If rules are triggered, boost the score
            final_score = min(10.0, (0.6 * ml_score) + (0.4 * max_rule_score * 10) + 1.0)
        else:
            # No rules triggered, use mostly ML score
            final_score = (0.8 * ml_score) + (0.2 * max_rule_score * 10)
        
        return round(final_score, 2)
    
    async def _update_transaction(self, transaction_id: str, ml_prediction: Optional[Dict], 
                                rule_results: Dict, final_risk_score: float):
        """Update transaction in database with risk assessment results"""
        
        try:
            update_data = {
                "risk_score": final_risk_score,
                "ml_prediction": ml_prediction,
                "rules_hit": rule_results.get("triggered_rules", []),
                "status": "flagged" if final_risk_score >= 6.0 else "clear",
                "processed_at": datetime.utcnow().isoformat()
            }
            
            # In a real implementation, this would update the database directly
            # For now, we'll log the update
            logger.info("Transaction updated", transaction_id=transaction_id, update_data=update_data)
            
        except Exception as e:
            logger.error("Failed to update transaction", transaction_id=transaction_id, error=str(e))
    
    async def _create_alert(self, transaction: Dict, ml_prediction: Optional[Dict], 
                          rule_results: Dict, risk_score: float):
        """Create alert for high-risk transaction"""
        
        try:
            # Determine alert severity
            if risk_score >= 8.0:
                severity = "critical"
            elif risk_score >= 6.0:
                severity = "high"
            else:
                severity = "medium"
            
            # Determine alert type based on triggered rules
            triggered_rules = rule_results.get("triggered_rules", [])
            if "structuring" in triggered_rules:
                alert_type = "structuring"
                title = "Potential Structuring Pattern Detected"
            elif "velocity" in triggered_rules:
                alert_type = "velocity"
                title = "High-Velocity Transaction Pattern"
            elif "geographic" in triggered_rules:
                alert_type = "geographic"
                title = "Unusual Geographic Activity"
            else:
                alert_type = "anomaly"
                title = "Anomalous Transaction Detected"
            
            # Generate description
            description = f"Transaction flagged with risk score {risk_score}. "
            if triggered_rules:
                description += f"Rules triggered: {', '.join(triggered_rules)}. "
            if ml_prediction:
                description += f"ML confidence: {ml_prediction.get('confidence', 0):.2f}"
            
            alert_data = {
                "transaction_id": transaction.get("transaction_id"),
                "customer_id": transaction.get("customer_id"),
                "alert_type": alert_type,
                "severity": severity,
                "title": title,
                "description": description,
                "risk_score": risk_score
            }
            
            # In a real implementation, this would create an alert in the database
            logger.info("Alert created", alert_data=alert_data)
            
        except Exception as e:
            logger.error("Failed to create alert", error=str(e))

async def main():
    """Main entry point for stream processor"""
    processor = StreamProcessor()
    
    try:
        await processor.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await processor.stop()

if __name__ == "__main__":
    asyncio.run(main())
