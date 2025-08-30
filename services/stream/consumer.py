"""
Transaction Stream Consumer
Consumes transactions from asyncio queue for processing
"""

import asyncio
import json
import structlog
from typing import Dict, Any, Optional
from datetime import datetime

logger = structlog.get_logger()

class TransactionConsumer:
    """Asyncio-based transaction consumer"""
    
    def __init__(self, queue_size: int = 1000):
        self.transaction_queue = asyncio.Queue(maxsize=queue_size)
        self.running = False
        self.processed_count = 0
        
    async def start(self):
        """Start the consumer"""
        logger.info("Starting transaction consumer")
        self.running = True
        
        # In a real implementation, this would connect to a message broker
        # For now, we'll simulate receiving transactions
        await self._simulate_transaction_stream()
    
    async def stop(self):
        """Stop the consumer"""
        logger.info("Stopping transaction consumer")
        self.running = False
    
    async def get_transaction(self) -> Optional[Dict[str, Any]]:
        """Get next transaction from queue"""
        try:
            # Use timeout to avoid blocking indefinitely
            transaction = await asyncio.wait_for(
                self.transaction_queue.get(), 
                timeout=1.0
            )
            self.processed_count += 1
            return transaction
        except asyncio.TimeoutError:
            return None
    
    async def add_transaction(self, transaction: Dict[str, Any]):
        """Add transaction to processing queue"""
        try:
            # Add timestamp if not present
            if "timestamp" not in transaction:
                transaction["timestamp"] = datetime.utcnow().isoformat()
            
            await self.transaction_queue.put(transaction)
            logger.debug("Transaction queued", transaction_id=transaction.get("transaction_id"))
            
        except asyncio.QueueFull:
            logger.warning("Transaction queue is full, dropping transaction")
    
    async def _simulate_transaction_stream(self):
        """Simulate incoming transaction stream for testing"""
        logger.info("Simulating transaction stream")
        
        while self.running:
            try:
                # Wait for transactions to be added by producer
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error("Error in transaction stream simulation", error=str(e))
                await asyncio.sleep(5)
    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.transaction_queue.qsize()
    
    def get_processed_count(self) -> int:
        """Get total processed transaction count"""
        return self.processed_count

# Global consumer instance for sharing across modules
transaction_consumer = TransactionConsumer()
