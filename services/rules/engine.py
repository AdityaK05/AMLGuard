"""
AML Rules Engine
YAML-based rule configuration system for detecting suspicious patterns
"""

import yaml
import structlog
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta
import re
import asyncio

logger = structlog.get_logger()

class RulesEngine:
    """YAML-based AML rules engine"""
    
    def __init__(self, rules_dir: str = "services/rules/configs"):
        self.rules_dir = Path(rules_dir)
        self.rules = {}
        self.rule_stats = {}
        
    async def load_rules(self):
        """Load all rule configurations from YAML files"""
        logger.info("Loading AML rules", rules_dir=str(self.rules_dir))
        
        if not self.rules_dir.exists():
            logger.warning("Rules directory does not exist", path=str(self.rules_dir))
            return
        
        # Load all YAML files in rules directory
        for rule_file in self.rules_dir.glob("*.yaml"):
            try:
                with open(rule_file, 'r') as f:
                    rule_config = yaml.safe_load(f)
                
                rule_name = rule_file.stem
                self.rules[rule_name] = rule_config
                self.rule_stats[rule_name] = {
                    "triggers": 0,
                    "evaluations": 0,
                    "last_triggered": None
                }
                
                logger.info("Loaded rule", rule_name=rule_name, file=str(rule_file))
                
            except Exception as e:
                logger.error("Failed to load rule", file=str(rule_file), error=str(e))
        
        logger.info(f"Loaded {len(self.rules)} rules successfully")
    
    async def evaluate_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate transaction against all rules"""
        
        triggered_rules = []
        rule_scores = {}
        
        for rule_name, rule_config in self.rules.items():
            try:
                self.rule_stats[rule_name]["evaluations"] += 1
                
                # Evaluate rule conditions
                is_triggered, score = await self._evaluate_rule(rule_config, transaction)
                
                if is_triggered:
                    triggered_rules.append(rule_name)
                    rule_scores[rule_name] = score
                    self.rule_stats[rule_name]["triggers"] += 1
                    self.rule_stats[rule_name]["last_triggered"] = datetime.utcnow()
                    
                    logger.info(
                        "Rule triggered",
                        rule_name=rule_name,
                        transaction_id=transaction.get("transaction_id"),
                        score=score
                    )
                
            except Exception as e:
                logger.error("Rule evaluation failed", rule_name=rule_name, error=str(e))
        
        return {
            "triggered_rules": triggered_rules,
            "rule_scores": rule_scores,
            "evaluation_timestamp": datetime.utcnow().isoformat()
        }
    
    async def _evaluate_rule(self, rule_config: Dict[str, Any], transaction: Dict[str, Any]) -> tuple[bool, float]:
        """Evaluate a single rule against transaction"""
        
        conditions = rule_config.get("conditions", [])
        logic = rule_config.get("logic", "AND")  # AND or OR
        base_score = rule_config.get("score", 0.5)
        
        if not conditions:
            return False, 0.0
        
        condition_results = []
        
        for condition in conditions:
            result = await self._evaluate_condition(condition, transaction)
            condition_results.append(result)
        
        # Apply logic operator
        if logic == "AND":
            is_triggered = all(condition_results)
        elif logic == "OR":
            is_triggered = any(condition_results)
        else:
            # Default to AND
            is_triggered = all(condition_results)
        
        # Calculate score based on number of conditions met
        if is_triggered:
            score_multiplier = sum(condition_results) / len(condition_results)
            final_score = base_score * score_multiplier
        else:
            final_score = 0.0
        
        return is_triggered, final_score
    
    async def _evaluate_condition(self, condition: Dict[str, Any], transaction: Dict[str, Any]) -> bool:
        """Evaluate a single condition"""
        
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")
        
        if not field or not operator:
            return False
        
        # Get field value from transaction
        field_value = self._get_field_value(transaction, field)
        
        if field_value is None:
            return False
        
        # Apply operator
        return self._apply_operator(field_value, operator, value)
    
    def _get_field_value(self, transaction: Dict[str, Any], field_path: str) -> Any:
        """Get field value using dot notation (e.g., 'location.country')"""
        
        parts = field_path.split(".")
        value = transaction
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        
        return value
    
    def _apply_operator(self, field_value: Any, operator: str, expected_value: Any) -> bool:
        """Apply comparison operator"""
        
        try:
            if operator == "equals":
                return field_value == expected_value
            
            elif operator == "not_equals":
                return field_value != expected_value
            
            elif operator == "greater_than":
                return float(field_value) > float(expected_value)
            
            elif operator == "less_than":
                return float(field_value) < float(expected_value)
            
            elif operator == "greater_equal":
                return float(field_value) >= float(expected_value)
            
            elif operator == "less_equal":
                return float(field_value) <= float(expected_value)
            
            elif operator == "in":
                return field_value in expected_value
            
            elif operator == "not_in":
                return field_value not in expected_value
            
            elif operator == "contains":
                return expected_value in str(field_value).lower()
            
            elif operator == "not_contains":
                return expected_value not in str(field_value).lower()
            
            elif operator == "regex":
                return bool(re.search(expected_value, str(field_value)))
            
            elif operator == "between":
                if isinstance(expected_value, list) and len(expected_value) == 2:
                    return expected_value[0] <= float(field_value) <= expected_value[1]
                return False
            
            elif operator == "near_threshold":
                # Special operator for detecting amounts near reporting thresholds
                threshold = float(expected_value)
                amount = float(field_value)
                return (threshold * 0.85) <= amount < threshold
            
            else:
                logger.warning("Unknown operator", operator=operator)
                return False
                
        except (ValueError, TypeError, AttributeError) as e:
            logger.debug("Operator evaluation failed", operator=operator, error=str(e))
            return False
    
    def get_rule_stats(self) -> Dict[str, Any]:
        """Get rule execution statistics"""
        return self.rule_stats.copy()
    
    def get_rules_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all loaded rules"""
        
        summary = []
        
        for rule_name, rule_config in self.rules.items():
            stats = self.rule_stats.get(rule_name, {})
            
            summary.append({
                "name": rule_name,
                "description": rule_config.get("description", ""),
                "enabled": rule_config.get("enabled", True),
                "severity": rule_config.get("severity", "medium"),
                "score": rule_config.get("score", 0.5),
                "triggers": stats.get("triggers", 0),
                "evaluations": stats.get("evaluations", 0),
                "last_triggered": stats.get("last_triggered")
            })
        
        return summary
    
    async def reload_rules(self):
        """Reload all rules from disk"""
        logger.info("Reloading AML rules")
        
        # Clear existing rules but preserve stats
        self.rules.clear()
        
        # Load rules again
        await self.load_rules()
