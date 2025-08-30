#!/usr/bin/env python3
"""
Fake KYC Data Generator for AML Testing
Generates realistic customer profiles and KYC documentation for testing compliance workflows
"""

import asyncio
import aiosqlite
import json
import structlog
from datetime import datetime, timedelta, date
from uuid import uuid4
from faker import Faker
import random
import base64
from pathlib import Path
import hashlib

# Configure logging
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
fake = Faker()

DATABASE_PATH = "data/amlguard.db"

class KYCDataGenerator:
    """Generates fake KYC data for AML testing purposes"""
    
    def __init__(self):
        self.document_types = [
            "passport", "drivers_license", "national_id", "birth_certificate",
            "utility_bill", "bank_statement", "tax_return", "employment_letter"
        ]
        
        self.id_document_types = ["passport", "drivers_license", "national_id"]
        self.address_document_types = ["utility_bill", "bank_statement", "tax_return"]
        
        self.occupations = [
            "Software Engineer", "Teacher", "Doctor", "Lawyer", "Accountant",
            "Sales Manager", "Marketing Director", "Consultant", "Engineer",
            "Nurse", "Police Officer", "Fire Fighter", "Pilot", "Chef",
            "Real Estate Agent", "Insurance Agent", "Bank Manager",
            "Business Owner", "Retired", "Student", "Unemployed"
        ]
        
        self.income_ranges = [
            "Under $25,000", "$25,000 - $50,000", "$50,000 - $75,000",
            "$75,000 - $100,000", "$100,000 - $150,000", "$150,000 - $250,000",
            "$250,000 - $500,000", "Over $500,000"
        ]
        
        self.risk_factors = [
            "pep_exposure",           # Politically Exposed Person
            "adverse_media",          # Negative news coverage
            "sanctions_screening",    # Sanctions list matches
            "high_risk_geography",    # High-risk country connections
            "complex_ownership",      # Complex beneficial ownership
            "cash_intensive_business", # Cash-intensive business model
            "correspondent_banking",   # Correspondent banking relationships
            "cryptocurrency_exposure", # Cryptocurrency transactions
            "offshore_structures",    # Offshore company structures
            "unusual_transaction_patterns" # Unusual transaction patterns
        ]

    async def generate_kyc_profiles(self, count: int = 50):
        """Generate comprehensive KYC profiles with documentation"""
        
        logger.info("Generating KYC profiles", count=count)
        
        async with aiosqlite.connect(DATABASE_PATH) as db:
            # Create KYC tables if they don't exist
            await self._create_kyc_tables(db)
            
            profiles_created = 0
            
            for _ in range(count):
                try:
                    # Generate customer profile
                    customer_profile = await self._generate_customer_profile()
                    
                    # Generate KYC documentation
                    kyc_documents = await self._generate_kyc_documents(customer_profile)
                    
                    # Generate risk assessment
                    risk_assessment = await self._generate_risk_assessment(customer_profile)
                    
                    # Generate compliance checks
                    compliance_checks = await self._generate_compliance_checks(customer_profile)
                    
                    # Insert customer
                    customer_id = await self._insert_customer(db, customer_profile)
                    
                    # Insert KYC record
                    await self._insert_kyc_record(db, customer_id, customer_profile, 
                                                 kyc_documents, risk_assessment, compliance_checks)
                    
                    # Insert documents
                    for doc in kyc_documents:
                        await self._insert_document(db, customer_id, doc)
                    
                    # Insert risk factors
                    for risk_factor in risk_assessment.get("risk_factors", []):
                        await self._insert_risk_factor(db, customer_id, risk_factor)
                    
                    profiles_created += 1
                    
                    if profiles_created % 10 == 0:
                        logger.info(f"Generated {profiles_created} KYC profiles")
                        
                except Exception as e:
                    logger.error("Failed to generate KYC profile", error=str(e))
                    continue
            
            await db.commit()
            
        logger.info(f"Successfully generated {profiles_created} KYC profiles")
        return profiles_created

    async def _create_kyc_tables(self, db):
        """Create KYC-related database tables"""
        
        await db.executescript("""
            -- KYC Records table
            CREATE TABLE IF NOT EXISTS kyc_records (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                kyc_status TEXT NOT NULL DEFAULT 'pending',
                verification_level TEXT NOT NULL DEFAULT 'basic',
                risk_rating TEXT NOT NULL DEFAULT 'low',
                last_review_date TIMESTAMP,
                next_review_date TIMESTAMP,
                assigned_officer TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
            
            -- KYC Documents table
            CREATE TABLE IF NOT EXISTS kyc_documents (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                document_type TEXT NOT NULL,
                document_number TEXT,
                issuing_authority TEXT,
                issue_date DATE,
                expiry_date DATE,
                document_status TEXT DEFAULT 'pending',
                verification_method TEXT,
                file_hash TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                verified_at TIMESTAMP,
                verified_by TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
            
            -- Risk Factors table
            CREATE TABLE IF NOT EXISTS risk_factors (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                factor_type TEXT NOT NULL,
                factor_description TEXT,
                risk_level TEXT NOT NULL,
                identified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'active',
                mitigation_measures TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
            
            -- Compliance Checks table
            CREATE TABLE IF NOT EXISTS compliance_checks (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                check_type TEXT NOT NULL,
                check_status TEXT NOT NULL,
                check_result TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                checked_by TEXT,
                details TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
            
            -- PEP Screening table
            CREATE TABLE IF NOT EXISTS pep_screening (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                screening_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pep_status TEXT NOT NULL DEFAULT 'not_pep',
                pep_category TEXT,
                pep_details TEXT,
                source_lists TEXT,
                confidence_score DECIMAL(3,2),
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
            
            -- Sanctions Screening table  
            CREATE TABLE IF NOT EXISTS sanctions_screening (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                screening_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sanctions_status TEXT NOT NULL DEFAULT 'clear',
                match_details TEXT,
                source_lists TEXT,
                confidence_score DECIMAL(3,2),
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
        """)

    async def _generate_customer_profile(self):
        """Generate a comprehensive customer profile"""
        
        # Determine customer type and risk level
        customer_types = ["individual", "business", "trust", "foundation"]
        customer_type = random.choices(customer_types, weights=[0.7, 0.2, 0.05, 0.05])[0]
        
        # Risk level distribution (realistic for financial institutions)
        risk_levels = ["low", "medium", "high"]
        risk_weights = [0.75, 0.20, 0.05]  # 75% low, 20% medium, 5% high
        risk_level = random.choices(risk_levels, weights=risk_weights)[0]
        
        profile = {
            "customer_type": customer_type,
            "risk_level": risk_level,
            "kyc_tier": self._determine_kyc_tier(risk_level),
        }
        
        if customer_type == "individual":
            profile.update(self._generate_individual_profile())
        else:
            profile.update(self._generate_business_profile())
        
        # Add financial information
        profile.update(self._generate_financial_profile(risk_level))
        
        # Add geographic information
        profile.update(self._generate_geographic_profile(risk_level))
        
        return profile

    def _determine_kyc_tier(self, risk_level):
        """Determine KYC verification tier based on risk level"""
        
        if risk_level == "high":
            return "enhanced"
        elif risk_level == "medium":
            return "standard"
        else:
            return "simplified"

    def _generate_individual_profile(self):
        """Generate individual customer profile"""
        
        # Generate person
        gender = random.choice(["male", "female"])
        if gender == "male":
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()
        
        last_name = fake.last_name()
        
        # Age influences risk and documentation
        age = random.randint(18, 85)
        date_of_birth = fake.date_of_birth(minimum_age=age, maximum_age=age)
        
        return {
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "email": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}",
            "phone": fake.phone_number(),
            "date_of_birth": date_of_birth,
            "gender": gender,
            "nationality": self._generate_nationality(),
            "occupation": random.choice(self.occupations),
            "marital_status": random.choice(["single", "married", "divorced", "widowed"]),
        }

    def _generate_business_profile(self):
        """Generate business customer profile"""
        
        business_types = [
            "LLC", "Corporation", "Partnership", "Sole Proprietorship",
            "Trust", "Foundation", "Cooperative", "Non-Profit"
        ]
        
        industry_sectors = [
            "Technology", "Healthcare", "Finance", "Real Estate", "Manufacturing",
            "Retail", "Construction", "Transportation", "Energy", "Agriculture",
            "Entertainment", "Education", "Government", "Non-Profit"
        ]
        
        company_name = fake.company()
        
        return {
            "business_name": company_name,
            "legal_name": f"{company_name} {random.choice(business_types)}",
            "business_type": random.choice(business_types),
            "industry_sector": random.choice(industry_sectors),
            "incorporation_date": fake.date_between(start_date="-20y", end_date="-1y"),
            "tax_id": fake.ein(),
            "registration_number": f"REG{random.randint(100000, 999999)}",
            "website": f"https://www.{company_name.lower().replace(' ', '').replace(',', '')}.com",
        }

    def _generate_financial_profile(self, risk_level):
        """Generate financial profile based on risk level"""
        
        # Income/Revenue ranges vary by risk level
        if risk_level == "high":
            income_multiplier = random.uniform(5, 50)  # High earners
            source_of_wealth_complexity = "complex"
        elif risk_level == "medium":
            income_multiplier = random.uniform(2, 10)
            source_of_wealth_complexity = "moderate"
        else:
            income_multiplier = random.uniform(0.5, 5)
            source_of_wealth_complexity = "simple"
        
        base_income = random.uniform(25000, 100000)
        annual_income = base_income * income_multiplier
        
        return {
            "annual_income": round(annual_income, 2),
            "income_range": self._categorize_income(annual_income),
            "source_of_funds": self._generate_source_of_funds(source_of_wealth_complexity),
            "source_of_wealth": self._generate_source_of_wealth(source_of_wealth_complexity),
            "expected_transaction_volume": self._calculate_expected_volume(annual_income),
            "account_purpose": self._generate_account_purpose(),
        }

    def _generate_geographic_profile(self, risk_level):
        """Generate geographic profile with risk considerations"""
        
        # Country selection based on risk level
        if risk_level == "high":
            # Include some high-risk jurisdictions
            countries = ["US", "UK", "DE", "FR", "CH"] + ["RU", "CN", "VE", "IR", "PK"]
            country = random.choice(countries)
        elif risk_level == "medium":
            # Mix of medium and low risk countries
            countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "CN", "RU", "BR", "IN"]
            country = random.choice(countries)
        else:
            # Primarily low-risk countries
            countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "CH", "NL", "SE"]
            country = random.choice(countries)
        
        # Generate addresses
        addresses = []
        num_addresses = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
        
        for i in range(num_addresses):
            address_type = "primary" if i == 0 else random.choice(["secondary", "business", "mailing"])
            
            addresses.append({
                "type": address_type,
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state() if country == "US" else fake.state(),
                "postal_code": fake.zipcode() if country == "US" else fake.postcode(),
                "country": country,
                "verified": random.choice([True, False])
            })
        
        return {
            "nationality": country,
            "residence_country": country,
            "addresses": addresses,
            "tax_residency": [country] + (
                [random.choice(["US", "UK", "CH", "SG"])] if random.random() < 0.1 else []
            )
        }

    async def _generate_kyc_documents(self, customer_profile):
        """Generate KYC documents for the customer"""
        
        documents = []
        
        # ID Documents (required)
        id_docs_needed = random.choices([1, 2], weights=[0.8, 0.2])[0]
        
        for _ in range(id_docs_needed):
            doc_type = random.choice(self.id_document_types)
            documents.append(self._create_document(doc_type, customer_profile))
        
        # Address Documents (required)
        address_docs_needed = random.choices([1, 2], weights=[0.9, 0.1])[0]
        
        for _ in range(address_docs_needed):
            doc_type = random.choice(self.address_document_types)
            documents.append(self._create_document(doc_type, customer_profile))
        
        # Additional documents for enhanced KYC
        if customer_profile.get("kyc_tier") == "enhanced":
            additional_docs = ["employment_letter", "tax_return", "bank_statement"]
            for doc_type in additional_docs:
                if random.random() < 0.7:  # 70% chance of having each additional doc
                    documents.append(self._create_document(doc_type, customer_profile))
        
        # Business-specific documents
        if customer_profile.get("customer_type") != "individual":
            business_docs = [
                "articles_of_incorporation", "business_license", "board_resolution",
                "beneficial_ownership_declaration", "audited_financials"
            ]
            
            for doc_type in business_docs:
                if random.random() < 0.8:  # 80% chance for business docs
                    documents.append(self._create_document(doc_type, customer_profile))
        
        return documents

    def _create_document(self, doc_type, customer_profile):
        """Create a document record"""
        
        # Document status distribution
        status_weights = {"verified": 0.7, "pending": 0.2, "rejected": 0.05, "expired": 0.05}
        status = random.choices(list(status_weights.keys()), weights=list(status_weights.values()))[0]
        
        # Issue and expiry dates
        issue_date = fake.date_between(start_date="-10y", end_date="-1d")
        
        # Some documents expire, others don't
        expiring_docs = ["passport", "drivers_license", "national_id", "business_license"]
        if doc_type in expiring_docs:
            expiry_date = issue_date + timedelta(days=random.randint(1825, 3650))  # 5-10 years
        else:
            expiry_date = None
        
        # Document number generation
        document_number = self._generate_document_number(doc_type)
        
        # Issuing authority
        issuing_authority = self._get_issuing_authority(doc_type, customer_profile.get("nationality", "US"))
        
        return {
            "id": str(uuid4()),
            "document_type": doc_type,
            "document_number": document_number,
            "issuing_authority": issuing_authority,
            "issue_date": issue_date,
            "expiry_date": expiry_date,
            "document_status": status,
            "verification_method": random.choice(["manual", "automated", "third_party"]),
            "file_hash": hashlib.sha256(f"{doc_type}_{document_number}_{issue_date}".encode()).hexdigest(),
            "uploaded_at": fake.date_time_between(start_date=issue_date, end_date="now"),
            "verified_at": fake.date_time_between(start_date=issue_date, end_date="now") if status == "verified" else None,
            "verified_by": f"Officer_{random.randint(1, 10)}" if status == "verified" else None,
        }

    async def _generate_risk_assessment(self, customer_profile):
        """Generate risk assessment with risk factors"""
        
        risk_level = customer_profile["risk_level"]
        identified_factors = []
        
        # Risk factors based on customer risk level
        if risk_level == "high":
            num_factors = random.randint(2, 5)
            factor_pool = self.risk_factors
        elif risk_level == "medium":
            num_factors = random.randint(1, 3)
            factor_pool = self.risk_factors[:7]  # Exclude some high-risk factors
        else:
            num_factors = random.randint(0, 2)
            factor_pool = self.risk_factors[:5]  # Only lower-risk factors
        
        # Select random risk factors
        if num_factors > 0:
            identified_factors = random.sample(factor_pool, min(num_factors, len(factor_pool)))
        
        # Generate detailed risk factors
        detailed_factors = []
        for factor in identified_factors:
            detailed_factors.append({
                "factor_type": factor,
                "factor_description": self._get_risk_factor_description(factor),
                "risk_level": self._determine_factor_risk_level(factor),
                "identified_date": fake.date_time_between(start_date="-1y", end_date="now"),
                "status": random.choice(["active", "monitored", "mitigated"]),
                "mitigation_measures": self._generate_mitigation_measures(factor)
            })
        
        return {
            "overall_risk_rating": risk_level,
            "risk_score": self._calculate_risk_score(risk_level, detailed_factors),
            "risk_factors": detailed_factors,
            "last_assessment_date": fake.date_time_between(start_date="-6m", end_date="now"),
            "next_review_date": fake.date_time_between(start_date="now", end_date="+1y"),
            "assessment_notes": f"Customer assessed as {risk_level} risk based on {len(detailed_factors)} identified risk factors."
        }

    async def _generate_compliance_checks(self, customer_profile):
        """Generate compliance screening results"""
        
        checks = []
        
        # Standard checks for all customers
        standard_checks = [
            "identity_verification",
            "sanctions_screening", 
            "pep_screening",
            "adverse_media_screening",
            "address_verification"
        ]
        
        # Enhanced checks for higher risk customers
        enhanced_checks = [
            "source_of_wealth_verification",
            "beneficial_ownership_identification",
            "enhanced_due_diligence",
            "ongoing_monitoring_setup"
        ]
        
        # Add standard checks
        for check_type in standard_checks:
            checks.append(self._create_compliance_check(check_type, customer_profile))
        
        # Add enhanced checks for medium/high risk customers
        if customer_profile["risk_level"] in ["medium", "high"]:
            for check_type in enhanced_checks:
                if random.random() < 0.8:  # 80% chance of having enhanced check
                    checks.append(self._create_compliance_check(check_type, customer_profile))
        
        return checks

    def _create_compliance_check(self, check_type, customer_profile):
        """Create a compliance check record"""
        
        risk_level = customer_profile["risk_level"]
        
        # Pass rates based on risk level and check type
        if check_type in ["sanctions_screening", "pep_screening"] and risk_level == "high":
            pass_rate = 0.3  # High-risk customers more likely to have hits
        elif check_type in ["identity_verification", "address_verification"]:
            pass_rate = 0.95  # Most customers pass basic verification
        else:
            pass_rate = 0.85  # Default pass rate
        
        status = "pass" if random.random() < pass_rate else "flag"
        if status == "flag" and random.random() < 0.1:
            status = "fail"  # Small chance of outright failure
        
        return {
            "id": str(uuid4()),
            "check_type": check_type,
            "check_status": status,
            "check_result": self._generate_check_result(check_type, status),
            "checked_at": fake.date_time_between(start_date="-3m", end_date="now"),
            "checked_by": f"System_Auto_{random.randint(1, 5)}" if check_type.endswith("_screening") else f"Officer_{random.randint(1, 10)}",
            "details": self._generate_check_details(check_type, status, customer_profile)
        }

    # Helper methods for data generation
    def _generate_nationality(self):
        """Generate nationality with realistic distribution"""
        countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "CN", "IN", "BR", "MX", "ES", "IT", "NL"]
        weights = [0.3, 0.1, 0.08, 0.05, 0.05, 0.05, 0.03, 0.08, 0.08, 0.05, 0.03, 0.03, 0.03, 0.02]
        return random.choices(countries, weights=weights)[0]

    def _categorize_income(self, income):
        """Categorize income into ranges"""
        if income < 25000:
            return "Under $25,000"
        elif income < 50000:
            return "$25,000 - $50,000"
        elif income < 75000:
            return "$50,000 - $75,000"
        elif income < 100000:
            return "$75,000 - $100,000"
        elif income < 150000:
            return "$100,000 - $150,000"
        elif income < 250000:
            return "$150,000 - $250,000"
        elif income < 500000:
            return "$250,000 - $500,000"
        else:
            return "Over $500,000"

    def _generate_source_of_funds(self, complexity):
        """Generate source of funds based on complexity"""
        if complexity == "simple":
            return random.choice(["salary", "wages", "pension", "social_benefits"])
        elif complexity == "moderate":
            return random.choice(["salary", "business_income", "investments", "rental_income", "inheritance"])
        else:
            return random.choice([
                "business_ownership", "investments", "trust_distributions", 
                "international_business", "cryptocurrency", "real_estate_development",
                "private_equity", "hedge_fund", "offshore_investments"
            ])

    def _generate_source_of_wealth(self, complexity):
        """Generate source of wealth based on complexity"""
        if complexity == "simple":
            return random.choice(["employment", "family_savings", "inheritance"])
        elif complexity == "moderate":
            return random.choice(["business_sale", "investment_gains", "real_estate", "inheritance"])
        else:
            return random.choice([
                "business_empire", "generational_wealth", "international_investments",
                "private_company_sale", "IPO_proceeds", "trust_fund", "offshore_structures"
            ])

    def _calculate_expected_volume(self, annual_income):
        """Calculate expected transaction volume based on income"""
        monthly_income = annual_income / 12
        # Assume 1.5-3x monthly income in transaction volume
        multiplier = random.uniform(1.5, 3.0)
        return round(monthly_income * multiplier, 2)

    def _generate_account_purpose(self):
        """Generate account purpose"""
        purposes = [
            "personal_banking", "business_operations", "savings_investment",
            "international_trade", "real_estate_transactions", "trust_management",
            "corporate_treasury", "family_office_operations"
        ]
        weights = [0.4, 0.2, 0.15, 0.1, 0.05, 0.03, 0.04, 0.03]
        return random.choices(purposes, weights=weights)[0]

    def _generate_document_number(self, doc_type):
        """Generate realistic document numbers"""
        if doc_type == "passport":
            return f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))}{''.join(random.choices('0123456789', k=7))}"
        elif doc_type == "drivers_license":
            return f"DL{''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=8))}"
        elif doc_type == "national_id":
            return f"ID{''.join(random.choices('0123456789', k=9))}"
        else:
            return f"DOC{''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=10))}"

    def _get_issuing_authority(self, doc_type, country):
        """Get issuing authority based on document type and country"""
        authorities = {
            "passport": f"{country} Department of State",
            "drivers_license": f"{country} DMV",
            "national_id": f"{country} National Registry",
            "utility_bill": f"{random.choice(['Electric', 'Gas', 'Water', 'Internet'])} Company",
            "bank_statement": f"{fake.company()} Bank",
            "tax_return": f"{country} Tax Authority"
        }
        return authorities.get(doc_type, f"{country} Government Agency")

    def _get_risk_factor_description(self, factor):
        """Get description for risk factor"""
        descriptions = {
            "pep_exposure": "Customer or related party identified as Politically Exposed Person",
            "adverse_media": "Negative media coverage or public records found",
            "sanctions_screening": "Potential match found in sanctions screening",
            "high_risk_geography": "Customer has connections to high-risk jurisdictions",
            "complex_ownership": "Complex beneficial ownership structure identified",
            "cash_intensive_business": "Customer operates cash-intensive business",
            "correspondent_banking": "Customer uses correspondent banking relationships",
            "cryptocurrency_exposure": "Customer has significant cryptocurrency exposure",
            "offshore_structures": "Customer uses offshore corporate structures",
            "unusual_transaction_patterns": "Customer exhibits unusual transaction patterns"
        }
        return descriptions.get(factor, "Risk factor identified during assessment")

    def _determine_factor_risk_level(self, factor):
        """Determine risk level for specific factor"""
        high_risk_factors = ["sanctions_screening", "pep_exposure", "adverse_media"]
        if factor in high_risk_factors:
            return "high"
        elif factor in ["high_risk_geography", "complex_ownership", "offshore_structures"]:
            return "medium"
        else:
            return "low"

    def _generate_mitigation_measures(self, factor):
        """Generate mitigation measures for risk factor"""
        measures = {
            "pep_exposure": "Enhanced ongoing monitoring, senior management approval required",
            "adverse_media": "Quarterly adverse media rescreening, documented assessment",
            "sanctions_screening": "Regular sanctions list updates, false positive analysis",
            "high_risk_geography": "Enhanced transaction monitoring, source of funds verification",
            "complex_ownership": "Annual beneficial ownership updates, ownership charts maintained",
            "cash_intensive_business": "Cash transaction reporting, enhanced record keeping",
            "correspondent_banking": "Due diligence on correspondent banks, AML certifications",
            "cryptocurrency_exposure": "Cryptocurrency transaction monitoring, blockchain analysis",
            "offshore_structures": "Enhanced due diligence on offshore entities, tax compliance verification",
            "unusual_transaction_patterns": "Automated transaction monitoring, analyst review"
        }
        return measures.get(factor, "Standard risk mitigation procedures applied")

    def _calculate_risk_score(self, risk_level, risk_factors):
        """Calculate numerical risk score"""
        base_scores = {"low": 25, "medium": 50, "high": 75}
        base_score = base_scores[risk_level]
        
        # Add points for each risk factor
        factor_points = sum(10 if f["risk_level"] == "high" else 5 if f["risk_level"] == "medium" else 2 
                           for f in risk_factors)
        
        return min(100, base_score + factor_points)

    def _generate_check_result(self, check_type, status):
        """Generate check result description"""
        if status == "pass":
            return f"{check_type.replace('_', ' ').title()} completed successfully - no issues identified"
        elif status == "flag":
            return f"{check_type.replace('_', ' ').title()} flagged for manual review"
        else:
            return f"{check_type.replace('_', ' ').title()} failed - customer does not meet requirements"

    def _generate_check_details(self, check_type, status, customer_profile):
        """Generate detailed check information"""
        details = {
            "check_type": check_type,
            "status": status,
            "customer_risk_level": customer_profile["risk_level"],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if check_type == "sanctions_screening" and status == "flag":
            details["potential_matches"] = random.randint(1, 3)
            details["confidence_scores"] = [random.uniform(0.6, 0.9) for _ in range(details["potential_matches"])]
        
        if check_type == "pep_screening" and status == "flag":
            details["pep_category"] = random.choice(["head_of_state", "senior_official", "family_member", "close_associate"])
            details["confidence_score"] = random.uniform(0.7, 0.95)
        
        return json.dumps(details)

    # Database insertion methods
    async def _insert_customer(self, db, profile):
        """Insert customer into database"""
        customer_id = str(uuid4())
        
        # Prepare address for storage
        primary_address = profile["addresses"][0] if profile["addresses"] else {}
        
        await db.execute("""
            INSERT INTO customers (id, first_name, last_name, email, phone, date_of_birth,
                                 address, risk_level, kyc_status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            customer_id,
            profile.get("first_name", profile.get("business_name", "Unknown")),
            profile.get("last_name", ""),
            profile.get("email", f"customer_{customer_id}@kyc.test"),
            profile.get("phone", fake.phone_number()),
            profile.get("date_of_birth"),
            json.dumps(primary_address),
            profile["risk_level"],
            "pending",  # Will be updated after KYC completion
            datetime.utcnow(),
            datetime.utcnow()
        ))
        
        return customer_id

    async def _insert_kyc_record(self, db, customer_id, profile, documents, risk_assessment, compliance_checks):
        """Insert KYC record"""
        
        # Determine KYC status based on document and check statuses
        doc_statuses = [doc["document_status"] for doc in documents]
        check_statuses = [check["check_status"] for check in compliance_checks]
        
        if all(status == "verified" for status in doc_statuses) and all(status == "pass" for status in check_statuses):
            kyc_status = "approved"
        elif any(status == "rejected" for status in doc_statuses) or any(status == "fail" for status in check_statuses):
            kyc_status = "rejected"
        else:
            kyc_status = "pending"
        
        await db.execute("""
            INSERT INTO kyc_records (id, customer_id, kyc_status, verification_level, risk_rating,
                                   last_review_date, next_review_date, assigned_officer, notes,
                                   created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid4()),
            customer_id,
            kyc_status,
            profile["kyc_tier"],
            risk_assessment["overall_risk_rating"],
            risk_assessment["last_assessment_date"],
            risk_assessment["next_review_date"],
            f"KYC_Officer_{random.randint(1, 5)}",
            f"KYC assessment completed. Risk score: {risk_assessment['risk_score']}",
            datetime.utcnow(),
            datetime.utcnow()
        ))

    async def _insert_document(self, db, customer_id, document):
        """Insert document record"""
        await db.execute("""
            INSERT INTO kyc_documents (id, customer_id, document_type, document_number,
                                     issuing_authority, issue_date, expiry_date, document_status,
                                     verification_method, file_hash, uploaded_at, verified_at, verified_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            document["id"],
            customer_id,
            document["document_type"],
            document["document_number"],
            document["issuing_authority"],
            document["issue_date"],
            document["expiry_date"],
            document["document_status"],
            document["verification_method"],
            document["file_hash"],
            document["uploaded_at"],
            document["verified_at"],
            document["verified_by"]
        ))

    async def _insert_risk_factor(self, db, customer_id, risk_factor):
        """Insert risk factor record"""
        await db.execute("""
            INSERT INTO risk_factors (id, customer_id, factor_type, factor_description,
                                    risk_level, identified_date, status, mitigation_measures)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid4()),
            customer_id,
            risk_factor["factor_type"],
            risk_factor["factor_description"],
            risk_factor["risk_level"],
            risk_factor["identified_date"],
            risk_factor["status"],
            risk_factor["mitigation_measures"]
        ))

async def main():
    """Main function to generate KYC data"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate fake KYC data for AML testing")
    parser.add_argument("--count", type=int, default=50, help="Number of KYC profiles to generate")
    parser.add_argument("--clear", action="store_true", help="Clear existing KYC data before generating")
    
    args = parser.parse_args()
    
    # Ensure database exists
    db_path = Path(DATABASE_PATH)
    if not db_path.exists():
        logger.error("Database does not exist. Please run init_db.py first.")
        return
    
    # Clear existing KYC data if requested
    if args.clear:
        logger.info("Clearing existing KYC data")
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("DELETE FROM compliance_checks")
            await db.execute("DELETE FROM pep_screening")
            await db.execute("DELETE FROM sanctions_screening")
            await db.execute("DELETE FROM risk_factors")
            await db.execute("DELETE FROM kyc_documents")
            await db.execute("DELETE FROM kyc_records")
            await db.commit()
        logger.info("Existing KYC data cleared")
    
    # Generate KYC data
    generator = KYCDataGenerator()
    profiles_created = await generator.generate_kyc_profiles(args.count)
    
    print(f"Successfully generated {profiles_created} KYC profiles with comprehensive documentation")
    logger.info("KYC data generation completed successfully", profiles_created=profiles_created)

if __name__ == "__main__":
    asyncio.run(main())
