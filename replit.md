# AMLGuard Platform

## Overview

AMLGuard is a comprehensive Anti-Money Laundering (AML) monitoring platform designed for financial institutions to detect, analyze, and manage suspicious transactions in real-time. The platform combines machine learning models, rules-based detection, and stream processing to provide comprehensive compliance monitoring capabilities.

The system is built as a full-stack application with a React frontend, Express.js backend, and multiple microservices for specialized functions including ML-based risk scoring, rules engine processing, and real-time transaction stream analysis. The platform supports user authentication, transaction monitoring, alert management, case investigation, and regulatory compliance workflows.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: React with TypeScript using Vite as the build tool
- **UI Library**: Shadcn/ui components built on Radix UI primitives with Tailwind CSS for styling
- **State Management**: TanStack Query for server state and data fetching
- **Routing**: Wouter for client-side routing
- **Authentication**: JWT-based authentication with token storage in localStorage

### Backend Architecture
- **Primary API**: Express.js server with TypeScript providing REST endpoints
- **Service Architecture**: Microservices pattern with separate services for:
  - API service (FastAPI) for core business logic
  - ML service for risk scoring and anomaly detection
  - Stream processing service for real-time transaction analysis
  - Rules engine for configurable detection patterns
- **Authentication**: JWT token verification middleware with role-based access control

### Data Storage Solutions
- **Primary Database**: PostgreSQL with Drizzle ORM for schema management and migrations
- **Development Database**: SQLite for local development and testing
- **Session Storage**: PostgreSQL with connect-pg-simple for session management
- **Data Models**: Comprehensive schema covering customers, accounts, transactions, alerts, cases, users, and audit logs

### Authentication and Authorization
- **Authentication Method**: JWT tokens with configurable expiration
- **Password Security**: Bcrypt hashing for password storage
- **Authorization**: Role-based permissions system supporting multiple user roles (analyst, admin, etc.)
- **Session Management**: Server-side session storage with automatic cleanup

### Machine Learning Components
- **Ensemble Model**: Combination of XGBoost classifier and Isolation Forest for anomaly detection
- **Feature Engineering**: Automated feature extraction from transaction data including temporal, geographic, and behavioral patterns
- **Model Management**: Versioned model registry with performance tracking and automated retraining capabilities
- **Risk Scoring**: Real-time transaction risk assessment with configurable thresholds

### Stream Processing Architecture
- **Message Processing**: Asyncio-based queue system for development (designed to integrate with Kafka for production)
- **Real-time Analysis**: Event-driven transaction processing with immediate risk assessment
- **Data Pipeline**: Orchestrated flow from transaction ingestion through ML analysis to alert generation

### Rules Engine
- **Configuration**: YAML-based rule definitions for flexible pattern detection
- **Rule Types**: Support for various suspicious activity patterns including structuring, unusual geographic activity, and velocity checks
- **Dynamic Updates**: Hot-reloading of rule configurations without system restart
- **Performance Tracking**: Detailed metrics on rule effectiveness and trigger rates

## External Dependencies

### Database Services
- **Neon Database**: PostgreSQL hosting service for production data storage
- **Drizzle Kit**: Database migration and schema management tool

### Frontend Libraries
- **Radix UI**: Comprehensive component library for accessible UI primitives
- **TanStack Query**: Server state management and data synchronization
- **React Hook Form**: Form state management with validation
- **Zod**: Runtime type validation and schema definition
- **Date-fns**: Date manipulation and formatting utilities

### Backend Dependencies
- **Express.js**: Web application framework for the main API server
- **FastAPI**: High-performance API framework for microservices
- **Bcrypt**: Password hashing and security
- **JWT**: Token-based authentication implementation

### Machine Learning Stack
- **Scikit-learn**: Core machine learning algorithms and preprocessing
- **XGBoost**: Gradient boosting framework for classification
- **NumPy/Pandas**: Data manipulation and numerical computing
- **Joblib**: Model serialization and persistence

### Development Tools
- **Vite**: Fast build tool and development server
- **TypeScript**: Static type checking and enhanced developer experience
- **Tailwind CSS**: Utility-first CSS framework
- **ESBuild**: Fast JavaScript/TypeScript bundler for production builds

### Monitoring and Logging
- **Structlog**: Structured logging for Python services with JSON output
- **Performance Metrics**: Built-in metrics collection for system monitoring