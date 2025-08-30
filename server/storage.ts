import { 
  type User, 
  type InsertUser, 
  type Transaction, 
  type Alert, 
  type Customer, 
  type Account 
} from "@shared/schema";
import { randomUUID } from "crypto";
import bcrypt from "bcrypt";

export interface IStorage {
  // User methods
  getUser(id: string): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  createUser(user: InsertUser): Promise<User>;
  updateUserLastLogin(id: string): Promise<void>;
  
  // Dashboard methods
  getDashboardMetrics(): Promise<any>;
  getSystemStatus(): Promise<any>;
  
  // Alert methods
  getRecentAlerts(): Promise<Alert[]>;
  
  // Transaction methods
  getRecentTransactions(): Promise<Transaction[]>;
  createTransaction(transaction: any): Promise<Transaction>;
}

export class MemStorage implements IStorage {
  private users: Map<string, User>;
  private customers: Map<string, Customer>;
  private accounts: Map<string, Account>;
  private transactions: Map<string, Transaction>;
  private alerts: Map<string, Alert>;

  constructor() {
    this.users = new Map();
    this.customers = new Map();
    this.accounts = new Map();
    this.transactions = new Map();
    this.alerts = new Map();
    this.initializeData();
  }

  private async initializeData() {
    // Create default admin user
    const hashedPassword = await bcrypt.hash("admin123", 10);
    const adminUser: User = {
      id: randomUUID(),
      username: "admin",
      email: "admin@amlguard.com",
      password: hashedPassword,
      firstName: "Sarah",
      lastName: "Chen",
      role: "admin",
      permissions: ["read", "write", "admin"],
      lastLogin: null,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.users.set(adminUser.id, adminUser);

    // Create sample customer
    const customer: Customer = {
      id: randomUUID(),
      firstName: "Marcus",
      lastName: "Johnson",
      email: "marcus.johnson@email.com",
      phone: "+1-555-0123",
      dateOfBirth: new Date("1985-03-15"),
      address: {
        street: "123 Main St",
        city: "New York",
        state: "NY",
        zipCode: "10001",
        country: "US"
      },
      riskLevel: "medium",
      kycStatus: "approved",
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.customers.set(customer.id, customer);

    // Create sample account
    const account: Account = {
      id: randomUUID(),
      customerId: customer.id,
      accountNumber: "****4521",
      accountType: "checking",
      balance: "25000.00",
      currency: "USD",
      status: "active",
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.accounts.set(account.id, account);

    // Create sample transactions
    const transactions = [
      {
        id: randomUUID(),
        fromAccountId: account.id,
        toAccountId: null,
        amount: "9850.00",
        currency: "USD",
        transactionType: "Wire Transfer",
        description: "International wire transfer",
        location: { country: "US", city: "New York" },
        riskScore: "8.7",
        mlPrediction: { score: 8.7, features: {}, model_version: "v1.0" },
        rulesHit: ["structuring"],
        status: "flagged",
        processedAt: new Date(),
        createdAt: new Date(Date.now() - 2 * 60 * 1000), // 2 minutes ago
      },
      {
        id: randomUUID(),
        fromAccountId: account.id,
        toAccountId: null,
        amount: "2450.00",
        currency: "USD",
        transactionType: "ATM Withdrawal",
        description: "ATM withdrawal",
        location: { country: "UK", city: "London" },
        riskScore: "9.2",
        mlPrediction: { score: 9.2, features: {}, model_version: "v1.0" },
        rulesHit: ["geographic"],
        status: "flagged",
        processedAt: new Date(),
        createdAt: new Date(Date.now() - 5 * 60 * 1000), // 5 minutes ago
      },
      {
        id: randomUUID(),
        fromAccountId: account.id,
        toAccountId: null,
        amount: "567.89",
        currency: "USD",
        transactionType: "Online Transfer",
        description: "Online transfer",
        location: { country: "US", city: "Los Angeles" },
        riskScore: "5.4",
        mlPrediction: { score: 5.4, features: {}, model_version: "v1.0" },
        rulesHit: [],
        status: "review",
        processedAt: new Date(),
        createdAt: new Date(Date.now() - 12 * 60 * 1000), // 12 minutes ago
      },
      {
        id: randomUUID(),
        fromAccountId: account.id,
        toAccountId: null,
        amount: "125.00",
        currency: "USD",
        transactionType: "Card Payment",
        description: "Merchant payment",
        location: { country: "US", city: "New York" },
        riskScore: "1.2",
        mlPrediction: { score: 1.2, features: {}, model_version: "v1.0" },
        rulesHit: [],
        status: "clear",
        processedAt: new Date(),
        createdAt: new Date(Date.now() - 15 * 60 * 1000), // 15 minutes ago
      },
    ];

    transactions.forEach(txn => {
      this.transactions.set(txn.id, txn as Transaction);
    });

    // Create sample alerts
    const alerts = [
      {
        id: randomUUID(),
        transactionId: transactions[0].id,
        customerId: customer.id,
        alertType: "structuring",
        severity: "critical",
        title: "Structuring Pattern Detected",
        description: "Multiple transactions just below $10,000 threshold within 24 hours",
        riskScore: "8.7",
        assignedTo: null,
        status: "open",
        resolvedAt: null,
        createdAt: new Date(Date.now() - 2 * 60 * 1000),
        updatedAt: new Date(Date.now() - 2 * 60 * 1000),
      },
      {
        id: randomUUID(),
        transactionId: transactions[1].id,
        customerId: customer.id,
        alertType: "geographic",
        severity: "critical",
        title: "Unusual Geographic Activity",
        description: "Transactions from 3 different countries within 1 hour",
        riskScore: "9.2",
        assignedTo: null,
        status: "open",
        resolvedAt: null,
        createdAt: new Date(Date.now() - 5 * 60 * 1000),
        updatedAt: new Date(Date.now() - 5 * 60 * 1000),
      },
      {
        id: randomUUID(),
        transactionId: transactions[2].id,
        customerId: customer.id,
        alertType: "velocity",
        severity: "medium",
        title: "High-Velocity Transfers",
        description: "15 outbound transfers in rapid succession",
        riskScore: "5.4",
        assignedTo: null,
        status: "open",
        resolvedAt: null,
        createdAt: new Date(Date.now() - 12 * 60 * 1000),
        updatedAt: new Date(Date.now() - 12 * 60 * 1000),
      },
    ];

    alerts.forEach(alert => {
      this.alerts.set(alert.id, alert as Alert);
    });
  }

  async getUser(id: string): Promise<User | undefined> {
    return this.users.get(id);
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(
      (user) => user.username === username,
    );
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const id = randomUUID();
    const hashedPassword = await bcrypt.hash(insertUser.password, 10);
    const user: User = { 
      ...insertUser, 
      id, 
      password: hashedPassword,
      role: insertUser.role || "analyst",
      permissions: insertUser.permissions || null,
      lastLogin: null,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    this.users.set(id, user);
    return user;
  }

  async updateUserLastLogin(id: string): Promise<void> {
    const user = this.users.get(id);
    if (user) {
      user.lastLogin = new Date();
      user.updatedAt = new Date();
      this.users.set(id, user);
    }
  }

  async getDashboardMetrics(): Promise<any> {
    const alerts = Array.from(this.alerts.values());
    const transactions = Array.from(this.transactions.values());
    
    const activeAlerts = alerts.filter(a => a.status === "open").length;
    const dailyTransactions = transactions.length;
    const avgRiskScore = transactions.reduce((sum, t) => sum + parseFloat(t.riskScore || "0"), 0) / transactions.length;
    const openCases = 7; // Mock data
    const urgentCases = 2; // Mock data

    return {
      activeAlerts,
      dailyTransactions,
      avgRiskScore,
      openCases,
      alertsChange: "+12% from yesterday",
      transactionsChange: "+5.2% from yesterday", 
      riskScoreChange: "-0.1 from yesterday",
      urgentCases,
    };
  }

  async getSystemStatus(): Promise<any> {
    return {
      mlEngine: "operational",
      rulesEngine: "operational",
      streamProcessing: "operational",
      dataPipeline: "degraded",
      modelPerformance: {
        accuracy: 0.942,
        precision: 0.897,
      },
    };
  }

  async getRecentAlerts(): Promise<Alert[]> {
    return Array.from(this.alerts.values())
      .sort((a, b) => new Date(b.createdAt!).getTime() - new Date(a.createdAt!).getTime())
      .slice(0, 10);
  }

  async getRecentTransactions(): Promise<Transaction[]> {
    return Array.from(this.transactions.values())
      .sort((a, b) => new Date(b.createdAt!).getTime() - new Date(a.createdAt!).getTime())
      .slice(0, 10);
  }

  async createTransaction(transactionData: any): Promise<Transaction> {
    const id = randomUUID();
    const transaction: Transaction = {
      ...transactionData,
      id,
      createdAt: new Date(),
    };
    this.transactions.set(id, transaction);
    return transaction;
  }
}

export const storage = new MemStorage();
