#!/usr/bin/env python3
"""
AMLGuard Platform - Main Entry Point
Orchestrates all services: API, ML, Stream Processing
"""

import asyncio
import subprocess
import sys
import signal
import os
from pathlib import Path

class ServiceManager:
    def __init__(self):
        self.processes = []
        self.running = True

    async def start_service(self, cmd, name, cwd=None):
        """Start a service process"""
        print(f"Starting {name}...")
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd
            )
            self.processes.append((process, name))
            
            # Monitor output
            asyncio.create_task(self.monitor_output(process, name))
            return process
        except Exception as e:
            print(f"Failed to start {name}: {e}")
            return None

    async def monitor_output(self, process, name):
        """Monitor service output"""
        while True:
            try:
                line = await process.stdout.readline()
                if not line:
                    break
                print(f"[{name}] {line.decode().strip()}")
            except Exception:
                break

    async def start_all_services(self):
        """Start all AMLGuard services"""
        print("🚀 Starting AMLGuard Platform...")
        
        # Initialize database first
        print("📊 Initializing database...")
        init_result = subprocess.run([
            sys.executable, "scripts/init_db.py"
        ], capture_output=True, text=True)
        
        if init_result.returncode != 0:
            print(f"Database initialization failed: {init_result.stderr}")
            return
        
        print("✅ Database initialized")

        # Start API service
        await self.start_service([
            sys.executable, "-m", "uvicorn", 
            "services.api.main:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--reload"
        ], "API", cwd=".")

        # Start ML service  
        await self.start_service([
            sys.executable, "-m", "uvicorn",
            "services.ml.main:app", 
            "--host", "0.0.0.0",
            "--port", "8001",
            "--reload"
        ], "ML", cwd=".")

        # Start Stream Processing
        await self.start_service([
            sys.executable, "services/stream/main.py"
        ], "Stream", cwd=".")

        print("🎯 All services started successfully!")
        print("📊 Dashboard: http://localhost:5000")
        print("🔧 API Docs: http://localhost:8000/docs")
        print("🧠 ML API: http://localhost:8001/docs")

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\n🛑 Shutting down AMLGuard Platform...")
        self.running = False
        
        for process, name in self.processes:
            print(f"Stopping {name}...")
            try:
                process.terminate()
            except Exception:
                pass

    async def run(self):
        """Main run loop"""
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        await self.start_all_services()
        
        # Keep running until shutdown
        try:
            while self.running:
                await asyncio.sleep(1)
                
                # Check if any process died
                for process, name in self.processes:
                    if process.returncode is not None:
                        print(f"⚠️  Service {name} stopped unexpectedly")
                        
        except KeyboardInterrupt:
            pass
        finally:
            print("👋 AMLGuard Platform shutdown complete")

if __name__ == "__main__":
    print("🛡️  AMLGuard Anti-Money Laundering Platform")
    print("=" * 50)
    
    # Create necessary directories
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    manager = ServiceManager()
    asyncio.run(manager.run())
