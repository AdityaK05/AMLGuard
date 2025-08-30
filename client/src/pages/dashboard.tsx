import Sidebar from "@/components/layout/sidebar";
import Header from "@/components/layout/header";
import MetricsCards from "@/components/dashboard/metrics-cards";
import AlertsList from "@/components/dashboard/alerts-list";
import SystemStatus from "@/components/dashboard/system-status";
import TransactionsTable from "@/components/dashboard/transactions-table";

export default function Dashboard() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <Header 
          title="AML Monitoring Dashboard" 
          subtitle="Real-time compliance monitoring and risk assessment" 
        />
        
        <div className="p-6 space-y-6">
          <MetricsCards />
          
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <AlertsList />
            <SystemStatus />
          </div>
          
          <TransactionsTable />
        </div>
      </main>
    </div>
  );
}
