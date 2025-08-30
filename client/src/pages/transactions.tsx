import Sidebar from "@/components/layout/sidebar";
import Header from "@/components/layout/header";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function Transactions() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <Header 
          title="Transaction Monitoring" 
          subtitle="Monitor and analyze transaction patterns" 
        />
        
        <div className="p-6">
          <Card>
            <CardHeader>
              <h3 className="text-lg font-semibold">Transaction Dashboard</h3>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground">
                Transaction monitoring functionality will be implemented here.
              </p>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
}
