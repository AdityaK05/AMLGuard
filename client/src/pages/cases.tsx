import Sidebar from "@/components/layout/sidebar";
import Header from "@/components/layout/header";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function Cases() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <Header 
          title="Case Management" 
          subtitle="Manage compliance investigation cases" 
        />
        
        <div className="p-6">
          <Card>
            <CardHeader>
              <h3 className="text-lg font-semibold">Cases Dashboard</h3>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground">
                Case management functionality will be implemented here.
              </p>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
}
