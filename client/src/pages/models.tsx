import Sidebar from "@/components/layout/sidebar";
import Header from "@/components/layout/header";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function Models() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <Header 
          title="ML Models" 
          subtitle="Monitor and manage machine learning models" 
        />
        
        <div className="p-6">
          <Card>
            <CardHeader>
              <h3 className="text-lg font-semibold">Model Registry</h3>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground">
                ML model management functionality will be implemented here.
              </p>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
}
