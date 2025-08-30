import Sidebar from "@/components/layout/sidebar";
import Header from "@/components/layout/header";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function Rules() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <Header 
          title="Rules Engine" 
          subtitle="Configure and manage AML detection rules" 
        />
        
        <div className="p-6">
          <Card>
            <CardHeader>
              <h3 className="text-lg font-semibold">Rules Configuration</h3>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground">
                Rules engine configuration will be implemented here.
              </p>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
}
