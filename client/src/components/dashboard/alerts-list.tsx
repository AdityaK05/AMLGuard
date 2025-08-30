import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useQuery } from "@tanstack/react-query";
import { Alert } from "@shared/schema";

export default function AlertsList() {
  const { data: alerts, isLoading } = useQuery<Alert[]>({
    queryKey: ["/api/alerts", "recent"],
  });

  if (isLoading) {
    return (
      <Card className="lg:col-span-2">
        <CardContent className="p-6">
          <div className="space-y-4">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="animate-pulse">
                <div className="h-16 bg-muted rounded"></div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  const getSeverityBadge = (severity: string) => {
    switch (severity.toLowerCase()) {
      case "critical":
        return <Badge className="status-high">Critical</Badge>;
      case "high":
        return <Badge className="status-high">High</Badge>;
      case "medium":
        return <Badge className="status-medium">Medium</Badge>;
      case "low":
        return <Badge className="status-low">Low</Badge>;
      default:
        return <Badge variant="secondary">{severity}</Badge>;
    }
  };

  const formatTimeAgo = (date: string | Date) => {
    const now = new Date();
    const alertDate = new Date(date);
    const diffMinutes = Math.floor((now.getTime() - alertDate.getTime()) / (1000 * 60));
    
    if (diffMinutes < 1) return "Just now";
    if (diffMinutes < 60) return `${diffMinutes} min ago`;
    if (diffMinutes < 1440) return `${Math.floor(diffMinutes / 60)} hr ago`;
    return `${Math.floor(diffMinutes / 1440)} day ago`;
  };

  return (
    <Card className="lg:col-span-2">
      <CardHeader className="p-6 border-b border-border">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-foreground">High-Risk Alerts</h3>
          <Button variant="ghost" size="sm" data-testid="button-view-all-alerts">
            View All
          </Button>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="divide-y divide-border">
          {alerts?.slice(0, 3).map((alert) => (
            <div 
              key={alert.id} 
              className="table-row p-4 hover:bg-muted/50 cursor-pointer" 
              data-testid={`alert-${alert.id}`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className={`w-3 h-3 rounded-full ${
                    alert.severity === "critical" ? "bg-red-500" : 
                    alert.severity === "high" ? "bg-red-500" : 
                    alert.severity === "medium" ? "bg-amber-500" : 
                    "bg-blue-500"
                  }`}></div>
                  <div>
                    <p className="font-medium text-foreground" data-testid={`alert-title-${alert.id}`}>
                      {alert.title}
                    </p>
                    <p className="text-sm text-muted-foreground" data-testid={`alert-customer-${alert.id}`}>
                      Alert ID: {alert.id.slice(0, 8)}...
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  {getSeverityBadge(alert.severity)}
                  <p className="text-xs text-muted-foreground mt-1" data-testid={`alert-time-${alert.id}`}>
                    {formatTimeAgo(alert.createdAt!)}
                  </p>
                </div>
              </div>
              <div className="mt-2 ml-6">
                <p className="text-sm text-muted-foreground" data-testid={`alert-description-${alert.id}`}>
                  {alert.description}
                </p>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
