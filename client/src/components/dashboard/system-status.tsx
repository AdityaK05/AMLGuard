import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { useQuery } from "@tanstack/react-query";

interface SystemStatus {
  mlEngine: "operational" | "degraded" | "down";
  rulesEngine: "operational" | "degraded" | "down";
  streamProcessing: "operational" | "degraded" | "down";
  dataPipeline: "operational" | "degraded" | "down";
  modelPerformance: {
    accuracy: number;
    precision: number;
  };
}

export default function SystemStatus() {
  const { data: status, isLoading } = useQuery<SystemStatus>({
    queryKey: ["/api/system/status"],
  });

  if (isLoading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="animate-pulse space-y-4">
            <div className="h-4 bg-muted rounded w-3/4"></div>
            <div className="h-4 bg-muted rounded w-1/2"></div>
            <div className="h-4 bg-muted rounded w-5/6"></div>
          </div>
        </CardContent>
      </Card>
    );
  }

  const getStatusIndicator = (status: string) => {
    switch (status) {
      case "operational":
        return <div className="w-2 h-2 bg-green-500 rounded-full"></div>;
      case "degraded":
        return <div className="w-2 h-2 bg-amber-500 rounded-full"></div>;
      case "down":
        return <div className="w-2 h-2 bg-red-500 rounded-full"></div>;
      default:
        return <div className="w-2 h-2 bg-gray-500 rounded-full"></div>;
    }
  };

  const getStatusText = (status: string) => {
    return status.charAt(0).toUpperCase() + status.slice(1);
  };

  const services = [
    { name: "ML Engine", status: status?.mlEngine || "operational" },
    { name: "Rules Engine", status: status?.rulesEngine || "operational" },
    { name: "Stream Processing", status: status?.streamProcessing || "operational" },
    { name: "Data Pipeline", status: status?.dataPipeline || "degraded" },
  ];

  return (
    <Card>
      <CardHeader className="p-6 border-b border-border">
        <h3 className="text-lg font-semibold text-foreground">System Status</h3>
      </CardHeader>
      <CardContent className="p-6 space-y-4">
        {services.map((service) => (
          <div key={service.name} className="flex items-center justify-between" data-testid={`status-${service.name.toLowerCase().replace(" ", "-")}`}>
            <div className="flex items-center space-x-3">
              {getStatusIndicator(service.status)}
              <span className="text-sm font-medium text-foreground">{service.name}</span>
            </div>
            <span className="text-xs text-muted-foreground" data-testid={`status-text-${service.name.toLowerCase().replace(" ", "-")}`}>
              {getStatusText(service.status)}
            </span>
          </div>
        ))}
        
        <div className="pt-4 border-t border-border">
          <div className="text-sm font-medium text-foreground mb-2">Model Performance</div>
          <div className="space-y-2">
            <div className="flex justify-between text-xs">
              <span className="text-muted-foreground">Accuracy</span>
              <span className="text-foreground font-medium" data-testid="metric-accuracy">
                {((status?.modelPerformance?.accuracy || 0.942) * 100).toFixed(1)}%
              </span>
            </div>
            <div className="w-full bg-muted rounded-full h-2">
              <div 
                className="bg-green-500 h-2 rounded-full" 
                style={{ width: `${((status?.modelPerformance?.accuracy || 0.942) * 100)}%` }}
              ></div>
            </div>
            
            <div className="flex justify-between text-xs">
              <span className="text-muted-foreground">Precision</span>
              <span className="text-foreground font-medium" data-testid="metric-precision">
                {((status?.modelPerformance?.precision || 0.897) * 100).toFixed(1)}%
              </span>
            </div>
            <div className="w-full bg-muted rounded-full h-2">
              <div 
                className="bg-blue-500 h-2 rounded-full" 
                style={{ width: `${((status?.modelPerformance?.precision || 0.897) * 100)}%` }}
              ></div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
