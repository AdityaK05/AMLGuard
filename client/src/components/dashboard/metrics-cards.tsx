import { Card, CardContent } from "@/components/ui/card";
import { useQuery } from "@tanstack/react-query";

interface Metrics {
  activeAlerts: number;
  dailyTransactions: number;
  avgRiskScore: number;
  openCases: number;
  alertsChange: string;
  transactionsChange: string;
  riskScoreChange: string;
  urgentCases: number;
}

export default function MetricsCards() {
  const { data: metrics, isLoading } = useQuery<Metrics>({
    queryKey: ["/api/metrics/dashboard"],
  });

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {[...Array(4)].map((_, i) => (
          <Card key={i} className="animate-pulse">
            <CardContent className="p-6">
              <div className="h-20 bg-muted rounded"></div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  const metricCards = [
    {
      title: "Active Alerts",
      value: metrics?.activeAlerts || 0,
      change: metrics?.alertsChange || "+0%",
      changeType: "increase",
      icon: "fas fa-exclamation-triangle",
      iconBg: "bg-red-100",
      iconColor: "text-red-600",
      testId: "metric-active-alerts",
    },
    {
      title: "Daily Transactions",
      value: (metrics?.dailyTransactions || 0).toLocaleString(),
      change: metrics?.transactionsChange || "+0%",
      changeType: "increase",
      icon: "fas fa-exchange-alt",
      iconBg: "bg-blue-100",
      iconColor: "text-blue-600",
      testId: "metric-daily-transactions",
    },
    {
      title: "Risk Score Avg",
      value: metrics?.avgRiskScore?.toFixed(1) || "0.0",
      change: metrics?.riskScoreChange || "0.0",
      changeType: "decrease",
      icon: "fas fa-chart-line",
      iconBg: "bg-green-100",
      iconColor: "text-green-600",
      testId: "metric-risk-score",
    },
    {
      title: "Open Cases",
      value: metrics?.openCases || 0,
      change: `${metrics?.urgentCases || 0} urgent`,
      changeType: "neutral",
      icon: "fas fa-briefcase",
      iconBg: "bg-amber-100",
      iconColor: "text-amber-600",
      testId: "metric-open-cases",
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      {metricCards.map((card) => (
        <Card key={card.title} className="metric-card" data-testid={card.testId}>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-muted-foreground text-sm font-medium">{card.title}</p>
                <p className="text-2xl font-bold text-foreground" data-testid={`${card.testId}-value`}>
                  {card.value}
                </p>
                <p className={`text-xs mt-1 ${
                  card.changeType === "increase" ? "text-green-600" : 
                  card.changeType === "decrease" ? "text-green-600" : 
                  "text-amber-600"
                }`} data-testid={`${card.testId}-change`}>
                  {card.change}
                </p>
              </div>
              <div className={`w-12 h-12 ${card.iconBg} rounded-lg flex items-center justify-center`}>
                <i className={`${card.icon} ${card.iconColor} text-lg`}></i>
              </div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
