import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useQuery } from "@tanstack/react-query";
import { Transaction } from "@shared/schema";

export default function TransactionsTable() {
  const { data: transactions, isLoading } = useQuery<Transaction[]>({
    queryKey: ["/api/transactions", "recent"],
  });

  if (isLoading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="space-y-4">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="animate-pulse h-12 bg-muted rounded"></div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  const getRiskScoreColor = (score: number) => {
    if (score >= 7) return "text-red-600";
    if (score >= 4) return "text-amber-600";
    return "text-green-600";
  };

  const getRiskScoreWidth = (score: number) => {
    return `${(score / 10) * 100}%`;
  };

  const getRiskScoreBarColor = (score: number) => {
    if (score >= 7) return "bg-red-500";
    if (score >= 4) return "bg-amber-500";
    return "bg-green-500";
  };

  const getStatusBadge = (status: string, riskScore: number) => {
    if (riskScore >= 7) return <Badge className="status-high">Flagged</Badge>;
    if (riskScore >= 4) return <Badge className="status-medium">Review</Badge>;
    return <Badge className="status-resolved">Clear</Badge>;
  };

  const formatTimeAgo = (date: string | Date) => {
    const now = new Date();
    const txnDate = new Date(date);
    const diffMinutes = Math.floor((now.getTime() - txnDate.getTime()) / (1000 * 60));
    
    if (diffMinutes < 1) return "Just now";
    if (diffMinutes < 60) return `${diffMinutes} min ago`;
    if (diffMinutes < 1440) return `${Math.floor(diffMinutes / 60)} hr ago`;
    return `${Math.floor(diffMinutes / 1440)} day ago`;
  };

  return (
    <Card>
      <CardHeader className="p-6 border-b border-border">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-foreground">Recent Transactions</h3>
          <div className="flex items-center space-x-3">
            <Select defaultValue="all">
              <SelectTrigger className="w-40" data-testid="select-risk-filter">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Risk Levels</SelectItem>
                <SelectItem value="high">High Risk Only</SelectItem>
                <SelectItem value="medium">Medium Risk Only</SelectItem>
              </SelectContent>
            </Select>
            <Button size="sm" data-testid="button-export-transactions">
              Export
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-muted/30">
              <tr>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">Transaction ID</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">Amount</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">Type</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">Risk Score</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">Status</th>
                <th className="text-left p-4 text-sm font-medium text-muted-foreground">Time</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {transactions?.slice(0, 4).map((transaction) => {
                const riskScore = parseFloat(transaction.riskScore || "0");
                return (
                  <tr 
                    key={transaction.id} 
                    className="table-row hover:bg-muted/50 cursor-pointer" 
                    data-testid={`transaction-${transaction.id}`}
                  >
                    <td className="p-4">
                      <span className="text-sm font-mono text-foreground" data-testid={`transaction-id-${transaction.id}`}>
                        {transaction.id.slice(0, 12)}...
                      </span>
                    </td>
                    <td className="p-4">
                      <span className="text-sm font-medium text-foreground" data-testid={`transaction-amount-${transaction.id}`}>
                        ${parseFloat(transaction.amount).toLocaleString('en-US', { minimumFractionDigits: 2 })}
                      </span>
                    </td>
                    <td className="p-4">
                      <span className="text-sm text-muted-foreground" data-testid={`transaction-type-${transaction.id}`}>
                        {transaction.transactionType}
                      </span>
                    </td>
                    <td className="p-4">
                      <div className="flex items-center space-x-2">
                        <span className={`text-sm font-medium ${getRiskScoreColor(riskScore)}`} data-testid={`transaction-risk-score-${transaction.id}`}>
                          {riskScore.toFixed(1)}
                        </span>
                        <div className="w-16 bg-muted rounded-full h-2">
                          <div 
                            className={`h-2 rounded-full ${getRiskScoreBarColor(riskScore)}`}
                            style={{ width: getRiskScoreWidth(riskScore) }}
                          ></div>
                        </div>
                      </div>
                    </td>
                    <td className="p-4">
                      {getStatusBadge(transaction.status, riskScore)}
                    </td>
                    <td className="p-4">
                      <span className="text-sm text-muted-foreground" data-testid={`transaction-time-${transaction.id}`}>
                        {formatTimeAgo(transaction.createdAt!)}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="p-4 border-t border-border">
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground" data-testid="text-transactions-count">
              Showing {Math.min(transactions?.length || 0, 4)} of {transactions?.length || 0} transactions
            </p>
            <div className="flex items-center space-x-2">
              <Button variant="outline" size="sm" data-testid="button-previous-page">
                Previous
              </Button>
              <Button size="sm" data-testid="button-page-1">
                1
              </Button>
              <Button variant="outline" size="sm" data-testid="button-page-2">
                2
              </Button>
              <Button variant="outline" size="sm" data-testid="button-next-page">
                Next
              </Button>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
