import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import Dashboard from "@/pages/dashboard";
import Alerts from "@/pages/alerts";
import Transactions from "@/pages/transactions";
import Cases from "@/pages/cases";
import Rules from "@/pages/rules";
import Models from "@/pages/models";
import Login from "@/pages/login";
import NotFound from "@/pages/not-found";
import { useAuth } from "@/lib/auth";

function Router() {
  const { user, isLoading } = useAuth();

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (!user) {
    return <Login />;
  }

  return (
    <Switch>
      <Route path="/" component={Dashboard} />
      <Route path="/dashboard" component={Dashboard} />
      <Route path="/alerts" component={Alerts} />
      <Route path="/transactions" component={Transactions} />
      <Route path="/cases" component={Cases} />
      <Route path="/rules" component={Rules} />
      <Route path="/models" component={Models} />
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
