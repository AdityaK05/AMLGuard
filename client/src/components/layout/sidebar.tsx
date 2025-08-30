import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";

const navigationItems = [
  {
    name: "Dashboard",
    href: "/",
    icon: "fas fa-chart-line",
  },
  {
    name: "Alerts",
    href: "/alerts",
    icon: "fas fa-exclamation-triangle",
    badge: "23",
    badgeVariant: "destructive" as const,
  },
  {
    name: "Transactions",
    href: "/transactions",
    icon: "fas fa-exchange-alt",
  },
  {
    name: "Cases",
    href: "/cases",
    icon: "fas fa-briefcase",
    badge: "7",
    badgeVariant: "secondary" as const,
  },
  {
    name: "Rules Engine",
    href: "/rules",
    icon: "fas fa-cogs",
  },
  {
    name: "ML Models",
    href: "/models",
    icon: "fas fa-brain",
  },
  {
    name: "Analytics",
    href: "/analytics",
    icon: "fas fa-chart-bar",
  },
];

const bottomItems = [
  {
    name: "Users",
    href: "/users",
    icon: "fas fa-users",
  },
  {
    name: "Settings",
    href: "/settings",
    icon: "fas fa-cog",
  },
];

export default function Sidebar() {
  const [location] = useLocation();

  return (
    <aside className="w-64 bg-sidebar border-r border-sidebar-border shadow-sm">
      <div className="p-6 border-b border-sidebar-border">
        <div className="flex items-center space-x-3">
          <div className="w-8 h-8 bg-sidebar-primary rounded-lg flex items-center justify-center">
            <i className="fas fa-shield-alt text-sidebar-primary-foreground text-sm"></i>
          </div>
          <div>
            <h1 className="text-lg font-semibold text-sidebar-foreground">AMLGuard</h1>
            <p className="text-xs text-muted-foreground">Compliance Platform</p>
          </div>
        </div>
      </div>
      
      <nav className="p-4 space-y-1">
        {navigationItems.map((item) => (
          <Link
            key={item.href}
            href={item.href}
            className={cn(
              "sidebar-link flex items-center space-x-3 px-3 py-2 text-sm font-medium transition-all",
              location === item.href || (location === "/dashboard" && item.href === "/")
                ? "active"
                : "text-muted-foreground hover:text-foreground"
            )}
            data-testid={`nav-${item.name.toLowerCase().replace(" ", "-")}`}
          >
            <i className={`${item.icon} w-4`}></i>
            <span>{item.name}</span>
            {item.badge && (
              <Badge variant={item.badgeVariant} className="ml-auto text-xs">
                {item.badge}
              </Badge>
            )}
          </Link>
        ))}
        
        <div className="pt-4 border-t border-sidebar-border mt-4">
          {bottomItems.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "sidebar-link flex items-center space-x-3 px-3 py-2 text-sm font-medium transition-all",
                location === item.href
                  ? "active"
                  : "text-muted-foreground hover:text-foreground"
              )}
              data-testid={`nav-${item.name.toLowerCase()}`}
            >
              <i className={`${item.icon} w-4`}></i>
              <span>{item.name}</span>
            </Link>
          ))}
        </div>
      </nav>
    </aside>
  );
}
