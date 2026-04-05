import { NavLink } from 'react-router-dom'
import type { ReactNode } from 'react'
import { LayoutDashboard, FolderOpen, ScrollText, Activity } from 'lucide-react'
import { Separator } from '@/components/ui/separator'
import type { LucideIcon } from 'lucide-react'

const NAV_ITEMS: { to: string; label: string; icon: LucideIcon }[] = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/resources', label: 'Resources', icon: FolderOpen },
  { to: '/logs', label: 'Logs', icon: ScrollText },
  { to: '/requests', label: 'Requests', icon: Activity },
]

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-screen bg-background text-foreground">
      {/* Sidebar */}
      <nav aria-label="Main navigation" className="w-56 bg-card border-r flex flex-col">
        <div className="px-4 py-4 flex items-center gap-3">
          <img src="/_ministack/ui/logo.png" alt="MiniStack" className="h-9 w-auto" />
          <div>
            <h1 className="text-lg font-bold tracking-tight leading-tight">MiniStack</h1>
            <p className="text-xs text-muted-foreground">Local AWS Emulator</p>
          </div>
        </div>
        <Separator />
        <ul className="flex-1 py-2">
          {NAV_ITEMS.map((item) => (
            <li key={item.to}>
              <NavLink
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-4 py-2.5 text-sm transition-colors ${
                    isActive
                      ? 'bg-primary/10 text-primary border-r-2 border-primary font-medium'
                      : 'text-muted-foreground hover:text-foreground hover:bg-accent/50'
                  }`
                }
              >
                <item.icon className="h-4 w-4" />
                {item.label}
              </NavLink>
            </li>
          ))}
        </ul>
        <Separator />
        <div className="px-4 py-3 text-xs text-muted-foreground">
          Port 4566
        </div>
      </nav>

      {/* Main content */}
      <main className="flex-1 overflow-auto">
        {children}
      </main>
    </div>
  )
}
