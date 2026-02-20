import { NavLink } from 'react-router-dom'
import { LayoutDashboard, Table2, Upload, PackageCheck, Users, Shield, Eye, ScrollText, ChevronLeft, ChevronRight, Box } from 'lucide-react'
import useAuthStore from '@/store/authStore'
import clsx from 'clsx'

const navItems = [
  { label: 'Dashboard', path: '/', icon: LayoutDashboard, end: true },
  { label: 'Tables', path: '/tables', icon: Table2 },
  { label: 'Upload', path: '/upload', icon: Upload },
  { label: 'Allocations', path: '/allocations', icon: PackageCheck },
]

const adminItems = [
  { label: 'Users', path: '/admin/users', icon: Users, permission: 'ADMIN_USERS_READ' },
  { label: 'Roles', path: '/admin/roles', icon: Shield, permission: 'ADMIN_ROLES_MANAGE' },
  { label: 'Row-Level Security', path: '/admin/rls', icon: Eye, permission: 'ADMIN_RLS_MANAGE' },
  { label: 'Audit Log', path: '/admin/audit', icon: ScrollText, permission: 'ADMIN_AUDIT_READ' },
]

function SideLink({ item, collapsed }) {
  return (
    <NavLink
      to={item.path}
      end={item.end}
      className={({ isActive }) => clsx(
        'flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150',
        isActive
          ? 'bg-primary-600 text-white shadow-md shadow-primary-600/25'
          : 'text-sidebar-text hover:bg-sidebar-hover hover:text-white'
      )}
    >
      <item.icon size={20} />
      {!collapsed && <span>{item.label}</span>}
    </NavLink>
  )
}

export default function Sidebar({ collapsed, onToggle }) {
  const { hasPermission } = useAuthStore()
  const visibleAdmin = adminItems.filter(i => hasPermission(i.permission))

  return (
    <aside className={clsx(
      'flex flex-col bg-sidebar-bg border-r border-gray-800 transition-all duration-200',
      collapsed ? 'w-16' : 'w-60'
    )}>
      {/* Logo */}
      <div className="flex items-center gap-3 px-4 py-5 border-b border-gray-800">
        <div className="w-8 h-8 rounded-lg bg-primary-600 flex items-center justify-center">
          <Box size={18} className="text-white" />
        </div>
        {!collapsed && <span className="text-white font-bold text-lg tracking-tight">ARS</span>}
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {navItems.map(item => <SideLink key={item.path} item={item} collapsed={collapsed} />)}
        {visibleAdmin.length > 0 && (
          <>
            {!collapsed && <div className="text-xs text-gray-500 uppercase tracking-wider px-3 pt-6 pb-2">Admin</div>}
            {collapsed && <div className="border-t border-gray-700 my-3" />}
            {visibleAdmin.map(item => <SideLink key={item.path} item={item} collapsed={collapsed} />)}
          </>
        )}
      </nav>

      {/* Collapse toggle */}
      <button
        onClick={onToggle}
        className="flex items-center justify-center py-3 border-t border-gray-800 text-sidebar-text hover:text-white transition-colors"
      >
        {collapsed ? <ChevronRight size={18} /> : <ChevronLeft size={18} />}
      </button>
    </aside>
  )
}
