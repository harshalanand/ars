import { useState, useRef, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { LogOut, User, ChevronDown } from 'lucide-react'
import useAuthStore from '@/store/authStore'

export default function Header() {
  const { user, logout, roles } = useAuthStore()
  const [open, setOpen] = useState(false)
  const ref = useRef()
  const navigate = useNavigate()

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const handleLogout = () => { logout(); navigate('/login') }
  const displayRole = roles[0] || 'User'

  return (
    <header className="h-14 bg-white border-b border-gray-200 flex items-center justify-between px-6 shrink-0">
      <div />
      <div className="relative" ref={ref}>
        <button onClick={() => setOpen(!open)} className="flex items-center gap-2 hover:bg-gray-50 rounded-lg px-3 py-1.5 transition-colors">
          <div className="w-8 h-8 rounded-full bg-primary-100 flex items-center justify-center">
            <User size={16} className="text-primary-600" />
          </div>
          <div className="text-left hidden sm:block">
            <div className="text-sm font-medium text-gray-900">{user?.full_name || user?.username || 'User'}</div>
            <div className="text-xs text-gray-500">{displayRole}</div>
          </div>
          <ChevronDown size={14} className="text-gray-400" />
        </button>
        {open && (
          <div className="absolute right-0 mt-2 w-48 bg-white rounded-lg shadow-lg border border-gray-200 py-1 z-50 animate-fade-in">
            <div className="px-4 py-2 border-b border-gray-100">
              <div className="text-sm font-medium">{user?.username}</div>
              <div className="text-xs text-gray-500">{user?.email}</div>
            </div>
            <button onClick={handleLogout} className="w-full flex items-center gap-2 px-4 py-2 text-sm text-red-600 hover:bg-red-50">
              <LogOut size={14} /> Sign Out
            </button>
          </div>
        )}
      </div>
    </header>
  )
}
