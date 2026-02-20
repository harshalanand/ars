import { create } from 'zustand'
import { authAPI } from '@/services/api'
import toast from 'react-hot-toast'

const useAuthStore = create((set, get) => ({
  user: null,
  isAuthenticated: !!localStorage.getItem('access_token'),
  loading: false,
  permissions: [],
  roles: [],

  login: async (username, password) => {
    set({ loading: true })
    try {
      const { data } = await authAPI.login(username, password)
      const d = data.data
      localStorage.setItem('access_token', d.access_token)
      localStorage.setItem('refresh_token', d.refresh_token)
      set({ isAuthenticated: true, loading: false })
      await get().fetchUser()
      toast.success('Login successful')
      return true
    } catch (e) {
      set({ loading: false })
      toast.error(e.response?.data?.detail || 'Login failed')
      return false
    }
  },

  fetchUser: async () => {
    try {
      const { data } = await authAPI.me()
      const u = data.data
      set({
        user: u,
        permissions: u.permissions || [],
        roles: (u.roles || []).map(r => r.role_name || r),
        isAuthenticated: true,
      })
    } catch {
      set({ user: null, isAuthenticated: false })
    }
  },

  logout: () => {
    localStorage.clear()
    set({ user: null, isAuthenticated: false, permissions: [], roles: [] })
    toast.success('Logged out')
  },

  hasPermission: (perm) => {
    const { roles, permissions } = get()
    if (roles.includes('SUPER_ADMIN')) return true
    return permissions.includes(perm)
  },

  hasRole: (role) => {
    return get().roles.includes(role)
  },

  isSuperAdmin: () => get().roles.includes('SUPER_ADMIN'),
}))

export default useAuthStore
