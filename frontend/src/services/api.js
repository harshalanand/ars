import axios from 'axios'
import toast from 'react-hot-toast'

const API_BASE = import.meta.env.VITE_API_URL || '/api/v1'

const api = axios.create({ baseURL: API_BASE, timeout: 60000 })

// Request interceptor: attach JWT
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token')
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})

// Response interceptor: handle 401, errors
api.interceptors.response.use(
  (res) => res,
  async (error) => {
    const status = error.response?.status
    if (status === 401) {
      // Try refresh
      const refresh = localStorage.getItem('refresh_token')
      if (refresh && !error.config._retry) {
        error.config._retry = true
        try {
          const { data } = await axios.post(`${API_BASE}/auth/refresh`, { refresh_token: refresh })
          localStorage.setItem('access_token', data.data.access_token)
          localStorage.setItem('refresh_token', data.data.refresh_token)
          error.config.headers.Authorization = `Bearer ${data.data.access_token}`
          return api(error.config)
        } catch {
          localStorage.clear()
          window.location.href = '/login'
        }
      } else {
        localStorage.clear()
        window.location.href = '/login'
      }
    }
    const msg = error.response?.data?.detail || error.message
    if (status !== 401) toast.error(msg)
    return Promise.reject(error)
  }
)

// ============== Auth ==============
export const authAPI = {
  login: (username, password) => api.post('/auth/login', { username, password }),
  me: () => api.get('/auth/me'),
  changePassword: (data) => api.post('/auth/change-password', data),
}

// ============== Users ==============
export const usersAPI = {
  list: (params) => api.get('/users', { params }),
  get: (id) => api.get(`/users/${id}`),
  create: (data) => api.post('/users', data),
  update: (id, data) => api.put(`/users/${id}`, data),
  unlock: (id) => api.post(`/users/${id}/unlock`),
}

// ============== Roles ==============
export const rolesAPI = {
  list: () => api.get('/roles'),
  create: (data) => api.post('/roles', data),
  update: (id, data) => api.put(`/roles/${id}`, data),
  permissions: () => api.get('/permissions'),
  assignPermissions: (id, data) => api.post(`/roles/${id}/permissions`, data),
}

// ============== RLS ==============
export const rlsAPI = {
  stores: () => api.get('/rls/stores'),
  storeAccess: (uid) => api.get(`/rls/store-access/${uid}`),
  addStoreAccess: (data) => api.post('/rls/store-access', data),
  deleteStoreAccess: (uid, code) => api.delete(`/rls/store-access/${uid}/${code}`),
  regionAccess: (uid) => api.get(`/rls/region-access/${uid}`),
  addRegionAccess: (data) => api.post('/rls/region-access', data),
  columnRestrictions: (table) => api.get(`/rls/column-restrictions/${table}`),
  addColumnRestrictions: (data) => api.post('/rls/column-restrictions', data),
}

// ============== Tables ==============
export const tablesAPI = {
  list: (params) => api.get('/tables', { params }),
  listAll: () => api.get('/tables/database/all'),
  schema: (name) => api.get(`/tables/${name}/schema`),
  create: (data) => api.post('/tables', data),
  alter: (name, data) => api.put(`/tables/${name}/alter`, data),
  delete: (name) => api.delete(`/tables/${name}`),
  data: (name, params) => api.get(`/tables/${name}/data`, { params }),
  truncate: (name) => api.delete(`/tables/${name}/data`),
}

// ============== Data Operations ==============
export const dataAPI = {
  upsert: (data) => api.post('/data/upsert', data),
  update: (data) => api.put('/data/update', data),
  delete: (data) => api.post('/data/delete', data),
}

// ============== Upload ==============
export const uploadAPI = {
  upload: (formData, onProgress) =>
    api.post('/upload/', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress,
      timeout: 300000,
    }),
  preview: (formData) =>
    api.post('/upload/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
  sheets: (formData) =>
    api.post('/upload/sheets', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
}

// ============== Allocations ==============
export const allocAPI = {
  run: (data) => api.post('/allocations/run', data),
  list: (params) => api.get('/allocations', { params }),
  details: (id, params) => api.get(`/allocations/${id}/details`, { params }),
  summary: (id) => api.get(`/allocations/${id}/summary`),
  overrides: (id, data) => api.post(`/allocations/${id}/overrides`, data),
  approve: (id) => api.post(`/allocations/${id}/approve`),
  execute: (id) => api.post(`/allocations/${id}/execute`),
  cancel: (id) => api.post(`/allocations/${id}/cancel`),
  grid: (id, storeCode, params) => api.get(`/allocations/${id}/grid/${storeCode}`, { params }),
}

// ============== Audit ==============
export const auditAPI = {
  list: (params) => api.get('/audit', { params }),
  get: (id) => api.get(`/audit/${id}`),
}

export default api
