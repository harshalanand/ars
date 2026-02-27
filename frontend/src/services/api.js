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
          localStorage.setItem('access_token', data.access_token)
          localStorage.setItem('refresh_token', data.refresh_token)
          error.config.headers.Authorization = `Bearer ${data.access_token}`
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
  updateProfile: (data) => api.put('/auth/profile', data),
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
  permissions: () => api.get('/roles/permissions'),
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
  listAll: (params) => api.get('/tables/database/all', { params }),
  listAllVisible: () => api.get('/tables/database/all', { params: { visible_only: true } }),
  schema: (name) => api.get(`/tables/${name}/schema`),
  create: (data) => api.post('/tables', data),
  alter: (name, data) => api.put(`/tables/${name}/alter`, data),
  delete: (name) => api.delete(`/tables/${name}`),
  data: (name, params) => api.get(`/tables/${name}/data`, { params }),
  truncate: (name) => api.delete(`/tables/${name}/data`),
  rowCount: (name) => api.get(`/tables/${name}/row-count`),
  settings: (name) => api.get(`/tables/${name}/settings`),
  updateSettings: (name, params) => api.put(`/tables/settings/${name}`, null, { params }),
  allSettings: () => api.get('/tables/settings/all'),
  distinct: (name, column, params) => api.get(`/tables/${name}/distinct/${column}`, { params }),
  exportSettings: () => api.get('/tables/export/settings'),
  updateExportSettings: (settings) => api.post('/tables/export/settings', settings),
  // Table permissions
  tablePermissions: () => api.get('/tables/permissions'),
  allowedTables: (action) => api.get('/tables/permissions/allowed', { params: { action } }),
  saveTablePermissions: (permissions) => api.post('/tables/permissions', permissions),
  // Export jobs
  startExportJob: (data) => api.post('/tables/export/jobs/start', data),
  listExportJobs: (limit = 20) => api.get('/tables/export/jobs', { params: { limit } }),
  getExportJobStatus: (jobId) => api.get(`/tables/export/jobs/${jobId}`),
  deleteExportJob: (jobId) => api.delete(`/tables/export/jobs/${jobId}`),
  downloadExportJob: (jobId) => `/api/v1/tables/export/jobs/${jobId}/download`,
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
  uploadAsync: (formData, onProgress) =>
    api.post('/upload/async', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress,
      timeout: 60000,  // Shorter timeout since it's async
    }),
  preview: (formData) =>
    api.post('/upload/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
  sheets: (formData) =>
    api.post('/upload/sheets', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
  // Job management
  listJobs: (limit = 20) => api.get('/upload/jobs', { params: { limit } }),
  listAllJobs: (limit = 50) => api.get('/upload/jobs/all', { params: { limit } }),
  getJob: (jobId) => api.get(`/upload/jobs/${jobId}`),
  cancelJob: (jobId, force = false) => api.post(`/upload/jobs/${jobId}/cancel`, null, { params: { force } }),
  deleteJob: (jobId) => api.delete(`/upload/jobs/${jobId}`),
  queueStatus: () => api.get('/upload/queue/status'),
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

// ============== MSA Analysis ==============
export const msaAPI = {
  dates: () => api.get('/msa/dates'),
  columns: () => api.get('/msa/columns'),
  distinct: (column, date) => api.get(`/msa/distinct/${column}`, { params: { date } }),
  data: (params) => api.get('/msa/data', { params }),
  summary: (params) => api.get('/msa/summary', { params }),
  pivot: (params) => api.get('/msa/pivot', { params }),
  pending: (params) => api.get('/msa/pending', { params }),
}

// ============== Audit ==============
export const auditAPI = {
  list: (params) => api.get('/audit', { params }),
  get: (id) => api.get(`/audit/${id}`),
}

// ============== Settings ==============
export const settingsAPI = {
  getAll: () => api.get('/settings'),
  get: (category) => api.get(`/settings/${category}`),
  update: (category, settings) => api.put('/settings', { category, settings }),
  testConnection: () => api.post('/settings/test-connection'),
  testEmail: (to) => api.post('/settings/test-email', { to_address: to }),
  systemInfo: () => api.get('/settings/system/info'),
  // Backup
  listBackups: () => api.get('/settings/backup/list'),
  createBackup: (database) => api.post('/settings/backup/create', { database }),
  deleteBackup: (filename) => api.delete(`/settings/backup/${filename}`),
}

export default api
