import { useState, useMemo } from 'react'
import { Search, Download, RefreshCw } from 'lucide-react'
import { auditAPI } from '@/services/api'
import { AgGridReact } from 'ag-grid-react'
import 'ag-grid-community/styles/ag-grid.css'
import 'ag-grid-community/styles/ag-theme-alpine.css'
import { format, subDays } from 'date-fns'

export default function AuditPage() {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [total, setTotal] = useState(0)
  const [filters, setFilters] = useState({
    table_name: '',
    operation: '',
    changed_by: '',
    start_date: format(subDays(new Date(), 7), 'yyyy-MM-dd'),
    end_date: format(new Date(), 'yyyy-MM-dd'),
    limit: 500,
  })

  const update = (k, v) => setFilters(f => ({ ...f, [k]: v }))

  const load = async () => {
    setLoading(true)
    try {
      const params = {}
      if (filters.table_name) params.table_name = filters.table_name
      if (filters.operation) params.operation = filters.operation
      if (filters.changed_by) params.changed_by = filters.changed_by
      if (filters.start_date) params.start_date = filters.start_date
      if (filters.end_date) params.end_date = filters.end_date
      params.limit = filters.limit
      const { data } = await auditAPI.list(params)
      setRows(data.data?.audit_rows || data.data || [])
      setTotal(data.data?.total || rows.length)
    } catch {} finally { setLoading(false) }
  }

  const colDefs = useMemo(() => [
    { field: 'audit_id', headerName: 'ID', width: 80 },
    { field: 'table_name', headerName: 'Table', width: 150 },
    { field: 'operation', headerName: 'Op', width: 90,
      cellStyle: (p) => {
        if (p.value === 'INSERT') return { color: '#16a34a', fontWeight: 600 }
        if (p.value === 'UPDATE') return { color: '#2563eb', fontWeight: 600 }
        if (p.value === 'DELETE') return { color: '#dc2626', fontWeight: 600 }
        return null
      }
    },
    { field: 'pk_json', headerName: 'Primary Key', width: 200 },
    { field: 'column_name', headerName: 'Column', width: 140 },
    { field: 'old_value', headerName: 'Old Value', width: 150 },
    { field: 'new_value', headerName: 'New Value', width: 150 },
    { field: 'changed_by', headerName: 'Changed By', width: 140 },
    { field: 'changed_at', headerName: 'Timestamp', width: 180 },
    { field: 'batch_id', headerName: 'Batch', width: 120 },
  ], [])

  const defaultColDef = useMemo(() => ({ sortable: true, filter: true, resizable: true, floatingFilter: true }), [])

  const exportCSV = () => {
    if (!rows.length) return
    const cols = Object.keys(rows[0])
    const csv = [cols.join(','), ...rows.map(r => cols.map(c => `"${r[c] ?? ''}"`).join(','))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'audit_log.csv'; a.click()
  }

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Audit Log</h1>
        <p className="text-gray-500 text-sm mt-0.5">Track all data changes across tables</p>
      </div>

      {/* Filters */}
      <div className="card p-4">
        <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
          <div>
            <label className="label">Table</label>
            <input value={filters.table_name} onChange={e => update('table_name', e.target.value)} className="input" placeholder="Any" />
          </div>
          <div>
            <label className="label">Operation</label>
            <select value={filters.operation} onChange={e => update('operation', e.target.value)} className="input">
              <option value="">All</option>
              {['INSERT', 'UPDATE', 'DELETE'].map(o => <option key={o}>{o}</option>)}
            </select>
          </div>
          <div>
            <label className="label">Changed By</label>
            <input value={filters.changed_by} onChange={e => update('changed_by', e.target.value)} className="input" placeholder="username" />
          </div>
          <div>
            <label className="label">From</label>
            <input type="date" value={filters.start_date} onChange={e => update('start_date', e.target.value)} className="input" />
          </div>
          <div>
            <label className="label">To</label>
            <input type="date" value={filters.end_date} onChange={e => update('end_date', e.target.value)} className="input" />
          </div>
          <div className="flex items-end gap-2">
            <button onClick={load} className="btn-primary flex-1"><Search size={14} /> Search</button>
            <button onClick={exportCSV} className="btn-secondary"><Download size={14} /></button>
          </div>
        </div>
      </div>

      {/* Grid */}
      <div className="card">
        <div className="card-header flex items-center justify-between">
          <span className="text-sm text-gray-500">{rows.length.toLocaleString()} rows loaded</span>
          <button onClick={load} className="btn-ghost btn-sm"><RefreshCw size={14} /></button>
        </div>
        <div className="ag-theme-alpine" style={{ width: '100%', height: 500 }}>
          <AgGridReact
            rowData={rows}
            columnDefs={colDefs}
            defaultColDef={defaultColDef}
            animateRows
            pagination
            paginationPageSize={50}
            loading={loading}
          />
        </div>
      </div>
    </div>
  )
}
