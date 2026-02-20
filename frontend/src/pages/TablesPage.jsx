import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Plus, Search, Table2, Database, Trash2 } from 'lucide-react'
import { tablesAPI } from '@/services/api'
import useAuthStore from '@/store/authStore'
import toast from 'react-hot-toast'
import CreateTableModal from '@/components/tables/CreateTableModal'

export default function TablesPage() {
  const [tables, setTables] = useState([])
  const [allTables, setAllTables] = useState([])
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [showCreate, setShowCreate] = useState(false)
  const [viewMode, setViewMode] = useState('registered') // registered | all
  const { hasPermission } = useAuthStore()

  const load = async () => {
    setLoading(true)
    try {
      const [reg, all] = await Promise.allSettled([tablesAPI.list(), tablesAPI.listAll()])
      if (reg.status === 'fulfilled') setTables(reg.value.data.data || [])
      if (all.status === 'fulfilled') setAllTables(all.value.data.data || [])
    } finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const displayTables = viewMode === 'registered' ? tables : allTables
  const filtered = displayTables.filter(t =>
    (t.table_name || '').toLowerCase().includes(search.toLowerCase()) ||
    (t.display_name || '').toLowerCase().includes(search.toLowerCase())
  )

  const handleDelete = async (name) => {
    if (!confirm(`Soft-delete table "${name}"?`)) return
    try {
      await tablesAPI.delete(name)
      toast.success(`Table "${name}" deleted`)
      load()
    } catch {}
  }

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Tables</h1>
          <p className="text-gray-500 text-sm mt-0.5">Manage database tables and schemas</p>
        </div>
        {hasPermission('TABLE_CREATE') && (
          <button onClick={() => setShowCreate(true)} className="btn-primary"><Plus size={16} /> Create Table</button>
        )}
      </div>

      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-md">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input value={search} onChange={e => setSearch(e.target.value)} className="input pl-9" placeholder="Search tables..." />
        </div>
        <div className="flex bg-gray-100 rounded-lg p-0.5">
          {['registered', 'all'].map(v => (
            <button key={v} onClick={() => setViewMode(v)} className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${viewMode === v ? 'bg-white shadow text-gray-900' : 'text-gray-500'}`}>
              {v === 'registered' ? 'Registered' : 'All DB Tables'}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="text-center py-20 text-gray-400">Loading...</div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-20 text-gray-400">No tables found</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filtered.map(t => (
            <Link key={t.table_name} to={`/tables/${t.table_name}`} className="card p-5 hover:shadow-md transition-all group">
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-blue-50 flex items-center justify-center group-hover:bg-blue-100 transition-colors">
                    {t.is_system_table ? <Database size={18} className="text-blue-500" /> : <Table2 size={18} className="text-blue-500" />}
                  </div>
                  <div>
                    <div className="font-medium text-gray-900 text-sm">{t.display_name || t.table_name}</div>
                    {t.display_name && t.display_name !== t.table_name && (
                      <div className="text-xs text-gray-400">{t.table_name}</div>
                    )}
                  </div>
                </div>
                {!t.is_system_table && hasPermission('TABLE_DELETE') && (
                  <button onClick={(e) => { e.preventDefault(); handleDelete(t.table_name) }} className="opacity-0 group-hover:opacity-100 p-1 text-gray-400 hover:text-red-500 transition-all">
                    <Trash2 size={14} />
                  </button>
                )}
              </div>
              <div className="mt-3 flex items-center gap-4 text-xs text-gray-500">
                <span>{(t.row_count || 0).toLocaleString()} rows</span>
                {t.module && <span className="badge-gray">{t.module}</span>}
              </div>
            </Link>
          ))}
        </div>
      )}

      {showCreate && <CreateTableModal onClose={() => setShowCreate(false)} onCreated={() => { setShowCreate(false); load() }} />}
    </div>
  )
}
