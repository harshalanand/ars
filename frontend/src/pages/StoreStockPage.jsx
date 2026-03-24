import { useState, useEffect, useCallback } from 'react'
import { storeStockAPI } from '@/services/api'
import toast from 'react-hot-toast'
import {
  RefreshCw, Save, Search, CheckCircle2, XCircle,
  AlertTriangle, Database, Sparkles, Filter
} from 'lucide-react'
import clsx from 'clsx'

// ─── tiny helpers ────────────────────────────────────────────────────────────

const Badge = ({ active }) =>
  active ? (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
      <CheckCircle2 size={10} /> Active
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-rose-500/15 text-rose-400 border border-rose-500/30">
      <XCircle size={10} /> Inactive
    </span>
  )

const NewBadge = () => (
  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/30">
    <Sparkles size={10} /> New
  </span>
)

// ─── main component ───────────────────────────────────────────────────────────

export default function StoreStockPage() {
  const [rows, setRows]           = useState([])       // merged data (distinct slocs + saved settings)
  const [dirty, setDirty]         = useState({})       // { [sloc]: {kpi, is_active} } – unsaved edits
  const [loading, setLoading]     = useState(false)
  const [syncing, setSyncing]     = useState(false)
  const [saving, setSaving]       = useState(false)
  const [search, setSearch]       = useState('')
  const [filterStatus, setFilter] = useState('all')   // 'all' | 'active' | 'inactive' | 'new'

  // ── load data ────────────────────────────────────────────────────────────
  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const { data } = await storeStockAPI.getSlocSettings()
      setRows(data.data.items || [])
      setDirty({})
    } catch {
      /* toast already shown by interceptor */
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadData() }, [loadData])

  // ── sync new SLOCs ────────────────────────────────────────────────────────
  const handleSync = async () => {
    setSyncing(true)
    try {
      const { data } = await storeStockAPI.syncSlocs()
      toast.success(data.message)
      await loadData()
    } catch {
      /* handled */
    } finally {
      setSyncing(false)
    }
  }

  // ── local cell edit ───────────────────────────────────────────────────────
  const handleEdit = (sloc, field, value) => {
    setDirty(prev => ({
      ...prev,
      [sloc]: { ...(prev[sloc] || {}), [field]: value },
    }))
  }

  // ── get current value (dirty-first) ──────────────────────────────────────
  const getVal = (row, field) =>
    dirty[row.sloc] !== undefined && dirty[row.sloc][field] !== undefined
      ? dirty[row.sloc][field]
      : row[field]

  // ── save all dirty rows ───────────────────────────────────────────────────
  const handleSaveAll = async () => {
    const dirtySlocs = Object.keys(dirty)
    if (dirtySlocs.length === 0) {
      toast('Nothing to save.')
      return
    }
    setSaving(true)
    try {
      const items = dirtySlocs.map(sloc => {
        const base = rows.find(r => r.sloc === sloc) || {}
        return {
          sloc,
          kpi:       dirty[sloc].kpi       !== undefined ? dirty[sloc].kpi       : base.kpi,
          is_active: dirty[sloc].is_active !== undefined ? dirty[sloc].is_active : base.is_active,
        }
      })
      const { data } = await storeStockAPI.bulkUpdate(items)
      toast.success(data.message)
      await loadData()
    } catch {
      /* handled */
    } finally {
      setSaving(false)
    }
  }

  // ── toggle single row active flag ─────────────────────────────────────────
  const toggleActive = (sloc) => {
    const cur = getVal(rows.find(r => r.sloc === sloc), 'is_active')
    handleEdit(sloc, 'is_active', !cur)
  }

  // ── filter + search ───────────────────────────────────────────────────────
  const visible = rows.filter(r => {
    const matchSearch = r.sloc.toLowerCase().includes(search.toLowerCase()) ||
      (getVal(r, 'kpi') || '').toLowerCase().includes(search.toLowerCase())
    if (!matchSearch) return false
    if (filterStatus === 'active')   return getVal(r, 'is_active') === true
    if (filterStatus === 'inactive') return getVal(r, 'is_active') === false
    if (filterStatus === 'new')      return r.is_new
    return true
  })

  const dirtyCount = Object.keys(dirty).length
  const newCount   = rows.filter(r => r.is_new).length

  return (
    <div className="p-6 space-y-5">
      {/* ── Header ── */}
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold text-white flex items-center gap-2">
            <Database size={20} className="text-primary-400" />
            Store Stock – SLOC Settings
          </h1>
          <p className="text-sm text-gray-400 mt-0.5">
            Manage KPI labels and Active / Inactive status for each distinct SLOC in&nbsp;
            <code className="text-xs bg-gray-800 px-1 py-0.5 rounded text-amber-300">ET_STORE_STOCK</code>
          </p>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {/* Sync */}
          <button
            onClick={handleSync}
            disabled={syncing || loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium bg-amber-600/20 text-amber-400 border border-amber-600/30 hover:bg-amber-600/30 disabled:opacity-50 transition-colors"
          >
            <RefreshCw size={14} className={syncing ? 'animate-spin' : ''} />
            Sync New SLOCs
            {newCount > 0 && (
              <span className="ml-1 px-1.5 py-0.5 rounded-full text-[10px] font-bold bg-amber-500 text-black">
                {newCount}
              </span>
            )}
          </button>

          {/* Save */}
          <button
            onClick={handleSaveAll}
            disabled={saving || dirtyCount === 0}
            className={clsx(
              'flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors',
              dirtyCount > 0
                ? 'bg-primary-600 hover:bg-primary-500 text-white shadow-lg shadow-primary-600/30'
                : 'bg-gray-700 text-gray-400 cursor-not-allowed'
            )}
          >
            <Save size={14} />
            Save Changes
            {dirtyCount > 0 && (
              <span className="ml-1 px-1.5 py-0.5 rounded-full text-[10px] font-bold bg-white text-primary-700">
                {dirtyCount}
              </span>
            )}
          </button>
        </div>
      </div>

      {/* ── Stats strip ── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: 'Total SLOCs',  value: rows.length,                          color: 'text-white' },
          { label: 'Active',       value: rows.filter(r => getVal(r,'is_active')).length,  color: 'text-emerald-400' },
          { label: 'Inactive',     value: rows.filter(r => !getVal(r,'is_active')).length, color: 'text-rose-400' },
          { label: 'Unsaved Edits',value: dirtyCount,                            color: 'text-amber-400' },
        ].map(s => (
          <div key={s.label} className="bg-gray-800/60 border border-gray-700/50 rounded-xl px-4 py-3">
            <div className={`text-2xl font-bold ${s.color}`}>{s.value}</div>
            <div className="text-xs text-gray-400 mt-0.5">{s.label}</div>
          </div>
        ))}
      </div>

      {/* ── Filters ── */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="relative flex-1 min-w-[200px]">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search SLOC or KPI…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full pl-8 pr-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-white placeholder-gray-500 focus:outline-none focus:border-primary-500"
          />
        </div>

        <div className="flex items-center gap-1 bg-gray-800 border border-gray-700 rounded-lg p-1">
          {[
            { key: 'all',      label: 'All' },
            { key: 'active',   label: 'Active' },
            { key: 'inactive', label: 'Inactive' },
            { key: 'new',      label: `New${newCount > 0 ? ` (${newCount})` : ''}` },
          ].map(f => (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              className={clsx(
                'px-3 py-1 rounded-md text-xs font-medium transition-colors',
                filterStatus === f.key
                  ? 'bg-primary-600 text-white'
                  : 'text-gray-400 hover:text-white'
              )}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── New-SLOC banner ── */}
      {newCount > 0 && (
        <div className="flex items-center gap-3 px-4 py-3 bg-amber-500/10 border border-amber-500/30 rounded-xl text-sm text-amber-300">
          <AlertTriangle size={16} className="shrink-0" />
          <span>
            <strong>{newCount} new SLOC{newCount > 1 ? 's' : ''}</strong> detected in{' '}
            <code className="text-xs bg-amber-900/40 px-1 rounded">ET_STORE_STOCK</code> that
            haven't been saved yet. Click <strong>Sync New SLOCs</strong> to persist them, then set their KPI and status.
          </span>
        </div>
      )}

      {/* ── Table ── */}
      <div className="bg-gray-800/50 border border-gray-700/50 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700 bg-gray-800/80">
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide w-[160px]">
                SLOC
              </th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                KPI Label
              </th>
              <th className="text-center px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide w-[160px]">
                Active / Inactive
              </th>
              <th className="text-center px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide w-[80px]">
                Status
              </th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={4} className="text-center py-16 text-gray-500">
                  <RefreshCw size={20} className="animate-spin mx-auto mb-2" />
                  Loading SLOC data…
                </td>
              </tr>
            ) : visible.length === 0 ? (
              <tr>
                <td colSpan={4} className="text-center py-16 text-gray-500">
                  No SLOC records found.
                </td>
              </tr>
            ) : (
              visible.map((row, idx) => {
                const isDirty   = !!dirty[row.sloc]
                const kpiVal    = getVal(row, 'kpi') || ''
                const activeVal = getVal(row, 'is_active')

                return (
                  <tr
                    key={row.sloc}
                    className={clsx(
                      'border-b border-gray-700/40 transition-colors',
                      isDirty
                        ? 'bg-primary-900/20 hover:bg-primary-900/30'
                        : idx % 2 === 0
                          ? 'bg-transparent hover:bg-gray-700/30'
                          : 'bg-gray-800/20 hover:bg-gray-700/30'
                    )}
                  >
                    {/* SLOC */}
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <code className="font-mono font-semibold text-white text-sm">{row.sloc}</code>
                        {row.is_new && <NewBadge />}
                        {isDirty && (
                          <span className="w-1.5 h-1.5 rounded-full bg-primary-400 shrink-0" title="Unsaved change" />
                        )}
                      </div>
                    </td>

                    {/* KPI editable */}
                    <td className="px-4 py-2">
                      <input
                        type="text"
                        value={kpiVal}
                        onChange={e => handleEdit(row.sloc, 'kpi', e.target.value)}
                        placeholder="Enter KPI label…"
                        className={clsx(
                          'w-full px-3 py-1.5 rounded-lg text-sm bg-gray-700/60 border text-white placeholder-gray-500',
                          'focus:outline-none focus:border-primary-500 transition-colors',
                          isDirty && dirty[row.sloc]?.kpi !== undefined
                            ? 'border-primary-500/60'
                            : 'border-gray-600/50 hover:border-gray-500'
                        )}
                      />
                    </td>

                    {/* Active toggle */}
                    <td className="px-4 py-2 text-center">
                      <button
                        onClick={() => toggleActive(row.sloc)}
                        className={clsx(
                          'relative inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-semibold border transition-all duration-150',
                          activeVal
                            ? 'bg-emerald-500/15 text-emerald-400 border-emerald-500/40 hover:bg-emerald-500/25'
                            : 'bg-rose-500/15 text-rose-400 border-rose-500/40 hover:bg-rose-500/25'
                        )}
                      >
                        {/* toggle pill */}
                        <span
                          className={clsx(
                            'w-8 h-4 rounded-full relative transition-colors duration-200 inline-block',
                            activeVal ? 'bg-emerald-500' : 'bg-gray-600'
                          )}
                        >
                          <span
                            className={clsx(
                              'absolute top-0.5 w-3 h-3 rounded-full bg-white shadow transition-all duration-200',
                              activeVal ? 'left-4' : 'left-0.5'
                            )}
                          />
                        </span>
                        {activeVal ? 'Active' : 'Inactive'}
                      </button>
                    </td>

                    {/* Status badge */}
                    <td className="px-4 py-2 text-center">
                      <Badge active={activeVal} />
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>

        {/* Footer */}
        {!loading && visible.length > 0 && (
          <div className="px-4 py-2.5 bg-gray-800/60 border-t border-gray-700/50 text-xs text-gray-500 flex items-center justify-between">
            <span>Showing {visible.length} of {rows.length} records</span>
            {dirtyCount > 0 && (
              <span className="text-amber-400 font-medium">
                ● {dirtyCount} unsaved change{dirtyCount > 1 ? 's' : ''} — click Save Changes
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
