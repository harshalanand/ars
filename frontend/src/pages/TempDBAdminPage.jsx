import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Database, RefreshCw, Trash2, Zap, AlertTriangle, CheckCircle2,
  Activity, Clock, HardDrive, XCircle, Eye, Skull,
} from 'lucide-react'
import toast from 'react-hot-toast'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts'
import { maintenanceAPI } from '@/services/api'
import useAuthStore from '@/store/authStore'

const POLL_MS = 15000   // 15 s auto-refresh

function StatCard({ icon: Icon, label, value, sub, tone = 'default' }) {
  const toneClass = {
    default: 'bg-slate-50 border-slate-200 text-slate-700',
    good:    'bg-emerald-50 border-emerald-200 text-emerald-700',
    warn:    'bg-amber-50 border-amber-200 text-amber-700',
    bad:     'bg-rose-50 border-rose-200 text-rose-700',
  }[tone]
  return (
    <div className={`border rounded-lg p-3 ${toneClass}`}>
      <div className="flex items-center gap-2 text-[11px] uppercase tracking-wide font-semibold">
        <Icon size={14} /> {label}
      </div>
      <div className="text-xl font-bold mt-1">{value}</div>
      {sub && <div className="text-[11px] opacity-70 mt-0.5">{sub}</div>}
    </div>
  )
}

function fmtMb(mb) {
  if (mb == null) return '—'
  if (mb >= 1024) return `${(mb / 1024).toFixed(2)} GB`
  return `${Number(mb).toFixed(0)} MB`
}

function fmtTime(iso) {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleString()
}

export default function TempDBAdminPage() {
  const { isSuperAdmin } = useAuthStore()
  const superadmin = isSuperAdmin()

  const [status, setStatus]     = useState(null)
  const [files, setFiles]       = useState([])
  const [breakdown, setBreakdown] = useState(null)
  const [history, setHistory]   = useState([])
  const [sessions, setSessions] = useState([])
  const [longTxns, setLongTxns] = useState([])
  const [loading, setLoading]   = useState(false)
  const [running, setRunning]   = useState(false)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const pollRef = useRef(null)

  const loadAll = useCallback(async (silent = false) => {
    if (!superadmin) return
    if (!silent) setLoading(true)
    try {
      const [s, f, h, sess, tx] = await Promise.all([
        maintenanceAPI.tempdbStatus(),
        maintenanceAPI.tempdbSize(),
        maintenanceAPI.tempdbHistory(),
        maintenanceAPI.tempdbSessions().catch(() => ({ data: { sessions: [] } })),
        maintenanceAPI.tempdbLongTransactions().catch(() => ({ data: { transactions: [] } })),
      ])
      setStatus(s.data)
      setFiles(f.data?.files || [])
      setBreakdown(f.data?.breakdown || null)
      setHistory(h.data?.history || [])
      setSessions(sess.data?.sessions || [])
      setLongTxns(tx.data?.transactions || [])
    } catch (err) {
      if (!silent) toast.error('Failed to load TempDB status')
    } finally {
      if (!silent) setLoading(false)
    }
  }, [superadmin])

  useEffect(() => { loadAll() }, [loadAll])

  useEffect(() => {
    if (!autoRefresh) { if (pollRef.current) clearInterval(pollRef.current); return }
    pollRef.current = setInterval(() => loadAll(true), POLL_MS)
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [autoRefresh, loadAll])

  const doCleanup = async (dryRun) => {
    setRunning(true)
    try {
      const { data } = await maintenanceAPI.tempdbCleanup(dryRun)
      const s = data?.stats || {}
      toast.success(
        dryRun
          ? `Dry run: ${s.skipped?.length || 0} orphans would be dropped`
          : `Cleanup done — dropped ${s.dropped_count || 0}, freed ${s.mb_freed || 0} MB`
      )
      await loadAll(true)
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Cleanup failed')
    } finally {
      setRunning(false)
    }
  }

  const doAggressive = async () => {
    if (!window.confirm(
      'Aggressive shrink will:\n' +
      '  • DBCC FREEPROCCACHE (clears plan cache)\n' +
      '  • DBCC FREESYSTEMCACHE (clears system caches)\n' +
      '  • Hard SHRINKFILE on every tempdb data file\n\n' +
      'This will cause a brief query-plan recompile spike. Continue?'
    )) return
    setRunning(true)
    try {
      const { data } = await maintenanceAPI.tempdbAggressiveShrink()
      const s = data?.stats || {}
      toast.success(`Aggressive shrink done — freed ${s.mb_freed || 0} MB`)
      await loadAll(true)
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Aggressive shrink failed')
    } finally {
      setRunning(false)
    }
  }

  const killSession = async (sid, loginName) => {
    if (!window.confirm(
      `KILL session ${sid}${loginName ? ` (${loginName})` : ''}?\n\n` +
      `This will terminate the session and roll back its open transaction. ` +
      `Anything the user was doing will fail.`
    )) return
    try {
      await maintenanceAPI.tempdbKillSession(sid)
      toast.success(`Session ${sid} killed`)
      await loadAll(true)
    } catch (err) {
      toast.error(err.response?.data?.detail || `Kill ${sid} failed`)
    }
  }

  const clearAlert = async () => {
    try {
      await maintenanceAPI.tempdbClearAlert()
      toast.success('Alert dismissed')
      await loadAll(true)
    } catch { /* ignore */ }
  }

  if (!superadmin) {
    return (
      <div className="p-10 text-center text-gray-500">
        Access denied. Superadmin only.
      </div>
    )
  }

  const rowsFiles = files.filter(f => f.file_type === 'ROWS')
  const logsFiles = files.filter(f => f.file_type === 'LOG')
  const totalAllocated = rowsFiles.reduce((a, f) => a + (f.allocated_mb || 0), 0)
  const totalUsed      = rowsFiles.reduce((a, f) => a + (f.used_mb || 0), 0)
  const pctUsed        = totalAllocated > 0 ? (totalUsed / totalAllocated) * 100 : 0

  const tone = (() => {
    if (!status) return 'default'
    if (totalAllocated >= (status.alert_threshold_mb || 0)) return 'bad'
    if (totalAllocated >= (status.aggressive_threshold_mb || 0)) return 'warn'
    return 'good'
  })()

  const chartData = history.map(h => ({
    t: h.ts ? new Date(h.ts).toLocaleTimeString() : '',
    before: h.mb_before,
    after:  h.mb_after,
  }))

  return (
    <div className="p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Database className="text-primary-600" size={22} />
          <h1 className="text-xl font-bold text-slate-800">TempDB Maintenance</h1>
        </div>
        <div className="flex items-center gap-2">
          <label className="flex items-center gap-1.5 text-xs text-slate-600">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={e => setAutoRefresh(e.target.checked)}
            />
            Auto-refresh (15 s)
          </label>
          <button
            onClick={() => loadAll()}
            disabled={loading}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs border border-slate-300 rounded-md hover:bg-slate-50 disabled:opacity-50"
          >
            <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* Alert banner */}
      {status?.last_alert && (
        <div className="flex items-start gap-3 bg-rose-50 border border-rose-300 text-rose-800 rounded-lg p-3">
          <AlertTriangle size={18} className="shrink-0 mt-0.5" />
          <div className="flex-1">
            <div className="font-semibold text-sm">TempDB Alert</div>
            <div className="text-xs mt-0.5">{status.last_alert.message}</div>
            <div className="text-[10px] opacity-60 mt-0.5">
              Raised at {fmtTime(status.last_alert.raised_at)}
            </div>
          </div>
          <button
            onClick={clearAlert}
            className="text-xs px-2 py-1 bg-white border border-rose-300 rounded hover:bg-rose-100"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          icon={HardDrive}
          label="Allocated"
          value={fmtMb(totalAllocated)}
          sub={`${rowsFiles.length} data file${rowsFiles.length === 1 ? '' : 's'}`}
          tone={tone}
        />
        <StatCard
          icon={Activity}
          label="In Use"
          value={fmtMb(totalUsed)}
          sub={`${pctUsed.toFixed(0)}% of allocated`}
        />
        <StatCard
          icon={Clock}
          label="Last Run"
          value={status?.last_run ? fmtTime(status.last_run).split(', ')[1] || fmtTime(status.last_run) : '—'}
          sub={status?.last_stats?.mode ? `mode: ${status.last_stats.mode}` : ''}
        />
        <StatCard
          icon={status?.running ? CheckCircle2 : XCircle}
          label="Service"
          value={status?.running ? 'Running' : 'Stopped'}
          sub={status ? `every ${status.interval_minutes} min` : ''}
          tone={status?.running ? 'good' : 'bad'}
        />
      </div>

      {/* Thresholds strip */}
      {status && (
        <div className="bg-slate-50 border border-slate-200 rounded-lg p-3 text-xs text-slate-700 grid grid-cols-2 md:grid-cols-4 gap-3">
          <div><span className="opacity-60">Orphan age:</span> <b>{status.orphan_age_minutes} min</b></div>
          <div><span className="opacity-60">Aggressive threshold:</span> <b>{fmtMb(status.aggressive_threshold_mb)}</b></div>
          <div><span className="opacity-60">Alert threshold:</span> <b>{fmtMb(status.alert_threshold_mb)}</b></div>
          <div><span className="opacity-60">Aggressive target:</span> <b>{fmtMb(status.aggressive_target_mb)}</b> per file</div>
        </div>
      )}

      {/* Actions */}
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => doCleanup(true)}
          disabled={running}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs border border-slate-300 bg-white rounded-md hover:bg-slate-50 disabled:opacity-50"
        >
          <Eye size={13} /> Dry Run
        </button>
        <button
          onClick={() => doCleanup(false)}
          disabled={running}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs bg-primary-600 text-white rounded-md hover:bg-primary-700 disabled:opacity-50"
        >
          <Trash2 size={13} /> Run Cleanup Now
        </button>
        <button
          onClick={doAggressive}
          disabled={running}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs bg-rose-600 text-white rounded-md hover:bg-rose-700 disabled:opacity-50"
        >
          <Zap size={13} /> Aggressive Shrink
        </button>
      </div>

      {/* Usage breakdown — shows WHERE the space went */}
      {breakdown && (
        <div className="bg-white border border-slate-200 rounded-lg p-3">
          <div className="text-sm font-semibold text-slate-800 mb-2">
            TempDB Space Breakdown
            <span className="ml-2 text-[11px] font-normal text-slate-500">
              (from sys.dm_db_file_space_usage — what's holding the space)
            </span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
            <div className="bg-indigo-50 border border-indigo-200 rounded p-2">
              <div className="text-[10px] uppercase text-indigo-700 font-semibold">User Objects</div>
              <div className="text-lg font-bold text-indigo-900">{fmtMb(breakdown.user_objects_mb)}</div>
              <div className="text-[10px] text-indigo-600">## / # temp tables</div>
            </div>
            <div className="bg-amber-50 border border-amber-200 rounded p-2">
              <div className="text-[10px] uppercase text-amber-700 font-semibold">Internal Work</div>
              <div className="text-lg font-bold text-amber-900">{fmtMb(breakdown.internal_objects_mb)}</div>
              <div className="text-[10px] text-amber-600">sorts / hash joins / spills</div>
            </div>
            <div className="bg-rose-50 border border-rose-200 rounded p-2">
              <div className="text-[10px] uppercase text-rose-700 font-semibold">Version Store</div>
              <div className="text-lg font-bold text-rose-900">{fmtMb(breakdown.version_store_mb)}</div>
              <div className="text-[10px] text-rose-600">RCSI / long transactions</div>
            </div>
            <div className="bg-slate-50 border border-slate-200 rounded p-2">
              <div className="text-[10px] uppercase text-slate-700 font-semibold">Mixed Extents</div>
              <div className="text-lg font-bold text-slate-900">{fmtMb(breakdown.mixed_extent_mb)}</div>
              <div className="text-[10px] text-slate-600">small object pages</div>
            </div>
            <div className="bg-emerald-50 border border-emerald-200 rounded p-2">
              <div className="text-[10px] uppercase text-emerald-700 font-semibold">Free</div>
              <div className="text-lg font-bold text-emerald-900">{fmtMb(breakdown.unallocated_mb)}</div>
              <div className="text-[10px] text-emerald-600">reclaimable via shrink</div>
            </div>
          </div>
        </div>
      )}

      {/* Long-running transactions — the usual cause of stuck shrink */}
      <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-3 py-2 border-b border-slate-200 text-sm font-semibold text-slate-800 flex items-center justify-between">
          <div>
            Long-Running Transactions
            <span className="ml-2 text-[11px] font-normal text-slate-500">
              (open transactions pinning tempdb space — oldest first)
            </span>
          </div>
          {longTxns.length > 0 && (
            <span className="text-[10px] px-2 py-0.5 rounded bg-rose-100 text-rose-700 font-semibold">
              {longTxns.length} open
            </span>
          )}
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="bg-slate-50 text-slate-600">
              <tr>
                <th className="text-left px-3 py-1.5">SID</th>
                <th className="text-left px-3 py-1.5">Status</th>
                <th className="text-left px-3 py-1.5">Login</th>
                <th className="text-left px-3 py-1.5">Host</th>
                <th className="text-left px-3 py-1.5">Program</th>
                <th className="text-left px-3 py-1.5">Database</th>
                <th className="text-right px-3 py-1.5">Open (min)</th>
                <th className="text-right px-3 py-1.5">Log MB</th>
                <th className="text-left px-3 py-1.5">Command</th>
                <th className="text-left px-3 py-1.5">Wait</th>
                <th className="text-right px-3 py-1.5">Action</th>
              </tr>
            </thead>
            <tbody>
              {longTxns.map(t => {
                const ageTone =
                  t.mins_open >= 30 ? 'text-rose-700 font-semibold'
                  : t.mins_open >= 10 ? 'text-amber-700 font-semibold'
                  : 'text-slate-700'
                return (
                  <tr key={`${t.session_id}-${t.begin_time}`} className="border-t border-slate-100">
                    <td className="px-3 py-1.5 font-mono">{t.session_id}</td>
                    <td className="px-3 py-1.5">{t.status}</td>
                    <td className="px-3 py-1.5">{t.login_name}</td>
                    <td className="px-3 py-1.5">{t.host_name}</td>
                    <td className="px-3 py-1.5 truncate max-w-[160px]" title={t.program_name}>{t.program_name}</td>
                    <td className="px-3 py-1.5">{t.database_name}</td>
                    <td className={`px-3 py-1.5 text-right ${ageTone}`}>{t.mins_open}</td>
                    <td className="px-3 py-1.5 text-right">{fmtMb(t.log_mb)}</td>
                    <td className="px-3 py-1.5">{t.command}</td>
                    <td className="px-3 py-1.5 text-slate-500">{t.wait_type}</td>
                    <td className="px-3 py-1.5 text-right">
                      <button
                        onClick={() => killSession(t.session_id, t.login_name)}
                        className="inline-flex items-center gap-1 px-2 py-0.5 text-[10px] border border-rose-300 text-rose-700 rounded hover:bg-rose-50"
                      >
                        <Skull size={11} /> KILL
                      </button>
                    </td>
                  </tr>
                )
              })}
              {longTxns.length === 0 && (
                <tr>
                  <td colSpan={11} className="text-center py-4 text-slate-400">
                    No open transactions — tempdb bloat is not from a live txn
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Trend chart */}
      <div className="bg-white border border-slate-200 rounded-lg p-3">
        <div className="text-sm font-semibold text-slate-800 mb-2">
          TempDB Size Over Time
          <span className="ml-2 text-[11px] font-normal text-slate-500">
            ({history.length} points · before/after each run)
          </span>
        </div>
        <div style={{ width: '100%', height: 240 }}>
          <ResponsiveContainer>
            <LineChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="t" tick={{ fontSize: 10 }} />
              <YAxis tick={{ fontSize: 10 }} label={{ value: 'MB', angle: -90, position: 'insideLeft', fontSize: 10 }} />
              <Tooltip formatter={(v) => fmtMb(v)} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Line type="monotone" dataKey="before" name="Before" stroke="#dc2626" dot={false} strokeWidth={1.5} />
              <Line type="monotone" dataKey="after"  name="After"  stroke="#059669" dot={false} strokeWidth={1.5} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Files table */}
      <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-3 py-2 border-b border-slate-200 text-sm font-semibold text-slate-800">
          TempDB Files
        </div>
        <table className="w-full text-xs">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-left px-3 py-1.5">File</th>
              <th className="text-left px-3 py-1.5">Type</th>
              <th className="text-right px-3 py-1.5">Allocated</th>
              <th className="text-right px-3 py-1.5">Used</th>
              <th className="text-right px-3 py-1.5">Free</th>
              <th className="text-left px-3 py-1.5 w-40">Usage</th>
            </tr>
          </thead>
          <tbody>
            {files.map(f => {
              const pct = f.allocated_mb > 0 ? (f.used_mb / f.allocated_mb) * 100 : 0
              const bar = pct > 85 ? 'bg-rose-500' : pct > 60 ? 'bg-amber-500' : 'bg-emerald-500'
              return (
                <tr key={f.file_name} className="border-t border-slate-100">
                  <td className="px-3 py-1.5 font-mono">{f.file_name}</td>
                  <td className="px-3 py-1.5">{f.file_type}</td>
                  <td className="px-3 py-1.5 text-right">{fmtMb(f.allocated_mb)}</td>
                  <td className="px-3 py-1.5 text-right">{fmtMb(f.used_mb)}</td>
                  <td className="px-3 py-1.5 text-right">{fmtMb(f.free_mb)}</td>
                  <td className="px-3 py-1.5">
                    <div className="flex items-center gap-2">
                      <div className="flex-1 h-1.5 bg-slate-100 rounded">
                        <div className={`h-full rounded ${bar}`} style={{ width: `${Math.min(100, pct)}%` }} />
                      </div>
                      <span className="text-[10px] text-slate-500 w-8 text-right">{pct.toFixed(0)}%</span>
                    </div>
                  </td>
                </tr>
              )
            })}
            {files.length === 0 && (
              <tr><td colSpan={6} className="text-center py-4 text-slate-400">No data</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Top sessions */}
      <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-3 py-2 border-b border-slate-200 text-sm font-semibold text-slate-800">
          Top TempDB-Consuming Sessions
          <span className="ml-2 text-[11px] font-normal text-slate-500">
            (live from sys.dm_db_session_space_usage)
          </span>
        </div>
        <table className="w-full text-xs">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-left px-3 py-1.5">SID</th>
              <th className="text-left px-3 py-1.5">Status</th>
              <th className="text-left px-3 py-1.5">Login</th>
              <th className="text-left px-3 py-1.5">Host</th>
              <th className="text-left px-3 py-1.5">Program</th>
              <th className="text-right px-3 py-1.5">Total MB</th>
              <th className="text-right px-3 py-1.5">User MB</th>
              <th className="text-right px-3 py-1.5">Internal MB</th>
            </tr>
          </thead>
          <tbody>
            {sessions.map(s => (
              <tr key={s.session_id} className="border-t border-slate-100">
                <td className="px-3 py-1.5 font-mono">{s.session_id}</td>
                <td className="px-3 py-1.5">{s.status}</td>
                <td className="px-3 py-1.5">{s.login_name}</td>
                <td className="px-3 py-1.5">{s.host_name}</td>
                <td className="px-3 py-1.5 truncate max-w-[180px]" title={s.program_name}>{s.program_name}</td>
                <td className="px-3 py-1.5 text-right font-semibold">{fmtMb(s.mb_used)}</td>
                <td className="px-3 py-1.5 text-right">{fmtMb(s.user_mb)}</td>
                <td className="px-3 py-1.5 text-right">{fmtMb(s.internal_mb)}</td>
              </tr>
            ))}
            {sessions.length === 0 && (
              <tr><td colSpan={8} className="text-center py-4 text-slate-400">No active tempdb consumers</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Last run details */}
      {status?.last_stats && (
        <div className="bg-white border border-slate-200 rounded-lg p-3">
          <div className="text-sm font-semibold text-slate-800 mb-2">Last Run Details</div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
            <div><span className="text-slate-500">Mode:</span> <b>{status.last_stats.mode}</b></div>
            <div><span className="text-slate-500">Dropped:</span> <b>{status.last_stats.dropped_count}</b></div>
            <div><span className="text-slate-500">Before:</span> <b>{fmtMb(status.last_stats.tempdb_mb_before)}</b></div>
            <div><span className="text-slate-500">After:</span> <b>{fmtMb(status.last_stats.tempdb_mb_after)}</b></div>
            <div><span className="text-slate-500">Freed:</span> <b>{fmtMb(status.last_stats.mb_freed)}</b></div>
            <div><span className="text-slate-500">Shrunk files:</span> <b>{status.last_stats.shrunk_files?.length || 0}</b></div>
            <div><span className="text-slate-500">Errors:</span> <b>{status.last_stats.errors?.length || 0}</b></div>
            <div><span className="text-slate-500">Ran at:</span> <b>{fmtTime(status.last_stats.run_at)}</b></div>
          </div>
          {status.last_stats.errors?.length > 0 && (
            <div className="mt-2 text-[11px] text-rose-700 bg-rose-50 border border-rose-200 rounded p-2">
              {status.last_stats.errors.map((e, i) => (
                <div key={i}>• {e.file || e.table || e.step || 'error'}: {e.error}</div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
