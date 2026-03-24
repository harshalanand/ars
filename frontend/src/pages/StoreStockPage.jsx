import { useState, useEffect, useCallback } from 'react'
import { storeStockAPI } from '@/services/api'
import toast from 'react-hot-toast'
import {
  RefreshCw, Save, Search, CheckCircle2, XCircle,
  AlertTriangle, Database, Sparkles
} from 'lucide-react'

/* ─── colour tokens (always explicit – no Tailwind dark-mode guessing) ────── */
const C = {
  bg:          '#0f172a',
  card:        '#1e293b',
  cardBorder:  '#334155',
  cardHover:   '#263046',
  rowAlt:      '#172032',
  text:        '#f1f5f9',   /* primary text  – always white  */
  textSub:     '#94a3b8',   /* secondary     */
  textMuted:   '#64748b',   /* muted / placeholder */
  inputBg:     '#0f172a',
  inputBorder: '#475569',
  inputFocus:  '#6366f1',
  green:       '#34d399',
  greenBg:     'rgba(52,211,153,0.12)',
  greenBd:     'rgba(52,211,153,0.35)',
  red:         '#f87171',
  redBg:       'rgba(248,113,113,0.12)',
  redBd:       'rgba(248,113,113,0.35)',
  amber:       '#fbbf24',
  amberBg:     'rgba(251,191,36,0.1)',
  amberBd:     'rgba(251,191,36,0.35)',
  indigo:      '#818cf8',
  indigoBg:    'rgba(99,102,241,0.15)',
  indigoBd:    'rgba(99,102,241,0.4)',
  primary:     '#4f46e5',
  primaryHov:  '#4338ca',
}

/* ─── tiny reusable components ──────────────────────────────────────────────── */
const StatusBadge = ({ status }) => (
  <span style={{
    display:'inline-flex', alignItems:'center', gap:4,
    padding:'3px 10px', borderRadius:20, fontSize:11, fontWeight:700,
    background: status === 'Active' ? C.greenBg : C.redBg,
    color:      status === 'Active' ? C.green   : C.red,
    border:     `1px solid ${status === 'Active' ? C.greenBd : C.redBd}`,
    whiteSpace: 'nowrap',
  }}>
    {status === 'Active'
      ? <CheckCircle2 size={10} style={{flexShrink:0}} />
      : <XCircle      size={10} style={{flexShrink:0}} />}
    {status}
  </span>
)

const NewBadge = () => (
  <span style={{
    display:'inline-flex', alignItems:'center', gap:3,
    padding:'2px 7px', borderRadius:20, fontSize:10, fontWeight:700,
    background: C.amberBg, color: C.amber, border:`1px solid ${C.amberBd}`,
  }}>
    <Sparkles size={9}/> New
  </span>
)

const Toggle = ({ active, onClick }) => (
  <button onClick={onClick} style={{
    display:'inline-flex', alignItems:'center', gap:7,
    padding:'5px 12px', borderRadius:8, fontSize:12, fontWeight:700,
    cursor:'pointer', border:`1px solid ${active ? C.greenBd : C.redBd}`,
    background: active ? C.greenBg : C.redBg,
    color:      active ? C.green   : C.red,
    whiteSpace:'nowrap', transition:'all .15s',
  }}>
    {/* pill */}
    <span style={{
      width:32, height:16, borderRadius:8, position:'relative', display:'inline-block',
      flexShrink:0, transition:'background .2s',
      background: active ? '#10b981' : '#475569',
    }}>
      <span style={{
        position:'absolute', top:2, width:12, height:12, borderRadius:'50%',
        background:'#fff', boxShadow:'0 1px 4px rgba(0,0,0,.5)',
        transition:'left .2s', left: active ? 18 : 2,
      }}/>
    </span>
    {active ? 'Active' : 'Inactive'}
  </button>
)

/* ─── main page ──────────────────────────────────────────────────────────── */
export default function StoreStockPage() {
  const [rows,      setRows]      = useState([])
  const [dirty,     setDirty]     = useState({})
  const [loading,   setLoading]   = useState(false)
  const [syncing,   setSyncing]   = useState(false)
  const [saving,    setSaving]    = useState(false)
  const [search,    setSearch]    = useState('')
  const [filterTab, setFilterTab] = useState('all')

  /* load */
  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const { data } = await storeStockAPI.getSlocSettings()
      setRows(data.data.items || [])
      setDirty({})
    } catch {}
    finally { setLoading(false) }
  }, [])
  useEffect(() => { loadData() }, [loadData])

  /* sync */
  const handleSync = async () => {
    setSyncing(true)
    try {
      const { data } = await storeStockAPI.syncSlocs()
      toast.success(data.message)
      await loadData()
    } catch {} finally { setSyncing(false) }
  }

  /* field edit */
  const setField = (sloc, field, val) =>
    setDirty(p => ({ ...p, [sloc]: { ...(p[sloc]||{}), [field]: val } }))

  const getVal = (row, field) =>
    dirty[row.sloc]?.[field] !== undefined ? dirty[row.sloc][field] : row[field]

  const toggleStatus = (sloc) => {
    const row = rows.find(r => r.sloc === sloc)
    setField(sloc, 'status', getVal(row,'status') === 'Active' ? 'Inactive' : 'Active')
  }

  /* save */
  const handleSave = async () => {
    const keys = Object.keys(dirty)
    if (!keys.length) { toast('Nothing to save.'); return }
    setSaving(true)
    try {
      const items = keys.map(sloc => {
        const base = rows.find(r => r.sloc === sloc) || {}
        return {
          sloc,
          kpi:    dirty[sloc]?.kpi    !== undefined ? dirty[sloc].kpi    : base.kpi,
          status: dirty[sloc]?.status !== undefined ? dirty[sloc].status : base.status,
        }
      })
      const { data } = await storeStockAPI.bulkUpdate(items)
      toast.success(data.message)
      await loadData()
    } catch {} finally { setSaving(false) }
  }

  /* filter */
  const visible = rows.filter(r => {
    const q = search.toLowerCase()
    const match = r.sloc.toLowerCase().includes(q) ||
      (getVal(r,'kpi')||'').toLowerCase().includes(q)
    if (!match) return false
    const st = getVal(r,'status')
    if (filterTab === 'active')   return st === 'Active'
    if (filterTab === 'inactive') return st === 'Inactive'
    if (filterTab === 'new')      return r.is_new
    return true
  })

  const dirtyCount    = Object.keys(dirty).length
  const newCount      = rows.filter(r => r.is_new).length
  const activeCount   = rows.filter(r => getVal(r,'status') === 'Active').length
  const inactiveCount = rows.filter(r => getVal(r,'status') === 'Inactive').length

  /* ── render ────────────────────────────────────────────────────────────── */
  return (
    <div style={{ padding:24, color: C.text, fontFamily:'inherit' }}>

      {/* ── Page title ── */}
      <div style={{ marginBottom:20 }}>
        <h1 style={{ fontSize:20, fontWeight:700, color: C.text, margin:0,
          display:'flex', alignItems:'center', gap:8 }}>
          <Database size={20} color={C.indigo} />
          Store Stock – SLOC Settings
        </h1>
        <p style={{ fontSize:13, color: C.textSub, marginTop:5 }}>
          Configure&nbsp;<strong style={{color:C.text}}>KPI</strong>&nbsp;labels and&nbsp;
          <strong style={{color:C.text}}>Active / Inactive</strong>&nbsp;status per SLOC.
          &nbsp;Stored in&nbsp;
          <code style={{background:'#1e293b',color:C.amber,padding:'1px 6px',borderRadius:4,fontSize:11}}>
            ARS_STORE_SLOC_SETTINGS
          </code>
        </p>
      </div>

      {/* ── Main card wrapper ── */}
      <div style={{
        background: C.card, border:`1px solid ${C.cardBorder}`,
        borderRadius:14, overflow:'hidden',
      }}>

        {/* Card header: action buttons */}
        <div style={{
          display:'flex', justifyContent:'space-between', alignItems:'center',
          flexWrap:'wrap', gap:10, padding:'14px 18px',
          borderBottom:`1px solid ${C.cardBorder}`,
          background:'rgba(15,23,42,0.5)',
        }}>
          <span style={{ fontSize:13, fontWeight:600, color: C.textSub }}>
            {rows.length} distinct SLOCs from ET_STORE_STOCK
          </span>
          <div style={{ display:'flex', gap:8 }}>
            <button onClick={handleSync} disabled={syncing||loading} style={{
              display:'flex', alignItems:'center', gap:6,
              padding:'7px 14px', borderRadius:8, fontSize:13, fontWeight:600,
              cursor:'pointer', border:`1px solid ${C.amberBd}`,
              background: C.amberBg, color: C.amber,
              opacity:(syncing||loading)?.5:1,
            }}>
              <RefreshCw size={14} style={{animation:syncing?'spin 1s linear infinite':'none'}}/>
              Sync New SLOCs
              {newCount > 0 && (
                <span style={{background:C.amber,color:'#000',borderRadius:99,
                  padding:'1px 7px',fontSize:10,fontWeight:800}}>{newCount}</span>
              )}
            </button>

            <button onClick={handleSave} disabled={saving||dirtyCount===0} style={{
              display:'flex', alignItems:'center', gap:6,
              padding:'7px 16px', borderRadius:8, fontSize:13, fontWeight:600,
              cursor: dirtyCount>0 ? 'pointer' : 'not-allowed',
              border:'none',
              background: dirtyCount>0 ? C.primary : '#2d3748',
              color:      dirtyCount>0 ? '#fff'    : C.textMuted,
              opacity: saving ? .6 : 1,
              boxShadow: dirtyCount>0 ? '0 0 14px rgba(79,70,229,.35)' : 'none',
              transition:'all .15s',
            }}>
              <Save size={14}/>
              Save Changes
              {dirtyCount > 0 && (
                <span style={{background:'#fff',color:C.primary,borderRadius:99,
                  padding:'1px 7px',fontSize:10,fontWeight:800}}>{dirtyCount}</span>
              )}
            </button>
          </div>
        </div>

        {/* Stats row */}
        <div style={{
          display:'grid', gridTemplateColumns:'repeat(4,1fr)',
          borderBottom:`1px solid ${C.cardBorder}`,
        }}>
          {[
            { label:'Total SLOCs',   value:rows.length,   color:C.text  },
            { label:'Active',        value:activeCount,   color:C.green },
            { label:'Inactive',      value:inactiveCount, color:C.red   },
            { label:'Unsaved Edits', value:dirtyCount,    color:C.amber },
          ].map((s,i) => (
            <div key={s.label} style={{
              padding:'12px 18px',
              borderRight: i<3 ? `1px solid ${C.cardBorder}` : 'none',
            }}>
              <div style={{fontSize:24,fontWeight:800,color:s.color,lineHeight:1}}>{s.value}</div>
              <div style={{fontSize:11,color:C.textSub,marginTop:3}}>{s.label}</div>
            </div>
          ))}
        </div>

        {/* Search + filter */}
        <div style={{
          display:'flex', gap:10, flexWrap:'wrap', alignItems:'center',
          padding:'12px 18px', borderBottom:`1px solid ${C.cardBorder}`,
          background:'rgba(15,23,42,.3)',
        }}>
          <div style={{ position:'relative', flex:1, minWidth:200 }}>
            <Search size={13} style={{position:'absolute',left:10,top:'50%',transform:'translateY(-50%)',color:C.textMuted}}/>
            <input
              type="text" value={search} placeholder="Search SLOC or KPI…"
              onChange={e => setSearch(e.target.value)}
              style={{
                width:'100%', padding:'7px 10px 7px 30px', borderRadius:7,
                background: C.inputBg, border:`1px solid ${C.inputBorder}`,
                color: C.text,           /* ← always white */
                fontSize:13, outline:'none', boxSizing:'border-box',
              }}
            />
          </div>

          <div style={{
            display:'flex', background:C.inputBg,
            border:`1px solid ${C.cardBorder}`, borderRadius:7, padding:3, gap:2,
          }}>
            {[
              { key:'all',      label:'All'                                          },
              { key:'active',   label:'Active'                                       },
              { key:'inactive', label:'Inactive'                                     },
              { key:'new',      label: newCount>0 ? `New (${newCount})` : 'New'     },
            ].map(f => (
              <button key={f.key} onClick={() => setFilterTab(f.key)} style={{
                padding:'4px 12px', borderRadius:5, fontSize:12, fontWeight:600,
                border:'none', cursor:'pointer', transition:'all .15s',
                background: filterTab===f.key ? C.primary      : 'transparent',
                color:      filterTab===f.key ? '#fff'          : C.textSub,
              }}>
                {f.label}
              </button>
            ))}
          </div>
        </div>

        {/* New SLOC alert */}
        {newCount > 0 && (
          <div style={{
            display:'flex', alignItems:'center', gap:10,
            padding:'10px 18px', background: C.amberBg,
            borderBottom:`1px solid ${C.amberBd}`,
            fontSize:13, color: C.amber,
          }}>
            <AlertTriangle size={14} style={{flexShrink:0}}/>
            <span>
              <strong>{newCount} new SLOC{newCount>1?'s':''}</strong> found in ET_STORE_STOCK but not yet saved.
              Click <strong>Sync New SLOCs</strong> to persist them with default settings.
            </span>
          </div>
        )}

        {/* Table */}
        <div style={{ overflowX:'auto' }}>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:13, minWidth:640 }}>
            <thead>
              <tr style={{ background:'rgba(15,23,42,.6)', borderBottom:`2px solid ${C.cardBorder}` }}>
                <th style={{ padding:'10px 18px', textAlign:'left', fontSize:11, fontWeight:700,
                  color: C.textSub, textTransform:'uppercase', letterSpacing:'.07em', width:170 }}>
                  SLOC
                </th>
                <th style={{ padding:'10px 18px', textAlign:'left', fontSize:11, fontWeight:700,
                  color: C.textSub, textTransform:'uppercase', letterSpacing:'.07em' }}>
                  KPI
                </th>
                <th style={{ padding:'10px 18px', textAlign:'center', fontSize:11, fontWeight:700,
                  color: C.textSub, textTransform:'uppercase', letterSpacing:'.07em', width:180 }}>
                  Active / Inactive
                </th>
                <th style={{ padding:'10px 18px', textAlign:'center', fontSize:11, fontWeight:700,
                  color: C.textSub, textTransform:'uppercase', letterSpacing:'.07em', width:110 }}>
                  Status
                </th>
              </tr>
            </thead>

            <tbody>
              {loading ? (
                <tr><td colSpan={4} style={{textAlign:'center',padding:60,color:C.textMuted}}>
                  <RefreshCw size={18} style={{display:'block',margin:'0 auto 8px',animation:'spin 1s linear infinite'}}/>
                  Loading SLOC data…
                </td></tr>
              ) : visible.length === 0 ? (
                <tr><td colSpan={4} style={{textAlign:'center',padding:60,color:C.textMuted}}>
                  No SLOC records found.
                </td></tr>
              ) : visible.map((row, idx) => {
                const isDirty  = !!dirty[row.sloc]
                const kpiVal   = getVal(row,'kpi') ?? ''
                const statusVal = getVal(row,'status') ?? 'Active'

                return (
                  <tr key={row.sloc} style={{
                    borderBottom: `1px solid ${C.cardBorder}`,
                    background: isDirty ? 'rgba(79,70,229,.09)'
                      : idx%2===0 ? 'transparent' : C.rowAlt,
                    transition:'background .12s',
                  }}>

                    {/* SLOC */}
                    <td style={{ padding:'9px 18px' }}>
                      <div style={{display:'flex',alignItems:'center',gap:7}}>
                        <code style={{
                          fontFamily:'Consolas,monospace', fontSize:13,
                          fontWeight:700, color: C.text,   /* always visible */
                          letterSpacing:'.04em',
                        }}>
                          {row.sloc}
                        </code>
                        {row.is_new && <NewBadge/>}
                        {isDirty && (
                          <span title="Unsaved change" style={{
                            width:6,height:6,borderRadius:'50%',
                            background:C.indigo,flexShrink:0,
                          }}/>
                        )}
                      </div>
                    </td>

                    {/* KPI input */}
                    <td style={{ padding:'7px 18px' }}>
                      <input
                        type="text"
                        value={kpiVal}
                        onChange={e => setField(row.sloc,'kpi',e.target.value)}
                        placeholder="Enter KPI label…"
                        style={{
                          width:'100%', padding:'6px 11px', borderRadius:6,
                          fontSize:13,
                          background: isDirty && dirty[row.sloc]?.kpi !== undefined
                            ? 'rgba(79,70,229,.12)' : C.inputBg,
                          border: `1px solid ${
                            isDirty && dirty[row.sloc]?.kpi !== undefined
                              ? C.indigoBd : C.inputBorder
                          }`,
                          color: C.text,               /* ← ALWAYS WHITE */
                          caretColor: C.indigo,
                          outline:'none', boxSizing:'border-box',
                          fontFamily:'inherit',
                        }}
                      />
                    </td>

                    {/* Toggle */}
                    <td style={{ padding:'7px 18px', textAlign:'center' }}>
                      <Toggle
                        active={statusVal === 'Active'}
                        onClick={() => toggleStatus(row.sloc)}
                      />
                    </td>

                    {/* Status badge */}
                    <td style={{ padding:'7px 18px', textAlign:'center' }}>
                      <StatusBadge status={statusVal}/>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* Card footer */}
        {!loading && rows.length > 0 && (
          <div style={{
            padding:'9px 18px', borderTop:`1px solid ${C.cardBorder}`,
            background:'rgba(15,23,42,.5)', fontSize:12,
            display:'flex', justifyContent:'space-between', alignItems:'center',
          }}>
            <span style={{color:C.textMuted}}>
              Showing <strong style={{color:C.textSub}}>{visible.length}</strong> of{' '}
              <strong style={{color:C.textSub}}>{rows.length}</strong> records
            </span>
            {dirtyCount > 0 && (
              <span style={{color:C.amber,fontWeight:600}}>
                ● {dirtyCount} unsaved change{dirtyCount>1?'s':''} — click Save Changes
              </span>
            )}
          </div>
        )}
      </div>

      <style>{`@keyframes spin{to{transform:rotate(360deg);}}`}</style>
    </div>
  )
}
