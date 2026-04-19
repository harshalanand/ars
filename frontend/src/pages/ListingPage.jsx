/**
 * ListingPage — Build & view ARS_LISTING master table (Data Preparation)
 */
import { useState, useEffect, useCallback, useRef } from 'react'
import { listingAPI } from '@/services/api'
import toast from 'react-hot-toast'
import {
  List, RefreshCw, Loader2, Database, Play, ChevronLeft, ChevronRight,
  Eye, BarChart3, Search, Filter, Download, X
} from 'lucide-react'
import { C } from '@/theme/colors'

/* ── Searchable Multi-Select (dropdown only on search) ────────────────── */
function SearchSelect({ label, items, selected, setSelected, placeholder }) {
  const [search, setSearch] = useState('')
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState(0)
  const ref = useRef(null)
  const listRef = useRef(null)

  const filtered = items.filter(s =>
    search ? s.toLowerCase().includes(search.toLowerCase()) : false
  ).slice(0, 40)

  // Reset active index when filter results change
  useEffect(() => { setActiveIdx(0) }, [search])

  // Scroll active item into view when navigating with arrow keys
  useEffect(() => {
    if (!open || !listRef.current) return
    const el = listRef.current.querySelector(`[data-idx="${activeIdx}"]`)
    if (el) el.scrollIntoView({ block: 'nearest' })
  }, [activeIdx, open])

  // close dropdown on outside click
  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const toggle = (item) => {
    setSelected(prev => prev.includes(item) ? prev.filter(x => x !== item) : [...prev, item])
  }

  const handleKeyDown = (e) => {
    if (!open || filtered.length === 0) {
      if (e.key === 'ArrowDown' && search) setOpen(true)
      return
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActiveIdx(i => (i + 1) % filtered.length)
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIdx(i => (i - 1 + filtered.length) % filtered.length)
    } else if (e.key === 'Enter') {
      e.preventDefault()
      const item = filtered[activeIdx]
      if (item) {
        toggle(item)
        setSearch('')
        setOpen(false)
      }
    } else if (e.key === 'Escape') {
      setOpen(false)
    } else if (e.key === 'Tab') {
      setOpen(false)
    }
  }

  return (
    <div style={{ background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 8, padding: 12 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: C.text, marginBottom: 4, display: 'flex', alignItems: 'center', gap: 4 }}>
        <Filter size={11} color={C.primary}/>
        {label}
        {selected.length > 0
          ? <span style={{ fontSize: 9, color: C.primary, fontWeight: 600 }}>({selected.length} selected)</span>
          : <span style={{ fontSize: 9, color: C.textMuted, fontWeight: 400 }}>All</span>}
      </div>

      {/* Selected tags */}
      {selected.length > 0 && (
        <div style={{ display: 'flex', gap: 2, flexWrap: 'wrap', marginBottom: 4, alignItems: 'center' }}>
          {selected.map(s => (
            <span key={s} onClick={() => toggle(s)}
              style={{ fontSize: 8, padding: '1px 5px', borderRadius: 3, cursor: 'pointer',
                background: C.primaryLt, color: C.primary, border: `1px solid ${C.primaryBd}` }}>
              {s} x
            </span>
          ))}
          <button onClick={() => setSelected([])}
            style={{ fontSize: 8, color: C.red, background: 'none', border: 'none', cursor: 'pointer', textDecoration: 'underline' }}>
            Clear
          </button>
        </div>
      )}

      {/* Search + Paste input */}
      <div ref={ref} style={{ position: 'relative' }}>
        <Search size={9} style={{ position: 'absolute', left: 5, top: 7, color: C.textMuted, pointerEvents: 'none' }}/>
        <input value={search}
          onChange={e => { setSearch(e.target.value); setOpen(true) }}
          onFocus={() => { if (search) setOpen(true) }}
          onKeyDown={handleKeyDown}
          onPaste={e => {
            e.preventDefault()
            const pasted = e.clipboardData.getData('text')
            // Parse pasted: comma, newline, tab, space separated
            const vals = pasted.split(/[,\n\t;]+/).map(v => v.trim()).filter(Boolean)
            if (vals.length > 1) {
              // Multi-paste: add all valid items
              const valid = vals.filter(v => items.includes(v))
              if (valid.length > 0) {
                setSelected(prev => [...new Set([...prev, ...valid])])
                setSearch('')
              } else {
                // Try case-insensitive match
                const lower = items.map(i => ({ orig: i, low: i.toLowerCase() }))
                const matched = vals.map(v => lower.find(l => l.low === v.toLowerCase())?.orig).filter(Boolean)
                if (matched.length > 0) setSelected(prev => [...new Set([...prev, ...matched])])
              }
            } else {
              setSearch(pasted.trim())
              setOpen(true)
            }
          }}
          placeholder={placeholder || 'Search or paste multiple...'}
          style={{ height: 24, fontSize: 10, padding: '0 6px 0 18', borderRadius: 4,
            border: '1px solid #e2e8f0', outline: 'none', background: '#fff',
            width: '100%', boxSizing: 'border-box' }}/>

        {/* Dropdown */}
        {open && search && filtered.length > 0 && (
          <div ref={listRef} style={{ position: 'absolute', top: 26, left: 0, right: 0, zIndex: 20,
            background: '#fff', border: '1px solid #e2e8f0', borderRadius: 4, boxShadow: '0 4px 12px rgba(0,0,0,0.1)',
            maxHeight: 120, overflowY: 'auto' }}>
            {filtered.map((item, idx) => {
              const isSel = selected.includes(item)
              const isActive = idx === activeIdx
              const bg = isActive ? '#dbeafe' : (isSel ? C.primaryLt : 'transparent')
              return (
                <div key={item} data-idx={idx}
                  onClick={() => { toggle(item); setSearch(''); setOpen(false) }}
                  onMouseEnter={() => setActiveIdx(idx)}
                  style={{ padding: '3px 8px', fontSize: 9, cursor: 'pointer',
                    background: bg,
                    color: isSel ? C.primary : C.text,
                    fontWeight: isSel ? 700 : 400,
                    borderLeft: isActive ? `2px solid ${C.primary}` : '2px solid transparent' }}>
                  {item} {isSel && '(selected)'}
                </div>
              )
            })}
          </div>
        )}
        {open && search && filtered.length === 0 && (
          <div style={{ position: 'absolute', top: 26, left: 0, right: 0, zIndex: 20,
            background: '#fff', border: '1px solid #e2e8f0', borderRadius: 4, padding: '6px 8px',
            fontSize: 9, color: C.textMuted }}>
            No match
          </div>
        )}
      </div>
    </div>
  )
}

export default function ListingPage() {
  const [config, setConfig] = useState(null)
  const [loading, setLoading] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [summary, setSummary] = useState(null)
  const [preview, setPreview] = useState(null)
  const [previewPage, setPreviewPage] = useState(1)
  const [previewPageSize, setPreviewPageSize] = useState(100)
  const [globalSearch, setGlobalSearch] = useState('')
  const [previewTable, setPreviewTable] = useState('working') // 'working' | 'listing' | 'alloc'

  // Column filters for preview (key = column name, value = filter text)
  const [colFilters, setColFilters] = useState({})

  // Generate settings
  const [rdcMode, setRdcMode] = useState('all')
  const [crossFrom, setCrossFrom] = useState([])
  const [selectedStores, setSelectedStores] = useState([])
  const [selectedMajCats, setSelectedMajCats] = useState([])
  const [runMode, setRunMode] = useState('listing') // 'listing' | 'full'
  // 'st_maj_rng' (default — 1 line per WERKS+MAJ_CAT+RNG_SEG)
  // 'st_maj'     (1 line per WERKS+MAJ_CAT)
  // 'each'       (no aggregation, keep every MIX line)
  const [mixMode, setMixMode] = useState('st_maj_rng')
  // Configurable variables
  const [stockThresholdPct, setStockThresholdPct] = useState(0.6)   // OPT_TYPE threshold (60%)
  const [excessMultiplier, setExcessMultiplier] = useState(2.0)     // Excess = STK > X × OPT_MBQ
  const [holdDays, setHoldDays] = useState(0)                       // OPT_MBQ_WH hold days
  const [ageThreshold, setAgeThreshold] = useState(15)              // AGE < X → use PER_OPT_SALE
  const [reqWeight, setReqWeight] = useState(0.4)                   // Store ranking: req weight
  const [fillWeight, setFillWeight] = useState(0.6)                 // Store ranking: fill weight
  const [enableFallback, setEnableFallback] = useState(false)       // Fallback allocation (grid demotion)

  // (Async/job/cancel facility removed — listing runs synchronously again)

  const loadConfig = useCallback(async () => {
    try {
      const { data } = await listingAPI.config()
      setConfig(data.data)
      // Restore saved settings from DB
      const s = data.data?.settings
      if (s) {
        if (s.stock_threshold_pct) setStockThresholdPct(parseFloat(s.stock_threshold_pct))
        if (s.excess_multiplier) setExcessMultiplier(parseFloat(s.excess_multiplier))
        if (s.hold_days) setHoldDays(parseInt(s.hold_days, 10))
        if (s.age_threshold) setAgeThreshold(parseInt(s.age_threshold, 10))
        if (s.mix_mode) setMixMode(s.mix_mode)
        if (s.rdc_mode) setRdcMode(s.rdc_mode)
        if (s.run_mode) setRunMode(s.run_mode)
        if (s.req_weight) setReqWeight(parseFloat(s.req_weight))
        if (s.fill_weight) setFillWeight(parseFloat(s.fill_weight))
        if (s.enable_fallback !== undefined) setEnableFallback(s.enable_fallback === 'true' || s.enable_fallback === true)
      }
    } catch { toast.error('Failed to load config') }
  }, [])

  const loadSummary = useCallback(async () => {
    try {
      const { data } = await listingAPI.summary()
      setSummary(data.data)
    } catch {}
  }, [])

  useEffect(() => { loadConfig(); loadSummary() }, [])

  // Auto-detect RDC(s) from selected stores via store_rdc_map
  const storeRdcMap = config?.store_rdc_map || {}
  const autoRdcs = [...new Set((selectedStores || []).map(s => storeRdcMap[s]).filter(Boolean))]
  const otherRdcs = (config?.rdcs || []).filter(r => !autoRdcs.includes(r))

  const handleGenerate = async () => {
    setGenerating(true)
    try {
      const payload = {
        rdc_mode: rdcMode,
        store_codes: selectedStores,
        maj_cat_values: selectedMajCats,
        run_mode: runMode,
        mix_mode: mixMode,
        stock_threshold_pct: parseFloat(stockThresholdPct) || 0.6,
        excess_multiplier: parseFloat(excessMultiplier) || 2.0,
        hold_days: parseInt(holdDays, 10) || 0,
        age_threshold: parseInt(ageThreshold, 10) || 15,
        req_weight: parseFloat(reqWeight) || 0.4,
        fill_weight: parseFloat(fillWeight) || 0.6,
        enable_fallback: !!enableFallback,
      }
      if (rdcMode === 'own') {
        payload.rdc_values = autoRdcs
      } else if (rdcMode === 'cross') {
        payload.cross_from = crossFrom
        payload.cross_to = autoRdcs
      }
      const { data } = await listingAPI.generate(payload)
      toast.success(data.message || 'Listing generated')
      loadConfig(); loadSummary(); setColFilters({}); loadPreview(1, {})
    } catch (e) {
      toast.error(e.response?.data?.detail || 'Generate failed')
    } finally {
      setGenerating(false)
    }
  }

  const getActiveFilters = (overrideFilters) => {
    const f = overrideFilters !== undefined ? overrideFilters : colFilters
    const active = {}
    for (const [k, v] of Object.entries(f)) {
      if (v && v.trim()) active[k] = v.trim()
    }
    return active
  }

  const loadPreview = async (page = 1, overrideFilters, overrideSearch, overrideTable) => {
    setLoading(true)
    try {
      const tbl = overrideTable || previewTable
      const params = { page, page_size: previewPageSize, table: tbl }
      const active = getActiveFilters(overrideFilters)
      if (Object.keys(active).length > 0) params.filters = JSON.stringify(active)
      const srch = overrideSearch !== undefined ? overrideSearch : globalSearch
      if (srch && srch.trim()) params.search = srch.trim()
      const { data } = await listingAPI.preview(params)
      setPreview(data.data)
      setPreviewPage(page)
    } catch (e) {
      if (e.response?.status === 404) setPreview(null)
      else toast.error('Failed to load preview')
    } finally { setLoading(false) }
  }

  const handleFilterKeyDown = (e) => {
    if (e.key === 'Enter') loadPreview(1)
  }

  const clearAllFilters = () => {
    setColFilters({})
    loadPreview(1, {})
  }

  const handleExport = async () => {
    try {
      toast.loading('Exporting...', { id: 'export' })
      const params = { table: previewTable }
      const active = getActiveFilters()
      if (Object.keys(active).length > 0) params.filters = JSON.stringify(active)
      const { data } = await listingAPI.export(params)
      const url = URL.createObjectURL(data)
      const a = document.createElement('a')
      a.href = url
      a.download = previewTable === 'working' ? 'ARS_LISTING_WORKING.xlsx'
                 : previewTable === 'alloc'   ? 'ARS_ALLOC_WORKING.xlsx'
                 : 'ARS_LISTING.xlsx'
      a.click()
      URL.revokeObjectURL(url)
      toast.success('Export complete', { id: 'export' })
    } catch (e) {
      toast.error('Export failed', { id: 'export' })
    }
  }

  // no manual RDC toggle needed — auto-detected from stores

  const totalPages = preview ? Math.ceil(preview.total / preview.page_size) : 0
  const hasColFilters = Object.values(colFilters).some(v => v && v.trim())

  const _btn = (active, color = C.primary) => ({
    height: 24, fontSize: 9, fontWeight: 700, borderRadius: 4, cursor: 'pointer', padding: '0 10px',
    background: active ? color : '#fff', color: active ? '#fff' : C.textSub,
    border: `1px solid ${active ? color : '#e2e8f0'}`,
  })
  const _lbl = { fontSize: 8, fontWeight: 600, color: C.textSub, marginBottom: 2, letterSpacing: '.03em' }
  const _inp = { height: 22, fontSize: 11, fontWeight: 700, textAlign: 'center', borderRadius: 4,
    border: `1px solid ${C.inputBd}`, background: C.inputBg, padding: '0 4px', width: '100%' }
  const _card = { background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 6, padding: '8px 10px' }

  return (
    <div style={{ color: C.text, fontFamily: 'inherit', display: 'flex', flexDirection: 'column', gap: 8, height: 'calc(100vh - 80px)' }}>

      {/* ═══ ROW 1: Source info + Filters + Generate + Summary ═══ */}
      <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr 1fr auto auto auto', gap: 8, alignItems: 'start' }}>

        {/* Source Tables — compact */}
        <div style={_card}>
          <div style={_lbl}>SOURCE</div>
          {config ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2, fontSize: 9, minWidth: 160 }}>
              {[
                ['MSA', config.msa_gen_art_rows, C.green],
                ['Grid', config.grid_gen_art_rows, C.green],
                ['Stores', config.store_count, C.blue],
                ['Listing', config.listing_exists ? config.listing_rows : 0,
                  config.listing_exists ? C.green : C.textMuted],
              ].map(([k, v, c]) => (
                <div key={k} style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: C.textSub }}>{k}</span>
                  <b style={{ color: c }}>{(v||0).toLocaleString()}</b>
                </div>
              ))}
            </div>
          ) : <Loader2 size={12} className="animate-spin" style={{ color: C.primary }}/>}
        </div>

        {/* Select Store */}
        <SearchSelect label="Select Store" items={config?.stores || []}
          selected={selectedStores} setSelected={setSelectedStores}
          placeholder="Search store..." />

        {/* Select MAJ_CAT */}
        <SearchSelect label="Select MAJ_CAT" items={config?.maj_cats || []}
          selected={selectedMajCats} setSelected={setSelectedMajCats}
          placeholder="Search MAJ_CAT..." />

        {/* Generate button */}
        <button onClick={handleGenerate} disabled={generating}
          style={{ height: 60, borderRadius: 6, fontSize: 12, fontWeight: 700, color: '#fff', padding: '0 20px',
            cursor: generating ? 'not-allowed' : 'pointer',
            background: generating ? '#94a3b8' : runMode === 'full' ? 'linear-gradient(135deg, #7c3aed, #9333ea)' : 'linear-gradient(135deg, #4f46e5, #7c3aed)',
            border: 'none', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
          {generating ? <Loader2 size={14} className="animate-spin"/> : <Play size={14}/>}
          {generating ? 'Running...' : 'Generate'}
        </button>

        {/* Export */}
        {config?.listing_exists && (
          <button onClick={handleExport}
            style={{ height: 60, borderRadius: 6, fontSize: 10, fontWeight: 600, color: C.green, padding: '0 12px',
              background: C.greenBg, border: `1px solid ${C.greenBd}`, cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4, flexDirection: 'column' }}>
            <Download size={14}/> Export
          </button>
        )}

        {/* Summary — compact inline */}
        {summary?.totals && (
          <div style={{ ..._card, minWidth: 200 }}>
            <div style={{ ..._lbl, display: 'flex', alignItems: 'center', gap: 4 }}>
              <BarChart3 size={10}/> SUMMARY
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2px 12px', fontSize: 9 }}>
              {[
                ['Total', summary.totals.total, C.text],
                ['Existing', summary.totals.existing, C.green],
                ['New', summary.totals.new, C.amber],
                ['Stores', summary.totals.stores, C.blue],
              ].map(([k, v, c]) => (
                <div key={k}>{k}: <b style={{ color: c }}>{(v||0).toLocaleString()}</b></div>
              ))}
            </div>
            {summary.by_opt_type && (
              <div style={{ marginTop: 4, paddingTop: 4, borderTop: '1px solid #f1f5f9', display: 'flex', gap: 6, fontSize: 8, flexWrap: 'wrap' }}>
                {Object.entries(summary.by_opt_type).map(([t, n]) => (
                  <span key={t} style={{ fontWeight: 700,
                    color: t === 'RL' ? C.green : t === 'TBL' ? C.blue : t === 'TOC' ? C.amber : t === 'MIX' ? C.red : C.textMuted }}>
                    {t}:{(n||0).toLocaleString()}
                  </span>
                ))}
              </div>
            )}
            {summary.by_rdc?.length > 0 && summary.by_rdc.some(r => r.alloc_qty > 0) && (
              <div style={{ marginTop: 4, paddingTop: 4, borderTop: '1px solid #f1f5f9', fontSize: 8 }}>
                <div style={{ fontWeight: 700, color: C.textSub, marginBottom: 2 }}>ALLOC QTY / RDC</div>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  {summary.by_rdc.filter(r => r.alloc_qty > 0).map(r => (
                    <span key={r.rdc} style={{ color: C.text }}>
                      <b style={{ color: C.primary }}>{r.rdc}</b>: {(r.alloc_qty||0).toLocaleString()}
                    </span>
                  ))}
                  <span style={{ color: C.green, fontWeight: 700 }}>
                    Total: {summary.by_rdc.reduce((s, r) => s + (r.alloc_qty || 0), 0).toLocaleString()}
                  </span>
                </div>
              </div>
            )}
            {summary.alloc_by_opt_type && Object.values(summary.alloc_by_opt_type).some(v => v > 0) && (
              <div style={{ marginTop: 4, paddingTop: 4, borderTop: '1px solid #f1f5f9', fontSize: 8 }}>
                <div style={{ fontWeight: 700, color: C.textSub, marginBottom: 2 }}>ALLOC QTY / TYPE</div>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  {Object.entries(summary.alloc_by_opt_type).filter(([,v]) => v > 0).map(([t, n]) => (
                    <span key={t} style={{ fontWeight: 700,
                      color: t === 'RL' ? C.green : t === 'TBL' ? C.blue : t === 'TBC' ? C.amber : t === 'MIX' ? C.red : C.textMuted }}>
                      {t}: {(n||0).toLocaleString()}
                    </span>
                  ))}
                  <span style={{ color: C.green, fontWeight: 700 }}>
                    Total: {Object.values(summary.alloc_by_opt_type).reduce((s, v) => s + (v || 0), 0).toLocaleString()}
                  </span>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* ═══ ROW 2: RDC Mode | Run Mode | MIX Mode | Variables ═══ */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'stretch', flexWrap: 'wrap' }}>

        {/* RDC Mode */}
        <div style={_card}>
          <div style={_lbl}>RDC MODE</div>
          <div style={{ display: 'flex', gap: 3 }}>
            {[['all','All'],['own','Own'],['cross','Cross']].map(([v, l]) => (
              <button key={v} onClick={() => { setRdcMode(v); setCrossFrom([]) }} style={_btn(rdcMode===v)}>{l}</button>
            ))}
          </div>
          {rdcMode === 'own' && autoRdcs.length > 0 && (
            <div style={{ marginTop: 4, display: 'flex', gap: 3 }}>
              {autoRdcs.map(r => <span key={r} style={{ fontSize: 8, fontWeight: 700, color: C.primary, background: C.primaryLt, padding: '1px 6px', borderRadius: 3 }}>{r}</span>)}
            </div>
          )}
          {rdcMode === 'cross' && otherRdcs.length > 0 && (
            <div style={{ marginTop: 4, display: 'flex', gap: 3, flexWrap: 'wrap' }}>
              {otherRdcs.map(r => {
                const on = crossFrom.includes(r)
                return <button key={r} onClick={() => setCrossFrom(p => on ? p.filter(x=>x!==r) : [...p, r])} style={{..._btn(on, C.amber), height: 20, fontSize: 8, padding: '0 6px'}}>{r}</button>
              })}
            </div>
          )}
        </div>

        {/* Run Mode */}
        <div style={_card}>
          <div style={_lbl}>RUN MODE</div>
          <div style={{ display: 'flex', gap: 3 }}>
            {[['listing','Listing'],['full','Full Pipeline']].map(([v, l]) => (
              <button key={v} onClick={() => setRunMode(v)} style={_btn(runMode===v, v==='full'?'#7c3aed':C.primary)}>{l}</button>
            ))}
          </div>
        </div>

        {/* MIX Mode */}
        <div style={_card}>
          <div style={_lbl}>MIX ROWS</div>
          <div style={{ display: 'flex', gap: 3 }}>
            {[['st_maj_rng','MAJ+RNG'],['st_maj','MAJ'],['each','Each']].map(([v, l]) => (
              <button key={v} onClick={() => setMixMode(v)} style={_btn(mixMode===v, '#0891b2')}>{l}</button>
            ))}
          </div>
        </div>

        {/* Variables — inline row */}
        <div style={{ ..._card, flex: 1 }}>
          <div style={_lbl}>VARIABLES</div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {[
              ['Stock%', stockThresholdPct, setStockThresholdPct, 0.05, `${Math.round(stockThresholdPct*100)}%`],
              ['Excess×', excessMultiplier, setExcessMultiplier, 0.5, `${excessMultiplier}×`],
              ['Hold', holdDays, setHoldDays, 1, `${holdDays}d`],
              ['AGE<', ageThreshold, setAgeThreshold, 1, `${ageThreshold}d`],
              ['Req%', reqWeight, setReqWeight, 0.1, `${Math.round(reqWeight*100)}%`],
              ['Fill%', fillWeight, setFillWeight, 0.1, `${Math.round(fillWeight*100)}%`],
            ].map(([label, val, setter, step, hint]) => (
              <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                <span style={{ fontSize: 8, color: C.textSub, whiteSpace: 'nowrap' }}>{label}</span>
                <input type="number" step={step} value={val} onChange={e => setter(e.target.value)}
                  style={{ ..._inp, width: 50 }}/>
                <span style={{ fontSize: 7, color: C.textMuted }}>{hint}</span>
              </div>
            ))}
            {/* Fallback toggle */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginLeft: 4,
              cursor: 'pointer', userSelect: 'none' }}
              onClick={() => setEnableFallback(f => !f)}>
              <div style={{ width: 14, height: 14, borderRadius: 3,
                border: `2px solid ${enableFallback ? C.primary : C.textMuted}`,
                background: enableFallback ? C.primary : 'transparent',
                display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                {enableFallback && <span style={{ color: '#fff', fontSize: 10, fontWeight: 700, lineHeight: 1 }}>✓</span>}
              </div>
              <span style={{ fontSize: 8, color: enableFallback ? C.primary : C.textSub, fontWeight: enableFallback ? 700 : 400 }}>Fallback</span>
            </div>
          </div>
        </div>

        {/* ═══ ROW 3: Full-width Preview ═══ */}
        <div style={{ width: '100%' }}>
          <div style={{ background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 8, overflow: 'hidden' }}>
            {/* Preview header bar with search + page size */}
            <div style={{ padding: '6px 12px', background: C.headerBg, borderBottom: `1px solid ${C.cardBorder}`,
              display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Eye size={12} color={C.textSub}/>
                {/* Table toggle: Working / Full Listing / Alloc */}
                {[['working', 'Working', C.green], ['listing', 'Full Listing', C.primary], ['alloc', 'Alloc', C.amber]].map(([v, l, clr]) => (
                  <button key={v}
                    onClick={() => { setPreviewTable(v); setColFilters({}); loadPreview(1, {}, undefined, v) }}
                    style={{ height: 22, fontSize: 9, fontWeight: 700, borderRadius: 4, padding: '0 8px', cursor: 'pointer',
                      background: previewTable === v ? clr : '#fff',
                      color: previewTable === v ? '#fff' : C.textSub,
                      border: `1px solid ${previewTable === v ? clr : '#e2e8f0'}` }}>
                    {l}
                  </button>
                ))}
                {preview && <span style={{ fontSize: 9, color: C.textMuted }}>({preview.total.toLocaleString()} rows)</span>}
              </div>
              <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
                {/* Global search */}
                <div style={{ position: 'relative' }}>
                  <Search size={10} style={{ position: 'absolute', left: 5, top: 6, color: C.textMuted, pointerEvents: 'none' }}/>
                  <input value={globalSearch}
                    onChange={e => setGlobalSearch(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') loadPreview(1, undefined, globalSearch) }}
                    placeholder="Search all columns..."
                    style={{ height: 24, width: 200, fontSize: 10, padding: '0 6px 0 20', borderRadius: 4,
                      border: `1px solid ${globalSearch ? C.primaryBd : '#e2e8f0'}`,
                      background: globalSearch ? '#eff6ff' : '#fff', outline: 'none' }}/>
                </div>
                {/* Page size */}
                <select value={previewPageSize}
                  onChange={e => setPreviewPageSize(parseInt(e.target.value, 10))}
                  style={{ height: 24, fontSize: 10, borderRadius: 3, border: '1px solid #e2e8f0', padding: '0 4px' }}>
                  {[50, 100, 200, 500, 1000, 2000, 5000].map(n => (
                    <option key={n} value={n}>{n} rows</option>
                  ))}
                </select>
                {hasColFilters && (
                  <button onClick={clearAllFilters}
                    style={{ height: 22, padding: '0 6px', borderRadius: 3, fontSize: 8, fontWeight: 600,
                      background: '#fef2f2', color: C.red, border: '1px solid #fecaca', cursor: 'pointer',
                      display: 'flex', alignItems: 'center', gap: 2 }}>
                    <X size={8}/> Clear Filters
                  </button>
                )}
                <button onClick={() => loadPreview(1)} disabled={loading}
                  style={{ height: 22, padding: '0 8px', borderRadius: 3, fontSize: 9, fontWeight: 700,
                    background: C.primary, color: '#fff', border: 'none', cursor: 'pointer',
                    display: 'flex', alignItems: 'center', gap: 3 }}>
                  {loading ? <Loader2 size={9} className="animate-spin"/> : <RefreshCw size={9}/>} Fetch
                </button>
              </div>
            </div>

            {/* Table with column filters */}
            {preview?.data?.length > 0 ? (
              <>
                <div style={{ overflowX: 'auto', maxHeight: 'calc(100vh - 350px)' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 9 }}>
                    <thead>
                      {/* Column headers */}
                      <tr style={{ background: C.headerBg }}>
                        {preview.columns.map(col => (
                          <th key={col} style={{ padding: '4px 5px', textAlign: col === 'IS_NEW' ? 'center' : 'left',
                            borderBottom: '1px solid #e2e8f0', fontWeight: 700, fontSize: 8,
                            color: C.textSub, whiteSpace: 'nowrap', position: 'sticky', top: 0, background: C.headerBg, zIndex: 2 }}>
                            {col}
                          </th>
                        ))}
                      </tr>
                      {/* Filter row */}
                      <tr style={{ background: '#f1f5f9' }}>
                        {preview.columns.map(col => (
                          <th key={`f-${col}`} style={{ padding: '2px 2px', borderBottom: '1px solid #e2e8f0',
                            position: 'sticky', top: 22, background: '#f1f5f9', zIndex: 2 }}>
                            <div style={{ position: 'relative' }}>
                              <Filter size={7} style={{ position: 'absolute', left: 2, top: 5, color: colFilters[col] ? C.primary : '#cbd5e1', pointerEvents: 'none' }}/>
                              <input
                                value={colFilters[col] || ''}
                                onChange={e => setColFilters(prev => ({ ...prev, [col]: e.target.value }))}
                                onKeyDown={handleFilterKeyDown}
                                style={{ width: '100%', minWidth: 30, height: 18, fontSize: 8, padding: '0 3px 0 12',
                                  border: `1px solid ${colFilters[col] ? C.primaryBd : '#e2e8f0'}`, borderRadius: 2,
                                  outline: 'none', background: colFilters[col] ? '#eff6ff' : '#fff', boxSizing: 'border-box' }}
                              />
                            </div>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {preview.data.map((row, i) => (
                        <tr key={i} style={{ background: row.IS_NEW ? '#fffbeb' : i % 2 ? '#fafbfc' : '#fff' }}>
                          {preview.columns.map(col => (
                            <td key={col} style={{ padding: '3px 5px', borderBottom: '1px solid #f1f5f9',
                              whiteSpace: 'nowrap', fontFamily: typeof row[col] === 'number' ? 'monospace' : 'inherit',
                              textAlign: col === 'IS_NEW' ? 'center' : typeof row[col] === 'number' ? 'right' : 'left',
                              color: col === 'IS_NEW' ? (row[col] ? C.amber : C.green)
                                : col === 'OPT_TYPE' ? (row[col] === 'RL' ? C.green : row[col] === 'NL' ? C.amber : row[col] === 'MIX-L' ? C.red : C.textMuted)
                                : C.text,
                              fontWeight: col === 'IS_NEW' || col === 'OPT_TYPE' ? 700 : 400 }}>
                              {col === 'IS_NEW' ? (row[col] ? 'NEW' : 'OK')
                                : col === 'OPT_TYPE' ? (row[col] || '-')
                                : col === 'GEN_ART_NUMBER' || col === 'ARTICLE_NUMBER' || col === 'MATNR'
                                  ? row[col] ?? ''
                                : typeof row[col] === 'number'
                                  ? (col.toUpperCase().includes('CONT') ? row[col].toFixed(4)
                                    : col.toUpperCase().includes('SAL') || col.toUpperCase().includes('SALE') ? row[col].toFixed(2)
                                    : Math.round(row[col]).toLocaleString())
                                : row[col] ?? ''}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {/* Pagination */}
                <div style={{ padding: '6px 12px', borderTop: '1px solid #e2e8f0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontSize: 9, color: C.textMuted }}>
                    Page {previewPage} of {totalPages} ({preview.total.toLocaleString()} rows)
                  </span>
                  <div style={{ display: 'flex', gap: 4 }}>
                    <button disabled={previewPage <= 1} onClick={() => loadPreview(previewPage - 1)}
                      style={{ height: 22, fontSize: 9, padding: '0 6px', borderRadius: 3, border: '1px solid #e2e8f0',
                        background: '#fff', cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
                      <ChevronLeft size={10}/> Prev
                    </button>
                    <button disabled={previewPage >= totalPages} onClick={() => loadPreview(previewPage + 1)}
                      style={{ height: 22, fontSize: 9, padding: '0 6px', borderRadius: 3, border: '1px solid #e2e8f0',
                        background: '#fff', cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
                      Next <ChevronRight size={10}/>
                    </button>
                  </div>
                </div>
              </>
            ) : (
              <div style={{ padding: 30, textAlign: 'center' }}>
                <Database size={24} style={{ color: '#c7d2fe', margin: '0 auto 8px' }}/>
                <div style={{ fontSize: 11, fontWeight: 600, color: C.textSub }}>
                  {config?.listing_exists ? 'Click Fetch to load preview' : 'Generate listing first'}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
