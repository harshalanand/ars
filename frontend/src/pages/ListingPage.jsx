/**
 * ListingPage — Build & view ARS_LISTING master table
 */
import { useState, useEffect, useCallback } from 'react'
import { listingAPI } from '@/services/api'
import { Link } from 'react-router-dom'
import toast from 'react-hot-toast'
import {
  List, RefreshCw, Loader2, Database, Play, Download, Filter, ChevronLeft, ChevronRight,
  Eye, X, BarChart3
} from 'lucide-react'

const C = {
  primary: '#4f46e5', primaryLt: '#eff6ff', primaryBd: '#bfdbfe',
  green: '#059669', greenBg: '#ecfdf5', greenBd: '#a7f3d0',
  amber: '#d97706', amberBg: '#fffbeb', amberBd: '#fde68a',
  red: '#dc2626', redBg: '#fef2f2', blue: '#2563eb', blueBg: '#eff6ff',
  text: '#0f172a', textSub: '#475569', textMuted: '#94a3b8',
  card: '#fff', cardBorder: '#e2e8f0', headerBg: '#f8fafc',
}

export default function ListingPage() {
  const [config, setConfig] = useState(null)
  const [loading, setLoading] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [summary, setSummary] = useState(null)
  const [preview, setPreview] = useState(null)
  const [previewPage, setPreviewPage] = useState(1)
  const [previewFilter, setPreviewFilter] = useState({ rdc: '', werks: '', maj_cat: '', is_new: '' })

  // Generate settings
  const [rdcMode, setRdcMode] = useState('all')
  const [selectedRdcs, setSelectedRdcs] = useState([])

  const loadConfig = useCallback(async () => {
    try {
      const { data } = await listingAPI.config()
      setConfig(data.data)
    } catch { toast.error('Failed to load config') }
  }, [])

  const loadSummary = useCallback(async () => {
    try {
      const { data } = await listingAPI.summary()
      setSummary(data.data)
    } catch {}
  }, [])

  useEffect(() => { loadConfig(); loadSummary() }, [])

  const handleGenerate = async () => {
    setGenerating(true)
    try {
      const { data } = await listingAPI.generate({
        rdc_mode: rdcMode,
        rdc_values: selectedRdcs,
      })
      toast.success(data.message)
      loadConfig()
      loadSummary()
      loadPreview(1)
    } catch (e) {
      toast.error(e.response?.data?.detail || 'Generate failed')
    } finally { setGenerating(false) }
  }

  const loadPreview = async (page = 1) => {
    setLoading(true)
    try {
      const params = { page, page_size: 100 }
      if (previewFilter.rdc) params.rdc = previewFilter.rdc
      if (previewFilter.werks) params.werks = previewFilter.werks
      if (previewFilter.maj_cat) params.maj_cat = previewFilter.maj_cat
      if (previewFilter.is_new !== '') params.is_new = Number(previewFilter.is_new)
      const { data } = await listingAPI.preview(params)
      setPreview(data.data)
      setPreviewPage(page)
    } catch (e) {
      if (e.response?.status === 404) setPreview(null)
      else toast.error('Failed to load preview')
    } finally { setLoading(false) }
  }

  const toggleRdc = (rdc) => {
    setSelectedRdcs(prev => prev.includes(rdc) ? prev.filter(r => r !== rdc) : [...prev, rdc])
  }

  const inp = { height: 24, fontSize: 10, padding: '0 6px', borderRadius: 4, border: '1px solid #e2e8f0', outline: 'none', background: '#fff' }
  const totalPages = preview ? Math.ceil(preview.total / preview.page_size) : 0

  return (
    <div style={{ color: C.text, fontFamily: 'inherit' }}>
      {/* Header */}
      <div style={{ marginBottom: 16 }}>
        <h1 style={{ fontSize: 18, fontWeight: 700, margin: 0, display: 'flex', alignItems: 'center', gap: 8 }}>
          <List size={20} color={C.primary}/> Listing Master
        </h1>
        <p style={{ fontSize: 12, color: C.textSub, marginTop: 4 }}>
          Build <code style={{ fontSize: 10, background: C.primaryLt, color: C.primary, padding: '1px 5px', borderRadius: 3, border: `1px solid ${C.primaryBd}` }}>ARS_LISTING</code> from
          MSA gen-art data + grid stock + store-RDC mapping
        </p>
      </div>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        {/* Left: Config & Generate */}
        <div style={{ flex: '0 0 320px', display: 'flex', flexDirection: 'column', gap: 10 }}>
          {/* Source tables status */}
          <div style={{ background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 8, padding: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.text, marginBottom: 8 }}>Source Tables</div>
            {config ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
                  <span style={{ color: C.textSub }}>ARS_MSA_GEN_ART</span>
                  <span style={{ fontWeight: 700, color: config.msa_gen_art_rows > 0 ? C.green : C.red }}>
                    {config.msa_gen_art_rows.toLocaleString()} rows
                  </span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
                  <span style={{ color: C.textSub }}>ARS_GRID_MJ_GEN_ART</span>
                  <span style={{ fontWeight: 700, color: config.grid_gen_art_rows > 0 ? C.green : C.red }}>
                    {config.grid_gen_art_rows.toLocaleString()} rows
                  </span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
                  <span style={{ color: C.textSub }}>Active Stores</span>
                  <span style={{ fontWeight: 700, color: C.blue }}>{config.store_count.toLocaleString()}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
                  <span style={{ color: C.textSub }}>ARS_LISTING</span>
                  <span style={{ fontWeight: 700, color: config.listing_exists ? C.green : C.textMuted }}>
                    {config.listing_exists ? `${config.listing_rows.toLocaleString()} rows` : 'Not generated'}
                  </span>
                </div>
              </div>
            ) : <Loader2 size={14} className="animate-spin" style={{ color: C.primary }}/>}
          </div>

          {/* RDC Mode */}
          <div style={{ background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 8, padding: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.text, marginBottom: 8 }}>RDC Mode</div>
            <div style={{ display: 'flex', gap: 4, marginBottom: 8 }}>
              {[['all', 'All'], ['own', 'Own RDC'], ['cross', 'Cross RDC']].map(([val, label]) => (
                <button key={val} onClick={() => setRdcMode(val)}
                  style={{ flex: 1, height: 26, fontSize: 9, fontWeight: 700, borderRadius: 4, cursor: 'pointer',
                    background: rdcMode === val ? C.primary : '#fff', color: rdcMode === val ? '#fff' : C.textSub,
                    border: `1px solid ${rdcMode === val ? C.primary : '#e2e8f0'}` }}>
                  {label}
                </button>
              ))}
            </div>
            <div style={{ fontSize: 9, color: C.textMuted, marginBottom: 8 }}>
              {rdcMode === 'all' && 'Include all stores across all RDCs'}
              {rdcMode === 'own' && 'Only stores tagged to selected RDC(s)'}
              {rdcMode === 'cross' && 'Stores NOT tagged to selected RDC(s) (cross-fill)'}
            </div>

            {/* RDC selector */}
            {config?.rdcs?.length > 0 && (
              <div>
                <div style={{ fontSize: 9, fontWeight: 600, color: C.textSub, marginBottom: 4 }}>
                  Select RDC(s): {selectedRdcs.length > 0 && <span style={{ color: C.primary }}>({selectedRdcs.length} selected)</span>}
                </div>
                <div style={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
                  {config.rdcs.map(rdc => {
                    const active = selectedRdcs.includes(rdc)
                    return (
                      <button key={rdc} onClick={() => toggleRdc(rdc)}
                        style={{ height: 22, padding: '0 8px', fontSize: 9, fontWeight: active ? 700 : 400, borderRadius: 4, cursor: 'pointer',
                          background: active ? C.primaryLt : '#fff', color: active ? C.primary : C.textMuted,
                          border: `1px solid ${active ? C.primary : '#e2e8f0'}` }}>
                        {rdc}
                      </button>
                    )
                  })}
                </div>
              </div>
            )}
          </div>

          {/* Generate button */}
          <button onClick={handleGenerate} disabled={generating}
            style={{ height: 36, borderRadius: 6, fontSize: 12, fontWeight: 700, color: '#fff', cursor: generating ? 'not-allowed' : 'pointer',
              background: generating ? '#94a3b8' : 'linear-gradient(135deg, #4f46e5, #7c3aed)',
              border: 'none', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
            {generating ? <Loader2 size={14} className="animate-spin"/> : <Play size={14}/>}
            {generating ? 'Generating...' : 'Generate Listing'}
          </button>

          {/* Summary */}
          {summary && summary.totals && (
            <div style={{ background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 8, padding: 12 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: C.text, marginBottom: 8, display: 'flex', alignItems: 'center', gap: 4 }}>
                <BarChart3 size={12}/> Summary
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
                {[
                  ['Total', summary.totals.total, C.text],
                  ['Existing', summary.totals.existing, C.green],
                  ['New (MSA)', summary.totals.new, C.amber],
                  ['Stores', summary.totals.stores, C.blue],
                  ['RDCs', summary.totals.rdcs, C.primary],
                  ['Gen Arts', summary.totals.gen_arts, C.textSub],
                ].map(([label, val, color]) => (
                  <div key={label} style={{ fontSize: 9, color: C.textSub }}>
                    {label}: <b style={{ color, fontSize: 11 }}>{val?.toLocaleString()}</b>
                  </div>
                ))}
              </div>

              {/* By RDC breakdown */}
              {summary.by_rdc?.length > 0 && (
                <div style={{ marginTop: 8, borderTop: '1px solid #f1f5f9', paddingTop: 6 }}>
                  <div style={{ fontSize: 9, fontWeight: 600, color: C.textSub, marginBottom: 4 }}>By RDC:</div>
                  {summary.by_rdc.map(r => (
                    <div key={r.rdc} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, padding: '1px 0' }}>
                      <span style={{ fontWeight: 600 }}>{r.rdc}</span>
                      <span>{r.total.toLocaleString()} <span style={{ color: C.green }}>({r.existing.toLocaleString()}</span> + <span style={{ color: C.amber }}>{r.new.toLocaleString()}</span>)</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Right: Preview table */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ background: C.card, border: `1px solid ${C.cardBorder}`, borderRadius: 8, overflow: 'hidden' }}>
            {/* Preview header */}
            <div style={{ padding: '8px 12px', background: C.headerBg, borderBottom: `1px solid ${C.cardBorder}`,
              display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 6 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Eye size={12} color={C.textSub}/>
                <span style={{ fontSize: 11, fontWeight: 700 }}>Preview</span>
                {preview && <span style={{ fontSize: 9, color: C.textMuted }}>({preview.total.toLocaleString()} rows)</span>}
              </div>
              <div style={{ display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap' }}>
                <input value={previewFilter.rdc} onChange={e => setPreviewFilter(p => ({ ...p, rdc: e.target.value }))}
                  placeholder="RDC" style={{ ...inp, width: 60 }}/>
                <input value={previewFilter.werks} onChange={e => setPreviewFilter(p => ({ ...p, werks: e.target.value }))}
                  placeholder="Store" style={{ ...inp, width: 60 }}/>
                <input value={previewFilter.maj_cat} onChange={e => setPreviewFilter(p => ({ ...p, maj_cat: e.target.value }))}
                  placeholder="MAJ_CAT" style={{ ...inp, width: 80 }}/>
                <select value={previewFilter.is_new} onChange={e => setPreviewFilter(p => ({ ...p, is_new: e.target.value }))}
                  style={{ ...inp, width: 70, cursor: 'pointer' }}>
                  <option value="">All</option>
                  <option value="1">New only</option>
                  <option value="0">Existing</option>
                </select>
                <button onClick={() => loadPreview(1)} disabled={loading}
                  style={{ height: 24, padding: '0 8px', borderRadius: 4, fontSize: 9, fontWeight: 700,
                    background: C.primary, color: '#fff', border: 'none', cursor: 'pointer',
                    display: 'flex', alignItems: 'center', gap: 3 }}>
                  {loading ? <Loader2 size={9} className="animate-spin"/> : <RefreshCw size={9}/>} Fetch
                </button>
              </div>
            </div>

            {/* Table */}
            {preview?.data?.length > 0 ? (
              <>
                <div style={{ overflowX: 'auto', maxHeight: 'calc(100vh - 280px)' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 9 }}>
                    <thead>
                      <tr style={{ background: C.headerBg }}>
                        {preview.columns.map(col => (
                          <th key={col} style={{ padding: '4px 6px', textAlign: col === 'IS_NEW' ? 'center' : 'left',
                            borderBottom: '1px solid #e2e8f0', fontWeight: 700, fontSize: 8,
                            color: C.textSub, whiteSpace: 'nowrap', position: 'sticky', top: 0, background: C.headerBg }}>
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {preview.data.map((row, i) => (
                        <tr key={i} style={{ background: row.IS_NEW ? '#fffbeb' : i % 2 ? '#fafbfc' : '#fff' }}>
                          {preview.columns.map(col => (
                            <td key={col} style={{ padding: '3px 6px', borderBottom: '1px solid #f1f5f9',
                              whiteSpace: 'nowrap', fontFamily: typeof row[col] === 'number' ? 'monospace' : 'inherit',
                              textAlign: col === 'IS_NEW' ? 'center' : typeof row[col] === 'number' ? 'right' : 'left',
                              color: col === 'IS_NEW' ? (row[col] ? C.amber : C.green) : C.text,
                              fontWeight: col === 'IS_NEW' ? 700 : 400 }}>
                              {col === 'IS_NEW' ? (row[col] ? 'NEW' : 'OK')
                                : typeof row[col] === 'number' ? row[col].toLocaleString()
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
                      style={{ ...inp, cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
                      <ChevronLeft size={10}/> Prev
                    </button>
                    <button disabled={previewPage >= totalPages} onClick={() => loadPreview(previewPage + 1)}
                      style={{ ...inp, cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
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
