import { useState, useRef } from 'react'
import { Upload, FileSpreadsheet, Eye, ArrowRight, Check, AlertCircle } from 'lucide-react'
import { uploadAPI, tablesAPI } from '@/services/api'
import toast from 'react-hot-toast'
import { useEffect } from 'react'

export default function UploadPage() {
  const [file, setFile] = useState(null)
  const [tables, setTables] = useState([])
  const [selectedTable, setSelectedTable] = useState('')
  const [pkColumns, setPkColumns] = useState('')
  const [preview, setPreview] = useState(null)
  const [result, setResult] = useState(null)
  const [progress, setProgress] = useState(0)
  const [uploading, setUploading] = useState(false)
  const [step, setStep] = useState(1) // 1=select, 2=preview, 3=upload, 4=done
  const fileRef = useRef()

  useEffect(() => {
    tablesAPI.listAll().then(r => setTables(r.data.data || [])).catch(() => {})
  }, [])

  const handleFileSelect = (f) => {
    setFile(f); setPreview(null); setResult(null); setStep(1)
  }

  const handleDrop = (e) => {
    e.preventDefault()
    const f = e.dataTransfer.files[0]
    if (f) handleFileSelect(f)
  }

  const handlePreview = async () => {
    if (!file) return
    const fd = new FormData(); fd.append('file', file); fd.append('rows', '20')
    try {
      const { data } = await uploadAPI.preview(fd)
      setPreview(data.data)
      setStep(2)
    } catch {}
  }

  const handleUpload = async () => {
    if (!file || !selectedTable || !pkColumns.trim()) return toast.error('Select table and enter PK columns')
    setUploading(true); setProgress(0); setStep(3)
    const fd = new FormData()
    fd.append('file', file)
    fd.append('table_name', selectedTable)
    fd.append('primary_key_columns', pkColumns.trim())
    try {
      const { data } = await uploadAPI.upload(fd, (e) => {
        if (e.total) setProgress(Math.round((e.loaded / e.total) * 100))
      })
      setResult(data.data)
      setStep(4)
      toast.success('Upload complete!')
    } catch {} finally { setUploading(false) }
  }

  return (
    <div className="space-y-6 max-w-4xl">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Upload Data</h1>
        <p className="text-gray-500 text-sm mt-0.5">Upload CSV or Excel files to upsert into database tables</p>
      </div>

      {/* Steps indicator */}
      <div className="flex items-center gap-2 text-sm">
        {[{ n: 1, l: 'Select File' }, { n: 2, l: 'Preview' }, { n: 3, l: 'Upload' }, { n: 4, l: 'Done' }].map((s, i) => (
          <div key={s.n} className="flex items-center gap-2">
            <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-medium ${step >= s.n ? 'bg-primary-600 text-white' : 'bg-gray-100 text-gray-400'}`}>
              {step > s.n ? <Check size={14} /> : s.n}
            </div>
            <span className={step >= s.n ? 'text-gray-900 font-medium' : 'text-gray-400'}>{s.l}</span>
            {i < 3 && <ArrowRight size={14} className="text-gray-300 mx-1" />}
          </div>
        ))}
      </div>

      {/* Drop zone */}
      <div
        className="card border-2 border-dashed border-gray-300 hover:border-primary-400 transition-colors cursor-pointer"
        onClick={() => fileRef.current?.click()}
        onDragOver={e => e.preventDefault()}
        onDrop={handleDrop}
      >
        <div className="p-10 text-center">
          <Upload size={40} className="mx-auto text-gray-300 mb-3" />
          <div className="text-sm text-gray-600 font-medium">{file ? file.name : 'Drag & drop or click to select file'}</div>
          <div className="text-xs text-gray-400 mt-1">Supports CSV, XLSX, XLS • Max 500MB</div>
          {file && <div className="text-xs text-primary-600 mt-2">{(file.size / 1024 / 1024).toFixed(1)} MB</div>}
        </div>
        <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" className="hidden" onChange={e => handleFileSelect(e.target.files[0])} />
      </div>

      {/* Config */}
      {file && (
        <div className="card p-5 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="label">Target Table</label>
              <select value={selectedTable} onChange={e => setSelectedTable(e.target.value)} className="input">
                <option value="">Select a table...</option>
                {tables.map(t => <option key={t.table_name} value={t.table_name}>{t.table_name}</option>)}
              </select>
            </div>
            <div>
              <label className="label">Primary Key Columns (comma-separated)</label>
              <input value={pkColumns} onChange={e => setPkColumns(e.target.value)} className="input" placeholder="store_code, variant_code" />
            </div>
          </div>
          <div className="flex gap-3">
            <button onClick={handlePreview} className="btn-secondary"><Eye size={14} /> Preview</button>
            <button onClick={handleUpload} disabled={uploading || !selectedTable || !pkColumns} className="btn-primary">
              <FileSpreadsheet size={14} /> {uploading ? 'Uploading...' : 'Upload & Upsert'}
            </button>
          </div>
        </div>
      )}

      {/* Preview */}
      {preview && step >= 2 && (
        <div className="card">
          <div className="card-header"><h3 className="font-semibold">File Preview ({preview.preview_rows} rows, {preview.total_columns} columns)</h3></div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="bg-gray-50 border-b">
                {preview.columns?.map(c => (
                  <th key={c.name} className="px-3 py-2 text-left font-medium text-gray-600">
                    {c.name}<div className="text-xs text-gray-400 font-normal">{c.dtype}</div>
                  </th>
                ))}
              </tr></thead>
              <tbody>
                {preview.data?.slice(0, 10).map((row, i) => (
                  <tr key={i} className="border-b hover:bg-gray-50">
                    {preview.columns?.map(c => <td key={c.name} className="px-3 py-2 text-gray-700 truncate max-w-[200px]">{row[c.name] ?? ''}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Progress */}
      {uploading && (
        <div className="card p-5">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium">Uploading...</span>
            <span className="text-sm text-gray-500">{progress}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div className="bg-primary-600 h-2 rounded-full transition-all" style={{ width: `${progress}%` }} />
          </div>
        </div>
      )}

      {/* Result */}
      {result && step === 4 && (
        <div className="card p-5 border-emerald-200 bg-emerald-50">
          <div className="flex items-center gap-2 mb-3">
            <Check size={20} className="text-emerald-600" />
            <h3 className="font-semibold text-emerald-900">Upload Complete</h3>
          </div>
          <div className="grid grid-cols-4 gap-4">
            {[
              { l: 'Total Records', v: result.total_records },
              { l: 'Inserted', v: result.inserted },
              { l: 'Updated', v: result.updated },
              { l: 'Errors', v: result.errors },
            ].map(s => (
              <div key={s.l} className="text-center">
                <div className="text-2xl font-bold text-gray-900">{s.v?.toLocaleString() || 0}</div>
                <div className="text-xs text-gray-500">{s.l}</div>
              </div>
            ))}
          </div>
          <div className="text-xs text-gray-500 mt-3">Batch: {result.batch_id} • Duration: {result.duration_ms}ms</div>
        </div>
      )}
    </div>
  )
}
