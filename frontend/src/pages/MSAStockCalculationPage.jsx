import React, { useState, useEffect } from 'react';
import { tablesAPI, msaAPI } from '../services/api';

function AddFilterColumnModal({ columns, existingFilters, onAdd, onClose }) {
  const [search, setSearch] = useState('');
  const availableColumns = columns.filter(
    c => !existingFilters.includes(c) && c.toLowerCase().includes(search.toLowerCase())
  );
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md" onClick={e => e.stopPropagation()}>
        <div className="p-4 border-b">
          <h3 className="font-semibold text-gray-800">Add Filter Column</h3>
        </div>
        <div className="p-3 border-b">
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search columns..."
            className="w-full px-3 py-2 text-sm border rounded-lg focus:outline-none focus:border-blue-500"
            autoFocus
          />
        </div>
        <div className="max-h-64 overflow-y-auto">
          {availableColumns.length === 0 ? (
            <div className="p-6 text-center text-gray-400">No more columns available</div>
          ) : (
            availableColumns.map(col => (
              <button
                key={col}
                onClick={() => { onAdd(col); onClose(); }}
                className="w-full text-left px-4 py-2.5 text-sm hover:bg-blue-50 flex items-center justify-between"
              >
                <span>{col}</span>
              </button>
            ))
          )}
        </div>
        <div className="p-3 border-t bg-gray-50">
          <button onClick={onClose} className="btn-secondary btn-sm w-full">Cancel</button>
        </div>
      </div>
    </div>
  );
}

export default function MSAStockCalculationPage() {
  const [date, setDate] = useState('');
  const [rdc, setRdc] = useState('');
  const [sloc, setSloc] = useState('');
  const [table, setTable] = useState('');
  const [tableList, setTableList] = useState([]);
  const [filterColumns, setFilterColumns] = useState([]); // dynamic filter columns
  const [filters, setFilters] = useState({});
  const [data, setData] = useState([]); // Table data
  const [columns, setColumns] = useState([]); // Table columns
  const [loading, setLoading] = useState(false);
  const [token, setToken] = useState('');
  const [saveStatus, setSaveStatus] = useState('');
  const [showAddColumnModal, setShowAddColumnModal] = useState(false);

  // Load table list and restore last values
  useEffect(() => {
    tablesAPI.listAllVisible().then(res => {
      setTableList(Array.isArray(res.data) ? res.data : []);
    });
    // Restore last values
    const last = localStorage.getItem('msa_stock_last');
    if (last) {
      const obj = JSON.parse(last);
      setTable(obj.table || '');
      setFilters(obj.filters || {});
      setDate(obj.date || '');
      setRdc(obj.rdc || '');
      setSloc(obj.sloc || '');
      setFilterColumns(obj.filterColumns || []);
    }
  }, []);

  // Load columns for selected table
  useEffect(() => {
    if (table) {
      tablesAPI.schema(table).then(res => {
        const cols = res.data?.columns || [];
        setColumns(cols);
        // Reset filters and filterColumns for new table
        setFilters({});
        setFilterColumns([]);
      });
    } else {
      setColumns([]);
      setFilters({});
      setFilterColumns([]);
    }
  }, [table]);
  // Filter UI (like Data Editor)
  const handleFilterChange = (col, value) => {
    setFilters(f => ({ ...f, [col]: value }));
  };
  const handleAddFilterColumn = (col) => {
    if (!filterColumns.includes(col)) setFilterColumns(prev => [...prev, col]);
  };
  const handleRemoveFilterColumn = (col) => {
    setFilterColumns(prev => prev.filter(c => c !== col));
    setFilters(f => { const n = { ...f }; delete n[col]; return n; });
  };

  // Save last values
  const saveLastValues = () => {
    localStorage.setItem('msa_stock_last', JSON.stringify({
      table, filters, date, rdc, sloc, filterColumns
    }));
  };
  // Fetch data based on filters
  const handleFetch = () => {
    setLoading(true);
    saveLastValues();
    // Example: fetch data from backend
    msaAPI.data({ date, rdc, sloc, table, filters }).then(res => {
      setData(res.data || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  // Save 3 different data sets to DB with token
  const handleSave = () => {
    saveLastValues();
    // Example: call backend to save 3 data sets and threshold, generate token
    fetch('/api/v1/msa/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        data1: data, data2: data, data3: data, threshold: filters.threshold, filters, table, date, rdc, sloc
      })
    })
      .then(r => r.json())
      .then(res => {
        setToken(res.data?.token || '');
        setSaveStatus(res.message || 'Saved 3 data sets with token: ' + (res.data?.token || ''));
      });
  };

  return (
    <div className="max-w-6xl mx-auto p-8">
      <h1 className="text-2xl font-bold mb-6">MSA Stock Calculation</h1>
      {/* Filter Section (like Data Editor) */}
      <div className="bg-white rounded shadow p-6 mb-8 space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
          <div>
            <label className="label">Date</label>
            <input type="date" className="input" value={date} onChange={e => setDate(e.target.value)} />
          </div>
          <div>
            <label className="label">RDC Code</label>
            <input className="input" value={rdc} onChange={e => setRdc(e.target.value)} placeholder="Enter RDC code" />
          </div>
          <div>
            <label className="label">SLOC</label>
            <input className="input" value={sloc} onChange={e => setSloc(e.target.value)} placeholder="Enter SLOC" />
          </div>
          <div>
            <label className="label">Table</label>
            <select className="input" value={table} onChange={e => setTable(e.target.value)}>
              <option value="">Select Table</option>
              {(Array.isArray(tableList) ? tableList : []).map(t => (
                <option key={t.name} value={t.name}>{t.name}</option>
              ))}
            </select>
          </div>
        </div>
        {/* Dynamic filter columns */}
        <div className="flex flex-wrap gap-4 mt-4">
          {filterColumns.map(col => (
            <div key={col}>
              <label className="label">{col === 'threshold' ? 'Threshold' : col}</label>
              <input className="input" value={filters[col] || ''} onChange={e => handleFilterChange(col, e.target.value)} placeholder={`Filter ${col}`} />
              <button className="ml-2 text-xs text-red-500" onClick={() => handleRemoveFilterColumn(col)}>Remove</button>
            </div>
          ))}
        </div>
        <button className="btn-secondary mt-2 mr-2" onClick={() => setShowAddColumnModal(true)} disabled={!table || columns.length === 0}>+ Add Filter Column</button>
        <button className="btn-primary mt-4" onClick={handleFetch} disabled={loading || !table}>
          {loading ? 'Loading...' : 'Fetch MSA Data'}
        </button>
        {showAddColumnModal && (
          <AddFilterColumnModal
            columns={[...columns, 'threshold']}
            existingFilters={filterColumns}
            onAdd={handleAddFilterColumn}
            onClose={() => setShowAddColumnModal(false)}
          />
        )}
      </div>
      {/* Data Table placeholder */}
      <div className="bg-white rounded shadow p-4 mb-8">
        <h2 className="text-lg font-semibold mb-2">MSA Data</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs border">
            <thead>
              <tr>
                {columns.map(col => (
                  <th key={col} className="border px-2 py-1 bg-gray-50">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, idx) => (
                <tr key={idx}>
                  {columns.map(col => (
                    <td key={col} className="border px-2 py-1">{row[col]}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {data.length === 0 && <div className="text-gray-400 text-center py-8">No data loaded.</div>}
        </div>
      </div>
      {/* Save Button */}
      <button className="btn-primary" onClick={handleSave} disabled={data.length === 0 || !filters.threshold}>
        Save 3 Data Sets & Generate Token
      </button>
      {saveStatus && (
        <div className="p-4 bg-green-50 border border-green-200 rounded mt-4">
          <div className="font-semibold text-green-700">{saveStatus}</div>
        </div>
      )}
    </div>
  );
}
