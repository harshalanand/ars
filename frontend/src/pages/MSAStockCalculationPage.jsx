import React, { useState, useEffect } from 'react';
import { Calculator, Filter, Calendar, Save, Plus, Trash2, Download, X, ChevronDown } from 'lucide-react';
import { msaAPI } from '../services/api';
import toast from 'react-hot-toast';

function AddFilterColumnModal({ columns, existingFilters, onAdd, onClose }) {
  const [search, setSearch] = useState('');
  const availableColumns = columns.filter(
    c => !existingFilters.includes(c) && c.toLowerCase().includes(search.toLowerCase())
  );
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md animate-fade-in" onClick={e => e.stopPropagation()}>
        <div className="card-header">
          <h3 className="font-semibold text-[13px] text-gray-900">Add Filter Column</h3>
        </div>
        <div className="p-3 border-b">
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search columns..."
            className="input"
            autoFocus
          />
        </div>
        <div className="max-h-64 overflow-y-auto">
          {availableColumns.length === 0 ? (
            <div className="p-6 text-center text-gray-400 text-[11px]">No more columns available</div>
          ) : (
            availableColumns.map(col => (
              <button
                key={col}
                onClick={() => { onAdd(col); onClose(); }}
                className="w-full text-left px-4 py-2.5 text-[11px] hover:bg-primary-50 flex items-center justify-between transition-colors"
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
  const [filterColumns, setFilterColumns] = useState([]);
  const [filters, setFilters] = useState({});
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [token, setToken] = useState('');
  const [saveStatus, setSaveStatus] = useState('');
  const [showAddColumnModal, setShowAddColumnModal] = useState(false);
  const [allAvailableColumns, setAllAvailableColumns] = useState([]);
  const [availableDates, setAvailableDates] = useState([]);
  const [distinctValues, setDistinctValues] = useState({});
  const [presetName, setPresetName] = useState('msa_filter');
  const [savedPresets, setSavedPresets] = useState({});
  const [selectedPreset, setSelectedPreset] = useState('msa_filter');
  const [calculationResults, setCalculationResults] = useState(null);
  const [expandResults, setExpandResults] = useState(false);

  // Helper function to download JSON as CSV
  const downloadCSV = (data, filename) => {
    if (!data || data.length === 0) return;
    const columns = Object.keys(data[0]);
    const csvRows = [
      columns.join(','),
      ...data.map(row => columns.map(col => `"${String(row[col] ?? '').replace(/"/g, '""')}"`).join(','))
    ].join('\n');
    const blob = new Blob([csvRows], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    if (link.download !== undefined) {
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      link.setAttribute('download', filename);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    }
  };

  // Helper function to export all calculation results
  const exportAllResults = () => {
    if (!calculationResults) {
      toast.error('No calculation results found. Please run calculation first.');
      return;
    }
    
    // Export each table
    if (calculationResults.msa) downloadCSV(calculationResults.msa, `MSA_Analysis_${date}.csv`);
    if (calculationResults.msa_gen_clr) downloadCSV(calculationResults.msa_gen_clr, `Generated_Colors_${date}.csv`);
    if (calculationResults.msa_gen_clr_var) downloadCSV(calculationResults.msa_gen_clr_var, `Color_Variants_${date}.csv`);
  };

  // Export specific table by container ID
  const exportIndividualTable = (containerId) => {
    if (!calculationResults) return;
    
    let tableData = [];
    let fileName = 'export.csv';
    
    if (containerId === 'msa-results') {
      tableData = calculationResults.msa;
      fileName = `MSA_Analysis_${date}.csv`;
    } else if (containerId === 'gen-clr-results') {
      tableData = calculationResults.msa_gen_clr;
      fileName = `Generated_Colors_${date}.csv`;
    } else if (containerId === 'variants-results') {
      tableData = calculationResults.msa_gen_clr_var;
      fileName = `Color_Variants_${date}.csv`;
    }
    
    downloadCSV(tableData, fileName);
  };

  // Attach export function to window so it can be called from dynamic HTML
  useEffect(() => {
    window.exportIndividualTable = exportIndividualTable;
    return () => {
      delete window.exportIndividualTable;
    };
  }, [calculationResults, date]);

  // Populate tables when calculation results are available and drawer is expanded
  useEffect(() => {
    if (calculationResults && expandResults) {
      console.log('📊 Populating tables now that they are in DOM...');
      // Use setTimeout to ensure DOM is fully updated
      setTimeout(() => {
        displayTable('msa-results', calculationResults.msa, 'MSA Analysis');
        displayTable('gen-clr-results', calculationResults.msa_gen_clr, 'Generated Colors');
        displayTable('variants-results', calculationResults.msa_gen_clr_var, 'Color Variants');
      }, 0);
    }
  }, [calculationResults, expandResults]);

  // Initialize: Load columns and dates from MSA view + Load presets from localStorage
  useEffect(() => {
    console.log('🚀 Initializing MSA page...');
    
    // Load filter configs from API
    msaAPI.getColumns()
      .then(res => {
        console.log('✅ Full API Response:', res);
        
        // Extract data from nested response structure
        let datesList = [];
        let columnsList = [];
        let configsList = [];
        
        if (res.data && res.data.data) {
          datesList = res.data.data.dates || [];
          columnsList = res.data.data.columns || [];
          configsList = res.data.data.filter_configs || [];
        } else if (res.data) {
          console.warn('⚠️ Unexpected response structure:', res.data);
        }
        
        console.log('📅 Dates from API:', datesList);
        console.log('📊 Columns from API:', columnsList);
        console.log('📋 Filter Configs from API:', configsList);
        
        // Load filter configs from API
        if (configsList && configsList.length > 0) {
          const presetsObj = {};
          configsList.forEach(config => {
            presetsObj[config.name] = {
              id: config.id,
              name: config.name,
              created_at: config.created_at
            };
          });
          setSavedPresets(presetsObj);
          console.log('✅ Loaded filter configs:', Object.keys(presetsObj));
          
          // Auto-select first config
          if (configsList.length > 0) {
            setSelectedPreset(configsList[0].name);
            console.log('🎯 Auto-selected first config:', configsList[0].name);
          }
        }
        
        // Set columns
        if (columnsList.length > 0) {
          setAllAvailableColumns(columnsList);
          console.log('✅ Set columns:', columnsList.length);
        } else {
          console.warn('⚠️ No columns returned');
        }
        
        // Set dates and auto-select previous day (yesterday)
        if (datesList.length > 0) {
          setAvailableDates(datesList);
          // Calculate yesterday (previous day)
          const now = new Date();
          now.setDate(now.getDate() - 1); // Set to previous day
          const yesterday = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
          
          // Check if yesterday exists in the dates list, if not use first available date
          const selectedDate = datesList.includes(yesterday) ? yesterday : datesList[0];
          setDate(selectedDate);
          console.log(`✅ Set dates (${datesList.length}), auto-selected yesterday:`, selectedDate);
        } else {
          console.warn('⚠️ No dates returned, using fallback');
          // Fallback: use yesterday's date (previous day)
          const now = new Date();
          now.setDate(now.getDate() - 1); // Set to previous day
          const yesterday = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
          setAvailableDates([yesterday]);
          setDate(yesterday);
          console.log('📅 Using fallback date (yesterday):', yesterday);
        }
      })
      .catch(err => {
        console.error('❌ Error loading MSA columns:', err);
        console.error('Error details:', err.response?.data || err.message);
        
        // Fallback behavior: use yesterday (previous day)
        const now = new Date();
        now.setDate(now.getDate() - 1); // Set to previous day
        const fallbackDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
        setAvailableDates([fallbackDate]);
        setDate(fallbackDate);
        
        // Try to show error to user
        console.warn('⚠️ Using fallback date due to API error');
      });
  }, []);

  // Handle filter column selection
  const handleFilterChange = (col, value) => {
    setFilters(f => {
      const current = f[col] || [];
      if (!Array.isArray(current)) {
        return { ...f, [col]: [value] };
      }
      // Toggle: add if not present, remove if present
      if (current.includes(value)) {
        return { ...f, [col]: current.filter(v => v !== value) };
      } else {
        return { ...f, [col]: [...current, value] };
      }
    });
  };

  // Toggle filter value (for tags)
  const handleToggleFilterValue = (col, value) => {
    handleFilterChange(col, value);
  };

  // Save filter preset
  const handleSavePreset = () => {
    if (!presetName.trim()) {
      toast.error('Please enter a preset name');
      return;
    }
    
    console.log(`💾 Saving preset to backend: ${presetName}`);
    setLoading(true);
    
    const threshold = parseInt(document.getElementById('msa-threshold')?.value || 25);
    
    // Call backend to save config
    msaAPI.saveConfig({
      name: presetName,
      filter_columns: filterColumns,
      filters: filters,
      sql_agg: threshold
    })
      .then(res => {
        console.log(`✅ Saved config to backend:`, res.data.data);
        
        // Update local presets list
        const updatedPresets = { 
          ...savedPresets, 
          [presetName]: {
            id: res.data.data.id || Math.random(),
            name: presetName,
            filter_columns: filterColumns,
            filters: filters,
            created_at: new Date().toISOString()
          }
        };
        setSavedPresets(updatedPresets);
        
        // Auto-select the newly saved preset
        setSelectedPreset(presetName);
        
        console.log(`📦 All presets now:`, Object.keys(updatedPresets));
        toast.success(`Preset "${presetName}" saved successfully`);
        setLoading(false);
      })
      .catch(err => {
        console.error(`❌ Error saving preset "${presetName}":`, err);
        setLoading(false);
        toast.error(`Error saving preset: ${err.response?.data?.detail || err.message}`);
      });
  };

  // Load filter preset from backend data
  const handleLoadPreset = () => {
    if (!selectedPreset || !savedPresets[selectedPreset]) {
      toast.error('Please select a valid preset');
      return;
    }
    
    console.log(`📂 Loading preset from backend: ${selectedPreset}`);
    setLoading(true);
    
    // Call backend to get full config
    msaAPI.loadConfig(selectedPreset)
      .then(res => {
        const configData = res.data.data;
        
        console.log(`✅ Loaded from backend:`, configData);
        console.log(`   filter_columns: ${JSON.stringify(configData.filter_columns)}`);
        console.log(`   filters: ${JSON.stringify(configData.filters)}`);
        
        // Store loaded config in a temporary variable to check later
        const loadedColumns = configData.filter_columns || [];
        const loadedFilters = configData.filters || {};
        
        // Update state - use setState callback to ensure state is updated before returning
        setFilterColumns(loadedColumns);
        setFilters(loadedFilters);
        
        // Also update the window object for debugging
        window.__loadedPresetConfig = { columns: loadedColumns, filters: loadedFilters };
        console.log(`💾 Stored in window.__loadedPresetConfig`);
        
        // Fetch distinct values for each loaded column
        if (loadedColumns && Array.isArray(loadedColumns)) {
          console.log(`🔄 Fetching distinct values for ${loadedColumns.length} columns...`);
          
          setDistinctValues(prev => {
            const updated = { ...prev };
            loadedColumns.forEach(col => {
              updated[col] = []; // Initialize with empty array
            });
            return updated;
          });
          
          // Fetch distinct values for each column
          const distinctPromises = loadedColumns.map(col =>
            msaAPI.getDistinct(col, date)
              .then(res => {
                const values = res.data?.data?.values || [];
                console.log(`  ✅ ${col}: ${values.length} values`);
                setDistinctValues(prev => ({ ...prev, [col]: values }));
              })
              .catch(err => {
                console.error(`  ❌ ${col}: Error - ${err.message}`);
                setDistinctValues(prev => ({ ...prev, [col]: [] }));
              })
          );
          
          // Wait for all distinct values to load
          Promise.all(distinctPromises).then(() => {
            console.log(`✅ All distinct values loaded!`);
          });
        }
        
        if (configData.sql_agg) {
          const thresholdInput = document.getElementById('msa-threshold');
          if (thresholdInput) {
            thresholdInput.value = configData.sql_agg;
            console.log(`⚙️ Threshold set to: ${configData.sql_agg}`);
          }
        }
        
        setLoading(false);
        
        // Verify state was updated
        setTimeout(() => {
          console.log(`✅ State verification after load:`);
          console.log(`   filterColumns: ${JSON.stringify(loadedColumns)}`);
          console.log(`   filters: ${JSON.stringify(loadedFilters)}`);
          toast.success(`Preset loaded: ${loadedColumns.length} columns, ${Object.keys(loadedFilters).length} filters. Click "Apply Filters" to continue.`, { duration: 4000 });
        }, 100);
      })
      .catch(err => {
        console.error(`❌ Error loading preset:`, err);
        setLoading(false);
        toast.error(`Error: ${err.response?.data?.message || err.message}`);
      });
  };

  // Delete filter preset from backend
  const handleDeletePreset = () => {
    if (!selectedPreset || !savedPresets[selectedPreset]) {
      toast.error('Please select a valid preset');
      return;
    }
    if (!window.confirm(`Delete preset "${selectedPreset}"?`)) return;
    
    // TODO: Delete from backend
    const updatedPresets = { ...savedPresets };
    delete updatedPresets[selectedPreset];
    setSavedPresets(updatedPresets);
    
    setSelectedPreset('');
    console.log(`🗑️ Deleted preset: ${selectedPreset}`);
    toast.success(`Preset "${selectedPreset}" deleted`);
  };

  const handleAddFilterColumn = (col) => {
    if (!filterColumns.includes(col)) {
      setFilterColumns(prev => [...prev, col]);
      console.log(`📋 Added filter column: ${col}`);
      
      // Initialize with "Loading..." state
      setDistinctValues(prev => ({ ...prev, [col]: [] }));
      
      // Fetch distinct values for this column with timeout
      console.log(`🔄 Fetching distinct values for ${col}...`);
      const timeoutId = setTimeout(() => {
        console.warn(`⏱️ Timeout loading values for ${col}`);
        setDistinctValues(prev => ({ ...prev, [col]: [] }));
      }, 10000); // 10 second timeout
      
      msaAPI.getDistinct(col, date)
        .then(res => {
          clearTimeout(timeoutId);
          console.log(`📨 Full API response for ${col}:`, res);
          console.log(`📦 Response data structure:`, res.data);
          
          const values = res.data?.data?.values || res.data?.values || [];
          console.log(`✅ Loaded ${values.length} distinct values for ${col}:`, values);
          
          if (values.length === 0) {
            console.warn(`⚠️ No values returned for ${col}, checking response...`);
          }
          
          // Store values in state for dropdown
          setDistinctValues(prev => ({ ...prev, [col]: values }));
        })
        .catch(err => {
          clearTimeout(timeoutId);
          console.error(`❌ Error loading values for ${col}:`, err.message);
          console.error(`Error response:`, err.response?.data);
          // On error, set empty array so dropdown still works
          setDistinctValues(prev => ({ ...prev, [col]: [] }));
        });
    }
  };

  const handleRemoveFilterColumn = (col) => {
    setFilterColumns(prev => prev.filter(c => c !== col));
    setFilters(f => { const n = { ...f }; delete n[col]; return n; });
    setDistinctValues(prev => { const n = { ...prev }; delete n[col]; return n; });
  };

  // Apply filters
  const handleFetch = () => {
    if (!date) {
      toast.error('Please select a date');
      return;
    }

    setLoading(true);
    console.log(`\n🔍 handleFetch called at ${new Date().toLocaleTimeString()}`);
    console.log(`   Current state - filterColumns:`, filterColumns);
    console.log(`   Current state - filters:`, filters);
    console.log(`   Date:`, date);

    // Build filter object with selected values
    const filterPayload = {};
    const colsToProcess = filterColumns && filterColumns.length > 0 ? filterColumns : [];
    
    console.log(`📋 Processing ${colsToProcess.length} filter columns:`);
    
    colsToProcess.forEach(col => {
      const colFilters = filters[col];
      console.log(`   [${col}] = ${JSON.stringify(colFilters)}`);
      
      if (colFilters && Array.isArray(colFilters) && colFilters.length > 0) {
        filterPayload[col] = colFilters;
      }
    });

    console.log(`📦 Final payload:`, {
      date,
      filters: filterPayload,
      numFilters: Object.keys(filterPayload).length
    });
    
    if (Object.keys(filterPayload).length === 0 && colsToProcess.length > 0) {
      console.warn(`⚠️ WARNING: No filter values in payload despite having ${colsToProcess.length} columns!`);
    }

    if (!date || !filterPayload || Object.keys(filterPayload).length === 0) {
      console.warn(`⚠️ Not sending empty request`);
      toast.warning('Please select at least one filter value');
      setLoading(false);
      return;
    }

    console.log(`📤 Sending to backend...`);
    msaAPI.applyFilters({
      date,
      filters: filterPayload
    })
      .then(res => {
        console.log(`✅ Backend response:`, res.data);
        const resultData = res.data.data;
        setColumns(resultData.columns || []);
        console.log(`📊 Result: ${resultData.row_count} rows, ${resultData.columns.length} columns, ${resultData.total_stock_qty} total stock`);
        
        // Set actual data rows or create placeholder if data not included in response
        const dataRows = resultData.data || Array(resultData.row_count).fill({});
        setData(dataRows);
        
        setLoading(false);
        toast.success(`Success! ${resultData.row_count} rows loaded. Total: ${resultData.total_stock_qty}`, { duration: 4000 });
      })
      .catch(err => {
        console.error(`❌ Error:`, err);
        console.error(`   Status:`, err.response?.status);
        console.error(`   Message:`, err.response?.data?.detail || err.message);
        setLoading(false);
        const errorMsg = err.response?.data?.detail || err.response?.data?.message || err.message || 'Unknown error';
        toast.error(`Error: ${errorMsg}`);
      });
  };

  // Calculate MSA - call new calculate endpoint
  const handleCalculateMSA = () => {
    if (!sloc) {
      toast.error('Please select SLOC(s)');
      return;
    }

    setLoading(true);
    const slocList = Array.isArray(sloc) ? sloc : [sloc];
    const threshold = parseInt(document.getElementById('msa-threshold')?.value || 25);

    console.log('🧮 Calculating MSA:', { slocs: slocList, threshold, date, filters });

    msaAPI.calculate({
      slocs: slocList,
      threshold,
      date,
      filters
    })
      .then(res => {
        console.log('✅ MSA Calculated:', res.data);
        const result = res.data.data;
        setCalculationResults(result); // Store for export
        setExpandResults(true); // Auto-expand results wrapper
        
        setSaveStatus(`✅ Calculation complete! Rows: ${result.row_counts.msa}`);
        setLoading(false);
        
        // Scroll to results with a small delay to ensure DOM is updated
        setTimeout(() => {
          const resultsElement = document.querySelector('[id*="msa-results"]');
          if (resultsElement) {
            resultsElement.scrollIntoView({ behavior: 'smooth', block: 'start' });
            console.log('📍 Scrolled to results');
          }
        }, 300);
      })
      .catch(err => {
        console.error('❌ Error calculating MSA:', err);
        const timeoutMsg = err.code === 'ECONNABORTED' 
          ? 'Calculation timed out. Try with fewer filters or a smaller date range.'
          : err.response?.data?.detail || err.message;
        setSaveStatus(`❌ Error: ${timeoutMsg}`);
        setLoading(false);
      });
  };

  // Display table results with proper framing
  const displayTable = (containerId, data, title) => {
    const container = document.getElementById(containerId);
    if (!container || !data || data.length === 0) {
      console.warn(`⚠️ Cannot display table ${containerId}:`, !container ? 'container not found' : 'no data');
      return;
    }

    const columns = data.length > 0 ? Object.keys(data[0]) : [];
    console.log(`📊 Displaying table ${containerId} with ${data.length} rows and ${columns.length} columns`);
    
    // Set inline style to ensure visibility, even if parent is hidden
    container.style.display = 'block';
    container.style.visibility = 'visible';
    container.className = 'card mb-4 animate-fade-in';
    
    let html = `
      <div class="card-header flex justify-between items-center">
        <div>
          <h2 class="font-semibold text-[13px] text-gray-900">${title}</h2>
          <p class="text-[10px] text-gray-500 mt-0.5">Total records: ${data.length}</p>
        </div>
        <button 
          onclick="window.exportIndividualTable('${containerId}')"
          class="btn-secondary btn-sm"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          Export CSV
        </button>
      </div>
      <div class="card-body">
        <div class="overflow-x-auto border border-gray-200 rounded-lg">
          <table class="w-full text-[11px]">
            <thead class="bg-gray-50 border-b-2 border-gray-200">
              <tr>
                ${columns.map(col => `<th class="px-3 py-2 text-left font-semibold text-gray-700 whitespace-nowrap">${col}</th>`).join('')}
              </tr>
            </thead>
            <tbody>
              ${data.slice(0, 100).map((row, idx) => `
                <tr class="border-b border-gray-100 hover:bg-gray-50 transition-colors ${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50/30'}">
                  ${columns.map(col => `<td class="px-3 py-2 text-gray-700">${row[col] !== null && row[col] !== undefined ? row[col] : '-'}</td>`).join('')}
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
        ${data.length > 100 ? `<div class="mt-3 p-2.5 bg-primary-50 border-l-4 border-primary-500 rounded text-[10px] text-primary-800"><strong>Showing 100 of ${data.length} rows</strong> - Export to see all</div>` : ''}
      </div>
    `;
    
    container.innerHTML = html;
  };

  // Save MSA results
  const handleSave = () => {
    if (!data || data.length === 0) {
      toast.error('No data to save');
      return;
    }

    console.log('💾 Saving MSA results...');

    msaAPI.save({
      data1: data,
      data2: data,
      data3: data,
      threshold: parseInt(document.getElementById('msa-threshold')?.value || 25),
      filters,
      date
    })
      .then(res => {
        console.log('✅ Saved:', res.data);
        const token = res.data.data.token;
        setToken(token);
        setSaveStatus(`✅ Saved with token: ${token}`);
      })
      .catch(err => {
        console.error('❌ Error saving:', err);
        setSaveStatus(`❌ Error: ${err.message}`);
      });
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-primary-100 rounded-lg">
            <Calculator size={20} className="text-primary-600" />
          </div>
          <div>
            <h1 className="page-title">MSA Stock Calculation</h1>
            <p className="page-subtitle">Configure filters, select SLOC codes, and calculate MSA analysis</p>
          </div>
        </div>
      </div>

      {/* Filter Configuration Card */}
      <div className="card">
        <div className="card-header flex items-center gap-2">
          <Filter size={16} className="text-primary-600" />
          <h2 className="font-semibold text-[13px] text-gray-900">Filter Configuration</h2>
        </div>
        
        <div className="card-body space-y-4">
          {/* Select filter columns */}
          <div>
            <label className="label mb-2">Select Filter Columns</label>
            <div className="flex flex-wrap gap-1.5 mb-3">
              {filterColumns && filterColumns.length > 0 ? (
                filterColumns.map(col => (
                  <button
                    key={col}
                    onClick={() => handleRemoveFilterColumn(col)}
                    className="inline-flex items-center gap-1 px-2.5 py-1 bg-primary-100 text-primary-700 rounded-full text-[10px] font-medium hover:bg-primary-200 transition-all"
                    type="button"
                  >
                    <span>{col}</span>
                    <X size={12} />
                  </button>
                ))
              ) : (
                <p className="text-gray-400 italic text-[11px]">No filter columns selected</p>
              )}
            </div>
            <button 
              className="btn-secondary btn-sm"
              onClick={() => setShowAddColumnModal(true)}
              disabled={!allAvailableColumns || allAvailableColumns.length === filterColumns.length}
              type="button"
            >
              <Plus size={12} /> Add Column
            </button>
            {showAddColumnModal && (
              <AddFilterColumnModal
                columns={allAvailableColumns || []}
                existingFilters={filterColumns || []}
                onAdd={handleAddFilterColumn}
                onClose={() => setShowAddColumnModal(false)}
              />
            )}
          </div>

          {/* Display selected filter values for each column */}
          {filterColumns && filterColumns.length > 0 && (
            <div className="space-y-3 border-t pt-3">
              {filterColumns.map(col => (
                <div key={col}>
                  <p className="label mb-1.5">Filter by {col}</p>
                  <div className="flex flex-wrap gap-1.5">
                    {distinctValues[col] && distinctValues[col].length > 0 ? (
                      distinctValues[col].map(val => (
                        <button
                          key={val}
                          onClick={() => handleToggleFilterValue(col, val)}
                          className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-md text-[10px] font-medium transition-all ${
                            filters[col]?.includes(val)
                              ? 'bg-red-500 hover:bg-red-600 text-white shadow-sm'
                              : 'bg-gray-100 hover:bg-gray-200 text-gray-700 border border-gray-200'
                          }`}
                          type="button"
                        >
                          <span>{val}</span>
                          {filters[col]?.includes(val) && <X size={10} />}
                        </button>
                      ))
                    ) : (
                      <p className="text-gray-400 italic text-[11px]">Loading values...</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Date & Presets Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Date Selection Card */}
        <div className="card">
          <div className="card-header flex items-center gap-2">
            <Calendar size={16} className="text-primary-600" />
            <h2 className="font-semibold text-[13px] text-gray-900">Date Selection</h2>
          </div>
          
          <div className="card-body">
            <label className="label mb-2">Select Date</label>
            <input 
              type="date"
              className="input"
              value={date}
              onChange={e => {
                const newDate = e.target.value;
                console.log('📅 Date selected:', newDate);
                setDate(newDate);
              }}
            />
          </div>
        </div>

        {/* Filter Presets Card */}
        <div className="card">
          <div className="card-header flex items-center gap-2">
            <Save size={16} className="text-primary-600" />
            <h2 className="font-semibold text-[13px] text-gray-900">Filter Presets</h2>
          </div>
          
          <div className="card-body space-y-3">
            <div>
              <label className="label mb-1.5">Select Existing Config</label>
              <select 
                className="input"
                value={selectedPreset}
                onChange={e => {
                  console.log('📝 Preset selected:', e.target.value);
                  setSelectedPreset(e.target.value);
                }}
              >
                <option value="">-- No Preset --</option>
                {Object.keys(savedPresets).length > 0 ? (
                  Object.keys(savedPresets).map(name => (
                    <option key={name} value={name}>{name}</option>
                  ))
                ) : (
                  <option disabled>No presets saved yet</option>
                )}
              </select>
              {Object.keys(savedPresets).length > 0 && (
                <p className="text-[10px] text-gray-500 mt-1">Saved: <strong>{Object.keys(savedPresets).join(', ')}</strong></p>
              )}
            </div>

            <div>
              <label className="label mb-1.5">Config Name</label>
              <input 
                type="text"
                className="input"
                value={presetName}
                onChange={e => setPresetName(e.target.value)}
                placeholder="e.g., msa_filter"
              />
            </div>

            <div className="flex gap-1.5 pt-1">
              <button 
                className="btn-primary btn-sm flex-1"
                onClick={handleSavePreset}
                type="button"
              >
                <Save size={12} /> Save
              </button>
              <button 
                className="btn-secondary btn-sm flex-1"
                onClick={handleLoadPreset}
                disabled={!selectedPreset}
                type="button"
              >
                <Download size={12} /> Load
              </button>
              <button 
                className="btn-danger btn-sm"
                onClick={handleDeletePreset}
                disabled={!selectedPreset}
                type="button"
              >
                <Trash2 size={12} />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Apply Filters Button */}
      <button 
        className="btn-primary w-full" 
        onClick={() => {
          console.log('🔍 Apply Filters clicked. Date:', date, 'Filters:', filters);
          if (!date) {
            console.warn('⚠️ No date selected');
            toast.error('Please select a date first');
            return;
          }
          handleFetch();
        }}
        disabled={loading || !date}
        type="button"
      >
        {loading ? '⏳ Loading...' : '✅ Apply Filters'}
      </button>

      {/* Configuration Summary Section */}
      {data && data.length > 0 && (
        <div className="card border-l-4 border-l-primary-500">
          <div className="card-header">
            <h2 className="font-semibold text-[13px] text-gray-900">📋 Active Configuration</h2>
          </div>
          
          <div className="card-body">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              {filterColumns && filterColumns.length > 0 && filterColumns.map(col => (
                <div key={col}>
                  <div className="text-[10px] font-semibold text-gray-600 uppercase tracking-wide mb-1.5">{col}</div>
                  {filters[col] && filters[col].length > 0 ? (
                    <div className="flex flex-wrap gap-1">
                      {filters[col].map((val, idx) => (
                        <span 
                          key={idx}
                          className="inline-flex items-center bg-primary-500 text-white px-2 py-0.5 rounded-full text-[9px] font-medium"
                        >
                          {val}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="text-gray-400 italic text-[10px]">No selection</span>
                  )}
                </div>
              ))}
            </div>

            <div className="pt-3 border-t border-gray-100 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-emerald-500 text-white text-[9px] font-bold">✓</span>
                <span className="text-[11px] text-gray-600">Status: <span className="text-emerald-600 font-semibold">Ready</span></span>
              </div>
              <p className="text-[10px] text-gray-500">📊 <strong>{data.length}</strong> rows loaded</p>
            </div>
          </div>
        </div>
      )}

      {/* MSA Calculation Section */}
      <div className="card">
        <div className="card-header">
          <h2 className="font-semibold text-[13px] text-gray-900">🧮 MSA Calculation</h2>
        </div>
        
        <div className="card-body space-y-4">
          {/* SLOC Selection */}
          <div>
            <label className="label mb-2">Select SLOC Codes <span className="text-red-500">*</span></label>
            {data && data.length > 0 && filters['SLOC'] && filters['SLOC'].length > 0 ? (
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
                {filters['SLOC'].map(slocCode => (
                  <label key={slocCode} className="flex items-center gap-2 cursor-pointer p-2.5 border border-gray-200 rounded-lg hover:bg-primary-50 hover:border-primary-300 transition-all">
                    <input 
                      type="checkbox"
                      checked={Array.isArray(sloc) ? sloc.includes(slocCode) : sloc === slocCode}
                      onChange={e => {
                        if (e.target.checked) {
                          setSloc(prev => Array.isArray(prev) ? [...prev, slocCode] : [slocCode]);
                        } else {
                          setSloc(prev => {
                            if (Array.isArray(prev)) {
                              return prev.filter(s => s !== slocCode);
                            }
                            return '';
                          });
                        }
                        console.log('🏢 SLOC(s) selected:', sloc);
                      }}
                      className="w-3.5 h-3.5 cursor-pointer text-primary-600 rounded focus:ring-primary-500"
                    />
                    <span className="font-medium text-gray-700 text-[11px]">{slocCode}</span>
                  </label>
                ))}
              </div>
            ) : (
              <input 
                type="text"
                className="input"
                placeholder="Enter SLOC code (comma-separated for multiple)"
                value={Array.isArray(sloc) ? sloc.join(', ') : sloc}
                onChange={e => {
                  const value = e.target.value;
                  if (value.includes(',')) {
                    setSloc(value.split(',').map(s => s.trim()));
                  } else {
                    setSloc(value);
                  }
                  console.log('🏢 SLOC changed:', sloc);
                }}
              />
            )}
            {sloc && (
              <p className="text-[10px] text-emerald-600 mt-1.5">
                ✅ Selected: {Array.isArray(sloc) ? sloc.join(', ') : sloc}
              </p>
            )}
          </div>

          {/* Threshold Input */}
          <div>
            <label className="label mb-2">Threshold (%) <span className="text-red-500">*</span></label>
            <input 
              type="number"
              id="msa-threshold"
              className="input"
              defaultValue={25}
              min={0}
              max={100}
              placeholder="25"
            />
          </div>

          <button 
            className="btn-primary w-full"
            onClick={() => {
              const selectedSlocs = Array.isArray(sloc) ? sloc : (sloc ? [sloc] : []);
              console.log('🧮 Calculate MSA clicked. SLOCs:', selectedSlocs, 'Threshold:', document.getElementById('msa-threshold').value);
              if (!selectedSlocs || selectedSlocs.length === 0) {
                console.warn('⚠️ No SLOC selected');
                toast.error('Please select at least one SLOC code');
                return;
              }
              handleCalculateMSA();
            }}
            disabled={loading || !sloc}
            type="button"
          >
            {loading ? '⏳ Calculating...' : '🧮 Calculate MSA'}
          </button>
        </div>
      </div>

<div className="space-y-4">
        {/* Results Sections Wrapper */}
        {calculationResults && (
          <div className="space-y-4">
            {/* Results Header */}
            <div className="card border-l-4 border-l-emerald-500 bg-emerald-50/50">
              <div className="card-header bg-gradient-to-r from-emerald-50 to-white flex items-center justify-between cursor-pointer hover:bg-emerald-100/30 transition-colors" onClick={() => setExpandResults(!expandResults)}>
                <h2 className="font-semibold text-[13px] text-emerald-900">📊 Calculation Results</h2>
                <ChevronDown size={16} className={`text-emerald-600 transition-transform ${expandResults ? 'rotate-180' : ''}`} />
              </div>
              <div className="card-body">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  {calculationResults.msa && calculationResults.msa.length > 0 && (
                    <div className="p-3 bg-white rounded-lg border border-emerald-200">
                      <div className="text-[10px] font-semibold text-gray-600 uppercase">MSA Analysis</div>
                      <div className="text-[16px] font-bold text-emerald-600 mt-1">{calculationResults.msa.length}</div>
                      <p className="text-[10px] text-gray-500 mt-1">records processed</p>
                    </div>
                  )}
                  {calculationResults.msa_gen_clr && calculationResults.msa_gen_clr.length > 0 && (
                    <div className="p-3 bg-white rounded-lg border border-blue-200">
                      <div className="text-[10px] font-semibold text-gray-600 uppercase">Generated Colors</div>
                      <div className="text-[16px] font-bold text-blue-600 mt-1">{calculationResults.msa_gen_clr.length}</div>
                      <p className="text-[10px] text-gray-500 mt-1">color combinations</p>
                    </div>
                  )}
                  {calculationResults.msa_gen_clr_var && calculationResults.msa_gen_clr_var.length > 0 && (
                    <div className="p-3 bg-white rounded-lg border border-purple-200">
                      <div className="text-[10px] font-semibold text-gray-600 uppercase">Color Variants</div>
                      <div className="text-[16px] font-bold text-purple-600 mt-1">{calculationResults.msa_gen_clr_var.length}</div>
                      <p className="text-[10px] text-gray-500 mt-1">variant records</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Result Tables Container - ALWAYS in DOM (outside conditional), shown/hidden with inline styles */}
        <div style={{ display: calculationResults && expandResults ? 'block' : 'none' }} className="border-t-2 border-gray-100 pt-4 space-y-4 transition-all duration-300">
          <div id="msa-results"></div>
          <div id="gen-clr-results"></div>
          <div id="variants-results"></div>
        </div>

        {/* Export & Save Actions */}
        <div className="flex gap-2 sticky bottom-4">
          {calculationResults && (
            <button 
              className="btn-secondary flex-1"
              onClick={exportAllResults}
              type="button"
            >
              <Download size={14} /> Export All Results as CSV
            </button>
          )}
          <button 
            className="btn-success flex-1"
            onClick={() => {
              console.log('💾 Save Results clicked. Data:', data);
              if (!data || (Array.isArray(data) && data.length === 0)) {
                console.warn('⚠️ No data to save');
                toast.error('Please calculate MSA first');
                return;
              }
              handleSave();
            }}
            disabled={!data || (Array.isArray(data) && data.length === 0)}
            type="button"
          >
            💾 Save Results
          </button>
        </div>
      </div>

      {/* Status Messages */}
      {saveStatus && (
        <div className={`card border-l-4 ${saveStatus.includes('✅') || saveStatus.includes('Successfully') 
          ? 'border-l-emerald-500 bg-emerald-50/50' 
          : 'border-l-red-500 bg-red-50/50'
        }`}>
          <div className="card-body">
            <div className={`font-semibold text-[11px] ${saveStatus.includes('✅') || saveStatus.includes('Successfully')
              ? 'text-emerald-700' 
              : 'text-red-700'
            }`}>
              {saveStatus}
            </div>
            {token && <div className="text-[10px] text-emerald-600 mt-1.5">✅ Token: {token}</div>}
          </div>
        </div>
      )}
    </div>
  );
}

