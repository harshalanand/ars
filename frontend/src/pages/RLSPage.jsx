import { useEffect, useState } from 'react'
import { Plus, Trash2, Eye, Search } from 'lucide-react'
import { rlsAPI, usersAPI } from '@/services/api'
import toast from 'react-hot-toast'

export default function RLSPage() {
  const [users, setUsers] = useState([])
  const [selectedUser, setSelectedUser] = useState(null)
  const [storeAccess, setStoreAccess] = useState([])
  const [regionAccess, setRegionAccess] = useState([])
  const [stores, setStores] = useState([])
  const [loading, setLoading] = useState(true)
  const [newStore, setNewStore] = useState('')
  const [newRegion, setNewRegion] = useState('')
  const [search, setSearch] = useState('')

  useEffect(() => {
    const load = async () => {
      try {
        const [u, s] = await Promise.allSettled([usersAPI.list(), rlsAPI.stores()])
        if (u.status === 'fulfilled') setUsers(u.value.data.data || [])
        if (s.status === 'fulfilled') setStores(s.value.data.data || [])
      } finally { setLoading(false) }
    }
    load()
  }, [])

  const selectUser = async (user) => {
    setSelectedUser(user)
    try {
      const [sa, ra] = await Promise.allSettled([
        rlsAPI.storeAccess(user.user_id),
        rlsAPI.regionAccess(user.user_id),
      ])
      if (sa.status === 'fulfilled') setStoreAccess(sa.value.data.data || [])
      if (ra.status === 'fulfilled') setRegionAccess(ra.value.data.data || [])
    } catch {}
  }

  const addStore = async () => {
    if (!selectedUser || !newStore.trim()) return
    try {
      await rlsAPI.addStoreAccess({ user_id: selectedUser.user_id, store_code: newStore.trim() })
      toast.success('Store access added')
      setNewStore('')
      selectUser(selectedUser)
    } catch {}
  }

  const removeStore = async (code) => {
    if (!selectedUser) return
    try {
      await rlsAPI.deleteStoreAccess(selectedUser.user_id, code)
      toast.success('Store access removed')
      selectUser(selectedUser)
    } catch {}
  }

  const addRegion = async () => {
    if (!selectedUser || !newRegion.trim()) return
    try {
      await rlsAPI.addRegionAccess({ user_id: selectedUser.user_id, region_code: newRegion.trim() })
      toast.success('Region access added')
      setNewRegion('')
      selectUser(selectedUser)
    } catch {}
  }

  const filteredUsers = users.filter(u =>
    (u.username || '').toLowerCase().includes(search.toLowerCase()) ||
    (u.full_name || '').toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Row-Level Security</h1>
        <p className="text-gray-500 text-sm mt-0.5">Control which stores and regions each user can access</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* User List */}
        <div className="card">
          <div className="card-header">
            <div className="relative">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input value={search} onChange={e => setSearch(e.target.value)} className="input pl-8 text-sm" placeholder="Filter users..." />
            </div>
          </div>
          <div className="max-h-[500px] overflow-y-auto divide-y">
            {loading ? (
              <div className="p-4 text-gray-400 text-sm">Loading...</div>
            ) : filteredUsers.map(u => (
              <button key={u.user_id} onClick={() => selectUser(u)}
                className={`w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 ${selectedUser?.user_id === u.user_id ? 'bg-primary-50 border-l-2 border-primary-600' : ''}`}>
                <Eye size={16} className={selectedUser?.user_id === u.user_id ? 'text-primary-600' : 'text-gray-400'} />
                <div>
                  <div className="text-sm font-medium">{u.username}</div>
                  <div className="text-xs text-gray-500">{u.full_name}</div>
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Store & Region Access */}
        <div className="lg:col-span-2 space-y-5">
          {selectedUser ? (
            <>
              <div className="card p-5">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-semibold text-gray-900">Store Access: {selectedUser.username}</h3>
                  <span className="text-sm text-gray-500">{storeAccess.length} store(s)</span>
                </div>
                <div className="flex gap-2 mb-3">
                  <input value={newStore} onChange={e => setNewStore(e.target.value)} className="input flex-1" placeholder="Store code (e.g. STR-001)" list="store-list" />
                  <datalist id="store-list">
                    {stores.map(s => <option key={s.store_code || s} value={s.store_code || s} />)}
                  </datalist>
                  <button onClick={addStore} className="btn-primary btn-sm"><Plus size={14} /> Add</button>
                </div>
                <div className="flex flex-wrap gap-2">
                  {storeAccess.length === 0 ? (
                    <span className="text-sm text-gray-400">No stores assigned (user may have full access via roles)</span>
                  ) : storeAccess.map(s => {
                    const code = s.store_code || s
                    return (
                      <div key={code} className="flex items-center gap-1.5 bg-blue-50 text-blue-700 px-3 py-1.5 rounded-lg text-sm">
                        {code}
                        <button onClick={() => removeStore(code)} className="hover:text-red-500"><Trash2 size={12} /></button>
                      </div>
                    )
                  })}
                </div>
              </div>

              <div className="card p-5">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-semibold text-gray-900">Region Access</h3>
                  <span className="text-sm text-gray-500">{regionAccess.length} region(s)</span>
                </div>
                <div className="flex gap-2 mb-3">
                  <input value={newRegion} onChange={e => setNewRegion(e.target.value)} className="input flex-1" placeholder="Region code (e.g. NORTH)" />
                  <button onClick={addRegion} className="btn-primary btn-sm"><Plus size={14} /> Add</button>
                </div>
                <div className="flex flex-wrap gap-2">
                  {regionAccess.length === 0 ? (
                    <span className="text-sm text-gray-400">No regions assigned</span>
                  ) : regionAccess.map(r => {
                    const code = r.region_code || r
                    return (
                      <div key={code} className="flex items-center gap-1.5 bg-emerald-50 text-emerald-700 px-3 py-1.5 rounded-lg text-sm">
                        {code}
                      </div>
                    )
                  })}
                </div>
              </div>
            </>
          ) : (
            <div className="card p-16 text-center text-gray-400">
              <Eye size={40} className="mx-auto mb-3 opacity-30" />
              <div>Select a user to manage their data access</div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
