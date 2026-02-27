import { useEffect, useState } from 'react'
import { Plus, Shield, Check, X } from 'lucide-react'
import { rolesAPI } from '@/services/api'
import toast from 'react-hot-toast'

export default function RolesPage() {
  const [roles, setRoles] = useState([])
  const [permissions, setPermissions] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedRole, setSelectedRole] = useState(null)
  const [rolePerms, setRolePerms] = useState([])
  const [showCreate, setShowCreate] = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const [r, p] = await Promise.allSettled([rolesAPI.list(), rolesAPI.permissions()])
      if (r.status === 'fulfilled') setRoles(r.value.data.data || [])
      if (p.status === 'fulfilled') setPermissions(p.value.data.data || [])
    } finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const selectRole = (role) => {
    setSelectedRole(role)
    setRolePerms(role.permissions || [])
  }

  const togglePerm = (code) => {
    setRolePerms(rp => rp.includes(code) ? rp.filter(c => c !== code) : [...rp, code])
  }

  const savePerms = async () => {
    if (!selectedRole) return
    try {
      await rolesAPI.assignPermissions(selectedRole.id, { permission_codes: rolePerms })
      toast.success('Permissions updated')
      load()
    } catch {}
  }

  // Group permissions by module prefix
  const permGroups = permissions.reduce((acc, p) => {
    const code = p.permission_code || p
    const group = code.split('_')[0] || 'OTHER'
    if (!acc[group]) acc[group] = []
    acc[group].push(code)
    return acc
  }, {})

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Roles & Permissions</h1>
          <p className="text-gray-500 text-sm mt-0.5">Manage role definitions and permission assignments</p>
        </div>
        <button onClick={() => setShowCreate(true)} className="btn-primary"><Plus size={16} /> Create Role</button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Role List */}
        <div className="card">
          <div className="card-header"><h3 className="font-semibold">Roles</h3></div>
          <div className="divide-y">
            {loading ? (
              <div className="p-4 text-gray-400">Loading...</div>
            ) : roles.map(r => (
              <button key={r.id} onClick={() => selectRole(r)}
                className={`w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 transition-colors ${selectedRole?.id === r.id ? 'bg-primary-50 border-l-2 border-primary-600' : ''}`}>
                <Shield size={16} className={selectedRole?.id === r.id ? 'text-primary-600' : 'text-gray-400'} />
                <div>
                  <div className="text-sm font-medium text-gray-900">{r.role_name}</div>
                  <div className="text-xs text-gray-500">{r.description || 'No description'}</div>
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Permissions Editor */}
        <div className="lg:col-span-2 card">
          <div className="card-header flex items-center justify-between">
            <h3 className="font-semibold">
              {selectedRole ? `Permissions: ${selectedRole.role_name}` : 'Select a role'}
            </h3>
            {selectedRole && (
              <button onClick={savePerms} className="btn-primary btn-sm"><Check size={14} /> Save</button>
            )}
          </div>
          {selectedRole ? (
            <div className="card-body space-y-5 max-h-[600px] overflow-y-auto">
              {Object.entries(permGroups).map(([group, codes]) => (
                <div key={group}>
                  <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">{group}</div>
                  <div className="grid grid-cols-2 gap-2">
                    {codes.map(code => (
                      <label key={code} className={`flex items-center gap-2 px-3 py-2 rounded-lg border cursor-pointer transition-colors ${rolePerms.includes(code) ? 'bg-primary-50 border-primary-300' : 'bg-white border-gray-200 hover:border-gray-300'}`}>
                        <input type="checkbox" checked={rolePerms.includes(code)} onChange={() => togglePerm(code)} className="rounded text-primary-600" />
                        <span className="text-sm">{code}</span>
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="card-body text-center py-16 text-gray-400">Select a role to manage permissions</div>
          )}
        </div>
      </div>

      {showCreate && <CreateRoleModal onClose={() => setShowCreate(false)} onCreated={() => { setShowCreate(false); load() }} />}
    </div>
  )
}

function CreateRoleModal({ onClose, onCreated }) {
  const [name, setName] = useState('')
  const [desc, setDesc] = useState('')
  const [saving, setSaving] = useState(false)

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!name.trim()) return toast.error('Role name required')
    setSaving(true)
    try {
      await rolesAPI.create({ role_name: name.trim(), description: desc })
      toast.success('Role created')
      onCreated()
    } catch {} finally { setSaving(false) }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md m-4">
        <div className="flex items-center justify-between px-6 py-4 border-b">
          <h2 className="text-lg font-semibold">Create Role</h2>
          <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded-lg"><X size={18} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div><label className="label">Role Name*</label><input value={name} onChange={e => setName(e.target.value)} className="input" required placeholder="PLANNER" /></div>
          <div><label className="label">Description</label><input value={desc} onChange={e => setDesc(e.target.value)} className="input" placeholder="Planning team role" /></div>
          <div className="flex justify-end gap-3">
            <button type="button" onClick={onClose} className="btn-secondary">Cancel</button>
            <button type="submit" disabled={saving} className="btn-primary">{saving ? 'Creating...' : 'Create'}</button>
          </div>
        </form>
      </div>
    </div>
  )
}
