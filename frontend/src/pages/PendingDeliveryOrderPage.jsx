import { useState } from 'react'

export default function PendingDeliveryOrderPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Pending Delivery Orders</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">View and manage pending delivery orders</p>
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-8 text-center">
        <p className="text-gray-500 dark:text-gray-400">No pending delivery orders found.</p>
      </div>
    </div>
  )
}
