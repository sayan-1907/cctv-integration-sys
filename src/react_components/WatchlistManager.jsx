import React, { useState, useEffect } from 'react';

const WatchlistManager = () => {
  const [targets, setTargets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Modal State
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [newPlate, setNewPlate] = useState('');
  const [newReason, setNewReason] = useState('Stolen Vehicle');
  const [newPriority, setNewPriority] = useState('high');

  const fetchTargets = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/watchlist');
      const data = await res.json();
      setTargets(data.targets || []);
    } catch (err) {
      setError('Failed to load watchlist targets');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTargets();
  }, []);

  const handleAddTarget = async (e) => {
    e.preventDefault();
    if (!newPlate.trim()) return;
    
    try {
      const res = await fetch('/api/watchlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          plate_number: newPlate,
          reason: newReason,
          priority: newPriority
        })
      });
      if (res.ok) {
        setIsModalOpen(false);
        setNewPlate('');
        fetchTargets();
      }
    } catch (err) {
      console.error(err);
      alert("Failed to add target");
    }
  };

  const handleRemove = async (plate) => {
    if (!window.confirm(`Are you sure you want to remove ${plate} from the watchlist?`)) return;
    try {
      const res = await fetch(`/api/watchlist/${plate}`, { method: 'DELETE' });
      if (res.ok) {
        fetchTargets();
      }
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="bg-[#111723] border border-[#1d2636] rounded-xl overflow-hidden font-sans text-slate-200 p-6 w-full max-w-4xl mx-auto shadow-2xl">
      <div className="flex justify-between items-center mb-6">
        <div>
          <h2 className="text-2xl font-bold text-white tracking-wide">Target Vehicle Watchlist</h2>
          <p className="text-slate-400 text-sm mt-1">Cross-referenced in real-time across the statewide grid.</p>
        </div>
        <button 
          onClick={() => setIsModalOpen(true)}
          className="bg-cyan-600 hover:bg-cyan-500 text-white px-4 py-2 rounded-lg font-semibold transition-colors flex items-center gap-2"
        >
          <span className="text-lg leading-none">+</span> Add Target
        </button>
      </div>

      {loading ? (
        <div className="text-center py-10 text-slate-400">Loading targets...</div>
      ) : error ? (
        <div className="text-center py-10 text-red-400">{error}</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-[#0e131d] border-b border-[#1d2636]">
                <th className="p-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Plate Number</th>
                <th className="p-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Reason</th>
                <th className="p-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Priority</th>
                <th className="p-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Added At</th>
                <th className="p-4 text-xs font-semibold text-slate-400 uppercase tracking-wider text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#1d2636]">
              {targets.length === 0 ? (
                <tr>
                  <td colSpan="5" className="p-8 text-center text-slate-500">No active targets on the watchlist.</td>
                </tr>
              ) : (
                targets.map((t, idx) => (
                  <tr key={idx} className="hover:bg-[#1a2333] transition-colors">
                    <td className="p-4">
                      <span className="bg-slate-800 text-cyan-300 font-mono px-2 py-1 rounded border border-cyan-900/50">
                        {t.plate_number}
                      </span>
                    </td>
                    <td className="p-4 text-sm">{t.reason}</td>
                    <td className="p-4">
                      <span className={`text-xs px-2 py-1 rounded-full uppercase font-bold tracking-wide 
                        ${t.priority === 'critical' ? 'bg-red-900/40 text-red-400 border border-red-800/50' : 
                          t.priority === 'high' ? 'bg-amber-900/40 text-amber-400 border border-amber-800/50' : 
                          'bg-slate-800 text-slate-300'}`}>
                        {t.priority}
                      </span>
                    </td>
                    <td className="p-4 text-sm text-slate-400">{new Date(t.added_at).toLocaleString()}</td>
                    <td className="p-4 text-right">
                      <button 
                        onClick={() => handleRemove(t.plate_number)}
                        className="text-red-400 hover:text-red-300 text-sm font-semibold transition-colors"
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Add Target Modal */}
      {isModalOpen && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="bg-[#0e131d] border border-[#1d2636] rounded-xl w-full max-w-md shadow-2xl p-6 relative">
            <button 
              onClick={() => setIsModalOpen(false)}
              className="absolute top-4 right-4 text-slate-400 hover:text-white"
            >
              ✕
            </button>
            <h3 className="text-xl font-bold text-white mb-6">Add Target Vehicle</h3>
            <form onSubmit={handleAddTarget} className="space-y-4">
              <div>
                <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">License Plate</label>
                <input 
                  type="text" 
                  value={newPlate} 
                  onChange={e => setNewPlate(e.target.value.toUpperCase())}
                  placeholder="e.g. GJ01AB1234"
                  className="w-full bg-[#111723] border border-[#1d2636] rounded-lg p-3 text-white font-mono placeholder:text-slate-600 focus:outline-none focus:border-cyan-500"
                  required
                />
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Reason</label>
                <input 
                  type="text" 
                  value={newReason} 
                  onChange={e => setNewReason(e.target.value)}
                  placeholder="e.g. Amber Alert, Stolen"
                  className="w-full bg-[#111723] border border-[#1d2636] rounded-lg p-3 text-white placeholder:text-slate-600 focus:outline-none focus:border-cyan-500"
                  required
                />
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Priority Level</label>
                <select 
                  value={newPriority}
                  onChange={e => setNewPriority(e.target.value)}
                  className="w-full bg-[#111723] border border-[#1d2636] rounded-lg p-3 text-white focus:outline-none focus:border-cyan-500"
                >
                  <option value="critical">CRITICAL (Immediate Intercept)</option>
                  <option value="high">HIGH (Observe & Report)</option>
                  <option value="medium">MEDIUM (Intelligence Gathering)</option>
                </select>
              </div>
              <div className="pt-4 flex gap-3">
                <button 
                  type="button" 
                  onClick={() => setIsModalOpen(false)}
                  className="flex-1 bg-slate-800 hover:bg-slate-700 text-white font-semibold py-3 rounded-lg transition-colors"
                >
                  Cancel
                </button>
                <button 
                  type="submit" 
                  className="flex-1 bg-red-600 hover:bg-red-500 text-white font-semibold py-3 rounded-lg transition-colors shadow-lg shadow-red-900/20"
                >
                  Add to Watchlist
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default WatchlistManager;
