import React, { useState } from 'react';

const ForensicDossierModal = ({ isOpen, onClose }) => {
  const [plate, setPlate] = useState('');
  const [startTime, setStartTime] = useState('');
  const [endTime, setEndTime] = useState('');
  const [operator, setOperator] = useState('');
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  if (!isOpen) return null;

  const handleGenerate = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setSuccess(false);

    try {
      // Ensure times are in ISO format for the backend
      const startIso = new Date(startTime).toISOString();
      const endIso = new Date(endTime).toISOString();

      const res = await fetch('/api/evidence/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          plate_number: plate,
          start_time: startIso,
          end_time: endIso,
          operator_name: operator
        })
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || 'Failed to generate dossier');
      }

      // Handle file download
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      // Extract filename from Content-Disposition if possible, else fallback
      const cd = res.headers.get('Content-Disposition');
      let filename = `Dossier_${plate}.pdf`;
      if (cd && cd.includes('filename=')) {
        filename = cd.split('filename=')[1].replace(/"/g, '');
      }
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      
      setSuccess(true);
      setTimeout(() => {
        onClose();
        setSuccess(false);
      }, 2000);
      
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-[9999] p-4 font-sans">
      <div className="bg-[#0e131d] border border-slate-700 rounded-xl w-full max-w-lg shadow-2xl relative overflow-hidden">
        
        {/* Header */}
        <div className="bg-slate-900 border-b border-slate-700 p-6">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-bold text-white flex items-center gap-2">
              <svg className="w-6 h-6 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
              Section 65B Evidence Dossier
            </h2>
            <button onClick={onClose} className="text-slate-400 hover:text-white transition-colors">
              ✕
            </button>
          </div>
          <p className="text-sm text-slate-400 mt-2">Generate a court-admissible PDF with cryptographically hashed evidence (SHA-256).</p>
        </div>

        {/* Form */}
        <form onSubmit={handleGenerate} className="p-6 space-y-4">
          
          {error && <div className="bg-red-900/50 border border-red-500 text-red-200 p-3 rounded text-sm">{error}</div>}
          {success && <div className="bg-green-900/50 border border-green-500 text-green-200 p-3 rounded text-sm">Dossier generated and downloaded successfully!</div>}

          <div>
            <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Target Plate Number</label>
            <input 
              type="text" 
              value={plate}
              onChange={e => setPlate(e.target.value.toUpperCase())}
              placeholder="e.g. GJ01AB1234"
              className="w-full bg-[#111723] border border-slate-700 rounded-lg p-3 text-white font-mono focus:outline-none focus:border-cyan-500"
              required
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Start Time</label>
              <input 
                type="datetime-local" 
                value={startTime}
                onChange={e => setStartTime(e.target.value)}
                className="w-full bg-[#111723] border border-slate-700 rounded-lg p-3 text-white focus:outline-none focus:border-cyan-500"
                required
              />
            </div>
            <div>
              <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">End Time</label>
              <input 
                type="datetime-local" 
                value={endTime}
                onChange={e => setEndTime(e.target.value)}
                className="w-full bg-[#111723] border border-slate-700 rounded-lg p-3 text-white focus:outline-none focus:border-cyan-500"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Authorized Operator Name</label>
            <input 
              type="text" 
              value={operator}
              onChange={e => setOperator(e.target.value)}
              placeholder="e.g. Inspector R. Sharma"
              className="w-full bg-[#111723] border border-slate-700 rounded-lg p-3 text-white focus:outline-none focus:border-cyan-500"
              required
            />
          </div>

          <div className="pt-6">
            <button 
              type="submit" 
              disabled={loading}
              className={`w-full py-3 rounded-lg font-bold tracking-wide transition-all flex items-center justify-center gap-2
                ${loading ? 'bg-cyan-900 text-cyan-400 cursor-not-allowed' : 'bg-cyan-600 hover:bg-cyan-500 text-white shadow-lg shadow-cyan-900/30'}`}
            >
              {loading ? (
                <>
                  <svg className="animate-spin h-5 w-5 text-cyan-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Compiling Hashes & PDF...
                </>
              ) : (
                'Generate Certified Dossier'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default ForensicDossierModal;
