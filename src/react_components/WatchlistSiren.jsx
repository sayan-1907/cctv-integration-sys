import React, { useState, useEffect, useCallback } from 'react';

const WatchlistSiren = () => {
  const [alert, setAlert] = useState(null);

  // Play high-tech notification chime using Web Audio API
  const playSiren = useCallback(() => {
    try {
      const AudioContext = window.AudioContext || window.webkitAudioContext;
      const ctx = new AudioContext();
      
      // First beep
      const osc1 = ctx.createOscillator();
      const gain1 = ctx.createGain();
      osc1.type = 'square';
      osc1.frequency.setValueAtTime(880, ctx.currentTime); // A5
      osc1.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.1);
      gain1.gain.setValueAtTime(0.5, ctx.currentTime);
      gain1.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.1);
      osc1.connect(gain1);
      gain1.connect(ctx.destination);
      osc1.start();
      osc1.stop(ctx.currentTime + 0.1);

      // Second beep (slightly higher)
      setTimeout(() => {
        const osc2 = ctx.createOscillator();
        const gain2 = ctx.createGain();
        osc2.type = 'square';
        osc2.frequency.setValueAtTime(1200, ctx.currentTime);
        osc2.frequency.exponentialRampToValueAtTime(600, ctx.currentTime + 0.15);
        gain2.gain.setValueAtTime(0.5, ctx.currentTime);
        gain2.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.15);
        osc2.connect(gain2);
        gain2.connect(ctx.destination);
        osc2.start();
        osc2.stop(ctx.currentTime + 0.15);
      }, 150);
      
    } catch (e) {
      console.warn("Web Audio API not supported or interaction required first", e);
    }
  }, []);

  useEffect(() => {
    // Connect to the global alerts WebSocket
    const wsUrl = `ws://${window.location.host}/ws/alerts`;
    const ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        // The API sends an array of new_alerts or a broadcasted watchlist_hit
        if (data.type === "watchlist_hit") {
          setAlert(data);
          playSiren();
          
          // Auto-dismiss after 10 seconds
          setTimeout(() => {
            setAlert(current => {
              if (current && current.timestamp === data.timestamp) return null;
              return current;
            });
          }, 10000);
        } else if (data.type === "new_alerts" && Array.isArray(data.alerts)) {
            // Check if any of the new regular alerts have watchlist priority attached
            // (If integrated into the standard pipeline)
            const hits = data.alerts.filter(a => a.priority === "CRITICAL" || a.priority === "HIGH");
            if (hits.length > 0) {
                setAlert(hits[0]); // Just show the most recent
                playSiren();
            }
        }
      } catch (err) {
        console.error("Error parsing WS message in Siren:", err);
      }
    };

    return () => {
      ws.close();
    };
  }, [playSiren]);

  if (!alert) return null;

  return (
    <div className="fixed top-6 left-1/2 -translate-x-1/2 z-[9999] w-full max-w-2xl animate-[modalPopIn_0.3s_ease-out]">
      <div className={`relative overflow-hidden rounded-xl shadow-2xl shadow-red-900/40 border 
        ${alert.priority === 'CRITICAL' ? 'bg-red-950/90 border-red-500' : 'bg-amber-950/90 border-amber-500'} backdrop-blur-md`}>
        
        {/* Pulsing background effect */}
        <div className={`absolute inset-0 opacity-20 ${alert.priority === 'CRITICAL' ? 'bg-red-500' : 'bg-amber-500'} animate-pulse`} />
        
        <div className="relative p-4 sm:p-6 flex items-start gap-4">
          {/* Icon */}
          <div className={`shrink-0 w-12 h-12 rounded-full flex items-center justify-center border-2 
            ${alert.priority === 'CRITICAL' ? 'bg-red-900 border-red-500 text-red-100' : 'bg-amber-900 border-amber-500 text-amber-100'} shadow-[0_0_15px_rgba(239,68,68,0.5)]`}>
            <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
          </div>

          {/* Content */}
          <div className="flex-1 text-white">
            <h3 className={`text-lg font-bold tracking-wider uppercase mb-1 
              ${alert.priority === 'CRITICAL' ? 'text-red-400' : 'text-amber-400'}`}>
              Watchlist Hit Detected
            </h3>
            
            <div className="bg-black/40 rounded-lg p-3 border border-white/10 mb-2 font-mono text-sm">
              <div className="grid grid-cols-2 gap-2">
                <div className="text-slate-400">Target Plate:</div>
                <div className="font-bold text-cyan-300">{alert.target_plate}</div>
                
                <div className="text-slate-400">Detected As:</div>
                <div className="font-bold text-white">{alert.plate_number}</div>
                
                <div className="text-slate-400">Location:</div>
                <div className="text-white">{alert.camera_id}</div>
                
                <div className="text-slate-400">Reason:</div>
                <div className="text-white uppercase">{alert.reason}</div>
              </div>
            </div>
            
            <div className="text-xs text-slate-400">
              {new Date(alert.timestamp).toLocaleTimeString()}
            </div>
          </div>

          {/* Snapshot if available */}
          {alert.snapshot_path && (
            <div className="shrink-0 w-32 h-24 rounded overflow-hidden border-2 border-white/20">
              <img src={alert.snapshot_path} alt="Hit Snapshot" className="w-full h-full object-cover" />
            </div>
          )}

          {/* Dismiss Button */}
          <button 
            onClick={() => setAlert(null)}
            className="absolute top-2 right-2 p-2 text-white/50 hover:text-white transition-colors"
          >
            ✕
          </button>
        </div>
      </div>
    </div>
  );
};

export default WatchlistSiren;
