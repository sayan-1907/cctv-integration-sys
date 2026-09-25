import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, Polyline, useMap } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

// Fix Leaflet's default icon path issues
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

// Component to auto-fit map bounds
const BoundsFitter = ({ geojson, sightings }) => {
  const map = useMap();
  useEffect(() => {
    if (geojson && geojson.coordinates && geojson.coordinates.length > 0) {
      // GeoJSON coords are [lng, lat], Leaflet wants [lat, lng]
      const latLngs = geojson.coordinates.map(coord => [coord[1], coord[0]]);
      const bounds = L.latLngBounds(latLngs);
      map.fitBounds(bounds, { padding: [50, 50] });
    } else if (sightings && sightings.length > 0) {
      const latLngs = sightings.map(s => [s.lat, s.lng]);
      const bounds = L.latLngBounds(latLngs);
      map.fitBounds(bounds, { padding: [50, 50], maxZoom: 15 });
    }
  }, [map, geojson, sightings]);
  return null;
};

// Create numbered icons for chronological order
const createNumberedIcon = (num) => {
  return L.divIcon({
    className: 'custom-div-icon',
    html: `<div style="background-color: #0ea5e9; color: white; width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; border: 2px solid white; box-shadow: 0 0 10px rgba(0,0,0,0.5);">${num}</div>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
};

const RouteReconstructor = ({ plateNumber }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!plateNumber) return;
    const fetchRoute = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch('/api/vehicles/track', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ plate_number: plateNumber })
        });
        
        if (!response.ok) throw new Error('Failed to fetch route data');
        
        const json = await response.json();
        setData(json);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    fetchRoute();
  }, [plateNumber]);

  if (loading) return <div className="text-white p-4">Reconstructing Route...</div>;
  if (error) return <div className="text-red-500 p-4">Error: {error}</div>;
  if (!data) return <div className="text-gray-400 p-4">Enter a plate to see its route.</div>;

  const { summary, sightings, route_segments, geojson } = data;

  return (
    <div className="flex h-[600px] w-full border border-slate-700 bg-slate-900 rounded-lg overflow-hidden font-sans">
      
      {/* Side Panel */}
      <div className="w-1/3 min-w-[300px] bg-slate-900 border-r border-slate-700 flex flex-col p-4 overflow-y-auto text-slate-200">
        <h2 className="text-xl font-bold text-white mb-2 uppercase tracking-wider">Route Analysis</h2>
        <h3 className="text-2xl font-mono text-cyan-400 mb-6 bg-slate-800 p-2 rounded text-center border border-cyan-900">{data.plate}</h3>
        
        {/* Telemetry */}
        <div className="grid grid-cols-2 gap-4 mb-6">
          <div className="bg-slate-800 p-3 rounded shadow-inner border border-slate-700">
            <div className="text-xs text-slate-400 uppercase">Total Distance</div>
            <div className="text-lg font-bold text-white">{summary.total_distance_km} km</div>
          </div>
          <div className="bg-slate-800 p-3 rounded shadow-inner border border-slate-700">
            <div className="text-xs text-slate-400 uppercase">Total Duration</div>
            <div className="text-lg font-bold text-white">{summary.total_duration_min} min</div>
          </div>
          <div className="bg-slate-800 p-3 rounded shadow-inner border border-slate-700">
            <div className="text-xs text-slate-400 uppercase">Avg Speed</div>
            <div className="text-lg font-bold text-white">{summary.avg_speed_kmh} km/h</div>
          </div>
          <div className="bg-slate-800 p-3 rounded shadow-inner border border-slate-700">
            <div className="text-xs text-slate-400 uppercase">Confidence</div>
            <div className={`text-lg font-bold ${summary.confidence_score === 'High' ? 'text-green-400' : 'text-red-400'}`}>
              {summary.confidence_score}
            </div>
          </div>
        </div>

        {/* Timeline */}
        <div className="flex-1">
          <h4 className="text-sm font-semibold uppercase text-slate-400 mb-4 border-b border-slate-700 pb-2">Chronological Log</h4>
          {sightings.length === 0 ? (
            <div className="text-sm text-slate-500">No sightings found for this plate.</div>
          ) : (
            <div className="space-y-4">
              {sightings.map((s, idx) => (
                <div key={idx} className="relative pl-6 pb-2 border-l-2 border-cyan-800 last:border-0">
                  <div className="absolute -left-[9px] top-1 w-4 h-4 rounded-full bg-cyan-500 border-2 border-slate-900" />
                  <div className="text-sm font-semibold text-cyan-200">#{idx + 1} - {s.name}</div>
                  <div className="text-xs text-slate-400 mb-1">{new Date(s.timestamp).toLocaleString()}</div>
                  
                  {/* Segment Info (Speed/Time to next point) */}
                  {idx < route_segments.length && (
                    <div className="mt-2 mb-2 ml-2 p-2 bg-slate-800/50 rounded text-xs border border-slate-700/50">
                      <div className="flex justify-between items-center">
                        <span className="text-slate-400">↓ {route_segments[idx].distance_km}km</span>
                        <span className={route_segments[idx].is_anomaly ? 'text-red-400 font-bold' : 'text-emerald-400'}>
                          {route_segments[idx].speed_kmh} km/h
                          {route_segments[idx].is_anomaly && " ⚠️"}
                        </span>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Map Area */}
      <div className="flex-1 bg-slate-800 relative z-0">
        <MapContainer 
          center={[23.0225, 72.5714]} 
          zoom={12} 
          style={{ height: '100%', width: '100%', backgroundColor: '#0f172a' }}
        >
          <TileLayer
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          />
          
          <BoundsFitter geojson={geojson} sightings={sightings} />

          {/* OSRM Snapped Polyline */}
          {geojson && geojson.coordinates && (
            <Polyline 
              positions={geojson.coordinates.map(coord => [coord[1], coord[0]])}
              color="#00e5ff" 
              weight={4} 
              opacity={0.8}
            />
          )}

          {/* Markers */}
          {sightings.map((s, idx) => (
            <Marker key={idx} position={[s.lat, s.lng]} icon={createNumberedIcon(idx + 1)}>
              <Popup className="bg-slate-800 text-white">
                <div className="text-center">
                  <div className="font-bold text-sm mb-1">{s.name}</div>
                  <div className="text-xs text-slate-400 mb-2">{new Date(s.timestamp).toLocaleString()}</div>
                  {s.snapshot && (
                    <img src={s.snapshot} alt="Plate Snapshot" className="w-32 h-24 object-cover rounded border border-slate-600" />
                  )}
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
};

export default RouteReconstructor;
