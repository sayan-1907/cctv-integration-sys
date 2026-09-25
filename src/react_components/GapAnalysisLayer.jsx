import React, { useState, useEffect } from 'react';
import { Circle, Marker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import ReactDOM from 'react-dom';

// ─── Custom Icons (lazy-initialised to avoid module-level Leaflet crash) ───
let _recommendedIcon = null;
let _gapIcon = null;

const getRecommendedIcon = () => {
  if (!_recommendedIcon) {
    _recommendedIcon = L.divIcon({
      className: '',
      html: `<div style="background-color:#f59e0b;color:#fff;width:22px;height:22px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:16px;border:2px solid white;box-shadow:0 0 15px rgba(245,158,11,0.9);">+</div>`,
      iconSize: [22, 22],
      iconAnchor: [11, 11]
    });
  }
  return _recommendedIcon;
};

const getGapIcon = () => {
  if (!_gapIcon) {
    _gapIcon = L.divIcon({
      className: '',
      html: `<div style="background-color:#ef4444;width:18px;height:18px;border-radius:50%;border:2px solid white;box-shadow:0 0 20px rgba(239,68,68,1);animation:pulseGlow 1.5s infinite;"></div>`,
      iconSize: [18, 18],
      iconAnchor: [9, 9]
    });
  }
  return _gapIcon;
};

// ─── HUD Stats Panel rendered as a Leaflet Control into the map container ───
const CoverageHUD = ({ data }) => {
  const map = useMap();

  useEffect(() => {
    if (!data) return;

    // Mount a React-managed DOM node as a Leaflet control
    const ControlClass = L.Control.extend({
      onAdd() {
        const div = L.DomUtil.create('div', '');
        // Prevent map click/scroll events from passing through the panel
        L.DomEvent.disableClickPropagation(div);
        L.DomEvent.disableScrollPropagation(div);
        ReactDOM.render(<HUDContent data={data} />, div);
        this._div = div;
        return div;
      },
      onRemove() {
        ReactDOM.unmountComponentAtNode(this._div);
      }
    });

    const control = new ControlClass({ position: 'bottomleft' });
    control.addTo(map);

    return () => {
      map.removeControl(control);
    };
  }, [map, data]);

  return null;
};

const HUDContent = ({ data }) => (
  <div style={{
    background: 'rgba(15,23,42,0.92)', border: '1px solid #334155',
    borderRadius: '10px', padding: '14px 18px', color: 'white',
    fontFamily: 'Inter, sans-serif', minWidth: '200px',
    boxShadow: '0 8px 32px rgba(0,0,0,0.5)'
  }}>
    <div style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '2px', color: '#64748b', textTransform: 'uppercase', borderBottom: '1px solid #1e293b', paddingBottom: '8px', marginBottom: '12px' }}>
      Coverage Analytics
    </div>
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
      <div>
        <div style={{ fontSize: '9px', color: '#64748b', textTransform: 'uppercase' }}>Covered Area</div>
        <div style={{ fontSize: '20px', fontWeight: 700, color: '#34d399' }}>{data.total_covered_area_km2} <span style={{ fontSize: '12px', color: '#94a3b8', fontWeight: 400 }}>km²</span></div>
      </div>
      <div>
        <div style={{ fontSize: '9px', color: '#64748b', textTransform: 'uppercase' }}>Critical Gaps</div>
        <div style={{ fontSize: '20px', fontWeight: 700, color: '#f87171' }}>{data.coverage_gaps?.length || 0}</div>
      </div>
      <div style={{ gridColumn: 'span 2', borderTop: '1px solid #1e293b', paddingTop: '8px' }}>
        <div style={{ fontSize: '9px', color: '#64748b', textTransform: 'uppercase' }}>Suggested Placements</div>
        <div style={{ fontSize: '13px', fontWeight: 600, color: '#fbbf24', marginTop: '4px' }}>{data.recommended_deployments?.length || 0} new nodes required</div>
      </div>
    </div>
  </div>
);

// ─── Main GapAnalysisLayer Component ───
const GapAnalysisLayer = ({ activeCameras = [] }) => {
  const [data, setData] = useState(null);

  useEffect(() => {
    const fetchAnalysis = async () => {
      try {
        const res = await fetch('/api/cameras/coverage-analysis');
        if (res.ok) {
          const json = await res.json();
          setData(json);
        } else {
          console.error('Coverage analysis request failed:', res.status);
        }
      } catch (err) {
        console.error('Failed to fetch gap analysis:', err);
      }
    };
    fetchAnalysis();
  }, []);

  if (!data) return null;

  return (
    <>
      {/* 1. Existing Coverage Buffers — 150m radius green circles */}
      {activeCameras.map(cam => (
        <Circle
          key={`cov-${cam.camera_id}`}
          center={[cam.latitude, cam.longitude]}
          radius={150}
          pathOptions={{ color: '#10b981', fillColor: '#10b981', fillOpacity: 0.12, weight: 1.5 }}
        />
      ))}

      {/* 2. Coverage Gap markers (red pulsing) */}
      {data.coverage_gaps?.map(gap => (
        <Marker
          key={gap.gap_id}
          position={[gap.location.lat, gap.location.lng]}
          icon={getGapIcon()}
        >
          <Popup>
            <div style={{ fontFamily: 'Inter, sans-serif', minWidth: '200px' }}>
              <div style={{ fontWeight: 700, color: '#ef4444', marginBottom: '4px', textTransform: 'uppercase', fontSize: '12px' }}>
                ⚠ {gap.severity} GAP
              </div>
              <div style={{ color: '#334155', fontSize: '12px', marginBottom: '4px' }}>{gap.description}</div>
              <div style={{ color: '#94a3b8', fontSize: '11px' }}>Nearest Camera: {gap.nearest_camera_distance_km}km away</div>
            </div>
          </Popup>
        </Marker>
      ))}

      {/* 3. Recommended Camera Deployment markers (amber) */}
      {data.recommended_deployments?.map((rec, idx) => (
        <Marker
          key={`rec-${idx}`}
          position={[rec.lat, rec.lng]}
          icon={getRecommendedIcon()}
        >
          <Popup>
            <div style={{ fontFamily: 'Inter, sans-serif' }}>
              <div style={{ fontWeight: 700, color: '#f59e0b', marginBottom: '4px', fontSize: '12px' }}>📍 Suggested Placement</div>
              <div style={{ color: '#334155', fontSize: '12px' }}>{rec.reason}</div>
            </div>
          </Popup>
        </Marker>
      ))}

      {/* 4. HUD Stats Panel — rendered as a proper Leaflet bottomleft control */}
      <CoverageHUD data={data} />
    </>
  );
};

export default GapAnalysisLayer;
