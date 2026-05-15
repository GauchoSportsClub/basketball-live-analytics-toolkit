import { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import './Heatmap.css';

// ── Color scale ──
const colorStops = [
  [0,    '#1a3a6b'],
  [0.3,  '#2563c0'],
  [0.55, '#93c5fd'],
  [0.7,  '#ffffff'],
  [0.82, '#fdba74'],
  [0.92, '#f97316'],
  [1,    '#dc2626'],
];
function heatColor(t) {
  for (let i = 1; i < colorStops.length; i++) {
    if (t <= colorStops[i][0]) {
      const [t0, c0] = colorStops[i - 1];
      const [t1, c1] = colorStops[i];
      const s = (t - t0) / (t1 - t0);
      return d3.interpolateRgb(c0, c1)(s);
    }
  }
  return colorStops[colorStops.length - 1][1];
}

// ── Kernel density ──
function kernelDensity(data, bw, W, courtH) {
  const GW = 80, GH = 75;
  const grid = new Float32Array(GW * GH);
  let maxV = 0;
  for (let gy = 0; gy < GH; gy++) {
    for (let gx = 0; gx < GW; gx++) {
      const nx = (gx + 0.5) / GW * W;
      const ny = (gy + 0.5) / GH * courtH;
      let v = 0;
      data.forEach(d => {
        const dx = nx - d.x, dy = ny - d.y;
        v += Math.exp(-(dx * dx + dy * dy) / (2 * bw * bw));
      });
      grid[gy * GW + gx] = v;
      if (v > maxV) maxV = v;
    }
  }
  return { grid, GW, GH, maxV };
}

export default function ShotChart({ gameId }) {
  const svgRef = useRef(null);
  const [allData, setAllData] = useState([]);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [player, setPlayer] = useState('all');
  const [period, setPeriod] = useState('all');
  const [shotType, setShotType] = useState('all');
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, data: null });
  const [stats, setStats] = useState({ fgpct: '--', makes: '--', attempts: '--', avgDist: '--' });

  // ── Fetch shot data from API ──
  useEffect(() => {
    const API_BASE = import.meta.env.VITE_API_BASE_URL || '';
    const gid = gameId || '401809104';
    setLoading(true);
    setError(null);
    fetch(`${API_BASE}/api/shots?game_id=${encodeURIComponent(gid)}`)
      .then(r => r.json())
      .then(data => {
        if (data.error) throw new Error(data.error);
        const shots = data.shots || [];
        setAllData(shots);
        setPlayers(['all', ...new Set(shots.map(d => d.player))]);
        setPlayer('all');
        setPeriod('all');
        setShotType('all');
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  const getFiltered = useCallback(() => {
    return allData.filter(d =>
      (player === 'all' || d.player === player) &&
      (period === 'all' || d.period === +period) &&
      (shotType === 'all' || d.shot_type === shotType)
    );
  }, [allData, player, period, shotType]);

  // ── Draw court (runs once) ──
  useEffect(() => {
    const svg = d3.select(svgRef.current);
    const W = 500, courtH = 470;
    const lc = 'rgba(255,255,255,0.65)';
    const lw = 1.6;

    // Court background
    svg.append('rect').attr('width', W).attr('height', 490).attr('fill', '#0a1628').attr('rx', 8);

    // Hardwood stripes
    for (let i = 0; i < 10; i++) {
      svg.append('rect')
        .attr('x', i * 50).attr('y', 0).attr('width', 50).attr('height', 490)
        .attr('fill', i % 2 === 0 ? 'rgba(255,200,80,0.022)' : 'none');
    }

    const courtG = svg.append('g').attr('class', 'court-lines');

    const line = (x1, y1, x2, y2) =>
      courtG.append('line').attr('x1', x1).attr('y1', y1).attr('x2', x2).attr('y2', y2)
        .attr('stroke', lc).attr('stroke-width', lw);

    const rectEl = (x, y, w, h) =>
      courtG.append('rect').attr('x', x).attr('y', y).attr('width', w).attr('height', h)
        .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw);

    // Boundary & half court
    rectEl(0, 0, W, courtH);
    line(0, courtH, W, courtH);

    // Paint, FT circle, restricted area
    rectEl(170, 0, 160, 190);
    courtG.append('path').attr('d', 'M 130 190 A 120 120 0 0 1 370 190')
      .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw);
    courtG.append('path').attr('d', 'M 130 190 A 120 120 0 0 0 370 190')
      .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw).attr('stroke-dasharray', '5,4');
    courtG.append('path').attr('d', 'M 210 47.5 A 40 40 0 0 1 290 47.5')
      .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw);

    // Backboard, hoop, box
    line(220, 40, 280, 40);
    courtG.append('circle').attr('cx', 250).attr('cy', 47.5).attr('r', 7.5)
      .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw);
    rectEl(210, 0, 80, 19);

    // 3pt arc + corner lines
    courtG.append('path').attr('d', 'M 47.5 140 A 237.5 237.5 0 0 1 452.5 140')
      .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw);
    line(47.5, 0, 47.5, 140);
    line(452.5, 0, 452.5, 140);

    // Half court circle
    courtG.append('path').attr('d', 'M 130 470 A 120 120 0 0 0 370 470')
      .attr('fill', 'none').attr('stroke', lc).attr('stroke-width', lw);

    // Center dot
    courtG.append('circle').attr('cx', 250).attr('cy', 47.5).attr('r', 2.5).attr('fill', lc);

    // Reserve layers for heat + dots (drawn in update effect)
    svg.append('g').attr('class', 'heat-layer');
    svg.append('g').attr('class', 'dots-layer');

    return () => {
      svg.selectAll('*').remove();
    };
  }, []); // empty dep array — court drawn once

  // ── Update heat + dots + stats when filters change ──
  useEffect(() => {
    const svg = d3.select(svgRef.current);
    const W = 500, courtH = 470;
    const data = getFiltered();

    // Stats
    const makes = data.filter(d => d.made).length;
    const total = data.length;
    setStats({
      fgpct:    total ? (makes / total * 100).toFixed(1) + '%' : '--%',
      makes:    makes,
      attempts: total,
      avgDist:  total ? (data.reduce((s, d) => s + d.distance_ft, 0) / total).toFixed(1) + ' ft' : '-- ft',
    });

    // Heat map
    const heatG = svg.select('g.heat-layer');
    heatG.selectAll('*').remove();
    if (data.length >= 3) {
      const bw = Math.max(18, 60 - data.length * 0.3);
      const { grid, GW, GH, maxV } = kernelDensity(data, bw, W, courtH);
      const cw = W / GW, ch = courtH / GH;
      for (let gy = 0; gy < GH; gy++) {
        for (let gx = 0; gx < GW; gx++) {
          const raw = grid[gy * GW + gx];
          if (raw < maxV * 0.05) continue;
          const t = Math.pow(raw / maxV, 0.6);
          heatG.append('rect')
            .attr('x', gx * cw).attr('y', gy * ch)
            .attr('width', cw + 0.5).attr('height', ch + 0.5)
            .attr('fill', heatColor(t))
            .attr('opacity', 0.1 + t * 0.72);
        }
      }
    }

    // Shot dots
    const dotsG = svg.select('g.dots-layer');
    dotsG.selectAll('*').remove();

    const misses = data.filter(d => !d.made);
    const makesData = data.filter(d => d.made);

    misses.forEach(d => {
      const g = dotsG.append('g')
        .attr('transform', `translate(${d.x},${d.y})`)
        .style('cursor', 'pointer')
        .on('mousemove', (ev) => setTooltip({ visible: true, x: ev.clientX, y: ev.clientY, data: d }))
        .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));
      g.append('line').attr('x1', -5).attr('y1', -5).attr('x2', 5).attr('y2', 5)
        .attr('stroke', '#f87171').attr('stroke-width', 1.5).attr('opacity', 0.75);
      g.append('line').attr('x1', 5).attr('y1', -5).attr('x2', -5).attr('y2', 5)
        .attr('stroke', '#f87171').attr('stroke-width', 1.5).attr('opacity', 0.75);
    });

    makesData.forEach(d => {
      dotsG.append('circle')
        .attr('cx', d.x).attr('cy', d.y).attr('r', 5)
        .attr('fill', '#4ade80').attr('stroke', '#166534').attr('stroke-width', 1).attr('opacity', 0.82)
        .style('cursor', 'pointer')
        .on('mousemove', (ev) => setTooltip({ visible: true, x: ev.clientX, y: ev.clientY, data: d }))
        .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));
    });
  }, [getFiltered]);

  return (
    <div className="shot-chart-wrapper">
      {/* Header */}
      <header className="shot-chart-header">
        <div className="shot-chart-badge">UCSB</div>
        <div>
          <h1>UCSB <span>Shot Chart &amp; Heatmap</span></h1>
          <p>Basketball Analytics · 2025–26 Season</p>
        </div>
      </header>

      <main className="shot-chart-main">
        {/* Controls */}
        <div className="shot-chart-controls">
          <div className="ctrl-group">
            <label>Player</label>
            <select
              className="shot-chart-select"
              value={player}
              onChange={e => setPlayer(e.target.value)}
              disabled={loading}
            >
              {players.map(p => <option key={p} value={p}>{p === 'all' ? 'All Players' : p}</option>)}
            </select>
          </div>

          <div className="ctrl-group">
            <label>Half</label>
            <div className="pill-group">
              {['all', '1', '2'].map(p => (
                <button
                  key={p}
                  className={`pill${period === p ? ' active' : ''}`}
                  onClick={() => setPeriod(p)}
                  disabled={loading}
                >
                  {p === 'all' ? 'All' : `H${p}`}
                </button>
              ))}
            </div>
          </div>

          <div className="ctrl-group">
            <label>Shot Type</label>
            <div className="pill-group">
              {['all', '2PT', '3PT'].map(t => (
                <button
                  key={t}
                  className={`pill${shotType === t ? ' active' : ''}`}
                  onClick={() => setShotType(t)}
                  disabled={loading}
                >
                  {t === 'all' ? 'All' : t}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Status */}
        {loading && <div className="shot-chart-status">Loading shot data…</div>}
        {error && <div className="shot-chart-status shot-chart-error">Error: {error}</div>}

        {/* Stat cards */}
        <div className="stat-row">
          <div className="stat-card"><div className="val">{stats.fgpct}</div><div className="lbl">FG %</div></div>
          <div className="stat-card"><div className="val">{stats.makes}</div><div className="lbl">Makes</div></div>
          <div className="stat-card"><div className="val">{stats.attempts}</div><div className="lbl">Attempts</div></div>
          <div className="stat-card"><div className="val">{stats.avgDist}</div><div className="lbl">Avg Distance</div></div>
        </div>

        {/* Chart */}
        <div className="chart-card">
          <svg ref={svgRef} id="court-svg" viewBox="0 0 500 490" xmlns="http://www.w3.org/2000/svg" />
          <div className="legend">
            <div className="leg-item">
              <span className="leg-dot" style={{ background: '#4ade80', border: '1.5px solid #166534' }} />
              Make
            </div>
            <div className="leg-item">
              <span style={{ display: 'inline-block', width: 12, height: 12, color: '#f87171', fontSize: 14, lineHeight: 1, textAlign: 'center' }}>✕</span>
              Miss
            </div>
            <div className="leg-item" style={{ gap: 10 }}>
              <span>Low</span>
              <div className="leg-grad" />
              <span>High density</span>
            </div>
          </div>
        </div>
      </main>

      {/* Tooltip */}
      {tooltip.data && (
        <div
          className={`shot-chart-tooltip${tooltip.visible ? ' visible' : ''}`}
          style={{ left: tooltip.x + 14, top: tooltip.y - 10 }}
        >
          <div className="tt-title">{tooltip.data.player}</div>
          <div className="tt-row"><span>Result</span><span className="tt-val">{tooltip.data.made ? '✅ Made' : '❌ Missed'}</span></div>
          <div className="tt-row"><span>Distance</span><span className="tt-val">{tooltip.data.distance_ft} ft</span></div>
          <div className="tt-row"><span>Half</span><span className="tt-val">H{tooltip.data.period}</span></div>
          <div className="tt-row"><span>Type</span><span className="tt-val">{tooltip.data.shot_type}</span></div>
        </div>
      )}
    </div>
  );
}
