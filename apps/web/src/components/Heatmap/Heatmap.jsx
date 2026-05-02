import { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import './Heatmap.css';

// ── Embedded shot data ──
const RAW_CSV = `player,x,y,made,quarter,shot_type,distance_ft
Gaucho #3,312,85,1,1,2PT,8.0
Gaucho #3,352,97,0,1,2PT,11.2
Gaucho #3,289,74,1,1,2PT,6.1
Gaucho #3,410,210,1,1,3PT,24.3
Gaucho #3,95,60,0,1,3PT,28.1
Gaucho #3,455,130,1,1,3PT,26.7
Gaucho #3,330,88,1,2,2PT,9.4
Gaucho #3,298,76,0,2,2PT,6.8
Gaucho #3,278,48,1,2,2PT,2.8
Gaucho #3,340,175,0,2,2PT,18.4
Gaucho #3,388,198,1,2,2PT,20.1
Gaucho #3,420,215,0,2,3PT,24.8
Gaucho #3,87,55,1,2,3PT,28.6
Gaucho #3,265,52,1,3,2PT,4.9
Gaucho #3,180,145,1,3,2PT,15.2
Gaucho #3,390,95,0,3,2PT,17.3
Gaucho #3,320,82,1,3,2PT,8.8
Gaucho #3,55,110,1,3,3PT,26.8
Gaucho #3,310,190,1,3,2PT,16.1
Gaucho #3,245,80,1,4,2PT,6.7
Gaucho #3,420,250,0,4,3PT,25.4
Gaucho #3,365,102,1,4,2PT,13.2
Gaucho #3,302,79,0,4,2PT,7.2
Gaucho #3,450,125,1,4,3PT,26.2
Gaucho #3,100,68,0,4,3PT,27.5
Gaucho #3,315,86,1,4,2PT,8.2
Gaucho #3,345,178,0,4,2PT,18.7
Gaucho #3,270,55,1,1,2PT,5.2
Gaucho #3,405,205,0,1,3PT,23.8
Gaucho #3,93,62,1,2,3PT,28.0
Gaucho #21,250,48,1,1,2PT,2.1
Gaucho #21,105,52,0,1,3PT,27.4
Gaucho #21,388,60,1,1,2PT,18.2
Gaucho #21,250,165,1,1,2PT,12.1
Gaucho #21,472,70,0,1,3PT,29.1
Gaucho #21,260,50,1,2,2PT,2.6
Gaucho #21,395,58,0,2,2PT,18.8
Gaucho #21,195,48,1,2,2PT,7.8
Gaucho #21,300,52,1,2,2PT,6.3
Gaucho #21,70,140,0,2,3PT,26.3
Gaucho #21,255,168,0,2,2PT,12.4
Gaucho #21,478,75,1,2,3PT,29.4
Gaucho #21,250,185,0,3,2PT,14.3
Gaucho #21,430,55,1,3,2PT,20.4
Gaucho #21,155,90,1,3,2PT,16.3
Gaucho #21,490,95,0,3,3PT,30.2
Gaucho #21,245,52,1,3,2PT,2.8
Gaucho #21,270,48,1,4,2PT,3.4
Gaucho #21,222,140,0,4,2PT,13.1
Gaucho #21,345,200,1,4,2PT,17.2
Gaucho #21,108,55,1,4,3PT,27.1
Gaucho #21,388,62,0,4,2PT,18.0
Gaucho #21,253,170,1,4,2PT,12.7
Gaucho #21,468,68,1,4,3PT,28.8
Gaucho #21,262,48,0,1,2PT,3.0
Gaucho #21,242,165,1,2,2PT,11.8
Gaucho #21,480,80,0,3,3PT,29.8
Gaucho #44,248,38,1,1,2PT,1.2
Gaucho #44,260,55,1,1,2PT,2.8
Gaucho #44,235,42,0,1,2PT,1.8
Gaucho #44,275,70,1,2,2PT,4.8
Gaucho #44,230,80,0,2,2PT,6.5
Gaucho #44,255,48,1,2,2PT,2.1
Gaucho #44,310,100,1,2,2PT,9.8
Gaucho #44,240,45,1,3,2PT,2.4
Gaucho #44,265,90,0,3,2PT,7.4
Gaucho #44,250,110,1,3,2PT,10.2
Gaucho #44,288,65,1,3,2PT,5.2
Gaucho #44,245,52,1,4,2PT,2.6
Gaucho #44,270,42,0,4,2PT,3.2
Gaucho #44,320,130,1,4,2PT,13.1
Gaucho #44,252,40,1,1,2PT,1.4
Gaucho #44,242,58,0,1,2PT,2.2
Gaucho #44,278,72,1,2,2PT,5.0
Gaucho #44,258,48,1,2,2PT,2.4
Gaucho #44,248,82,1,3,2PT,6.8
Gaucho #44,268,45,0,3,2PT,3.0
Gaucho #44,295,95,1,4,2PT,8.2
Gaucho #44,244,42,1,4,2PT,1.9
Gaucho #5,48,135,1,1,3PT,26.4
Gaucho #5,452,128,0,1,3PT,26.1
Gaucho #5,55,90,1,1,3PT,26.9
Gaucho #5,445,95,1,2,3PT,27.2
Gaucho #5,50,155,0,2,3PT,25.8
Gaucho #5,460,110,1,2,3PT,27.8
Gaucho #5,47,75,0,3,3PT,28.8
Gaucho #5,453,80,1,3,3PT,28.6
Gaucho #5,52,140,1,3,3PT,26.2
Gaucho #5,350,280,0,4,3PT,31.8
Gaucho #5,150,280,1,4,3PT,31.5
Gaucho #5,250,300,0,4,3PT,33.1
Gaucho #5,460,130,1,4,3PT,26.8
Gaucho #5,48,115,1,4,3PT,27.0
Gaucho #5,44,130,0,1,3PT,26.6
Gaucho #5,456,125,1,2,3PT,26.3
Gaucho #5,50,100,1,3,3PT,27.4
Gaucho #5,448,105,0,4,3PT,27.6
Gaucho #11,230,48,0,1,2PT,4.8
Gaucho #11,270,52,1,1,2PT,4.5
Gaucho #11,195,140,1,1,2PT,16.2
Gaucho #11,305,145,0,2,2PT,16.1
Gaucho #11,165,80,1,2,2PT,18.2
Gaucho #11,335,78,1,2,2PT,17.9
Gaucho #11,250,48,1,3,2PT,2.1
Gaucho #11,210,95,0,3,2PT,12.4
Gaucho #11,288,98,1,3,2PT,11.2
Gaucho #11,160,155,0,4,2PT,19.1
Gaucho #11,340,158,1,4,2PT,18.8
Gaucho #11,250,175,1,4,2PT,14.2
Gaucho #11,220,50,1,4,2PT,5.8
Gaucho #11,280,48,0,4,2PT,6.2
Gaucho #11,225,52,1,1,2PT,5.2
Gaucho #11,275,50,0,2,2PT,5.0
Gaucho #11,200,138,1,3,2PT,15.8
Gaucho #11,300,142,1,4,2PT,15.6`;

// ── Parse CSV ──
function parseCSV(text) {
  const lines = text.trim().split('\n');
  const headers = lines[0].split(',');
  return lines.slice(1).map(l => {
    const vals = l.split(',');
    return Object.fromEntries(
      headers.map((h, i) => [h.trim(), isNaN(vals[i]) ? vals[i].trim() : +vals[i]])
    );
  });
}

const allData = parseCSV(RAW_CSV);
const players = [...new Set(allData.map(d => d.player))];

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

export default function ShotChart() {
  const svgRef = useRef(null);
  const [player, setPlayer] = useState(players[0]);
  const [quarter, setQuarter] = useState('all');
  const [shotType, setShotType] = useState('all');
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, data: null });
  const [stats, setStats] = useState({ fgpct: '--', makes: '--', attempts: '--', avgDist: '--' });

  const getFiltered = useCallback(() => {
    return allData.filter(d =>
      d.player === player &&
      (quarter === 'all' || d.quarter === +quarter) &&
      (shotType === 'all' || d.shot_type === shotType)
    );
  }, [player, quarter, shotType]);

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
            >
              {players.map(p => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>

          <div className="ctrl-group">
            <label>Quarter</label>
            <div className="pill-group">
              {['all', '1', '2', '3', '4'].map(q => (
                <button
                  key={q}
                  className={`pill${quarter === q ? ' active' : ''}`}
                  onClick={() => setQuarter(q)}
                >
                  {q === 'all' ? 'All' : `Q${q}`}
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
                >
                  {t === 'all' ? 'All' : t}
                </button>
              ))}
            </div>
          </div>
        </div>

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
          <div className="tt-row"><span>Quarter</span><span className="tt-val">Q{tooltip.data.quarter}</span></div>
          <div className="tt-row"><span>Type</span><span className="tt-val">{tooltip.data.shot_type}</span></div>
        </div>
      )}
    </div>
  );
}
