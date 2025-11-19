const state = {
  products: [],
  lastFetched: null,
  selected: null,
  trendChart: null,
  partnerChart: null,
  watchlist: restoreJSON('bfi_watchlist', []),
  community: [],
  viewMode: 'grid' // 'grid' or 'table'
};

// Neoclassical Formatters
const numberFormatter = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });
const usdFormatter = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const inrFormatter = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 });

const CHART_COLORS = {
  primary: '#d4af37', secondary: '#2c3e50', accent: '#8b0000', neutral: '#e6e1d8',
  palette: ['#d4af37', '#2c3e50', '#5d5d5d', '#aa8c2c', '#1a1a1a']
};

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('footerYear').textContent = new Date().getFullYear();
  wireEvents();
  loadProducts();
  renderWatchlistSidebar();

  // Navigation
  document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
      document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
      document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));

      e.target.classList.add('active');
      const viewId = `view-${e.target.dataset.view}`;
      document.getElementById(viewId).classList.add('active');
    });
  });
});

function wireEvents() {
  // Filters & Actions
  document.getElementById('loadBtn').addEventListener('click', loadProducts);
  document.getElementById('refreshBtn').addEventListener('click', () => loadProducts(true));
  document.getElementById('downloadCsvBtn').addEventListener('click', downloadCsv);
  
  // Forms
  document.getElementById('businessCaseForm').addEventListener('submit', handleBusinessCaseSubmit);
  document.getElementById('compareForm').addEventListener('submit', handleCompareSubmit);
  document.getElementById('communityForm').addEventListener('submit', handleCommunitySubmit);
  document.getElementById('search').addEventListener('input', () => renderResults(filterProducts(state.products)));
  
  // Drawer
  document.getElementById('closeDrawer').addEventListener('click', closeDrawer);
  document.getElementById('drawerBackdrop').addEventListener('click', closeDrawer);
  document.getElementById('drawerWatchBtn').addEventListener('click', toggleWatchFromDrawer);

  // View Toggles
  document.getElementById('viewGrid').addEventListener('click', () => switchViewMode('grid'));
  document.getElementById('viewTable').addEventListener('click', () => switchViewMode('table'));
}

function switchViewMode(mode) {
  state.viewMode = mode;
  document.getElementById('viewGrid').classList.toggle('active', mode === 'grid');
  document.getElementById('viewTable').classList.toggle('active', mode === 'table');
  
  document.getElementById('cards').classList.toggle('hidden', mode !== 'grid');
  document.getElementById('tableContainer').classList.toggle('hidden', mode !== 'table');
}

async function loadProducts(force = false) {
  try {
    const params = buildQuery();
    const response = await fetch(`/api/products?${params.toString()}`);
    if (!response.ok) throw new Error(`Request failed: ${response.status}`);
    const data = await response.json();
    state.products = data.items || [];

    renderStats(state.products);
    renderResults(state.products);
    renderAlerts(state.products);
    populateCompareOptions(state.products);

    animateCounters();
    showToast(`Updated: ${state.products.length} items`);
  } catch (error) {
    console.error(error);
    showToast('Unable to fetch data', 'error');
  }
}

function buildQuery() {
  const params = new URLSearchParams();
  const searchValue = document.getElementById('search').value.trim();
  const sectorsValue = document.getElementById('sectors').value.trim();
  const minCapex = document.getElementById('minCapex').value;
  const maxCapex = document.getElementById('maxCapex').value;
  const sort = document.getElementById('sort').value;

  if (sectorsValue) params.set('sectors', sectorsValue);
  if (minCapex) params.set('min_capex', minCapex);
  if (maxCapex) params.set('max_capex', maxCapex);
  if (sort) params.set('sort', sort);
  if (searchValue) params.set('q', searchValue);
  params.set('limit', 200);
  state.searchKeyword = searchValue.toLowerCase();
  return params;
}

function filterProducts(items) {
  if (!state.searchKeyword) return items;
  return items.filter((item) => {
    return (
      item.title?.toLowerCase().includes(state.searchKeyword) ||
      item.hs_code?.toLowerCase().includes(state.searchKeyword) ||
      item.description?.toLowerCase().includes(state.searchKeyword)
    );
  });
}

function renderStats(items) {
  const total = items.length;
  const importUsd = items.reduce((acc, item) => acc + (item.last_12m_value_usd || 0), 0);
  const avgScore = items.length
    ? items.reduce((acc, item) => acc + (item.opportunity_score || 0), 0) / items.length
    : 0;
  const alertCount = computeAlerts(items).length;

  if (!document.getElementById('statTotalProducts').textContent.match(/\d/)) {
  document.getElementById('statTotalProducts').textContent = numberFormatter.format(total);
  document.getElementById('statImportUsd').textContent = usdFormatter.format(importUsd);
  document.getElementById('statAvgScore').textContent = avgScore.toFixed(2);
  document.getElementById('statAlerts').textContent = alertCount;
  }
}

function renderResults(items) {
  const filtered = filterProducts(items);
  document.getElementById('resultsMeta').textContent = `${filtered.length} records`;
  
  // Render Grid
  const cardContainer = document.getElementById('cards');
  cardContainer.innerHTML = filtered.map(item => `
    <article class="card" onclick="openDrawer('${item.hs_code}')">
      <h3><span>${item.title}</span><span>${item.hs_code}</span></h3>
      <div class="chips">${(item.sectors || []).map(s => `<span class="chip">${s}</span>`).join('')}</div>
      <div class="meta">12m: ${formatValue(item.last_12m_value_usd, 'usd')}</div>
      <div class="meta">Score: ${(item.opportunity_score ?? 0).toFixed(2)}</div>
    </article>
  `).join('');
  
  // Render Table
  const tableBody = document.querySelector('#resultsTable tbody');
  tableBody.innerHTML = filtered.map(item => `
    <tr onclick="openDrawer('${item.hs_code}')">
      <td><strong>${item.hs_code}</strong></td>
      <td>${item.title}</td>
      <td>${(item.sectors || []).join(', ')}</td>
      <td class="text-right">${formatValue(item.last_12m_value_usd, 'usd')}</td>
      <td class="text-right"><strong>${(item.opportunity_score ?? 0).toFixed(2)}</strong></td>
      <td class="text-right">→</td>
    </tr>
  `).join('');
}

// Drawer Logic
async function openDrawer(hsCode) {
  try {
    const response = await fetch(`/api/products/${hsCode}`);
    if (!response.ok) throw new Error();
    const data = await response.json();
    state.selected = data;
    
    // Populate Drawer
    const product = data.product;
    document.getElementById('drawerTitle').textContent = `${product.title} (${product.hs_code})`;
    
    // Update Watch Button State
    updateDrawerWatchButton();
    
    // Snapshot
    document.getElementById('detailSnapshot').innerHTML = `
      <li><span>Opp. Score</span> <strong>${(product.opportunity_score ?? 0).toFixed(2)}</strong></li>
      <li><span>Imports (12m)</span> <strong>${formatValue(product.last_12m_value_usd, 'usd')}</strong></li>
      <li><span>HHI</span> <strong>${(data.progress?.hhi_current ?? 0).toFixed(2)}</strong></li>
      <li><span>Capex Range</span> <strong>${formatRange(product.capex_min, product.capex_max)}</strong></li>
    `;
    
    // Pre-fill Business Case
    document.getElementById('caseHs').value = product.hs_code;
    const capex = product.capex_max || product.capex_min || 0;
    if (capex) document.getElementById('caseCapex').placeholder = usdFormatter.format(capex);
    document.getElementById('businessCaseResults').innerHTML = ''; // Clear previous results

    // Charts
    renderTrendChart(data.timeseries);
    renderPartnerChart(data.partners);
    renderPartnerList(data.partners);

    // Open UI
    document.getElementById('drawerBackdrop').classList.remove('hidden');
    document.getElementById('detailDrawer').classList.remove('hidden');
    setTimeout(() => document.getElementById('detailDrawer').classList.add('open'), 10);
    
  } catch (e) {
    console.error(e);
    showToast('Could not load details', 'error');
  }
}

function closeDrawer() {
  document.getElementById('detailDrawer').classList.remove('open');
  setTimeout(() => {
    document.getElementById('detailDrawer').classList.add('hidden');
    document.getElementById('drawerBackdrop').classList.add('hidden');
  }, 300);
}

function updateDrawerWatchButton() {
  const btn = document.getElementById('drawerWatchBtn');
  if (!state.selected) return;
  const hs = state.selected.product.hs_code;
  const exists = state.watchlist.some(i => i.hs_code === hs);
  btn.textContent = exists ? 'Unwatch' : 'Watch';
  btn.className = exists ? 'btn small primary' : 'btn small outline';
}

function toggleWatchFromDrawer() {
  if (!state.selected) return;
  const hs = state.selected.product.hs_code;
  const exists = state.watchlist.some(i => i.hs_code === hs);
  
  if (exists) {
    removeFromWatchlist(hs);
  } else {
    addToWatchlist(state.selected);
  }
  updateDrawerWatchButton();
}

// Charts
function renderTrendChart(timeseries = []) {
  const ctx = document.getElementById('trendChart');
  const labels = timeseries.map(d => `${d.year}-${String(d.month).padStart(2,'0')}`);
  const values = timeseries.map(d => d.value_usd || 0);
  if (state.trendChart) state.trendChart.destroy();
  
  Chart.defaults.font.family = "'Tenor Sans', sans-serif";
  state.trendChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'USD', data: values, borderColor: CHART_COLORS.primary,
        backgroundColor: 'rgba(212, 175, 55, 0.1)', fill: true, tension: 0.4, pointRadius: 0
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { display: false }, y: { beginAtZero: true, grid: { color: '#e6e1d8' } } }
    }
  });
}

function renderPartnerChart(partners = []) {
  const ctx = document.getElementById('partnerChart');
  const labels = partners.map(p => p.partner_country);
  const values = partners.map(p => p.value_usd);
  if (state.partnerChart) state.partnerChart.destroy();
  state.partnerChart = new Chart(ctx, {
    type: 'doughnut',
    data: { labels, datasets: [{ data: values, backgroundColor: CHART_COLORS.palette, borderWidth: 0 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: '60%', plugins: { legend: { display: false } } }
  });
}

function renderPartnerList(partners = []) {
  document.getElementById('partnerList').innerHTML = partners.slice(0,5).map(p => 
    `<li><span>${p.partner_country}</span> <span>${formatValue(p.value_usd, 'usd')}</span></li>`
  ).join('');
}

// Business Case
function handleBusinessCaseSubmit(event) {
  event.preventDefault();
  if (!state.selected) return;
  const product = state.selected.product;
  const form = event.target;
  
  const units = Number(form.caseUnits.value || 0);
  const price = Number(form.casePrice.value || 0);
  const replacement = Number(form.caseReplacement.value || 0);
  const capexInput = form.caseCapex.value ? Number(form.caseCapex.value) : null;
  const opex = Number(form.caseOpex.value || 0);

  const capex = capexInput || average([product.capex_min, product.capex_max]) || 0;
  const revenue = units * price * (replacement / 100);
  const grossMargin = Math.max(revenue - opex, 0);
  const paybackMonths = grossMargin > 0 ? capex / grossMargin : Infinity;

  document.getElementById('businessCaseResults').innerHTML = `
    <div class="result-card"><h4>Revenue</h4><p>${usdFormatter.format(revenue)}</p></div>
    <div class="result-card"><h4>Margin</h4><p>${usdFormatter.format(grossMargin)}</p></div>
    <div class="result-card"><h4>Capex</h4><p>${usdFormatter.format(capex)}</p></div>
    <div class="result-card"><h4>Payback</h4><p>${Number.isFinite(paybackMonths) ? paybackMonths.toFixed(1) + ' mo' : '—'}</p></div>
  `;
}

// Compare
function handleCompareSubmit(e) {
  e.preventDefault();
  const hsA = document.getElementById('compareA').value;
  const hsB = document.getElementById('compareB').value;
  if (!hsA || !hsB || hsA === hsB) {
    showToast('Select different items', 'error');
    return;
  }
  // Simple comparison display
  Promise.all([
    fetch(`/api/products/${hsA}`).then(r => r.json()),
    fetch(`/api/products/${hsB}`).then(r => r.json())
  ]).then(([dA, dB]) => {
    document.getElementById('compareResults').innerHTML = [dA, dB].map(d => `
      <div class="card compact">
        <strong>${d.product.title}</strong>
        <div class="meta">Score: ${d.product.opportunity_score.toFixed(2)}</div>
        <div class="meta">Imp: ${formatValue(d.product.last_12m_value_usd, 'usd')}</div>
        </div>
    `).join('');
  });
}

function populateCompareOptions(items) {
  const options = items.map(i => `<option value="${i.hs_code}">${i.title.substring(0,20)}...</option>`).join('');
  document.getElementById('compareA').innerHTML = `<option value="">Item A</option>${options}`;
  document.getElementById('compareB').innerHTML = `<option value="">Item B</option>${options}`;
}

// Sidebars
function renderWatchlistSidebar() {
  const list = document.getElementById('watchlistList');
  if (!state.watchlist.length) {
    list.innerHTML = '<li style="padding:1rem; text-align:center; color:var(--text-muted)">Empty</li>';
    return;
  }
  list.innerHTML = state.watchlist.map(item => `
    <li style="cursor:pointer" onclick="openDrawer('${item.hs_code}')">
      <strong>${item.hs_code}</strong>
      <span>${item.title.substring(0, 15)}...</span>
    </li>
  `).join('');
}

function renderAlerts(items) {
  const alerts = computeAlerts(items).slice(0, 8);
  document.getElementById('alertsList').innerHTML = alerts.map(a => `
    <li onclick="openDrawer('${a.hs_code}')" style="cursor:pointer">
      <strong>${a.hs_code}</strong>
      <span>Score: ${a.opportunity_score.toFixed(2)}</span>
    </li>
  `).join('');
}

function computeAlerts(items) {
  return items.filter(item => (item.opportunity_score ?? 0) >= 0.7);
}

function addToWatchlist(detail) {
  if (!state.watchlist.some(i => i.hs_code === detail.product.hs_code)) {
    state.watchlist.push({ hs_code: detail.product.hs_code, title: detail.product.title });
    persistJSON('bfi_watchlist', state.watchlist);
    renderWatchlistSidebar();
  }
}
function removeFromWatchlist(hs) {
  state.watchlist = state.watchlist.filter(i => i.hs_code !== hs);
  persistJSON('bfi_watchlist', state.watchlist);
  renderWatchlistSidebar();
}

// Helpers
function formatValue(v, m) { if (v == null) return '—'; return m==='usd'?usdFormatter.format(v):numberFormatter.format(v); }
function formatRange(min, max) { if(!min && !max) return '—'; return `${usdFormatter.format(min||0)}–${usdFormatter.format(max||0)}`; }
function formatPercentage(v) { return v == null ? '—' : `${(Number(v)*100).toFixed(1)}%`; }
function average(v) { const c = v.filter(x=>typeof x==='number'); return c.length ? c.reduce((a,b)=>a+b,0)/c.length : 0; }
function animateCounters() { /* ... Same as before ... */ }
function persistJSON(k, v) { localStorage.setItem(k, JSON.stringify(v)); }
function restoreJSON(k, d) { try { return JSON.parse(localStorage.getItem(k)) || d; } catch { return d; } }
function showToast(m, t) { 
  const el = document.getElementById('toast'); el.textContent = m; el.hidden=false; el.classList.add('show');
  if(t==='error') el.style.border='1px solid red'; else el.style.border='1px solid var(--text-gold)';
  setTimeout(()=> { el.classList.remove('show'); setTimeout(()=>el.hidden=true,400); }, 2000);
}
