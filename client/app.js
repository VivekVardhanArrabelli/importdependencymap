const state = {
  products: [],
  lastFetched: null,
  selected: null,
  trendChart: null,
  partnerChart: null,
  watchlist: restoreJSON('bfi_watchlist', []),
  community: [],
};

// Neoclassical Formatters (Standardized)
const numberFormatter = new Intl.NumberFormat('en-IN', {
  maximumFractionDigits: 2,
});

const usdFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
});

const inrFormatter = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

// Color Palette for Charts (Gold/Slate Theme)
const CHART_COLORS = {
  primary: '#d4af37',   // Gold
  secondary: '#2c3e50', // Slate
  accent: '#8b0000',    // Red/Alert
  neutral: '#e6e1d8',   // Stone
  palette: ['#d4af37', '#2c3e50', '#5d5d5d', '#aa8c2c', '#1a1a1a']
};

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('footerYear').textContent = new Date().getFullYear();
  try {
    localStorage.removeItem('bfi_filters');
    localStorage.removeItem('bfi_registry');
  } catch (err) {}

  try {
    const ids = ['caseUnits','casePrice','caseReplacement','caseCapex','caseOpex'];
    ids.forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
  } catch (_) {}
  
  wireEvents();
  loadProducts();
  renderCommunity();
  renderWatchlist(); // Render initial empty watchlist
  document.addEventListener('keydown', handleHotkeys);
});

function wireEvents() {
  document.getElementById('loadBtn').addEventListener('click', loadProducts);
  document.getElementById('refreshBtn').addEventListener('click', () => loadProducts(true));
  document.getElementById('downloadCsvBtn').addEventListener('click', downloadCsv);
  document.getElementById('businessCaseForm').addEventListener('submit', handleBusinessCaseSubmit);
  document.getElementById('compareForm').addEventListener('submit', handleCompareSubmit);
  document.getElementById('communityForm').addEventListener('submit', handleCommunitySubmit);
  document.getElementById('search').addEventListener('input', () => renderCards(filterProducts(state.products)));
}

async function loadProducts(force = false) {
  try {
    setStatus('Acquiring Data…', true);
    renderSkeletonCards();

    const params = buildQuery();
    const response = await fetch(`/api/products?${params.toString()}`);
    if (!response.ok) throw new Error(`Request failed: ${response.status}`);
    const data = await response.json();
    state.products = data.items || [];

    state.lastFetched = data.last_updated || null;
    renderStats(state.products);
    renderCards(state.products);
    renderAlerts(state.products);
    populateCompareOptions(state.products);

    setStatus('Ready');
    animateCounters();
    showToast(`Index updated: ${state.products.length} commodities`);
  } catch (error) {
    console.error(error);
    setStatus('Connection Error');
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

  // Animation handled by countUp in animateCounters, just setting text here as fallback
  if (!document.getElementById('statTotalProducts').textContent.match(/\d/)) {
      document.getElementById('statTotalProducts').textContent = numberFormatter.format(total);
      document.getElementById('statImportUsd').textContent = usdFormatter.format(importUsd);
      document.getElementById('statAvgScore').textContent = avgScore.toFixed(2);
      document.getElementById('statAlerts').textContent = alertCount;
  }
}

function renderCards(items) {
  const filtered = filterProducts(items);
  const container = document.getElementById('cards');
  container.innerHTML = '';

  if (!filtered.length) {
    container.innerHTML = '<p class="meta-serif">No commodities found matching criteria.</p>';
    return;
  }

  filtered.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'card';
    card.innerHTML = `
      <h3>
        <span>${item.title}</span>
        <span>${item.hs_code}</span>
      </h3>
      <div class="chips">${(item.sectors || []).map((s) => `<span class="chip">${s}</span>`).join('')}</div>
      <div class="meta">12m Imports: ${formatValue(item.last_12m_value_usd, 'usd')}</div>
      <div class="meta">Opp. Score: ${(item.opportunity_score ?? 0).toFixed(2)}</div>
      
      <div style="margin-top: 1rem; display: flex; justify-content: flex-end;">
        <button class="btn text-only" type="button" data-hs="${item.hs_code}">Analyze &rarr;</button>
      </div>
    `;
    card.addEventListener('click', () => loadDetail(item.hs_code));
    container.appendChild(card);
  });

  document.getElementById('resultsMeta').textContent = `${filtered.length} records`;
}

async function loadDetail(hsCode) {
  try {
    const response = await fetch(`/api/products/${hsCode}`);
    if (!response.ok) throw new Error(`Failed to load detail for ${hsCode}`);
    const data = await response.json();
    state.selected = data;
    
    const detailPanel = document.getElementById('detail');
    detailPanel.classList.remove('hidden');
    
    updateDetail(data);
    updateBusinessCase(data);
    updateWatchButton();
    
    detailPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  } catch (error) {
    console.error(error);
  }
}

function updateDetail(detail) {
  const product = detail.product;
  document.getElementById('detailTitle').textContent = `${product.title} — ${product.hs_code}`;

  const snapshot = document.getElementById('detailSnapshot');
  snapshot.innerHTML = `
    <li><span>Opportunity Score</span> <strong>${(product.opportunity_score ?? 0).toFixed(2)}</strong></li>
    <li><span>Annual Imports (USD)</span> <strong>${formatValue(product.last_12m_value_usd, 'usd')}</strong></li>
    <li><span>YoY Variation</span> <strong>${formatPercentage(detail.progress?.reduction_pct)}</strong></li>
    <li><span>Market Concentration (HHI)</span> <strong>${(detail.progress?.hhi_current ?? 0).toFixed(2)}</strong></li>
    <li><span>Est. Capital Req.</span> <strong>${formatRange(product.capex_min, product.capex_max)}</strong></li>
  `;

  renderTrendChart(detail.timeseries);
  renderPartnerChart(detail.partners);
  renderPartnerList(detail.partners);
}

function renderTrendChart(timeseries = []) {
  const ctx = document.getElementById('trendChart');
  const labels = timeseries.map((d) => `${d.year}-${String(d.month).padStart(2, '0')}`);
  const usdValues = timeseries.map((d) => d.value_usd || 0);

  if (state.trendChart) state.trendChart.destroy();
  
  // Neoclassical Chart Styling
  Chart.defaults.font.family = "'Tenor Sans', sans-serif";
  Chart.defaults.color = '#5d5d5d';
  
  state.trendChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Import Value (USD)',
        data: usdValues,
        borderColor: CHART_COLORS.primary,
        backgroundColor: 'rgba(212, 175, 55, 0.05)', // Gold wash
        borderWidth: 2,
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 6,
        fill: true,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1a1a1a',
          titleFont: { family: 'Cinzel' },
          padding: 12,
          displayColors: false
        }
      },
      scales: {
        x: { grid: { display: false } },
        y: { 
          beginAtZero: true,
          grid: { color: '#e6e1d8' },
          border: { display: false }
        },
      },
    },
  });
}

function renderPartnerChart(partners = []) {
  const ctx = document.getElementById('partnerChart');
  const labels = partners.map((p) => p.partner_country || 'Unknown');
  const values = partners.map((p) => p.value_usd || 0);
  if (state.partnerChart) state.partnerChart.destroy();
  
  state.partnerChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: CHART_COLORS.palette,
        borderWidth: 0,
        hoverOffset: 4
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '70%',
      plugins: {
        legend: { display: false },
      },
    },
  });
}

function renderPartnerList(partners = []) {
  const list = document.getElementById('partnerList');
  // Take top 5 only for the list to keep UI clean
  list.innerHTML = partners.slice(0, 5)
    .map((p) => `<li><span>${p.partner_country || 'Unknown'}</span> <span>${formatValue(p.value_usd, 'usd')}</span></li>`)
    .join('');
}

function renderAlerts(items) {
  const alerts = computeAlerts(items);
  const list = document.getElementById('alertsList');
  list.innerHTML = alerts.slice(0, 10) // Limit to 10 to fit sidebar
    .map(
      (alert) => `
      <li>
        <strong>${alert.hs_code}</strong>
        <div class="meta" style="font-size: 0.8rem; color: var(--text-main)">${alert.title}</div>
        <div class="meta">Score: ${(alert.opportunity_score ?? 0).toFixed(2)}</div>
      </li>
    `,
    )
    .join('');
}

function computeAlerts(items) {
  return items.filter((item) => {
    const riskyConcentration = (item.hhi_current ?? 0) > 0.6;
    const risingImports = (item.reduction_pct ?? 0) < -0.05;
    const bigOpportunity = (item.opportunity_score ?? 0) >= 0.65;
    return riskyConcentration || risingImports || bigOpportunity;
  });
}

function updateBusinessCase(detail) {
  if (!detail) return;
  document.getElementById('caseHs').value = detail.product.hs_code;
  const capexMin = detail.product.capex_min || 0;
  const capexMax = detail.product.capex_max || capexMin;
  const impliedCapex = capexMax || capexMin;
  if (impliedCapex) {
    document.getElementById('caseCapex').placeholder = usdFormatter.format(impliedCapex);
  }
}

function handleBusinessCaseSubmit(event) {
  event.preventDefault();
  if (!state.selected) {
    showToast('Please select a product first', 'error');
    return;
  }
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
  const paybackLabel = Number.isFinite(paybackMonths) ? `${paybackMonths.toFixed(1)} months` : 'N/A';

  const container = document.getElementById('businessCaseResults');
  container.innerHTML = `
    <div class="result-card">
      <h4>Monthly Revenue</h4>
      <p>${usdFormatter.format(revenue)}</p>
    </div>
    <div class="result-card">
      <h4>Gross Margin</h4>
      <p>${usdFormatter.format(grossMargin)}</p>
    </div>
    <div class="result-card">
      <h4>Est. Capex</h4>
      <p>${usdFormatter.format(capex)}</p>
    </div>
    <div class="result-card">
      <h4>Payback</h4>
      <p>${paybackLabel}</p>
    </div>
  `;
}

async function handleCompareSubmit(event) {
  event.preventDefault();
  const hsA = document.getElementById('compareA').value;
  const hsB = document.getElementById('compareB').value;
  if (!hsA || !hsB || hsA === hsB) {
    showToast('Select two distinct items to compare', 'error');
    return;
  }
  try {
    const [detailA, detailB] = await Promise.all([
      fetch(`/api/products/${hsA}`).then((res) => res.json()),
      fetch(`/api/products/${hsB}`).then((res) => res.json()),
    ]);
    renderCompare(detailA, detailB);
  } catch (error) {
    console.error(error);
    showToast('Comparison failed', 'error');
  }
}

function renderCompare(a, b) {
  const container = document.getElementById('compareResults');
  container.innerHTML = [a, b]
    .map((detail) => {
      const product = detail.product;
      return `
        <div class="compare-card" style="border: 1px solid var(--border-light); padding: 1rem; background: #fff;">
          <h3>${product.title}</h3>
          <p class="meta-serif">${product.hs_code}</p>
          <ul class="serif-list" style="list-style: none; padding: 0;">
            <li><span>Score</span> <span>${(product.opportunity_score ?? 0).toFixed(2)}</span></li>
            <li><span>Imports</span> <span>${formatValue(product.last_12m_value_usd, 'usd')}</span></li>
            <li><span>HHI</span> <span>${(detail.progress?.hhi_current ?? 0).toFixed(2)}</span></li>
          </ul>
        </div>
      `;
    })
    .join('');
}

function populateCompareOptions(items) {
  const selectA = document.getElementById('compareA');
  const selectB = document.getElementById('compareB');
  const options = items
    .map((item) => `<option value="${item.hs_code}">${item.hs_code} — ${item.title}</option>`)
    .join('');
  selectA.innerHTML = `<option value="">Item A</option>${options}`;
  selectB.innerHTML = `<option value="">Item B</option>${options}`;
}

function updateWatchButton() {
  const button = document.getElementById('watchBtn');
  if (!state.selected) {
    button.disabled = true;
    return;
  }
  button.disabled = false;
  const hs = state.selected.product.hs_code;
  const exists = state.watchlist.some((item) => item.hs_code === hs);
  button.textContent = exists ? 'Remove Watchlist' : 'Add to Watchlist';
  button.onclick = exists ? () => removeFromWatchlist(hs) : () => addToWatchlist(state.selected);
  renderWatchlist();
}

function addToWatchlist(detail) {
  const entry = {
    hs_code: detail.product.hs_code,
    title: detail.product.title,
  };
  if (!state.watchlist.some((item) => item.hs_code === entry.hs_code)) {
    state.watchlist.push(entry);
    persistJSON('bfi_watchlist', state.watchlist);
    showToast('Added to watchlist');
  }
  updateWatchButton();
}

function removeFromWatchlist(hsCode) {
  state.watchlist = state.watchlist.filter((item) => item.hs_code !== hsCode);
  persistJSON('bfi_watchlist', state.watchlist);
  updateWatchButton();
}

async function renderWatchlist() {
  const tbody = document.querySelector('#watchlistTable tbody');
  tbody.innerHTML = '';
  if (!state.watchlist.length) {
    tbody.innerHTML = '<tr><td colspan="3" class="meta-serif" style="text-align:center; padding: 2rem;">Watchlist Empty</td></tr>';
    return;
  }
  // Render stored items immediately, then update dynamically if needed
  // For speed, just rendering what we have in state for now, or minimal fetch
  for (const item of state.watchlist) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><strong>${item.hs_code}</strong></td>
        <td class="meta-serif">${item.title.substring(0, 20)}…</td>
        <td><button class="btn text-only" type="button" style="color: var(--accent-alert)">×</button></td>
      `;
      tr.querySelector('button').addEventListener('click', () => removeFromWatchlist(item.hs_code));
      tbody.appendChild(tr);
  }
}

function handleCommunitySubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.target);
  const entry = {
    name: formData.get('name'),
    category: formData.get('category'),
    location: formData.get('location'),
    notes: formData.get('notes'),
  };
  state.community.unshift(entry);
  persistJSON('bfi_registry', state.community);
  event.target.reset();
  renderCommunity();
}

function renderCommunity() {
  const list = document.getElementById('communityList');
  if (!state.community.length) {
    list.innerHTML = '<li class="meta-serif" style="padding: 1rem; color: var(--text-muted);">Registry Empty</li>';
    return;
  }
  list.innerHTML = state.community
    .map(
      (entry) => `
      <li style="padding: 0.8rem 0; border-bottom: 1px solid var(--border-light);">
        <strong style="display:block; font-family: 'Cinzel'">${entry.name}</strong>
        <span class="meta" style="font-size: 0.8rem; color: var(--text-gold); text-transform: uppercase;">${entry.category}</span>
      </li>
    `,
    )
    .join('');
}

function downloadCsv() {
  // Same logic, maybe different filename
  const rows = [
    ['hs_code', 'title', 'last_12m_usd', 'opportunity_score', 'reduction_pct', 'sectors'],
    ...state.products.map((item) => [
      item.hs_code,
      item.title,
      item.last_12m_value_usd || 0,
      item.opportunity_score ?? '',
      item.reduction_pct ?? '',
      (item.sectors || []).join(';'),
    ]),
  ];
  const csv = rows.map((r) => r.map((value) => JSON.stringify(value ?? '')).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `bfi-report-${new Date().toISOString().slice(0, 10)}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function setStatus(message, busy = false) {
  const el = document.getElementById('status');
  el.textContent = message;
}

function renderSkeletonCards(count = 6) {
  const container = document.getElementById('cards');
  container.innerHTML = '';
  for (let i = 0; i < count; i++) {
    const card = document.createElement('div');
    card.className = 'card skeleton-card';
    container.appendChild(card);
  }
}

function animateCounters() {
  const toAnimate = [
    ['statTotalProducts', state.products.length],
    ['statImportUsd', state.products.reduce((acc, item) => acc + (item.last_12m_value_usd || 0), 0)],
    ['statAlerts', computeAlerts(state.products).length],
  ];
  
  // Calculate average separately as it's a float
  const avg = state.products.length ? state.products.reduce((acc, item) => acc + (item.opportunity_score || 0), 0) / state.products.length : 0;
  
  toAnimate.forEach(([id, target]) => countUp(id, target));
  
  // Animate Average Score (Float)
  const el = document.getElementById('statAvgScore');
  let start = 0;
  const duration = 1000;
  const startTime = Date.now();
  function tick() {
    const progress = Math.min(1, (Date.now() - startTime) / duration);
    const value = start + (avg - start) * progress;
    el.textContent = value.toFixed(2);
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

function countUp(elementId, target, duration = 1000) {
  const el = document.getElementById(elementId);
  if (!el) return;
  const start = 0;
  const startTime = Date.now();
  function tick() {
    const progress = Math.min(1, (Date.now() - startTime) / duration);
    // Cubic ease out
    const ease = 1 - Math.pow(1 - progress, 3);
    const value = Math.floor(start + (target - start) * ease);
    
    // Format based on ID
    if (elementId === 'statImportUsd') {
        el.textContent = usdFormatter.format(value);
    } else {
        el.textContent = numberFormatter.format(value);
    }
    
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

function showToast(message, type = 'info') {
  const el = document.getElementById('toast');
  el.textContent = message;
  el.hidden = false;
  el.classList.add('show');
  // Dynamic background for error
  if (type === 'error') el.style.backgroundColor = '#8b0000';
  else el.style.backgroundColor = '#1a1a1a';
  
  setTimeout(() => {
    el.classList.remove('show');
    setTimeout(() => { el.hidden = true; }, 400);
  }, 2500);
}

function handleHotkeys(e) {
  if (e.target && ['INPUT','SELECT','TEXTAREA'].includes(e.target.tagName)) return;
  if (e.key.toLowerCase() === 'f') document.getElementById('search').focus();
}

function formatValue(value, mode = 'usd') {
  if (value === null || value === undefined) return '—';
  if (mode === 'inr') return inrFormatter.format(value);
  if (mode === 'usd') return usdFormatter.format(value);
  return numberFormatter.format(value);
}

function formatRange(min, max) {
  if (!min && !max) return '—';
  if (min && max) return `${usdFormatter.format(min)} – ${usdFormatter.format(max)}`;
  return usdFormatter.format(min || max || 0);
}

function formatPercentage(value) {
  if (value === null || value === undefined) return '—';
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function average(values) {
  const cleaned = values.filter((val) => typeof val === 'number' && !Number.isNaN(val));
  if (!cleaned.length) return null;
  return cleaned.reduce((acc, val) => acc + val, 0) / cleaned.length;
}

function findLatestValue(timeseries = [], key) {
  const latest = [...timeseries].reverse().find((entry) => entry[key] !== null && entry[key] !== undefined);
  return latest ? latest[key] : null;
}

function persistJSON(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function restoreJSON(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch (error) {
    return fallback;
  }
}
