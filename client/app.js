const state = {
  products: [],
  lastFetched: null,
  selected: null,
  trendChart: null,
  partnerChart: null,
  watchlist: restoreJSON('bfi_watchlist', []),
  community: [],
};

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

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('footerYear').textContent = new Date().getFullYear();
  // Clear previously persisted filters/community (no longer persisted by design)
  try {
    localStorage.removeItem('bfi_filters');
    localStorage.removeItem('bfi_registry');
  } catch (err) {
    // ignore storage errors
  }
  wireEvents();
  loadProducts();
  renderCommunity();
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

  // Do not persist filters anymore

}

async function loadProducts(force = false) {
  try {

    setStatus('Loading products…', true);
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

    setStatus(`Source: ${data.source ?? '—'} • Updated ${state.lastFetched ?? '—'}`);
    animateCounters();
    showToast(`Loaded ${state.products.length} items`);
  } catch (error) {
    console.error(error);
    setStatus('Unable to load products — check API status.');
    showToast('Load failed. Check API status.', 'error');

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

  document.getElementById('statTotalProducts').textContent = numberFormatter.format(total);
  document.getElementById('statImportUsd').textContent = usdFormatter.format(importUsd);
  document.getElementById('statAvgScore').textContent = avgScore.toFixed(2);
  document.getElementById('statAlerts').textContent = alertCount;
}

function renderCards(items) {
  const filtered = filterProducts(items);
  const container = document.getElementById('cards');
  container.innerHTML = '';

  if (!filtered.length) {
    container.innerHTML = '<p class="meta">No results match the current filters.</p>';
    return;
  }

  filtered.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'card';
    card.innerHTML = `
      <h3>
        <span>${item.title}</span>
        <span class="meta">${item.hs_code}</span>
      </h3>
      <div class="chips">${(item.sectors || []).map((s) => `<span class="chip">${s}</span>`).join('')}</div>
      <div class="meta">12m imports: ${formatValue(item.last_12m_value_usd, 'usd')}</div>
      <div class="meta">Opportunity score: ${(item.opportunity_score ?? 0).toFixed(2)}</div>
      <div class="meta">YoY change: ${formatPercentage(item.reduction_pct)}</div>

      <div>
        <button class="btn outline" type="button" data-watch="${item.hs_code}">Watch</button>
      </div>
    `;
    card.tabIndex = 0;
    card.setAttribute('role', 'button');
    card.setAttribute('aria-label', `${item.title} ${item.hs_code}`);
    card.addEventListener('click', () => loadDetail(item.hs_code));
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        loadDetail(item.hs_code);
      }
    });

    container.appendChild(card);
  });

  document.getElementById('resultsMeta').textContent = `${filtered.length} items shown`;


  container.querySelectorAll('button[data-watch]').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const hs = btn.getAttribute('data-watch');
      fetch(`/api/products/${hs}`).then((res) => res.json()).then((detail) => {
        addToWatchlist(detail);
        showToast(`Added ${detail.product.title} to watchlist`);
      });
    });
  });

}

async function loadDetail(hsCode) {
  try {
    const response = await fetch(`/api/products/${hsCode}`);
    if (!response.ok) throw new Error(`Failed to load detail for ${hsCode}`);
    const data = await response.json();
    state.selected = data;
    updateDetail(data);
    updateBusinessCase(data);
    updateWatchButton();
  } catch (error) {
    console.error(error);
  }
}

function updateDetail(detail) {
  const product = detail.product;
  document.getElementById('detailTitle').textContent = `${product.title} (${product.hs_code})`;

  document.getElementById('detailTitle').focus();
  document.getElementById('detail').scrollIntoView({ behavior: 'smooth', block: 'start' });


  const snapshot = document.getElementById('detailSnapshot');
  snapshot.innerHTML = `
    <li>Opportunity score: ${(product.opportunity_score ?? 0).toFixed(2)}</li>
    <li>12m imports (USD): ${formatValue(product.last_12m_value_usd, 'usd')}</li>
    <li>12m imports (INR): ${formatValue(findLatestValue(detail.timeseries, 'value_inr'), 'inr')}</li>
    <li>YoY change: ${formatPercentage(detail.progress?.reduction_pct)}</li>
    <li>Supplier HHI: ${(detail.progress?.hhi_current ?? 0).toFixed(2)}</li>
    <li>Capex (USD): ${formatRange(product.capex_min, product.capex_max)}</li>
  `;

  renderTrendChart(detail.timeseries);
  renderPartnerChart(detail.partners);
  renderPartnerList(detail.partners);
}

function renderTrendChart(timeseries = []) {
  const ctx = document.getElementById('trendChart');
  const labels = timeseries.map((d) => `${d.year}-${String(d.month).padStart(2, '0')}`);
  const usdValues = timeseries.map((d) => d.value_usd || 0);
  const inrValues = timeseries.map((d) => d.value_inr || 0);

  if (state.trendChart) state.trendChart.destroy();
  state.trendChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Monthly imports (USD)',
          data: usdValues,

          borderColor: '#f97316',
          backgroundColor: 'rgba(249, 115, 22, 0.18)',

          tension: 0.3,
          fill: true,
        },
        {
          label: 'Monthly imports (INR)',
          data: inrValues,

          borderColor: '#16a34a',
          backgroundColor: 'rgba(22, 163, 74, 0.18)',

          tension: 0.25,
          fill: true,
        },
      ],
    },
    options: {
      plugins: {
        legend: { display: true },
      },
      scales: {
        y: { beginAtZero: true },
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
      datasets: [
        {
          data: values,

          backgroundColor: ['#2563eb', '#16a34a', '#f97316', '#0ea5e9', '#64748b'],

        },
      ],
    },
    options: {
      plugins: {
        legend: { position: 'bottom' },
      },
    },
  });
}

function renderPartnerList(partners = []) {
  const list = document.getElementById('partnerList');
  list.innerHTML = partners
    .map((p) => `<li>${p.partner_country || 'Unknown'} — ${formatValue(p.value_usd, 'usd')}</li>`)
    .join('');
}

function renderAlerts(items) {
  const alerts = computeAlerts(items);
  const list = document.getElementById('alertsList');
  list.innerHTML = alerts
    .map(
      (alert) => `
      <li>
        <strong>${alert.title} (${alert.hs_code})</strong><br />
        Opportunity: ${(alert.opportunity_score ?? 0).toFixed(2)} • YoY change: ${formatPercentage(alert.reduction_pct)} • HHI: ${(alert.hhi_current ?? 0).toFixed(2)}
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
    alert('Select a product first.');
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
  const paybackLabel = Number.isFinite(paybackMonths) ? `${paybackMonths.toFixed(1)} months` : 'Not reachable';

  const container = document.getElementById('businessCaseResults');
  container.innerHTML = `
    <div class="result-card">
      <h4>Monthly revenue</h4>
      <p>${usdFormatter.format(revenue)}</p>
    </div>
    <div class="result-card">
      <h4>Gross margin (after Opex)</h4>
      <p>${usdFormatter.format(grossMargin)}</p>
    </div>
    <div class="result-card">
      <h4>Estimated Capex</h4>
      <p>${usdFormatter.format(capex)}</p>
    </div>
    <div class="result-card">
      <h4>Payback horizon</h4>
      <p>${paybackLabel}</p>
    </div>
  `;
}

async function handleCompareSubmit(event) {
  event.preventDefault();
  const hsA = document.getElementById('compareA').value;
  const hsB = document.getElementById('compareB').value;
  if (!hsA || !hsB || hsA === hsB) {
    alert('Select two distinct HS codes to compare.');
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
    alert('Unable to fetch comparison data.');
  }
}

function renderCompare(a, b) {
  const container = document.getElementById('compareResults');
  container.innerHTML = [a, b]
    .map((detail) => {
      const product = detail.product;
      return `
        <div class="compare-card">
          <h3>${product.title}</h3>
          <p class="meta">${product.hs_code}</p>
          <ul class="meta-list">
            <li>Opportunity score: ${(product.opportunity_score ?? 0).toFixed(2)}</li>
            <li>12m imports (USD): ${formatValue(product.last_12m_value_usd, 'usd')}</li>
            <li>YoY change: ${formatPercentage(detail.progress?.reduction_pct)}</li>
            <li>Supplier concentration (HHI): ${(detail.progress?.hhi_current ?? 0).toFixed(2)}</li>
            <li>CAPEX (USD): ${formatRange(product.capex_min, product.capex_max)}</li>
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
  selectA.innerHTML = `<option value="">Select HS code</option>${options}`;
  selectB.innerHTML = `<option value="">Select HS code</option>${options}`;
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
  button.textContent = exists ? 'Remove from watchlist' : 'Add to watchlist';
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
    tbody.innerHTML = '<tr><td colspan="6" class="meta">Empty watchlist</td></tr>';
    return;
  }
  for (const item of state.watchlist) {
    try {
      const detail = await fetch(`/api/products/${item.hs_code}`).then((res) => res.json());
      const tr = document.createElement('tr');
      const opportunity = detail.product.opportunity_score ?? 0;
      const yoy = detail.progress?.reduction_pct ?? 0;
      const alerts = [];
      if (opportunity >= 0.7) alerts.push('High opportunity');
      if (yoy < -0.05) alerts.push('Import spike');
      if ((detail.progress?.hhi_current ?? 0) > 0.6) alerts.push('Supplier concentration');
      tr.innerHTML = `
        <td>${detail.product.hs_code}</td>
        <td>${detail.product.title}</td>
        <td>${opportunity.toFixed(2)}</td>
        <td>${formatPercentage(yoy)}</td>
        <td>${alerts.join(', ') || '—'}</td>
        <td><button class="btn outline" type="button" data-hs="${detail.product.hs_code}">Remove</button></td>
      `;
      tr.querySelector('button').addEventListener('click', () => removeFromWatchlist(detail.product.hs_code));
      tbody.appendChild(tr);
    } catch (error) {
      console.error(error);
    }
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
    list.innerHTML = '<li class="meta">No community entries yet.</li>';
    return;
  }
  list.innerHTML = state.community
    .map(
      (entry) => `
      <li>
        <strong>${entry.name}</strong><br />
        <span class="meta">${entry.category}${entry.location ? ' • ' + entry.location : ''}</span>
        ${entry.notes ? `<p class="meta">${entry.notes}</p>` : ''}
      </li>
    `,
    )
    .join('');
}

function downloadCsv() {
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
  link.download = `build-for-india-${new Date().toISOString().slice(0, 10)}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}


function setStatus(message, busy = false) {
  const el = document.getElementById('status');
  el.textContent = message;
  el.setAttribute('aria-busy', busy ? 'true' : 'false');
}

function renderSkeletonCards(count = 8) {
  const container = document.getElementById('cards');
  container.innerHTML = '';
  const grid = document.createElement('div');
  grid.className = 'skeleton-grid';
  for (let i = 0; i < count; i++) {
    const card = document.createElement('div');
    card.className = 'skeleton-card';
    card.innerHTML = `
      <div class="skeleton-line lg"></div>
      <div class="skeleton-line md"></div>
      <div class="skeleton-line sm"></div>
    `;
    grid.appendChild(card);
  }
  container.appendChild(grid);
}

function animateCounters() {
  const toAnimate = [
    ['statTotalProducts', state.products.length],
    ['statAlerts', computeAlerts(state.products).length],
  ];
  toAnimate.forEach(([id, target]) => countUp(id, Number(target) || 0, 600));
}

function countUp(elementId, target, duration = 800) {
  const el = document.getElementById(elementId);
  if (!el) return;
  const start = 0;
  const startTime = Date.now();
  function tick() {
    const progress = Math.min(1, (Date.now() - startTime) / duration);
    const value = Math.floor(start + (target - start) * progress);
    el.textContent = new Intl.NumberFormat('en-IN').format(value);
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

function showToast(message, type = 'info') {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = message;
  el.hidden = false;
  el.classList.add('show');
  setTimeout(() => {
    el.classList.remove('show');
    el.hidden = true;
  }, 2200);
}

function handleHotkeys(e) {
  if (e.target && ['INPUT','SELECT','TEXTAREA'].includes(e.target.tagName)) return;
  if (e.key.toLowerCase() === 'f') {
    document.getElementById('search').focus();
  } else if (e.key.toLowerCase() === 'r') {
    document.getElementById('refreshBtn').click();
  }
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
    console.warn(`Failed to parse ${key} from localStorage`, error);
    return fallback;
  }

}

