const state = {
  products: [],
  total: 0,
  source: null,
  lastUpdated: null,
  latestPeriod: null,
  lastFetched: null,
  selected: null,
  trendChart: null,
  partnerChart: null,
  watchlist: restoreJSON('bfi_watchlist', []),
  community: [],
  viewMode: 'grid', // 'grid' or 'table'
  loadRequestId: 0,
  productAbortController: null
};

const PRODUCT_PAGE_SIZE = 500;
const MAX_PRODUCT_PAGES = 1000;
const MAX_RENDERED_PRODUCTS = 240;
const MAX_COMPARE_OPTIONS = 300;

// Neoclassical Formatters
const numberFormatter = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });
const usdFormatter = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const inrFormatter = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 });

const CHART_COLORS = {
  primary: '#d4af37', secondary: '#2c3e50', accent: '#8b0000', neutral: '#e6e1d8',
  palette: ['#d4af37', '#2c3e50', '#5d5d5d', '#aa8c2c', '#1a1a1a']
};

document.addEventListener('DOMContentLoaded', () => {
  const fy = document.getElementById('footerYear'); if (fy) fy.textContent = new Date().getFullYear();
  initTheme();
  wireEvents();
  loadProducts();
  renderWatchlistSidebar();

  // Navigation
  document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
      const btn = e.currentTarget;
      document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
      document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));
      btn.classList.add('active');
      const viewId = `view-${btn.dataset.view}`;
      const viewEl = document.getElementById(viewId);
      if (viewEl) viewEl.classList.add('active');
    });
  });
});

function wireEvents() {
  // Filters & Actions
  const loadBtn = document.getElementById('loadBtn'); if (loadBtn) loadBtn.addEventListener('click', () => loadProducts());
  const refreshBtn = document.getElementById('refreshBtn'); if (refreshBtn) refreshBtn.addEventListener('click', () => loadProducts(true));
  const downloadBtn = document.getElementById('downloadCsvBtn'); if (downloadBtn) downloadBtn.addEventListener('click', downloadCsv);
  
  // Forms
  const bcForm = document.getElementById('businessCaseForm'); if (bcForm) bcForm.addEventListener('submit', handleBusinessCaseSubmit);
  const compareForm = document.getElementById('compareForm'); if (compareForm) compareForm.addEventListener('submit', handleCompareSubmit);
  const communityForm = document.getElementById('communityForm'); if (communityForm) communityForm.addEventListener('submit', handleCommunitySubmit);
  const searchInput = document.getElementById('search'); if (searchInput) searchInput.addEventListener('input', (event) => {
    state.searchKeyword = event.currentTarget.value.trim().toLowerCase();
    if (state.productAbortController) return;
    renderResults(state.products);
  });
  
  // Drawer
  const closeBtn = document.getElementById('closeDrawer'); if (closeBtn) closeBtn.addEventListener('click', closeDrawer);
  const backdrop = document.getElementById('drawerBackdrop'); if (backdrop) backdrop.addEventListener('click', closeDrawer);
  const drawerWatch = document.getElementById('drawerWatchBtn'); if (drawerWatch) drawerWatch.addEventListener('click', toggleWatchFromDrawer);

  // View Toggles
  const viewGrid = document.getElementById('viewGrid'); if (viewGrid) viewGrid.addEventListener('click', () => switchViewMode('grid'));
  const viewTable = document.getElementById('viewTable'); if (viewTable) viewTable.addEventListener('click', () => switchViewMode('table'));
  const themeToggle = document.getElementById('themeToggle'); if (themeToggle) themeToggle.addEventListener('click', toggleTheme);
}

function switchViewMode(mode) {
  state.viewMode = mode;
  document.getElementById('viewGrid').classList.toggle('active', mode === 'grid');
  document.getElementById('viewTable').classList.toggle('active', mode === 'table');
  
  document.getElementById('cards').classList.toggle('hidden', mode !== 'grid');
  document.getElementById('tableContainer').classList.toggle('hidden', mode !== 'table');
  renderResults(state.products);
}

async function loadProducts(force = false) {
  const requestId = ++state.loadRequestId;
  if (state.productAbortController) state.productAbortController.abort();
  const controller = new AbortController();
  state.productAbortController = controller;
  const params = buildQuery();

  state.products = [];
  state.total = 0;
  state.source = null;
  state.lastUpdated = null;
  state.latestPeriod = null;
  setProductControlsLoading(true);
  renderProductLoadingState();

  try {
    const data = await fetchAllProductPages(params, {
      signal: controller.signal,
      cache: force ? 'no-store' : 'default'
    });
    if (requestId !== state.loadRequestId) return;

    state.products = data.items;
    state.total = data.total;
    state.source = data.source;
    state.lastUpdated = data.last_updated;
    state.latestPeriod = data.latest_period;
    state.lastFetched = new Date().toISOString();

    renderStats(state.products, state.total);
    renderResults(state.products);
    renderAlerts(state.products);
    populateCompareOptions(state.products);
    renderDataStatus();

    animateCounters();
    showToast(
      state.products.length
        ? `Updated: ${state.products.length} of ${state.total} items`
        : 'No products matched these filters'
    );
  } catch (error) {
    if (error.name === 'AbortError' || requestId !== state.loadRequestId) return;
    console.error(error);
    state.products = [];
    state.total = 0;
    state.source = null;
    state.lastUpdated = null;
    state.latestPeriod = null;
    renderProductErrorState(error);
    showToast('Unable to fetch complete product data', 'error');
  } finally {
    if (requestId === state.loadRequestId) {
      state.productAbortController = null;
      setProductControlsLoading(false);
    }
  }
}

async function fetchAllProductPages(baseParams, options = {}) {
  const requestedSort = baseParams.get('sort') || 'opportunity';
  const productsByCode = new Map();
  const seenPages = new Set();
  let rawOffset = 0;
  let reportedTotal = null;
  let source = null;
  let lastUpdated = null;
  let latestPeriod = null;
  let reachedEnd = false;

  for (let pageNumber = 0; pageNumber < MAX_PRODUCT_PAGES; pageNumber += 1) {
    const params = new URLSearchParams(baseParams);
    params.set('sort', 'catalog');
    params.set('limit', String(PRODUCT_PAGE_SIZE));
    params.set('offset', String(rawOffset));

    const response = await fetch(`/api/products?${params.toString()}`, options);
    if (!response.ok) {
      throw new Error(`Product request failed with status ${response.status}`);
    }

    const data = await response.json();
    if (!data || !Array.isArray(data.items)) {
      throw new Error('Product API returned an invalid response');
    }

    const pageItems = data.items;
    const pageTotal = asNonNegativeInteger(data.total);
    if (pageTotal !== null) reportedTotal = pageTotal;
    if (data.source != null) source = data.source;
    lastUpdated = newestTimestamp(lastUpdated, data.last_updated);
    if (data.latest_period != null) latestPeriod = data.latest_period;

    if (!pageItems.length) {
      reachedEnd = true;
      break;
    }

    const pageFingerprint = pageItems.map((item) => String(item?.hs_code ?? '')).join('|');
    if (seenPages.has(pageFingerprint)) {
      throw new Error('Product API repeated a page; loading stopped to avoid an endless loop');
    }
    seenPages.add(pageFingerprint);

    pageItems.forEach((item, index) => {
      const code = item?.hs_code;
      const key = code == null || code === '' ? `offset:${rawOffset + index}` : `hs:${code}`;
      if (!productsByCode.has(key)) productsByCode.set(key, item);
    });

    rawOffset += pageItems.length;

    if (reportedTotal !== null && rawOffset >= reportedTotal) {
      reachedEnd = true;
      break;
    }
    if (reportedTotal === null && pageItems.length < PRODUCT_PAGE_SIZE) {
      reachedEnd = true;
      break;
    }
  }

  if (!reachedEnd) {
    throw new Error(`Product pagination exceeded the ${MAX_PRODUCT_PAGES}-page safety limit`);
  }

  const items = Array.from(productsByCode.values());
  if (reportedTotal !== null && items.length < reportedTotal) {
    throw new Error(`Product API returned only ${items.length} unique records out of ${reportedTotal}`);
  }

  items.sort(productComparator(requestedSort));
  return {
    items,
    total: Math.max(reportedTotal ?? 0, items.length),
    source,
    last_updated: lastUpdated,
    latest_period: latestPeriod
  };
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

function renderStats(items, reportedTotal = items.length) {
  const importValues = items.map((item) => finiteNumber(item.last_12m_value_usd)).filter((value) => value !== null);
  const scores = items.map((item) => finiteNumber(item.opportunity_score)).filter((value) => value !== null);
  const importUsd = importValues.length ? importValues.reduce((acc, value) => acc + value, 0) : null;
  const avgScore = scores.length ? scores.reduce((acc, value) => acc + value, 0) / scores.length : null;
  const alertCount = computeAlerts(items).length;

  document.getElementById('statTotalProducts').textContent = numberFormatter.format(reportedTotal);
  document.getElementById('statImportUsd').textContent = formatValue(importUsd, 'usd');
  document.getElementById('statAvgScore').textContent = formatMetric(avgScore);
  document.getElementById('statAlerts').textContent = scores.length ? alertCount : 'Unavailable';

  const importStat = document.getElementById('statImportUsd');
  importStat.title = metricCoverageTitle('12-month import value', importValues.length, items.length);
  const scoreStat = document.getElementById('statAvgScore');
  scoreStat.title = metricCoverageTitle('opportunity score', scores.length, items.length);
  document.getElementById('statAlerts').title = metricCoverageTitle('opportunity score', scores.length, items.length);
}

function renderResults(items) {
  const filtered = filterProducts(items);
  const visible = filtered.slice(0, MAX_RENDERED_PRODUCTS);
  const loadedMeta = `${items.length} of ${state.total} loaded`;
  const visibleMeta = filtered.length > visible.length
    ? `${visible.length} shown · ${filtered.length} matches`
    : `${filtered.length} matches`;
  document.getElementById('resultsMeta').textContent =
    filtered.length === items.length
      ? `${Math.min(visible.length, filtered.length)} shown · ${loadedMeta}`
      : `${visibleMeta} · ${loadedMeta}`;
  
  // Render Grid
  const cardContainer = document.getElementById('cards');
  const tableBody = document.querySelector('#resultsTable tbody');
  if (!filtered.length) {
    cardContainer.innerHTML = `
      <div class="results-state">
        <strong>No commodities found</strong>
        <span>Try broadening the search or filters.</span>
      </div>
    `;
    tableBody.innerHTML = '<tr class="state-row"><td colspan="6">No commodities found. Try broadening the search or filters.</td></tr>';
    return;
  }

  if (state.viewMode === 'grid') {
    cardContainer.innerHTML = visible.map(item => `
      <article class="card" onclick="openDrawer('${escapeHTML(item.hs_code)}')">
        <h3><span>${escapeHTML(item.title || 'Untitled commodity')}</span><span>${escapeHTML(item.hs_code)}</span></h3>
        <div class="chips">${(item.sectors || []).map(s => `<span class="chip">${escapeHTML(s)}</span>`).join('')}</div>
        <div class="meta">12m: ${formatValue(item.last_12m_value_usd, 'usd')}</div>
        <div class="meta">Score: ${formatMetric(item.opportunity_score)}</div>
      </article>
    `).join('');
    tableBody.innerHTML = '';
  } else {
    cardContainer.innerHTML = '';
    tableBody.innerHTML = visible.map(item => `
      <tr onclick="openDrawer('${escapeHTML(item.hs_code)}')">
        <td><strong>${escapeHTML(item.hs_code)}</strong></td>
        <td>${escapeHTML(item.title || 'Untitled commodity')}</td>
        <td>${(item.sectors || []).map(escapeHTML).join(', ')}</td>
        <td class="text-right">${formatValue(item.last_12m_value_usd, 'usd')}</td>
        <td class="text-right"><strong>${formatMetric(item.opportunity_score)}</strong></td>
        <td class="text-right">→</td>
      </tr>
    `).join('');
  }
}

// Drawer Logic
async function openDrawer(hsCode) {
  try {
    const response = await fetch(`/api/products/${encodeURIComponent(hsCode)}`);
    if (!response.ok) throw new Error(`Detail request failed with status ${response.status}`);
    const data = await response.json();
    if (!data?.product) throw new Error('Product detail response is incomplete');
    state.selected = data;
    
    // Populate Drawer
    const product = data.product;
    document.getElementById('drawerTitle').textContent = `${product.title} (${product.hs_code})`;
    
    // Update Watch Button State
    updateDrawerWatchButton();
    
    // Snapshot
    document.getElementById('detailSnapshot').innerHTML = `
      <li><span>Opp. Score</span> <strong>${formatMetric(product.opportunity_score)}</strong></li>
      <li><span>Imports (12m)</span> <strong>${formatValue(product.last_12m_value_usd, 'usd')}</strong></li>
      <li><span>HHI</span> <strong>${formatMetric(data.progress?.hhi_current)}</strong></li>
      <li><span>Capex Range</span> <strong>${formatRange(product.capex_min, product.capex_max)}</strong></li>
    `;
    
    // Pre-fill Business Case
    const caseHsEl = document.getElementById('caseHs'); if (caseHsEl) caseHsEl.value = product.hs_code;
    const capex = product.capex_max || product.capex_min || 0;
    if (capex) document.getElementById('caseCapex').placeholder = usdFormatter.format(capex);
    document.getElementById('businessCaseResults').innerHTML = ''; // Clear previous results

    // Charts
    renderTrendChart(data.timeseries);
    renderPartnerChart(data.partners);
    renderPartnerList(data.partners);
    renderPartnerMeta(data.partner_period, data.partner_source);

    // Open UI
    document.getElementById('drawerBackdrop').classList.remove('hidden');
    document.getElementById('detailDrawer').classList.remove('hidden');
    setTimeout(() => document.getElementById('detailDrawer').classList.add('open'), 10);
    
  } catch (e) {
    console.error(e);
    state.selected = null;
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
  const values = timeseries.map(d => finiteNumber(d.value_usd));
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
  const available = partners.filter(p => finiteNumber(p.value_usd) !== null);
  const shares = available.map(p => finiteNumber(p.share));
  const useShares = available.length > 0 && shares.every(value => value !== null);
  const labels = available.map(p => p.partner_country);
  const values = useShares
    ? shares.map(value => value * 100)
    : available.map(p => finiteNumber(p.value_usd));
  if (useShares) {
    const other = Math.max(0, 100 - values.reduce((sum, value) => sum + value, 0));
    if (other > 0.01) {
      labels.push('Other');
      values.push(other);
    }
  }
  if (state.partnerChart) state.partnerChart.destroy();
  state.partnerChart = new Chart(ctx, {
    type: 'doughnut',
    data: { labels, datasets: [{ data: values, backgroundColor: CHART_COLORS.palette, borderWidth: 0 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: '60%', plugins: { legend: { display: false } } }
  });
}

function renderPartnerList(partners = []) {
  const list = document.getElementById('partnerList');
  if (!partners.length) {
    list.innerHTML = '<li><span>Partner data unavailable</span></li>';
    return;
  }
  list.innerHTML = partners.slice(0,5).map(p =>
    `<li><span>${escapeHTML(p.partner_country || 'Unknown')}</span> <span>${formatValue(p.value_usd, 'usd')}</span></li>`
  ).join('');
}

function renderPartnerMeta(period, source) {
  const element = document.getElementById('partnerMeta');
  if (!element) return;
  const details = [formatPeriod(period), formatSource(source)]
    .filter(value => value !== 'Unavailable');
  element.textContent = details.length ? details.join(' · ') : 'Partner data unavailable';
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

// Export CSV (current filtered view)
function downloadCsv() {
  try {
    const items = filterProducts(state.products);
    const rows = [
      ['hs_code', 'title', 'sectors', 'last_12m_usd', 'opportunity_score'],
      ...items.map((item) => [
        item.hs_code,
        item.title,
        (item.sectors || []).join(';'),
        item.last_12m_value_usd ?? '',
        item.opportunity_score ?? ''
      ]),
    ];
    const csv = rows.map(r => r.map(v => JSON.stringify(v ?? '')).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `bfi-commodities-${new Date().toISOString().slice(0,10)}.csv`;
    link.click();
    URL.revokeObjectURL(url);
    showToast('CSV exported');
  } catch (e) {
    console.error(e);
    showToast('Export failed', 'error');
  }
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
    fetch(`/api/products/${encodeURIComponent(hsA)}`).then(checkedJson),
    fetch(`/api/products/${encodeURIComponent(hsB)}`).then(checkedJson)
  ]).then(([dA, dB]) => {
    document.getElementById('compareResults').innerHTML = [dA, dB].map(d => `
      <div class="card compact">
        <strong>${d.product.title}</strong>
        <div class="meta">Score: ${formatMetric(d.product.opportunity_score)}</div>
        <div class="meta">Imp: ${formatValue(d.product.last_12m_value_usd, 'usd')}</div>
        </div>
    `).join('');
  }).catch((error) => {
    console.error(error);
    document.getElementById('compareResults').innerHTML = '<div class="results-state compact">Comparison unavailable.</div>';
    showToast('Could not load comparison', 'error');
  });
}

function populateCompareOptions(items) {
  const options = items.slice(0, MAX_COMPARE_OPTIONS).map(i =>
    `<option value="${escapeHTML(i.hs_code)}">${escapeHTML((i.title || 'Untitled commodity').substring(0,40))}</option>`
  ).join('');
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
  const list = document.getElementById('alertsList');
  if (!alerts.length) {
    list.innerHTML = '<li><span>No high-score signals available</span></li>';
    return;
  }
  list.innerHTML = alerts.map(a => `
    <li onclick="openDrawer('${a.hs_code}')" style="cursor:pointer">
      <strong>${a.hs_code}</strong>
      <span>Score: ${formatMetric(a.opportunity_score)}</span>
    </li>
  `).join('');
}

function computeAlerts(items) {
  return items.filter(item => {
    const score = finiteNumber(item.opportunity_score);
    return score !== null && score >= 0.7;
  });
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
function finiteNumber(value) {
  if (value == null || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}
function asNonNegativeInteger(value) {
  const number = finiteNumber(value);
  return number !== null && number >= 0 ? Math.floor(number) : null;
}
function formatValue(value, mode) {
  const number = finiteNumber(value);
  if (number === null) return 'Unavailable';
  return mode === 'usd' ? usdFormatter.format(number) : numberFormatter.format(number);
}
function formatMetric(value, digits = 2) {
  const number = finiteNumber(value);
  return number === null ? 'Unavailable' : number.toFixed(digits);
}
function formatRange(min, max) {
  const minimum = finiteNumber(min);
  const maximum = finiteNumber(max);
  if (minimum === null && maximum === null) return 'Unavailable';
  if (minimum === null) return `Up to ${usdFormatter.format(maximum)}`;
  if (maximum === null) return `From ${usdFormatter.format(minimum)}`;
  return `${usdFormatter.format(minimum)}–${usdFormatter.format(maximum)}`;
}
function formatPercentage(value) {
  const number = finiteNumber(value);
  return number === null ? 'Unavailable' : `${(number * 100).toFixed(1)}%`;
}
function average(values) {
  const available = values.map(finiteNumber).filter((value) => value !== null);
  return available.length ? available.reduce((a, b) => a + b, 0) / available.length : null;
}
function checkedJson(response) {
  if (!response.ok) throw new Error(`Request failed with status ${response.status}`);
  return response.json();
}
function productComparator(sort) {
  const valueFor = (item) => {
    if (sort === 'value') return finiteNumber(item.last_12m_value_usd);
    if (sort === 'progress') return finiteNumber(item.reduction_pct);
    return finiteNumber(item.opportunity_score);
  };
  return (left, right) => {
    const leftValue = valueFor(left);
    const rightValue = valueFor(right);
    if (leftValue !== rightValue) {
      if (leftValue === null) return 1;
      if (rightValue === null) return -1;
      return rightValue - leftValue;
    }
    return String(left.hs_code || '').localeCompare(String(right.hs_code || ''));
  };
}
function escapeHTML(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
function newestTimestamp(current, candidate) {
  if (candidate == null || candidate === '') return current;
  if (current == null || current === '') return candidate;
  const currentTime = Date.parse(current);
  const candidateTime = Date.parse(candidate);
  if (!Number.isFinite(currentTime)) return candidate;
  if (!Number.isFinite(candidateTime)) return current;
  return candidateTime > currentTime ? candidate : current;
}
function metricCoverageTitle(label, available, total) {
  if (!total) return `No ${label} data available`;
  return `${numberFormatter.format(available)} of ${numberFormatter.format(total)} products have ${label} data`;
}
function setProductControlsLoading(isLoading) {
  ['loadBtn', 'refreshBtn'].forEach((id) => {
    const button = document.getElementById(id);
    if (button) button.disabled = isLoading;
  });
  const dashboard = document.getElementById('view-dashboard');
  if (dashboard) dashboard.setAttribute('aria-busy', String(isLoading));
}
function renderProductLoadingState() {
  document.getElementById('resultsMeta').textContent = 'Loading all products…';
  document.getElementById('cards').innerHTML = '<div class="results-state"><strong>Loading products</strong><span>Retrieving the complete catalogue in batches…</span></div>';
  document.querySelector('#resultsTable tbody').innerHTML = '<tr class="state-row"><td colspan="6">Loading the complete product catalogue…</td></tr>';
  ['statTotalProducts', 'statImportUsd', 'statAvgScore', 'statAlerts'].forEach((id) => {
    document.getElementById(id).textContent = '—';
  });
  document.getElementById('alertsList').innerHTML = '<li><span>Loading signals…</span></li>';
  populateCompareOptions([]);
  document.getElementById('compareResults').innerHTML = '';
  renderDataStatus('loading');
}
function renderProductErrorState(error) {
  document.getElementById('resultsMeta').textContent = 'Data unavailable';
  document.getElementById('cards').innerHTML = '<div class="results-state error"><strong>Products could not be loaded</strong><span>Refresh to try the complete catalogue again.</span></div>';
  document.querySelector('#resultsTable tbody').innerHTML = '<tr class="state-row error"><td colspan="6">Products could not be loaded. Refresh to try again.</td></tr>';
  ['statTotalProducts', 'statImportUsd', 'statAvgScore', 'statAlerts'].forEach((id) => {
    document.getElementById(id).textContent = '—';
  });
  document.getElementById('alertsList').innerHTML = '<li><span>Signals unavailable</span></li>';
  populateCompareOptions([]);
  document.getElementById('compareResults').innerHTML = '';
  renderDataStatus('error', error?.message);
}
function renderDataStatus(status = 'ready', errorMessage = '') {
  const container = document.getElementById('dataStatus');
  const population = document.getElementById('populationStatus');
  const source = document.getElementById('sourceStatus');
  const period = document.getElementById('periodStatus');
  const freshness = document.getElementById('freshnessStatus');
  if (!container || !population || !source || !period || !freshness) return;

  container.dataset.state = status;
  if (status === 'loading') {
    population.textContent = 'Loading complete catalogue…';
    population.title = '';
    source.textContent = '—';
    period.textContent = '—';
    freshness.textContent = '—';
    freshness.title = '';
    return;
  }
  if (status === 'error') {
    population.textContent = 'Product data unavailable';
    population.title = errorMessage || '';
    source.textContent = 'Unavailable';
    period.textContent = 'Unavailable';
    freshness.textContent = 'Unavailable';
    freshness.title = '';
    return;
  }

  population.textContent = `${numberFormatter.format(state.products.length)} of ${numberFormatter.format(state.total)} products loaded`;
  population.title = '';
  source.textContent = formatSource(state.source);
  period.textContent = formatPeriod(state.latestPeriod);
  freshness.textContent = formatTimestamp(state.lastUpdated);
  freshness.title = state.lastUpdated || '';
}
function formatSource(value) {
  if (value == null || value === '') return 'Unavailable';
  const normalized = String(value).trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (normalized.startsWith('comtrade') || normalized === 'uncomtrade') return 'UN Comtrade';
  if (normalized === 'dgcis') return 'DGCI&S';
  if (normalized === 'database') return 'Database';
  return String(value);
}
function formatPeriod(value) {
  if (value == null || value === '') return 'Unavailable';
  const text = String(value).trim();
  const match = text.match(/^(\d{4})-?(\d{2})$/);
  if (!match) return text;
  const month = Number(match[2]);
  if (month < 1 || month > 12) return text;
  return new Intl.DateTimeFormat('en-IN', { month: 'short', year: 'numeric', timeZone: 'UTC' })
    .format(new Date(Date.UTC(Number(match[1]), month - 1, 1)));
}
function formatTimestamp(value) {
  if (value == null || value === '') return 'Unavailable';
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return String(value);
  return new Intl.DateTimeFormat('en-IN', {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(date);
}
function animateCounters() { /* ... Same as before ... */ }
function persistJSON(k, v) { localStorage.setItem(k, JSON.stringify(v)); }
function restoreJSON(k, d) { try { return JSON.parse(localStorage.getItem(k)) || d; } catch { return d; } }
function showToast(m, t) { 
  const el = document.getElementById('toast'); el.textContent = m; el.hidden=false; el.classList.add('show');
  if(t==='error') el.style.border='1px solid red'; else el.style.border='1px solid var(--text-gold)';
  setTimeout(()=> { el.classList.remove('show'); setTimeout(()=>el.hidden=true,400); }, 2000);
}

// Community Registry
function handleCommunitySubmit(event) {
  event.preventDefault();
  try {
    const form = event.target;
  const entry = {
      name: form.name.value?.trim(),
      category: form.category.value,
      location: form.location?.value?.trim() || '',
      notes: form.notes?.value?.trim() || '',
    };
    if (!entry.name || !entry.category) return;
  state.community.unshift(entry);
  persistJSON('bfi_registry', state.community);
    form.reset();
  renderCommunity();
    showToast('Added to registry');
  } catch (e) {
    console.error(e);
    showToast('Failed to add entry', 'error');
  }
}

function renderCommunity() {
  const list = document.getElementById('communityList');
  if (!list) return;
  if (!state.community.length) {
    list.innerHTML = '<li class="meta-serif" style="padding: 1rem; color: var(--text-muted);">Registry Empty</li>';
    return;
  }
  list.innerHTML = state.community.map((entry) => `
    <li>
      <strong style="display:block; font-family: 'Cinzel'">${entry.name}</strong>
      <span class="meta" style="font-size: 0.8rem; color: var(--text-gold); text-transform: uppercase;">${entry.category}${entry.location ? ' • ' + entry.location : ''}</span>
      ${entry.notes ? `<p class="meta-serif" style="margin:0.25rem 0 0">${entry.notes}</p>` : ''}
      </li>
  `).join('');
}

// Theme
function initTheme() {
  try {
    const saved = localStorage.getItem('theme'); // 'light' | 'dark' | null
    if (saved === 'dark' || saved === 'light') {
      document.documentElement.setAttribute('data-theme', saved);
    } else {
      document.documentElement.removeAttribute('data-theme'); // system default
    }
    refreshThemeToggleIcon();
  } catch (_) {}
}

function toggleTheme() {
  const html = document.documentElement;
  const current = html.getAttribute('data-theme');
  const next = current === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  refreshThemeToggleIcon();
}

function refreshThemeToggleIcon() {
  const btn = document.getElementById('themeToggle');
  if (!btn) return;
  const theme = document.documentElement.getAttribute('data-theme');
  const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const effectiveDark = theme ? (theme === 'dark') : prefersDark;
  btn.textContent = effectiveDark ? '☀' : '☾';
  btn.title = effectiveDark ? 'Switch to light' : 'Switch to dark';
}
