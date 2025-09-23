const cardsEl = document.getElementById('cards');
const statusEl = document.getElementById('status');
const chartCanvas = document.getElementById('chart');
const detailTitle = document.getElementById('detailTitle');
const detailMeta = document.getElementById('detailMeta');
const partnersEl = document.getElementById('partners');
const searchInput = document.getElementById('search');

let chartInstance = null;
let lastProducts = [];

async function fetchProducts() {
  const params = new URLSearchParams();
  const sectors = document.getElementById('sectors').value.trim();
  if (sectors) params.set('sectors', sectors);
  const minCapex = document.getElementById('minCapex').value;
  if (minCapex) params.set('min_capex', minCapex);
  const maxCapex = document.getElementById('maxCapex').value;
  if (maxCapex) params.set('max_capex', maxCapex);
  const sort = document.getElementById('sort').value;
  params.set('sort', sort);

  try {
    const response = await fetch(`/api/products?${params.toString()}`);
    if (!response.ok) throw new Error(`Request failed with ${response.status}`);
    const data = await response.json();
    lastProducts = data.items || [];
    renderStatus(data);
    renderCards(applySearchFilter(lastProducts));
  } catch (error) {
    console.error('Failed to load products', error);
    statusEl.textContent = 'Unable to load products — check API logs.';
    cardsEl.innerHTML = '<p class="meta">No products available.</p>';
  }
}

function applySearchFilter(items) {
  const keyword = searchInput.value.trim().toLowerCase();
  if (!keyword) return items;
  return items.filter((item) => {
    return (
      item.title?.toLowerCase().includes(keyword) ||
      item.hs_code?.toLowerCase().includes(keyword)
    );
  });
}

function renderStatus(meta) {
  const source = meta?.source || 'n/a';
  const timestamp = meta?.last_updated || 'n/a';
  statusEl.textContent = `Source: ${source} • Updated: ${timestamp}`;
}

function renderCards(items) {
  cardsEl.innerHTML = '';
  if (!items.length) {
    cardsEl.innerHTML = '<p class="meta">No products match the filters.</p>';
    return;
  }

  items.forEach((item) => {
    const card = document.createElement('div');
    card.className = 'card';
    card.innerHTML = `
      <h3>${item.title} <span class="meta">(${item.hs_code})</span></h3>
      <div class="chips">${(item.sectors || []).map((s) => `<span class="chip">${s}</span>`).join('')}</div>
      <p class="meta">Last 12m Imports: $${formatNumber(item.last_12m_value_usd)}</p>
      <p class="meta">Opportunity Score: ${formatNumber(item.opportunity_score)}</p>
    `;
    card.addEventListener('click', () => loadDetail(item.hs_code));
    cardsEl.appendChild(card);
  });
}

async function loadDetail(hsCode) {
  try {
    const response = await fetch(`/api/products/${hsCode}`);
    if (!response.ok) throw new Error(`Request failed with ${response.status}`);
    const data = await response.json();
    renderDetail(data);
  } catch (error) {
    console.error('Failed to load detail', error);
    detailMeta.innerHTML = '<p class="meta">Unable to load detail.</p>';
  }
}

function renderDetail(data) {
  detailTitle.textContent = `${data.product.title} (${data.product.hs_code})`;
  const timeseries = data.timeseries || [];
  const labels = timeseries.map((d) => `${d.year}-${String(d.month).padStart(2, '0')}`);
  const values = timeseries.map((d) => d.value_usd || 0);

  if (chartInstance) {
    chartInstance.destroy();
  }
  chartInstance = new Chart(chartCanvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Monthly imports (USD)',
          data: values,
          borderColor: '#0a4d8c',
          backgroundColor: 'rgba(10, 77, 140, 0.2)',
          tension: 0.25,
        },
      ],
    },
    options: {
      scales: {
        y: {
          beginAtZero: true,
        },
      },
    },
  });

  const progress = data.progress || {};
  detailMeta.innerHTML = `
    <div class="meta-block">
      <div><strong>Sectors</strong>: ${(data.product.sectors || []).join(', ') || 'n/a'}</div>
      <div><strong>Baseline period</strong>: ${data.baseline_period || 'n/a'}</div>
      <div><strong>Opportunity score</strong>: ${formatNumber(progress.opportunity_score)}</div>
      <div><strong>Reduction %</strong>: ${formatPercent(progress.reduction_pct)}</div>
    </div>
  `;

  partnersEl.innerHTML = '<h3>Top partners</h3>';
  const list = document.createElement('ul');
  (data.partners || []).forEach((partner) => {
    const li = document.createElement('li');
    li.textContent = `${partner.partner_country || 'Unknown'} — $${formatNumber(partner.value_usd)}`;
    list.appendChild(li);
  });
  if (!list.childElementCount) {
    const li = document.createElement('li');
    li.textContent = 'No partner data available.';
    list.appendChild(li);
  }
  partnersEl.appendChild(list);
}

function formatNumber(value) {
  if (value === null || value === undefined) {
    return '—';
  }
  return Number(value).toLocaleString('en-IN', { maximumFractionDigits: 2 });
}

function formatPercent(value) {
  if (value === null || value === undefined) {
    return '—';
  }
  return `${(Number(value) * 100).toFixed(2)}%`;
}

searchInput.addEventListener('input', () => {
  renderCards(applySearchFilter(lastProducts));
});

document.getElementById('loadBtn').addEventListener('click', fetchProducts);

fetchProducts();
