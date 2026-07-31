'use strict';

const Model = window.OpportunityModel;
if (!Model) throw new Error('Opportunity model failed to load');

const PRODUCT_PAGE_SIZE = 500;
const MAX_PRODUCT_PAGES = 1000;
const RESULT_INCREMENT = 18;
const EVIDENCE_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const STORAGE_KEYS = {
  profile: 'bfi_mandate_v3',
  shortlist: 'bfi_capital_plan_v2',
  allocations: 'bfi_allocations_v2',
  notes: 'bfi_opportunity_notes_v3',
  evidence: 'bfi_evidence_v3',
  theme: 'bfi_theme'
};

const ROLE_LABELS = {
  build: 'Build a company',
  back: 'Back an operator',
  partner: 'Form a strategic venture'
};

const ENTRY_MODEL_LABELS = {
  make: 'Manufacture',
  assemble: 'Assemble or integrate',
  contract: 'Contract make',
  enable: 'Enable suppliers'
};

const BUYER_LABELS = {
  none: 'No warm buyer access',
  factories: 'Factory buyers',
  oems: 'OEM buyers',
  government: 'Government buyers',
  consumer: 'Consumer channels',
  export: 'Export buyers'
};

const LENS_COPY = {
  fit: 'Leads with personal feasibility, then uses national importance to order credible routes.',
  national: 'Ranks import-value exposure and momentum; this does not prove a domestic capability gap.',
  rising: 'Ranks routes by modeled time to a first paid proof; unresolved categories are shown last.',
  wedge: 'Surfaces less-obvious, personally feasible routes with direct import-substitution potential.'
};

const usdCompactFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  notation: 'compact',
  maximumFractionDigits: 1
});
const usdFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0
});
const numberFormatter = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 });

const state = {
  products: [],
  routes: [],
  routeVersion: null,
  marketContext: null,
  ranked: [],
  filtered: [],
  metadata: {
    total: 0,
    source: null,
    latestPeriod: null,
    lastUpdated: null
  },
  profile: Model.sanitizeProfile(restoreJSON(STORAGE_KEYS.profile, {})),
  filters: {
    query: '',
    sector: 'all',
    lens: 'fit',
    visibleCount: RESULT_INCREMENT
  },
  shortlist: restoreShortlist(),
  allocations: sanitizeNumberMap(restoreJSON(STORAGE_KEYS.allocations, {})),
  notes: sanitizeNoteMap(restoreJSON(STORAGE_KEYS.notes, {})),
  detailEvidence: sanitizeEvidenceMap(restoreJSON(STORAGE_KEYS.evidence, {})),
  selected: null,
  view: 'discover',
  loading: false,
  loadRequestId: 0,
  catalogueController: null,
  detailRequestId: 0,
  detailController: null,
  lastFocusedElement: null,
  toastTimer: null,
  toastRequestId: 0,
  searchTimer: null
};

document.addEventListener('DOMContentLoaded', initialize);

function initialize() {
  initializeTheme();
  syncProfileControls();
  wireEvents();
  updateSavedCount();
  const initialView = ['discover', 'portfolio', 'method'].includes(location.hash.slice(1))
    ? location.hash.slice(1)
    : 'discover';
  switchView(initialView, { updateHash: false, focus: false });
  renderPortfolio();
  loadProducts();
}

function wireEvents() {
  document.addEventListener('click', handleDocumentClick);
  document.addEventListener('change', handleDocumentChange);
  document.addEventListener('input', handleDocumentInput);
  document.addEventListener('keydown', handleKeydown);

  document.getElementById('mandateForm').addEventListener('submit', handleMandateSubmit);
  document.getElementById('mandateForm').addEventListener('change', handleMandateChange);
  document.getElementById('searchInput').addEventListener('input', handleSearchInput);
  document.getElementById('sectorFilter').addEventListener('change', handleSectorFilter);
  document.getElementById('captureSelect').addEventListener('change', handleCaptureChange);
  document.getElementById('refreshBtn').addEventListener('click', () => loadProducts(true));
  document.getElementById('loadMoreBtn').addEventListener('click', handleLoadMore);
  document.getElementById('downloadCsvBtn').addEventListener('click', exportOpportunityScreen);
  document.getElementById('exportPlanBtn').addEventListener('click', exportCapitalPlan);
  document.getElementById('themeToggle').addEventListener('click', toggleTheme);
  document.getElementById('closeDossierBtn').addEventListener('click', closeDossier);
  document.getElementById('dossierBackdrop').addEventListener('click', closeDossier);
  document.getElementById('dossierSaveBtn').addEventListener('click', () => {
    if (state.selected) toggleShortlist(state.selected.opportunity.product.hs_code);
  });
}

function handleDocumentClick(event) {
  const actionTarget = event.target.closest('[data-action]');
  if (!actionTarget) return;
  const action = actionTarget.dataset.action;

  if (action === 'navigate') {
    event.preventDefault();
    closeDossier();
    switchView(actionTarget.dataset.view);
    return;
  }
  if (action === 'set-lens') {
    setLens(actionTarget.dataset.lens);
    return;
  }
  if (action === 'open-dossier') {
    openDossier(actionTarget.dataset.hsCode, actionTarget);
    return;
  }
  if (action === 'toggle-save') {
    const focusToken = interactionFocusToken(actionTarget);
    toggleShortlist(actionTarget.dataset.hsCode);
    restoreInteractionFocus(focusToken);
    return;
  }
  if (action === 'remove-plan') {
    removeFromPlan(actionTarget.dataset.hsCode);
    return;
  }
  if (action === 'save-notes') {
    saveOpportunityNotes(actionTarget.dataset.hsCode);
    return;
  }
  if (action === 'retry-load') {
    loadProducts(true);
  }
}

function handleDocumentChange() {}

function handleDocumentInput(event) {
  const target = event.target;
  if (target.matches('[data-action="set-allocation"]')) {
    const crore = Model.finiteNumber(target.value);
    const value = crore === null ? 0 : Math.max(0, crore * 10_000_000);
    state.allocations[target.dataset.hsCode] = value;
    persistJSON(STORAGE_KEYS.allocations, state.allocations);
    renderPortfolioStats();
  }
  if (target.id === 'dossierCapture') {
    const capturePct = Model.clamp(Number(target.value), 0.1, 5);
    state.profile.capturePct = capturePct;
    persistProfile();
    syncCaptureControl(capturePct);
    updateDossierScenario();
  }
}

function handleKeydown(event) {
  const dialog = document.getElementById('dossierDialog');
  if (dialog.hidden) return;
  if (event.key === 'Escape') {
    event.preventDefault();
    closeDossier();
    return;
  }
  if (event.key !== 'Tab') return;
  const focusable = [...dialog.querySelectorAll(
    'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])'
  )].filter((element) => !element.hidden && element.offsetParent !== null);
  if (!focusable.length) {
    event.preventDefault();
    dialog.focus();
    return;
  }
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
}

function handleMandateSubmit(event) {
  event.preventDefault();
  readProfileFromForm();
  renderDiscovery();
  document.getElementById('opportunityMap').scrollIntoView({ behavior: reducedMotion() ? 'auto' : 'smooth' });
  showToast('Opportunity map recalculated for your mandate');
}

function handleMandateChange() {
  readProfileFromForm();
  renderDiscovery();
  if (state.view === 'portfolio') renderPortfolio();
}

function handleSearchInput(event) {
  clearTimeout(state.searchTimer);
  const query = event.currentTarget.value.trim().toLowerCase();
  state.searchTimer = setTimeout(() => {
    state.filters.query = query;
    state.filters.visibleCount = RESULT_INCREMENT;
    renderOpportunityResults();
  }, 90);
}

function handleSectorFilter(event) {
  state.filters.sector = event.currentTarget.value;
  state.filters.visibleCount = RESULT_INCREMENT;
  renderOpportunityResults();
}

function handleCaptureChange(event) {
  state.profile.capturePct = Model.clamp(Number(event.currentTarget.value), 0.1, 5);
  persistProfile();
  renderDiscovery();
}

function handleLoadMore() {
  state.filters.visibleCount += RESULT_INCREMENT;
  renderOpportunityResults();
  const cards = document.querySelectorAll('.opportunity-card');
  const target = cards[Math.max(0, cards.length - RESULT_INCREMENT)];
  if (target) target.focus({ preventScroll: true });
}

function readProfileFromForm() {
  const form = new FormData(document.getElementById('mandateForm'));
  state.profile = Model.sanitizeProfile({
    capitalInr: Number(form.get('capital')),
    capitalMultiplier: Number(form.get('capitalMultiplier')),
    role: form.get('role'),
    horizonMonths: Number(form.get('horizon')),
    entryModel: form.get('entryModel'),
    strengths: form.getAll('strength'),
    buyerAccess: form.get('buyerAccess'),
    sectors: form.getAll('sector'),
    capturePct: Number(document.getElementById('captureSelect').value)
  });
  persistProfile();
  syncProfileControls();
}

function persistProfile() {
  persistJSON(STORAGE_KEYS.profile, state.profile);
}

function syncProfileControls() {
  const profile = state.profile;
  const role = document.querySelector(`input[name="role"][value="${cssEscape(profile.role)}"]`);
  const capital = document.querySelector(`input[name="capital"][value="${cssEscape(String(profile.capitalInr))}"]`);
  if (role) role.checked = true;
  if (capital) capital.checked = true;
  document.getElementById('capitalStackSelect').value = String(profile.capitalMultiplier);
  document.getElementById('horizonSelect').value = String(profile.horizonMonths);
  document.getElementById('entryModelSelect').value = profile.entryModel;
  document.getElementById('buyerSelect').value = profile.buyerAccess;
  document.querySelectorAll('input[name="strength"]').forEach((input) => {
    input.checked = profile.strengths.includes(input.value);
  });
  document.querySelectorAll('input[name="sector"]').forEach((input) => {
    input.checked = profile.sectors.includes(input.value);
  });
  syncCaptureControl(profile.capturePct);
}

function syncCaptureControl(value, select = document.getElementById('captureSelect')) {
  if (!select) return;
  select.querySelectorAll('option[data-custom="true"]').forEach((option) => option.remove());
  const normalized = Model.clamp(Number(value), 0.1, 5);
  const hasPreset = [...select.options].some((option) => Number(option.value) === normalized);
  if (!hasPreset) {
    const option = document.createElement('option');
    option.value = String(normalized);
    option.textContent = `${trimNumber(normalized)}% substitution`;
    option.dataset.custom = 'true';
    select.appendChild(option);
  }
  select.value = String(normalized);
}

function switchView(view, options = {}) {
  const normalized = ['discover', 'portfolio', 'method'].includes(view) ? view : 'discover';
  state.view = normalized;
  document.querySelectorAll('[data-view-panel]').forEach((panel) => {
    const active = panel.dataset.viewPanel === normalized;
    panel.hidden = !active;
    panel.classList.toggle('is-active', active);
  });
  document.querySelectorAll('.nav-item').forEach((item) => {
    const active = item.dataset.view === normalized;
    item.classList.toggle('is-active', active);
    if (active) item.setAttribute('aria-current', 'page');
    else item.removeAttribute('aria-current');
  });
  if (normalized === 'portfolio') renderPortfolio();
  if (normalized === 'method') renderMethodCoverage();
  if (options.updateHash !== false) history.replaceState(null, '', `#${normalized}`);
  if (options.focus !== false) window.scrollTo({ top: 0, behavior: reducedMotion() ? 'auto' : 'smooth' });
}

function setLens(lens) {
  if (!Object.prototype.hasOwnProperty.call(LENS_COPY, lens)) return;
  state.filters.lens = lens;
  state.filters.visibleCount = RESULT_INCREMENT;
  document.querySelectorAll('[data-action="set-lens"]').forEach((button) => {
    const active = button.dataset.lens === lens;
    button.classList.toggle('is-active', active);
    button.setAttribute('aria-pressed', String(active));
  });
  document.getElementById('lensExplainer').textContent = LENS_COPY[lens];
  renderOpportunityResults();
}

async function loadProducts(force = false) {
  const requestId = ++state.loadRequestId;
  if (state.catalogueController) state.catalogueController.abort();
  state.catalogueController = new AbortController();
  state.loading = true;
  renderLoadingState();

  try {
    const requestOptions = {
      signal: state.catalogueController.signal,
      cache: force ? 'no-store' : 'default'
    };
    const [data, routeRegistry] = await Promise.all([
      fetchCompleteCatalogue(requestOptions),
      fetchOpportunityRoutes(requestOptions)
    ]);
    if (requestId !== state.loadRequestId) return;
    state.products = data.items;
    state.routes = Model.normalizeRoutes(routeRegistry.routes);
    state.routeVersion = routeRegistry.version || null;
    state.marketContext = Model.createMarketContext(state.products);
    state.metadata = {
      total: data.total,
      source: data.source,
      latestPeriod: data.latest_period,
      lastUpdated: data.last_updated
    };
    pruneDetailEvidence();
    state.loading = false;
    renderDataStatus();
    renderDiscovery();
    renderMethodCoverage();
    renderPortfolio();
    showToast(`${numberFormatter.format(state.products.length)} markets and ${numberFormatter.format(state.routes.length)} reviewed routes ready`);
  } catch (error) {
    if (error.name === 'AbortError' || requestId !== state.loadRequestId) return;
    console.error(error);
    state.products = [];
    state.marketContext = null;
    state.loading = false;
    renderLoadError(error);
    showToast('The live opportunity universe could not be loaded', 'error');
  } finally {
    if (requestId === state.loadRequestId) state.catalogueController = null;
  }
}

async function fetchOpportunityRoutes(options) {
  try {
    const response = await fetch('/api/opportunity-routes', options);
    if (!response.ok) throw new Error(`Route request failed with status ${response.status}`);
    const data = await response.json();
    if (!data || !Array.isArray(data.routes)) throw new Error('Route API returned an invalid response');
    return data;
  } catch (error) {
    if (error.name === 'AbortError') throw error;
    console.warn('Reviewed opportunity routes are unavailable; showing trade signals as research only.', error);
    return { version: null, routes: [] };
  }
}

async function fetchCompleteCatalogue(options) {
  const byCode = new Map();
  const seenPages = new Set();
  let reportedTotal = null;
  let source = null;
  let latestPeriod = null;
  let lastUpdated = null;
  let offset = 0;
  let reachedEnd = false;

  for (let pageNumber = 0; pageNumber < MAX_PRODUCT_PAGES; pageNumber += 1) {
    const params = new URLSearchParams({
      sort: 'catalog',
      limit: String(PRODUCT_PAGE_SIZE),
      offset: String(offset)
    });
    const response = await fetch(`/api/products?${params.toString()}`, options);
    if (!response.ok) throw new Error(`Product request failed with status ${response.status}`);
    const data = await response.json();
    if (!data || !Array.isArray(data.items)) throw new Error('Product API returned an invalid response');

    const pageTotal = asNonNegativeInteger(data.total);
    if (pageTotal !== null) reportedTotal = pageTotal;
    source = data.source || source;
    latestPeriod = data.latest_period || latestPeriod;
    lastUpdated = newestTimestamp(lastUpdated, data.last_updated);

    if (!data.items.length) {
      reachedEnd = true;
      break;
    }
    const fingerprint = data.items.map((item) => String(item.hs_code || '')).join('|');
    if (seenPages.has(fingerprint)) {
      throw new Error('The product API repeated a page, so loading stopped safely');
    }
    seenPages.add(fingerprint);

    data.items.forEach((item) => {
      if (item && item.hs_code != null) byCode.set(String(item.hs_code), item);
    });
    offset += data.items.length;

    if (reportedTotal !== null && offset >= reportedTotal) {
      reachedEnd = true;
      break;
    }
    if (reportedTotal === null && data.items.length < PRODUCT_PAGE_SIZE) {
      reachedEnd = true;
      break;
    }
  }

  if (!reachedEnd) throw new Error('Product pagination exceeded its safety limit');
  const items = [...byCode.values()];
  if (reportedTotal !== null && items.length < reportedTotal) {
    throw new Error(`Only ${items.length} of ${reportedTotal} unique markets were returned`);
  }
  return {
    items,
    total: Math.max(reportedTotal || 0, items.length),
    source,
    latest_period: latestPeriod,
    last_updated: lastUpdated
  };
}

function renderLoadingState() {
  document.getElementById('dataStatus').setAttribute('aria-busy', 'true');
  document.getElementById('populationStatus').textContent = 'Loading the complete opportunity universe…';
  document.getElementById('sourceStatus').textContent = '—';
  document.getElementById('periodStatus').textContent = '—';
  document.getElementById('liveIndicator').classList.remove('is-live', 'is-error');
  document.querySelector('#liveIndicator span').textContent = 'Connecting';
  document.getElementById('heroMarketValue').textContent = '—';
  document.getElementById('heroProductCount').textContent = '—';
  document.getElementById('heroLatestPeriod').textContent = '—';
  document.getElementById('resultsMeta').textContent = 'Loading trade demand across every current HS6 market…';
  document.getElementById('opportunityGrid').innerHTML = `
    <article class="loading-card" aria-hidden="true"></article>
    <article class="loading-card" aria-hidden="true"></article>
    <article class="loading-card" aria-hidden="true"></article>
  `;
  document.getElementById('loadMoreBtn').hidden = true;
}

function renderLoadError(error) {
  document.getElementById('dataStatus').setAttribute('aria-busy', 'false');
  document.getElementById('populationStatus').textContent = 'Live opportunity data unavailable';
  document.getElementById('sourceStatus').textContent = 'Unavailable';
  document.getElementById('periodStatus').textContent = 'Unavailable';
  const indicator = document.getElementById('liveIndicator');
  indicator.classList.remove('is-live');
  indicator.classList.add('is-error');
  indicator.querySelector('span').textContent = 'Data offline';
  document.getElementById('resultsMeta').textContent = 'The evidence layer could not be loaded.';
  document.getElementById('opportunityGrid').innerHTML = `
    <div class="empty-state empty-state--error">
      <span>Connection interrupted</span>
      <h3>We could not assemble the opportunity universe.</h3>
      <p>${escapeHTML(error && error.message ? error.message : 'Try the live source again.')}</p>
      <button class="secondary-button" type="button" data-action="retry-load">Retry data load</button>
    </div>
  `;
}

function renderDataStatus() {
  const importTotal = state.products.reduce((sum, product) => {
    const value = Model.finiteNumber(product.last_12m_value_usd);
    return sum + (value === null ? 0 : value);
  }, 0);
  document.getElementById('dataStatus').setAttribute('aria-busy', 'false');
  document.getElementById('populationStatus').textContent =
    `${numberFormatter.format(state.products.length)} of ${numberFormatter.format(state.metadata.total)} markets loaded`;
  document.getElementById('sourceStatus').textContent = formatSource(state.metadata.source);
  document.getElementById('periodStatus').textContent = formatPeriod(state.metadata.latestPeriod);
  document.getElementById('dataStatus').title = state.metadata.lastUpdated
    ? `Analytics updated ${formatTimestamp(state.metadata.lastUpdated)}`
    : '';
  const indicator = document.getElementById('liveIndicator');
  indicator.classList.add('is-live');
  indicator.classList.remove('is-error');
  indicator.querySelector('span').textContent = 'Live data';
  document.getElementById('heroMarketValue').textContent = formatUsdCompact(importTotal);
  document.getElementById('heroProductCount').textContent = numberFormatter.format(state.metadata.total);
  document.getElementById('heroLatestPeriod').textContent = formatPeriod(state.metadata.latestPeriod);
}

function renderDiscovery() {
  renderMandateSummary();
  renderOpportunityResults();
}

function renderMandateSummary() {
  const selectedEdges = state.profile.sectors.length
    ? state.profile.sectors.map(titleCase).join(', ')
    : 'All sectors';
  const completeness = Model.profileCompleteness(state.profile);
  const projectCapital = Model.projectCapital(state.profile);
  document.getElementById('mandateSummary').textContent = [
    ROLE_LABELS[state.profile.role],
    `${formatInr(state.profile.capitalInr)} cheque`,
    `${formatInr(projectCapital)} modeled stack`,
    `paid proof within ${state.profile.horizonMonths}${state.profile.horizonMonths === 36 ? '+' : ''} months`,
    ENTRY_MODEL_LABELS[state.profile.entryModel],
    BUYER_LABELS[state.profile.buyerAccess],
    selectedEdges
  ].join(' · ');
  const status = document.getElementById('profileCompletenessStatus');
  if (status) status.textContent = `${completeness.answered}/${completeness.total} mandate signals · ${completeness.percentage}% specified`;
}

function renderOpportunityResults() {
  if (state.loading || !state.products.length) return;
  state.ranked = Model.rankOpportunities(
    state.products,
    state.profile,
    state.filters.lens,
    state.marketContext,
    state.routes
  );
  const query = state.filters.query;
  const sector = state.filters.sector;
  state.filtered = state.ranked.filter((opportunity) => {
    const product = opportunity.product;
    const searchText = [
      product.hs_code,
      product.title,
      product.description,
      opportunity.shortTitle,
      opportunity.route.wedge,
      opportunity.route.buyer,
      ...opportunity.sectors
    ].join(' ').toLowerCase();
    if (query && !searchText.includes(query)) return false;
    if (sector !== 'all' && !opportunity.sectors.includes(sector)) return false;
    if (!query && (opportunity.importValue === null || opportunity.importValue <= 0)) return false;
    if (!query && opportunity.classification.id === 'controlled') return false;
    return true;
  });

  const visible = state.filtered.slice(0, state.filters.visibleCount);
  const grid = document.getElementById('opportunityGrid');
  if (!visible.length) {
    grid.innerHTML = `
      <div class="empty-state">
        <span>No matching market</span>
        <h3>Try a broader sector or search term.</h3>
        <p>Zero-import markets stay out of the default ranked view but remain searchable.</p>
      </div>
    `;
  } else {
    grid.innerHTML = visible.map((opportunity, index) => opportunityCard(opportunity, index)).join('');
  }

  const lensName = {
    fit: 'personal feasibility tier, then fit',
    national: 'national import signal',
    rising: 'time to first paid proof',
    wedge: 'surprising executable wedge'
  }[state.filters.lens];
  document.getElementById('resultsMeta').textContent =
    `Showing ${numberFormatter.format(visible.length)} of ${numberFormatter.format(state.filtered.length)} markets ranked by ${lensName} · ${numberFormatter.format(state.routes.length)} reviewed entry routes`;
  const loadMore = document.getElementById('loadMoreBtn');
  loadMore.hidden = visible.length >= state.filtered.length;
  if (!loadMore.hidden) {
    loadMore.textContent = `Show ${Math.min(RESULT_INCREMENT, state.filtered.length - visible.length)} more opportunities`;
  }
}

function opportunityCard(opportunity, index) {
  const product = opportunity.product;
  const hsCode = escapeHTML(product.hs_code);
  const saved = isSaved(product.hs_code);
  const lensScore = Model.scoreForLens(opportunity, state.filters.lens);
  const lensScoreLabel = state.filters.lens === 'rising' && opportunity.momentum === null
    ? '—'
    : String(Math.round(lensScore));
  const sector = opportunity.sectors[0] || 'unclassified';
  const trendClass = opportunity.reductionPct === null
    ? 'neutral'
    : opportunity.reductionPct < -0.001
      ? 'up'
      : opportunity.reductionPct > 0.001
        ? 'down'
        : 'neutral';
  const structuralClass = opportunity.isStructural
    ? ' opportunity-card--structural'
    : ['controlled', 'strategic_asset', 'trade_signal'].includes(opportunity.classification.id)
      ? ' opportunity-card--gate'
      : '';

  return `
    <article class="opportunity-card${structuralClass}" tabindex="-1">
      <header class="opportunity-card__header">
        <div class="rank-mark">
          <span>#${String(index + 1).padStart(2, '0')}</span>
          <small>${escapeHTML(titleCase(sector))} · HS ${hsCode}</small>
        </div>
        <span class="fit-verdict fit-verdict--${escapeHTML(opportunity.fitVerdict.id)}">${escapeHTML(opportunity.fitVerdict.label)}</span>
        <button
          class="save-button${saved ? ' is-saved' : ''}"
          type="button"
          data-action="toggle-save"
          data-hs-code="${hsCode}"
          aria-pressed="${saved}"
          aria-label="${saved ? 'Remove' : 'Add'} ${escapeHTML(opportunity.shortTitle)} ${saved ? 'from' : 'to'} opportunity desk"
        >
          <svg aria-hidden="true" viewBox="0 0 24 24"><path d="M6 4h12v17l-6-4-6 4V4Z" /></svg>
        </button>
      </header>

      <div class="opportunity-card__identity">
        <h3 class="business-title">${escapeHTML(opportunity.shortTitle)}</h3>
        <p class="customs-title"><span>Official customs category</span>${escapeHTML(product.title || 'Title unavailable')}</p>
      </div>

      <div class="entry-wedge">
        <span>${escapeHTML(ENTRY_MODEL_LABELS[opportunity.route.entry_model] || titleCase(opportunity.route.entry_model))} route <i class="fact-type fact-type--assumption">Modeled</i></span>
        <p>${escapeHTML(opportunity.route.wedge)}</p>
      </div>

      <div class="launch-envelope">
        <div>
          <span>Entry envelope <i class="fact-type fact-type--assumption">Modeled</i></span>
          <strong>${escapeHTML(Model.capitalRangeLabel(opportunity.route))}</strong>
        </div>
        <div>
          <span>First paid proof <i class="fact-type fact-type--assumption">Modeled</i></span>
          <strong>${escapeHTML(Model.monthRangeLabel(opportunity.route))}</strong>
        </div>
      </div>

      <div class="opportunity-card__shock">
        <div>
          <span>India imported <i class="fact-type fact-type--observed">Observed</i></span>
          <strong>${formatUsdCompact(opportunity.importValue)}</strong>
          <small>in this HS6 market over 12 months</small>
        </div>
        <div>
          <span>Direction <i class="fact-type fact-type--observed">Observed</i></span>
          <strong class="trend-value trend-value--${trendClass}">${escapeHTML(Model.trendNarrative(opportunity.reductionPct))}</strong>
        </div>
      </div>

      <div class="complexity-strip">
        <div>
          <span>Startup complexity</span>
          <strong>${escapeHTML(opportunity.complexity.label)}</strong>
        </div>
        <div class="complexity-dots" aria-label="${opportunity.complexity.overall} out of 5 complexity">
          ${renderComplexityDots(opportunity.complexity.overall)}
        </div>
      </div>

      <div class="critical-info">
        <div><span>Likely buyer</span><strong>${escapeHTML(opportunity.route.buyer)}</strong></div>
        <div><span>First proof</span><strong>${escapeHTML(opportunity.route.first_proof)}</strong></div>
      </div>

      <footer class="opportunity-card__footer">
        <div class="dual-score" aria-label="National and personal scores">
          <span>India <strong>${Math.round(opportunity.countrySignal)}</strong></span>
          <span>You <strong>${Math.round(opportunity.personalFit)}</strong></span>
        </div>
        <button
          class="card-open-button"
          type="button"
          data-action="open-dossier"
          data-hs-code="${hsCode}"
        >
          See the business case
          <svg aria-hidden="true" viewBox="0 0 24 24"><path d="m5 12 14 0m-5-5 5 5-5 5" /></svg>
        </button>
        <span class="lens-score" title="Selected-lens score">${lensScoreLabel}</span>
      </footer>
    </article>
  `;
}

function toggleShortlist(hsCode) {
  const normalized = String(hsCode || '');
  const existingIndex = state.shortlist.findIndex((item) => item.hs_code === normalized);
  if (existingIndex >= 0) {
    state.shortlist.splice(existingIndex, 1);
    delete state.allocations[normalized];
    persistJSON(STORAGE_KEYS.allocations, state.allocations);
    showToast('Removed from opportunity desk');
  } else {
    const product = state.products.find((item) => String(item.hs_code) === normalized)
      || (state.selected && state.selected.opportunity.product);
    if (!product) return;
    state.shortlist.unshift(snapshotProduct(product));
    showToast('Added to opportunity desk');
  }
  persistJSON(STORAGE_KEYS.shortlist, state.shortlist);
  updateSavedCount();
  renderOpportunityResults();
  renderPortfolio();
  updateDossierSaveButton();
}

function removeFromPlan(hsCode) {
  const normalized = String(hsCode || '');
  state.shortlist = state.shortlist.filter((item) => item.hs_code !== normalized);
  delete state.allocations[normalized];
  persistJSON(STORAGE_KEYS.shortlist, state.shortlist);
  persistJSON(STORAGE_KEYS.allocations, state.allocations);
  updateSavedCount();
  renderPortfolio();
  renderOpportunityResults();
  showToast('Removed from opportunity desk');
}

function snapshotProduct(product) {
  return {
    hs_code: String(product.hs_code || ''),
    title: String(product.title || 'Untitled opportunity').slice(0, 500),
    sectors: Array.isArray(product.sectors) ? product.sectors.slice(0, 8).map(String) : [],
    last_12m_value_usd: Model.finiteNumber(product.last_12m_value_usd),
    reduction_pct: Model.finiteNumber(product.reduction_pct)
  };
}

function isSaved(hsCode) {
  return state.shortlist.some((item) => item.hs_code === String(hsCode));
}

function updateSavedCount() {
  document.getElementById('savedCount').textContent = numberFormatter.format(state.shortlist.length);
}

function renderPortfolio() {
  renderPortfolioStats();
  const list = document.getElementById('portfolioList');
  if (!state.shortlist.length) {
    list.innerHTML = `
      <div class="empty-state empty-state--portfolio">
        <span>No opportunities saved</span>
        <h3>Build a comparison set, not a bookmark pile.</h3>
        <p>Save specific entry routes, compare their launch envelope and complexity, then record what must be proven.</p>
        <button class="primary-button" type="button" data-action="navigate" data-view="discover">Explore opportunities</button>
      </div>
    `;
    return;
  }

  list.innerHTML = state.shortlist.map((saved, index) => {
    const product = state.products.find((item) => String(item.hs_code) === saved.hs_code) || saved;
    const opportunity = Model.deriveOpportunity(
      product,
      state.profile,
      state.marketContext || Model.createMarketContext([product]),
      state.routes
    );
    const allocation = Model.finiteNumber(state.allocations[saved.hs_code]) || 0;
    const notes = getOpportunityNotes(saved.hs_code);
    return `
      <article class="portfolio-row">
        <div class="portfolio-rank">${String(index + 1).padStart(2, '0')}</div>
        <div class="portfolio-identity">
          <span class="fit-verdict fit-verdict--${escapeHTML(opportunity.fitVerdict.id)}">${escapeHTML(opportunity.fitVerdict.label)}</span>
          <h3>${escapeHTML(opportunity.shortTitle)}</h3>
          <p>${escapeHTML(opportunity.route.wedge)}</p>
          <small>HS ${escapeHTML(saved.hs_code)} · ${formatUsdCompact(opportunity.importValue)} annual imports · ${escapeHTML(opportunity.complexity.label)}</small>
        </div>
        <div class="portfolio-field">
          <span>Modeled entry</span>
          <strong>${escapeHTML(Model.capitalRangeLabel(opportunity.route))}</strong>
          <small>${escapeHTML(Model.monthRangeLabel(opportunity.route))}</small>
        </div>
        <label class="portfolio-field portfolio-allocation">
          <span>Indicative cheque (₹ crore)</span>
          <input
            type="number"
            min="0"
            step="0.25"
            inputmode="decimal"
            value="${allocation ? trimNumber(allocation / 10_000_000) : ''}"
            placeholder="0"
            data-action="set-allocation"
            data-hs-code="${escapeHTML(saved.hs_code)}"
          />
        </label>
        <div class="portfolio-actions">
          <span>${notes ? 'Private note saved' : `${opportunity.route.critical_unknowns.length} critical unknowns`}</span>
          <button class="text-button" type="button" data-action="open-dossier" data-hs-code="${escapeHTML(saved.hs_code)}">Open business case</button>
          <button class="text-button text-button--danger" type="button" data-action="remove-plan" data-hs-code="${escapeHTML(saved.hs_code)}">Remove</button>
        </div>
      </article>
    `;
  }).join('');
}

function renderPortfolioStats() {
  const available = state.profile.capitalInr;
  const allocated = state.shortlist.reduce((sum, item) => {
    const value = Model.finiteNumber(state.allocations[item.hs_code]);
    return sum + (value === null ? 0 : Math.max(0, value));
  }, 0);
  const exposure = state.shortlist.reduce((sum, saved) => {
    const product = state.products.find((item) => String(item.hs_code) === saved.hs_code) || saved;
    const value = Model.finiteNumber(product.last_12m_value_usd);
    return sum + (value === null ? 0 : value);
  }, 0);
  const researchOnly = state.shortlist.reduce((sum, saved) => {
    const product = state.products.find((item) => String(item.hs_code) === saved.hs_code) || saved;
    const opportunity = Model.deriveOpportunity(
      product,
      state.profile,
      state.marketContext || Model.createMarketContext([product]),
      state.routes
    );
    return sum + Number(opportunity.fitVerdict.id === 'research');
  }, 0);

  document.getElementById('portfolioAvailable').textContent = formatInr(available);
  document.getElementById('portfolioAllocated').textContent = formatInr(allocated);
  document.getElementById('portfolioCount').textContent = numberFormatter.format(state.shortlist.length);
  document.getElementById('portfolioExposure').textContent = state.shortlist.length
    ? formatUsdCompact(exposure)
    : '—';

  const allocationStatus = document.getElementById('portfolioAllocationStatus');
  if (!allocated) {
    allocationStatus.textContent = 'Nothing allocated yet';
    allocationStatus.dataset.state = 'neutral';
  } else if (allocated > available) {
    allocationStatus.textContent = `${formatInr(allocated - available)} above mandate`;
    allocationStatus.dataset.state = 'error';
  } else {
    allocationStatus.textContent = `${formatInr(available - allocated)} unallocated`;
    allocationStatus.dataset.state = 'ready';
  }
  document.getElementById('portfolioEvidenceSummary').textContent = state.shortlist.length
    ? `${state.shortlist.length - researchOnly} reviewed business ${state.shortlist.length - researchOnly === 1 ? 'route' : 'routes'} · ${researchOnly} trade ${researchOnly === 1 ? 'signal' : 'signals'} still need route definition.`
    : 'Add opportunities to compare launch routes and unresolved evidence.';
}

function saveOpportunityNotes(hsCode) {
  const textarea = document.getElementById('decisionNotes');
  if (!textarea) return;
  state.notes[hsCode] = textarea.value.trim().slice(0, 5000);
  persistJSON(STORAGE_KEYS.notes, state.notes);
  renderPortfolio();
  showToast('Opportunity note saved on this device');
}

function getOpportunityNotes(hsCode) {
  return typeof state.notes[hsCode] === 'string' ? state.notes[hsCode] : '';
}

function exportOpportunityScreen() {
  if (!state.filtered.length) {
    showToast('No ranked opportunities to export', 'error');
    return;
  }
  const rows = [
    [
      'rank',
      'hs_code',
      'business_name',
      'official_customs_title',
      'route_id',
      'entry_model',
      'fit_verdict',
      'sectors',
      'annual_import_usd',
      'import_value_change_vs_baseline_pct',
      'country_import_signal_derived',
      'mandate_match_derived',
      'combined_fit_derived',
      'scenario_pct_assumption',
      'gross_import_exposure_addressed_usd',
      'modeled_entry_capital_min_inr',
      'modeled_entry_capital_max_inr',
      'modeled_first_proof_min_months',
      'modeled_first_proof_max_months',
      'complexity_out_of_5',
      'likely_buyer',
      'entry_wedge',
      'critical_unknowns',
      'route_confidence'
    ],
    ...state.filtered.map((opportunity, index) => [
      index + 1,
      opportunity.product.hs_code,
      opportunity.shortTitle,
      opportunity.product.title,
      opportunity.route.route_id,
      opportunity.route.entry_model,
      opportunity.fitVerdict.label,
      opportunity.sectors.join(';'),
      opportunity.importValue,
      opportunity.reductionPct === null ? '' : -opportunity.reductionPct * 100,
      opportunity.countrySignal.toFixed(2),
      opportunity.mandateMatch.toFixed(2),
      opportunity.combinedFit.toFixed(2),
      state.profile.capturePct,
      opportunity.scenarioValue,
      opportunity.route.capital_min_inr,
      opportunity.route.capital_max_inr,
      opportunity.route.months_min,
      opportunity.route.months_max,
      opportunity.complexity.overall,
      opportunity.route.buyer,
      opportunity.route.wedge,
      opportunity.route.critical_unknowns.join(';'),
      opportunity.route.confidence
    ])
  ];
  downloadCsv(rows, `build-for-india-opportunity-screen-${isoDate()}.csv`);
  showToast('Opportunity screen exported');
}

function exportCapitalPlan() {
  if (!state.shortlist.length) {
    showToast('Add opportunities before exporting a comparison', 'error');
    return;
  }
  const rows = [
    [
      'hs_code',
      'business_name',
      'official_customs_title',
      'route_id',
      'fit_verdict',
      'indicative_cheque_inr',
      'modeled_entry_capital',
      'modeled_first_paid_proof',
      'complexity_out_of_5',
      'annual_import_usd',
      'critical_unknowns',
      'private_notes'
    ],
    ...state.shortlist.map((saved) => {
      const product = state.products.find((item) => String(item.hs_code) === saved.hs_code) || saved;
      const opportunity = Model.deriveOpportunity(
        product,
        state.profile,
        state.marketContext || Model.createMarketContext([product]),
        state.routes
      );
      return [
        saved.hs_code,
        opportunity.shortTitle,
        product.title,
        opportunity.route.route_id,
        opportunity.fitVerdict.label,
        state.allocations[saved.hs_code] || 0,
        Model.capitalRangeLabel(opportunity.route),
        Model.monthRangeLabel(opportunity.route),
        opportunity.complexity.overall,
        product.last_12m_value_usd,
        opportunity.route.critical_unknowns.join(';'),
        getOpportunityNotes(saved.hs_code)
      ];
    })
  ];
  downloadCsv(rows, `build-for-india-opportunity-comparison-${isoDate()}.csv`);
  showToast('Opportunity comparison exported');
}

async function openDossier(hsCode, trigger) {
  const product = state.products.find((item) => String(item.hs_code) === String(hsCode));
  if (!product) {
    showToast('That opportunity is not in the loaded catalogue', 'error');
    return;
  }

  const requestId = ++state.detailRequestId;
  if (state.detailController) state.detailController.abort();
  state.detailController = new AbortController();
  state.lastFocusedElement = trigger || document.activeElement;
  const opportunity = Model.deriveOpportunity(product, state.profile, state.marketContext, state.routes);
  state.selected = { opportunity, detail: null, capability: null };
  showDossierShell(opportunity);

  try {
    const [detailResult, capabilityResult] = await Promise.allSettled([
      fetch(`/api/products/${encodeURIComponent(hsCode)}`, {
        signal: state.detailController.signal
      }).then(checkedJson),
      fetch(`/api/domestic_capability/${encodeURIComponent(hsCode)}`, {
        signal: state.detailController.signal
      }).then(checkedJson)
    ]);
    if (requestId !== state.detailRequestId) return;
    if (detailResult.status === 'rejected') throw detailResult.reason;

    const detail = detailResult.value;
    const capability = capabilityResult.status === 'fulfilled'
      ? capabilityResult.value
      : { items: [] };
    const verifiedCapex = Array.isArray(capability.items) && capability.items.some((item) => (
      item
      && item.verified === true
      && (
        Model.finiteNumber(item.capex_min) !== null
        || Model.finiteNumber(item.capex_max) !== null
      )
    ));
    state.selected = { opportunity, detail, capability };
    state.detailEvidence[hsCode] = {
      supplier: Array.isArray(detail.partners) && detail.partners.length > 0,
      capex: verifiedCapex,
      snapshot: currentEvidenceSnapshot(),
      checkedAt: Date.now()
    };
    persistJSON(STORAGE_KEYS.evidence, state.detailEvidence);
    renderDossier();
    renderPortfolioStats();
    renderOpportunityResults();
  } catch (error) {
    if (error.name === 'AbortError' || requestId !== state.detailRequestId) return;
    console.error(error);
    renderDossierError(error);
  } finally {
    if (requestId === state.detailRequestId) state.detailController = null;
  }
}

function showDossierShell(opportunity) {
  const dialog = document.getElementById('dossierDialog');
  document.getElementById('dossierBackdrop').hidden = false;
  dialog.hidden = false;
  document.body.classList.add('dialog-open');
  document.getElementById('dossierTitle').textContent = 'Opportunity dossier';
  document.getElementById('dossierDescription').textContent =
    `Business route, complexity, evidence, and unresolved questions for ${opportunity.shortTitle}.`;
  document.getElementById('dossierCode').textContent = `HS ${opportunity.product.hs_code}`;
  document.getElementById('dossierContent').innerHTML = `
    <div class="dossier-loading" role="status">
      <span></span>
      <strong>Assembling demand, supplier, and capability evidence…</strong>
    </div>
  `;
  updateDossierSaveButton();
  requestAnimationFrame(() => {
    document.getElementById('closeDossierBtn').focus();
  });
}

function closeDossier() {
  const dialog = document.getElementById('dossierDialog');
  if (dialog.hidden) return;
  if (state.detailController) state.detailController.abort();
  state.detailRequestId += 1;
  state.detailController = null;
  dialog.hidden = true;
  document.getElementById('dossierBackdrop').hidden = true;
  document.body.classList.remove('dialog-open');
  state.selected = null;
  if (state.lastFocusedElement && document.contains(state.lastFocusedElement)) {
    state.lastFocusedElement.focus();
  }
  state.lastFocusedElement = null;
}

function renderDossier() {
  if (!state.selected || !state.selected.detail) return;
  const { opportunity, detail, capability } = state.selected;
  const product = detail.product;
  const progress = detail.progress || {};
  const partners = Array.isArray(detail.partners) ? detail.partners : [];
  const capabilityItems = capability && Array.isArray(capability.items) ? capability.items : [];
  const verifiedCapability = capabilityItems.find((item) => (
    item
    && item.verified === true
    && (
      Model.finiteNumber(item.capex_min) !== null
      || Model.finiteNumber(item.capex_max) !== null
    )
  )) || null;
  const route = opportunity.route;
  const notes = getOpportunityNotes(product.hs_code);
  const hhi = Model.finiteNumber(progress.hhi_current);
  const topPartner = partners[0] || null;
  const routeUnknowns = Array.isArray(route.critical_unknowns) ? route.critical_unknowns : [];
  const requiredStrengths = Array.isArray(route.required_strengths) ? route.required_strengths : [];
  const complexityDimensions = Object.entries(opportunity.complexity.dimensions);

  document.getElementById('dossierCode').textContent = `HS ${product.hs_code}`;
  document.getElementById('dossierContent').innerHTML = `
    <article class="dossier-body">
      <header class="dossier-hero">
        <div class="dossier-tags">
          <span class="sector-tag">${escapeHTML(titleCase(opportunity.sectors[0] || 'unclassified'))}</span>
          <span class="classification-tag">${escapeHTML(opportunity.classification.label)}</span>
          <span class="fit-verdict fit-verdict--${escapeHTML(opportunity.fitVerdict.id)}">${escapeHTML(opportunity.fitVerdict.label)}</span>
          ${opportunity.isStructural ? '<span class="structural-tag">Structural exposure</span>' : ''}
        </div>
        <h1 class="business-title">${escapeHTML(opportunity.shortTitle)}</h1>
        <p class="customs-title"><span>Official HS6 category</span>${escapeHTML(product.title || detail.description || 'Title unavailable')}</p>
        <div class="source-line">
          <span>Trade source <strong>${escapeHTML(formatSource(state.metadata.source || detail.source))}</strong></span>
          <span>Through <strong>${escapeHTML(formatPeriod(detail.latest_period || state.metadata.latestPeriod))}</strong></span>
          <span>Route status <strong>${route.confidence === 'curated_model' ? 'Reviewed model' : 'Research only'}</strong></span>
        </div>
      </header>

      <section class="dossier-metrics" aria-label="Opportunity snapshot">
        <article>
          <span>Annual imports <i class="fact-type fact-type--observed">Observed</i></span>
          <strong>${formatUsdCompact(product.last_12m_value_usd)}</strong>
          <small>gross 12-month import demand</small>
        </article>
        <article>
          <span>Baseline signal <i class="fact-type fact-type--observed">Observed</i></span>
          <strong>${escapeHTML(shortTrendLabel(product.reduction_pct))}</strong>
          <small>${escapeHTML(Model.trendNarrative(product.reduction_pct))}</small>
        </article>
        <article>
          <span>Country signal <i class="fact-type fact-type--derived">Derived</i></span>
          <strong>${Math.round(opportunity.countrySignal)}<em>/100</em></strong>
          <small>import-value scale + momentum</small>
        </article>
        <article>
          <span>Your mandate <i class="fact-type fact-type--derived">Derived</i></span>
          <strong>${Math.round(opportunity.personalFit)}<em>/100</em></strong>
          <small>capital, time, route, strengths, buyers and sector</small>
        </article>
      </section>

      <section class="business-brief">
        <header>
          <div>
          <p class="section-kicker">Specific route in</p>
          <h2>${escapeHTML(route.wedge)}</h2>
          </div>
        </header>
        <div class="business-brief__scope">
          <p><strong>Includes</strong>${escapeHTML(route.includes)}</p>
          <p><strong>Excludes</strong>${escapeHTML(route.excludes)}</p>
        </div>
        <div class="fit-verdict-detail fit-verdict-detail--${escapeHTML(opportunity.fitVerdict.id)}">
          <strong>${escapeHTML(opportunity.fitVerdict.label)}</strong>
          <span>${escapeHTML(opportunity.fitVerdict.detail)}</span>
        </div>
      </section>

      <section class="launch-path" aria-labelledby="launch-path-title">
        <header>
          <div>
            <p class="section-kicker">Launch path · modeled, not quoted plant CAPEX</p>
            <h2 id="launch-path-title">How this could become a company test</h2>
          </div>
          <span class="evidence-status evidence-status--assumption">Modeled</span>
        </header>
        <div class="launch-envelope">
          <div><span>Entry capital</span><strong>${escapeHTML(Model.capitalRangeLabel(route))}</strong><small>Your modeled stack: ${escapeHTML(formatInr(opportunity.projectCapitalInr))}</small></div>
          <div><span>First paid proof</span><strong>${escapeHTML(Model.monthRangeLabel(route))}</strong><small>Your window: ${state.profile.horizonMonths} months</small></div>
          <div><span>Likely buyer</span><strong>${escapeHTML(route.buyer)}</strong></div>
          <div><span>First proof</span><strong>${escapeHTML(route.first_proof)}</strong></div>
        </div>
        <p class="source-note">${escapeHTML(route.source_note || 'Modeled screening route; validate with buyers, suppliers and current quotes.')}</p>
      </section>

      <section class="dossier-complexity" aria-labelledby="complexity-title">
        <header>
          <div>
            <p class="section-kicker">Startup complexity</p>
            <h2 id="complexity-title">${escapeHTML(opportunity.complexity.label)} · ${opportunity.complexity.overall}/5</h2>
          </div>
          <div class="complexity-dots" aria-hidden="true">${renderComplexityDots(opportunity.complexity.overall)}</div>
        </header>
        <div class="complexity-grid">
          ${complexityDimensions.map(([key, value]) => `
            <div>
              <span>${escapeHTML(complexityDimensionLabel(key))}</span>
              <strong>${value}/5</strong>
              <div class="complexity-dots" aria-hidden="true">${renderComplexityDots(value)}</div>
            </div>
          `).join('')}
        </div>
        <p>Scores are a comparative screening model, not a feasibility study. Standards and approvals still require current product-specific checks.</p>
      </section>

      <div class="dossier-layout">
        <div class="dossier-main">
          <section class="evidence-section" aria-labelledby="case-title">
            <header>
              <div>
                <p class="section-kicker">Why now</p>
                <h2 id="case-title">The business case to challenge</h2>
              </div>
            </header>
            <p>${escapeHTML(buildThesis(opportunity, topPartner))}</p>
            <div class="thesis-caveat">
              <strong>What this does not prove</strong>
              <span>Domestic absence, addressable demand, technical feasibility, customer conversion, verified project CAPEX, unit economics, policy eligibility, or investment return.</span>
            </div>
          </section>

          <section class="evidence-section" aria-labelledby="demand-evidence-title">
            <header>
              <div>
                <p class="section-kicker">Demand evidence</p>
                <h2 id="demand-evidence-title">Monthly import exposure</h2>
              </div>
              <span class="evidence-status evidence-status--ready">Observed</span>
            </header>
            ${renderTrendFigure(detail.timeseries)}
          </section>

          <section class="evidence-section" aria-labelledby="supplier-evidence-title">
            <header>
              <div>
                <p class="section-kicker">Supplier risk</p>
                <h2 id="supplier-evidence-title">Where imports concentrate</h2>
              </div>
              <span class="evidence-status ${partners.length ? 'evidence-status--ready' : 'evidence-status--gap'}">
                ${partners.length ? 'Observed' : 'Evidence gap'}
              </span>
            </header>
            ${renderPartnerEvidence(partners, hhi, detail.partner_period, detail.partner_source)}
          </section>

          <section class="evidence-section evidence-gaps" aria-labelledby="unknowns-title">
            <header>
              <div>
                <p class="section-kicker">Kill questions</p>
                <h2 id="unknowns-title">What must be true before capital moves</h2>
              </div>
              <span class="evidence-status evidence-status--gap">Unresolved</span>
            </header>
            <ol>${routeUnknowns.map((unknown) => `<li>${escapeHTML(unknown)}</li>`).join('')}</ol>
            ${requiredStrengths.length ? `<p><strong>Operator strengths this route likely needs:</strong> ${requiredStrengths.map(Model.strengthLabel).map(escapeHTML).join(', ')}.</p>` : ''}
          </section>
        </div>

        <aside class="dossier-side">
          <section class="scenario-panel">
            <p class="section-kicker">Import-substitution scenario</p>
            <div class="scenario-value">
              <span id="dossierScenarioLabel">${formatPercentageNumber(state.profile.capturePct)} of imports</span>
              <strong id="dossierScenarioValue">${formatUsdCompact(Model.scenarioValue(product.last_12m_value_usd, state.profile.capturePct))}</strong>
              <small>gross annual import exposure addressed</small>
            </div>
            <label for="dossierCapture">
              <span>Test a share from 0.1% to 5%</span>
              <input id="dossierCapture" type="range" min="0.1" max="5" value="${state.profile.capturePct}" step="0.1" />
            </label>
            <p>This is a planning assumption, not achievable market share, revenue, or expected return.</p>
          </section>

          <section class="capital-evidence">
            <header>
              <div>
                <p class="section-kicker">Capital evidence</p>
                <h2>${verifiedCapability ? 'External CAPEX record found' : 'Project CAPEX still unverified'}</h2>
              </div>
              <span class="evidence-status ${verifiedCapability ? 'evidence-status--ready' : 'evidence-status--gap'}">
                ${verifiedCapability ? 'Sourced' : 'Gap'}
              </span>
            </header>
            ${renderCapitalEvidence(product, verifiedCapability, opportunity)}
          </section>

          <section class="decision-panel">
            <p class="section-kicker">Private notebook</p>
            <h2>Record your thesis, not a fake stage.</h2>
            <label for="decisionNotes">
              <span>What did you learn? What would kill the case?</span>
              <textarea id="decisionNotes" rows="7" maxlength="5000" placeholder="Buyer, price, qualification path, quote, disconfirming evidence…">${escapeHTML(notes)}</textarea>
            </label>
            <button
              class="secondary-button secondary-button--wide"
              type="button"
              data-action="save-notes"
              data-hs-code="${escapeHTML(product.hs_code)}"
            >Save private note</button>
          </section>
        </aside>
      </div>

      <footer class="dossier-footer">
        <p>No real-money transaction occurs here. Validate the route independently before committing capital.</p>
        <button class="text-button" type="button" data-action="navigate" data-view="method">Read the ranking method</button>
      </footer>
    </article>
  `;
  updateDossierSaveButton();
}

function renderDossierError(error) {
  document.getElementById('dossierContent').innerHTML = `
    <div class="empty-state empty-state--error dossier-error">
      <span>Evidence request interrupted</span>
      <h3>The dossier could not be assembled.</h3>
      <p>${escapeHTML(error && error.message ? error.message : 'Try opening this opportunity again.')}</p>
      <button class="secondary-button" type="button" data-action="open-dossier" data-hs-code="${escapeHTML(state.selected ? state.selected.opportunity.product.hs_code : '')}">Try again</button>
    </div>
  `;
}

function updateDossierSaveButton() {
  const button = document.getElementById('dossierSaveBtn');
  if (!state.selected) {
    button.textContent = 'Add to opportunity desk';
    button.setAttribute('aria-pressed', 'false');
    return;
  }
  const saved = isSaved(state.selected.opportunity.product.hs_code);
  button.textContent = saved ? 'Remove from opportunity desk' : 'Add to opportunity desk';
  button.setAttribute('aria-pressed', String(saved));
}

function buildThesis(opportunity, topPartner) {
  const demand = formatUsdCompact(opportunity.importValue);
  const reasons = opportunity.reasons.join(' and ').replace(/\.$/, '');
  let thesis = `${opportunity.shortTitle} surfaced because ${reasons.toLowerCase()}. The route is to ${opportunity.route.wedge.charAt(0).toLowerCase()}${opportunity.route.wedge.slice(1)} India imported ${demand} in the broader HS6 category over the latest 12-month window.`;
  if (topPartner && Model.finiteNumber(topPartner.share) !== null) {
    thesis += ` The latest supplier snapshot places ${formatPercentage(topPartner.share)} with ${topPartner.partner_country || 'the leading partner'}.`;
  }
  if (opportunity.isStructural) {
    thesis += ' Treat this as a national strategy problem before treating it as a company-formation opportunity.';
  } else if (opportunity.fitVerdict.id === 'research') {
    thesis += ' The customs signal is not yet translated into a reviewed company route, so resolve the product mix before acting.';
  } else {
    thesis += ' The decisive question is whether a buyer will validate the specification, price and qualification path behind this modeled entry route.';
  }
  return thesis;
}

function renderTrendFigure(timeseriesInput) {
  const timeseries = (Array.isArray(timeseriesInput) ? timeseriesInput : [])
    .map((entry) => ({
      year: Number(entry.year),
      month: Number(entry.month),
      value: Model.finiteNumber(entry.value_usd)
    }))
    .filter((entry) => Number.isFinite(entry.year) && Number.isFinite(entry.month) && entry.value !== null);
  if (!timeseries.length) {
    return '<div class="evidence-empty"><strong>Monthly series unavailable</strong><span>Demand is known at catalogue level, but the detail series could not be retrieved.</span></div>';
  }

  const width = 760;
  const height = 250;
  const padX = 18;
  const padTop = 18;
  const padBottom = 32;
  const maximum = Math.max(...timeseries.map((entry) => entry.value), 1);
  const usableWidth = width - (padX * 2);
  const usableHeight = height - padTop - padBottom;
  const points = timeseries.map((entry, index) => {
    const x = timeseries.length === 1
      ? width / 2
      : padX + (index / (timeseries.length - 1)) * usableWidth;
    const y = padTop + usableHeight - ((entry.value / maximum) * usableHeight);
    return { ...entry, x, y };
  });
  const line = points.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(' ');
  const area = [
    `M ${points[0].x.toFixed(1)} ${(padTop + usableHeight).toFixed(1)}`,
    ...points.map((point) => `L ${point.x.toFixed(1)} ${point.y.toFixed(1)}`),
    `L ${points[points.length - 1].x.toFixed(1)} ${(padTop + usableHeight).toFixed(1)}`,
    'Z'
  ].join(' ');
  const first = points[0];
  const last = points[points.length - 1];
  const prior = points.length > 1 ? points[points.length - 2] : null;
  const recentChange = prior && prior.value
    ? ((last.value - prior.value) / prior.value) * 100
    : null;

  return `
    <figure class="trend-figure">
      <div class="trend-summary">
        <div><span>Latest month</span><strong>${formatUsdCompact(last.value)}</strong></div>
        <div><span>Month change</span><strong>${recentChange === null ? 'Unavailable' : `${recentChange >= 0 ? '+' : ''}${recentChange.toFixed(1)}%`}</strong></div>
        <div><span>Series</span><strong>${timeseries.length} months</strong></div>
      </div>
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="trendSvgTitle trendSvgDesc">
        <title id="trendSvgTitle">Monthly import value trend</title>
        <desc id="trendSvgDesc">From ${escapeHTML(monthLabel(first.year, first.month))} at ${escapeHTML(formatUsdCompact(first.value))} to ${escapeHTML(monthLabel(last.year, last.month))} at ${escapeHTML(formatUsdCompact(last.value))}.</desc>
        <line class="chart-grid" x1="${padX}" y1="${padTop + usableHeight * 0.25}" x2="${width - padX}" y2="${padTop + usableHeight * 0.25}" />
        <line class="chart-grid" x1="${padX}" y1="${padTop + usableHeight * 0.5}" x2="${width - padX}" y2="${padTop + usableHeight * 0.5}" />
        <line class="chart-grid" x1="${padX}" y1="${padTop + usableHeight * 0.75}" x2="${width - padX}" y2="${padTop + usableHeight * 0.75}" />
        <path class="chart-area" d="${area}" />
        <polyline class="chart-line" points="${line}" />
        <circle class="chart-point" cx="${last.x}" cy="${last.y}" r="5" />
        <text class="chart-label" x="${padX}" y="${height - 8}">${escapeHTML(monthLabel(first.year, first.month))}</text>
        <text class="chart-label chart-label--end" x="${width - padX}" y="${height - 8}">${escapeHTML(monthLabel(last.year, last.month))}</text>
      </svg>
      <figcaption>Observed monthly import value in USD. Zero months are retained as reported.</figcaption>
    </figure>
  `;
}

function renderPartnerEvidence(partnersInput, hhi, period, source) {
  const partners = (Array.isArray(partnersInput) ? partnersInput : []).slice(0, 8);
  if (!partners.length) {
    return `
      <div class="evidence-empty">
        <strong>Supplier concentration unavailable</strong>
        <span>Supplier risk cannot be evaluated. The country signal remains based only on observed import value and value momentum.</span>
      </div>
    `;
  }
  const total = partners.reduce((sum, item) => {
    const value = Model.finiteNumber(item.value_usd);
    return sum + (value === null ? 0 : value);
  }, 0);
  const rows = partners.map((item) => {
    const directShare = Model.finiteNumber(item.share);
    const value = Model.finiteNumber(item.value_usd);
    const share = directShare !== null
      ? directShare
      : (total > 0 && value !== null ? value / total : 0);
    return {
      name: String(item.partner_country || 'Unknown'),
      value,
      share: Model.clamp(share, 0, 1)
    };
  });
  const concentrationLabel = hhi === null
    ? 'HHI unavailable'
    : hhi >= 0.25
      ? 'High supplier concentration'
      : hhi >= 0.15
        ? 'Moderate supplier concentration'
        : 'Dispersed supplier base';
  return `
    <div class="supplier-summary">
      <div>
        <span>Concentration signal</span>
        <strong>${escapeHTML(concentrationLabel)}</strong>
      </div>
      <div>
        <span>HHI</span>
        <strong>${hhi === null ? 'Unavailable' : hhi.toFixed(3)}</strong>
      </div>
      <div>
        <span>Snapshot</span>
        <strong>${escapeHTML(formatPeriod(period))}</strong>
      </div>
    </div>
    <div class="supplier-bars">
      ${rows.map((row) => `
        <div class="supplier-row">
          <span>${escapeHTML(row.name)}</span>
          <i><b style="width:${(row.share * 100).toFixed(2)}%"></b></i>
          <strong>${formatPercentage(row.share)}</strong>
          <small>${formatUsdCompact(row.value)}</small>
        </div>
      `).join('')}
    </div>
    <p class="source-note">Observed partner snapshot · ${escapeHTML(formatSource(source))}</p>
  `;
}

function renderCapitalEvidence(product, capability, opportunity) {
  if (capability) {
    const capex = formatUsdRange(capability.capex_min, capability.capex_max);
    return `
      <div class="capital-record">
        <div><span>Sourced external CAPEX range</span><strong>${escapeHTML(capex)}</strong></div>
        <div><span>Source</span><strong>${escapeHTML(capability.source || 'Verified capability record')}</strong></div>
        ${capability.machines ? `<div><span>Machinery</span><strong>${escapeHTML(summarizeCapability(capability.machines))}</strong></div>` : ''}
        ${capability.skills ? `<div><span>Skills</span><strong>${escapeHTML(summarizeCapability(capability.skills))}</strong></div>` : ''}
      </div>
      <p>Revalidate scope, date, capacity, utilities, working capital, and commissioning before using this range.</p>
    `;
  }
  const catalogueEstimate = product.capex_min != null || product.capex_max != null
    ? `
      <div class="catalogue-warning">
        <strong>Unsourced catalogue estimate: ${escapeHTML(formatUsdRange(product.capex_min, product.capex_max))}</strong>
        <span>Excluded from ranking until it becomes a verified capability record.</span>
      </div>
    `
    : '';
  return `
    <div class="capital-gap">
      <span>Modeled validation / entry envelope</span>
      <strong>${escapeHTML(Model.capitalRangeLabel(opportunity.route))}</strong>
      <small>Compared with your ${escapeHTML(formatInr(opportunity.projectCapitalInr))} modeled capital stack</small>
    </div>
    <p>This range screens a narrow route to first paid proof. It is not a vendor quote, plant CAPEX estimate, or promise that the project can be financed.</p>
    ${catalogueEstimate}
    <ul class="gap-list">
      <li>Process and capacity definition</li>
      <li>Plant, equipment, and commissioning quotes</li>
      <li>Working capital and ramp losses</li>
      <li>Standards, utilities, land, and approvals</li>
    </ul>
  `;
}

function renderComplexityDots(valueInput) {
  const value = Math.round(Model.clamp(Number(valueInput) || 3, 1, 5));
  return Array.from({ length: 5 }, (_, index) => (
    `<i class="complexity-dot${index < value ? ' is-filled' : ''}"></i>`
  )).join('');
}

function complexityDimensionLabel(value) {
  return {
    capital: 'Capital intensity',
    technical: 'Technical difficulty',
    regulatory: 'Regulation and standards',
    supply_chain: 'Supply-chain difficulty',
    buyer_qualification: 'Buyer qualification'
  }[value] || titleCase(String(value).replaceAll('_', ' '));
}

function updateDossierScenario() {
  if (!state.selected) return;
  const value = Model.scenarioValue(
    state.selected.opportunity.product.last_12m_value_usd,
    state.profile.capturePct
  );
  const label = document.getElementById('dossierScenarioLabel');
  const output = document.getElementById('dossierScenarioValue');
  if (label) label.textContent = `${formatPercentageNumber(state.profile.capturePct)} of imports`;
  if (output) output.textContent = formatUsdCompact(value);
  renderOpportunityResults();
}

function renderMethodCoverage() {
  if (!state.products.length) return;
  const demand = state.products.filter((item) => Model.finiteNumber(item.last_12m_value_usd) !== null).length;
  const trend = state.products.filter((item) => Model.finiteNumber(item.reduction_pct) !== null).length;
  const capex = state.products.filter((item) => item.capex_min != null || item.capex_max != null).length;
  document.getElementById('methodDemandCoverage').textContent =
    `${numberFormatter.format(demand)} / ${numberFormatter.format(state.products.length)}`;
  document.getElementById('methodTrendCoverage').textContent =
    `${numberFormatter.format(trend)} / ${numberFormatter.format(state.products.length)}`;
  document.getElementById('methodRouteCoverage').textContent =
    numberFormatter.format(state.routes.length);
  document.getElementById('methodCapexCoverage').textContent =
    `${numberFormatter.format(capex)} / ${numberFormatter.format(state.products.length)}`;
}

function checkedJson(response) {
  if (!response.ok) throw new Error(`Request failed with status ${response.status}`);
  return response.json();
}

function downloadCsv(rows, filename) {
  const csv = rows.map((row) => row.map(csvCell).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function csvCell(value) {
  const spreadsheetSafe = Model.spreadsheetSafeText(value);
  return `"${spreadsheetSafe.replaceAll('"', '""')}"`;
}

function sanitizeNumberMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value).flatMap(([key, entry]) => {
    const number = Model.finiteNumber(entry);
    return /^\d{1,10}$/.test(key) && number !== null && number >= 0 ? [[key, number]] : [];
  }));
}

function sanitizeNoteMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value).flatMap(([key, entry]) => {
    return /^\d{1,10}$/.test(key) && typeof entry === 'string'
      ? [[key, entry.slice(0, 5000)]]
      : [];
  }));
}

function sanitizeEvidenceMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value).flatMap(([key, entry]) => {
    if (!/^\d{1,10}$/.test(key) || !entry || typeof entry !== 'object') return [];
    const checkedAt = Model.finiteNumber(entry.checkedAt);
    const snapshot = typeof entry.snapshot === 'string' ? entry.snapshot.slice(0, 500) : '';
    if (checkedAt === null || checkedAt <= 0 || !snapshot) return [];
    return [[key, {
      supplier: Boolean(entry.supplier),
      capex: Boolean(entry.capex),
      snapshot,
      checkedAt
    }]];
  }));
}

function currentEvidenceSnapshot() {
  if (!state.metadata.source || !state.metadata.latestPeriod) return '';
  return [
    state.metadata.source,
    state.metadata.latestPeriod,
    state.metadata.lastUpdated || ''
  ].join('|');
}

function isDetailEvidenceCurrent(entry) {
  if (!entry || typeof entry !== 'object') return false;
  const snapshot = currentEvidenceSnapshot();
  const checkedAt = Model.finiteNumber(entry.checkedAt);
  if (!snapshot || entry.snapshot !== snapshot || checkedAt === null) return false;
  const age = Date.now() - checkedAt;
  return age >= 0 && age <= EVIDENCE_CACHE_MAX_AGE_MS;
}

function detailEvidenceFor(hsCode) {
  const entry = state.detailEvidence[String(hsCode)] || null;
  return isDetailEvidenceCurrent(entry) ? entry : { supplier: false, capex: false };
}

function pruneDetailEvidence() {
  const entries = Object.entries(state.detailEvidence).filter(([, entry]) => (
    isDetailEvidenceCurrent(entry)
  ));
  if (entries.length === Object.keys(state.detailEvidence).length) return;
  state.detailEvidence = Object.fromEntries(entries);
  persistJSON(STORAGE_KEYS.evidence, state.detailEvidence);
}

function interactionFocusToken(element) {
  if (!element || !element.dataset || !element.dataset.action) return null;
  return {
    action: element.dataset.action,
    hsCode: element.dataset.hsCode || '',
    status: element.dataset.status || ''
  };
}

function restoreInteractionFocus(token) {
  if (!token) return;
  requestAnimationFrame(() => {
    let selector = `[data-action="${cssEscape(token.action)}"]`;
    if (token.hsCode) selector += `[data-hs-code="${cssEscape(token.hsCode)}"]`;
    if (token.status) selector += `[data-status="${cssEscape(token.status)}"]`;
    const replacement = document.querySelector(selector);
    if (replacement) replacement.focus();
  });
}

function restoreShortlist() {
  const current = restoreJSON(STORAGE_KEYS.shortlist, null);
  const legacy = current == null ? restoreJSON('bfi_watchlist', []) : [];
  const source = Array.isArray(current) ? current : Array.isArray(legacy) ? legacy : [];
  const seen = new Set();
  return source.flatMap((item) => {
    if (!item || typeof item !== 'object') return [];
    const hsCode = String(item.hs_code || '');
    if (!/^\d{1,10}$/.test(hsCode) || seen.has(hsCode)) return [];
    seen.add(hsCode);
    return [{
      hs_code: hsCode,
      title: String(item.title || 'Untitled opportunity').slice(0, 500),
      sectors: Array.isArray(item.sectors) ? item.sectors.slice(0, 8).map(String) : [],
      last_12m_value_usd: Model.finiteNumber(item.last_12m_value_usd),
      reduction_pct: Model.finiteNumber(item.reduction_pct)
    }];
  });
}

function restoreJSON(key, fallback) {
  try {
    const value = JSON.parse(localStorage.getItem(key));
    return value == null ? fallback : value;
  } catch (_) {
    return fallback;
  }
}

function persistJSON(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    console.warn(`Could not persist ${key}`, error);
  }
}

function initializeTheme() {
  const saved = restoreRaw(STORAGE_KEYS.theme);
  if (saved === 'dark' || saved === 'light') {
    document.documentElement.dataset.theme = saved;
  }
  syncThemeButton();
}

function toggleTheme() {
  const current = document.documentElement.dataset.theme;
  const systemDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const effectiveDark = current ? current === 'dark' : systemDark;
  const next = effectiveDark ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  try {
    localStorage.setItem(STORAGE_KEYS.theme, next);
  } catch (_) {}
  syncThemeButton();
}

function syncThemeButton() {
  const current = document.documentElement.dataset.theme;
  const systemDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const dark = current ? current === 'dark' : systemDark;
  document.getElementById('themeToggle').setAttribute(
    'aria-label',
    dark ? 'Switch to light theme' : 'Switch to dark theme'
  );
}

function restoreRaw(key) {
  try {
    return localStorage.getItem(key);
  } catch (_) {
    return null;
  }
}

function showToast(message, type = 'success') {
  const toast = document.getElementById('toast');
  const requestId = ++state.toastRequestId;
  clearTimeout(state.toastTimer);
  toast.textContent = message;
  toast.dataset.type = type;
  toast.hidden = false;
  requestAnimationFrame(() => toast.classList.add('is-visible'));
  state.toastTimer = setTimeout(() => {
    toast.classList.remove('is-visible');
    setTimeout(() => {
      if (requestId === state.toastRequestId && !toast.classList.contains('is-visible')) {
        toast.hidden = true;
      }
    }, 180);
  }, 2400);
}

function formatUsdCompact(value) {
  const number = Model.finiteNumber(value);
  return number === null ? 'Unavailable' : usdCompactFormatter.format(number);
}

function formatInr(value) {
  const number = Model.finiteNumber(value);
  if (number === null) return 'Unavailable';
  if (number >= 10_000_000) return `₹${trimNumber(number / 10_000_000)}Cr`;
  if (number >= 100_000) return `₹${trimNumber(number / 100_000)}L`;
  return `₹${numberFormatter.format(number)}`;
}

function formatUsdRange(minimumInput, maximumInput) {
  const minimum = Model.finiteNumber(minimumInput);
  const maximum = Model.finiteNumber(maximumInput);
  if (minimum === null && maximum === null) return 'Unavailable';
  if (minimum === null) return `Up to ${usdFormatter.format(maximum)}`;
  if (maximum === null) return `From ${usdFormatter.format(minimum)}`;
  return `${usdFormatter.format(minimum)}–${usdFormatter.format(maximum)}`;
}

function formatPercentage(value) {
  const number = Model.finiteNumber(value);
  return number === null ? 'Unavailable' : `${(number * 100).toFixed(1)}%`;
}

function formatPercentageNumber(value) {
  const number = Model.finiteNumber(value);
  if (number === null) return '—';
  return `${trimNumber(number)}%`;
}

function shortTrendLabel(reductionPct) {
  const reduction = Model.finiteNumber(reductionPct);
  if (reduction === null) return 'Unavailable';
  const growth = -reduction * 100;
  if (growth >= 1000) return '>10×';
  return `${growth >= 0 ? '+' : ''}${growth.toFixed(1)}%`;
}

function trimNumber(value) {
  return Number(value.toFixed(2)).toLocaleString('en-IN', { maximumFractionDigits: 2 });
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
  return new Intl.DateTimeFormat('en-IN', {
    month: 'short',
    year: 'numeric',
    timeZone: 'UTC'
  }).format(new Date(Date.UTC(Number(match[1]), month - 1, 1)));
}

function formatTimestamp(value) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return String(value || 'Unavailable');
  return new Intl.DateTimeFormat('en-IN', {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(date);
}

function monthLabel(year, month) {
  return new Intl.DateTimeFormat('en-IN', {
    month: 'short',
    year: '2-digit',
    timeZone: 'UTC'
  }).format(new Date(Date.UTC(year, month - 1, 1)));
}

function newestTimestamp(current, candidate) {
  if (!candidate) return current;
  if (!current) return candidate;
  const left = Date.parse(current);
  const right = Date.parse(candidate);
  if (!Number.isFinite(left)) return candidate;
  if (!Number.isFinite(right)) return current;
  return right > left ? candidate : current;
}

function asNonNegativeInteger(value) {
  const number = Model.finiteNumber(value);
  return number !== null && number >= 0 ? Math.floor(number) : null;
}

function summarizeCapability(value) {
  if (Array.isArray(value)) return value.map(String).slice(0, 6).join(', ');
  if (value && typeof value === 'object') {
    return Object.entries(value).slice(0, 6).map(([key, entry]) => `${key}: ${String(entry)}`).join(' · ');
  }
  return String(value || 'Unavailable');
}

function titleCase(value) {
  return String(value || '').replace(/\b\w/g, (character) => character.toUpperCase());
}

function escapeHTML(value) {
  const substitutions = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  };
  return String(value == null ? '' : value).replace(/[&<>"']/g, (character) => substitutions[character]);
}

function cssEscape(value) {
  if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(value);
  return String(value).replace(/["\\]/g, '\\$&');
}

function isoDate() {
  return new Date().toISOString().slice(0, 10);
}

function reducedMotion() {
  return window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}
