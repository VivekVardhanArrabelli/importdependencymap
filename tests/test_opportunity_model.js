'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Model = require('../client/opportunity-model.js');

function product(hsCode, title, overrides = {}) {
  return {
    hs_code: hsCode,
    title,
    last_12m_value_usd: 100_000_000,
    reduction_pct: -0.2,
    ...overrides
  };
}

function route(hsCode, routeId, overrides = {}) {
  return {
    route_id: routeId,
    hs_code: hsCode,
    business_name: `Business ${routeId}`,
    entry_model: 'assemble',
    wedge: `Launch ${routeId}`,
    includes: 'A narrowly specified product and launch route.',
    excludes: 'Adjacent products and a fully integrated plant.',
    capital_min_inr: 2_000_000,
    capital_max_inr: 5_000_000,
    months_min: 6,
    months_max: 12,
    buyer: 'Indian industrial buyers',
    first_proof: 'A paid pilot from one qualified buyer.',
    critical_unknowns: ['Buyer specification', 'Supplier qualification', 'Unit economics'],
    required_strengths: ['sourcing'],
    buyer_types: ['factories'],
    complexity: {
      capital: 2,
      technical: 3,
      regulatory: 2,
      supply_chain: 3,
      buyer_qualification: 3
    },
    impact_type: 'direct',
    confidence: 'curated_model',
    source_note: 'Modeled screening route for tests.',
    reviewed_at: '2026-07-31',
    ...overrides
  };
}

const PANEL = product(
  '853710',
  'Boards, panels, consoles, desks, cabinets and other bases; for electric control or the distribution of electricity'
);

const LOW_CAPITAL_PROFILE = {
  ...Model.DEFAULT_PROFILE,
  capitalInr: 2_500_000,
  capitalMultiplier: 1,
  role: 'build',
  horizonMonths: 12,
  entryModel: 'assemble',
  strengths: ['sourcing', 'workshop'],
  buyerAccess: 'factories',
  sectors: ['electronics']
};

const LARGE_CAPITAL_PROFILE = {
  ...Model.DEFAULT_PROFILE,
  capitalInr: 100_000_000,
  capitalMultiplier: 10,
  role: 'build',
  horizonMonths: 36,
  entryModel: 'make',
  strengths: ['engineering', 'land_power'],
  buyerAccess: 'oems',
  sectors: ['electronics']
};

const PANEL_ROUTES = [
  route('853710', 'panel-integration', {
    business_name: 'Custom low-voltage control-panel integration',
    entry_model: 'assemble',
    required_strengths: ['sourcing', 'workshop'],
    buyer_types: ['factories']
  }),
  route('853710', 'panel-component-plant', {
    business_name: 'Control-panel component manufacturing',
    entry_model: 'make',
    capital_min_inr: 100_000_000,
    capital_max_inr: 300_000_000,
    months_min: 18,
    months_max: 30,
    required_strengths: ['engineering', 'land_power'],
    buyer_types: ['oems'],
    complexity: {
      capital: 5,
      technical: 4,
      regulatory: 3,
      supply_chain: 4,
      buyer_qualification: 4
    }
  })
];

function rankedCodes(products, profile, lens, routes) {
  const context = Model.createMarketContext(products);
  return Model.rankOpportunities(products, profile, lens, context, routes)
    .map((opportunity) => opportunity.product.hs_code);
}

test('lens changes sort a shallow copy without re-deriving opportunities', () => {
  const products = [
    product('853710', 'Electrical control panels', { last_12m_value_usd: 10_000_000 }),
    product('850300', 'Motor parts', { last_12m_value_usd: 1_000_000_000 })
  ];
  const context = Model.createMarketContext(products);
  const opportunities = products.map((item) => Model.deriveOpportunity(
    item,
    LOW_CAPITAL_PROFILE,
    context,
    PANEL_ROUTES
  ));
  const originalOrder = opportunities.map((item) => item.product.hs_code);
  const national = Model.sortOpportunities(opportunities, 'national');

  assert.deepEqual(opportunities.map((item) => item.product.hs_code), originalOrder);
  assert.deepEqual(national.map((item) => item.product.hs_code), ['850300', '853710']);
  assert.equal(national.find((item) => item.product.hs_code === '853710'), opportunities[0]);
});

test('profile defaults describe the ₹25 lakh, 12-month assembly mandate', () => {
  assert.deepEqual(Model.sanitizeProfile(), {
    capitalInr: 2_500_000,
    capitalMultiplier: 1,
    role: 'build',
    horizonMonths: 12,
    entryModel: 'assemble',
    strengths: [],
    buyerAccess: 'none',
    sectors: [],
    capturePct: 1
  });
});

test('profile sanitization de-duplicates allowed attributes and migrates the old risk field', () => {
  const profile = Model.sanitizeProfile({
    capitalInr: '5000000',
    capitalMultiplier: '3',
    role: 'partner',
    horizonMonths: '24',
    risk: 'frontier',
    strengths: ['engineering', 'engineering', 'magic'],
    buyerAccess: 'oems',
    sectors: ['electronics', 'electronics', 'unknown'],
    capturePct: 99
  });
  assert.deepEqual(profile, {
    capitalInr: 5_000_000,
    capitalMultiplier: 3,
    role: 'partner',
    horizonMonths: 24,
    entryModel: 'make',
    strengths: ['engineering'],
    buyerAccess: 'oems',
    sectors: ['electronics'],
    capturePct: 5
  });
});

test('project capital and profile completeness reflect leverage and optional personalization', () => {
  assert.equal(Model.projectCapital({ capitalInr: 2_500_000, capitalMultiplier: 10 }), 25_000_000);
  assert.deepEqual(Model.profileCompleteness(Model.DEFAULT_PROFILE), {
    answered: 4,
    total: 7,
    percentage: 57
  });
  assert.deepEqual(Model.profileCompleteness(LOW_CAPITAL_PROFILE), {
    answered: 7,
    total: 7,
    percentage: 100
  });
});

test('route normalization accepts registry envelopes and rejects malformed HS records', () => {
  const valid = route('853710', 'valid');
  assert.deepEqual(Model.normalizeRoutes({ version: 'v1', routes: [valid, { hs_code: '85' }, null] }), [valid]);
  assert.deepEqual(Model.normalizeRoutes([valid]), [valid]);
  assert.deepEqual(Model.normalizeRoutes(null), []);
});

test('HS chapter sectors avoid title substring false positives', () => {
  assert.deepEqual(Model.sectorGroups(product('080280', 'Areca nuts, shelled or peeled')), ['agrifood']);
  assert.deepEqual(Model.sectorGroups(product('720836', 'Iron or steel, in coils')), ['metals']);
  assert.deepEqual(
    Model.sectorGroups(product('850760', 'Electric accumulators; lithium-ion batteries')),
    ['electronics', 'energy']
  );
});

test('the selected business route changes with capital, time, entry model and operating edge', () => {
  const context = Model.createMarketContext([PANEL]);
  const low = Model.deriveOpportunity(PANEL, LOW_CAPITAL_PROFILE, context, PANEL_ROUTES);
  const large = Model.deriveOpportunity(PANEL, LARGE_CAPITAL_PROFILE, context, PANEL_ROUTES);

  assert.equal(low.route.route_id, 'panel-integration');
  assert.equal(low.fitVerdict.id, 'actionable');
  assert.equal(low.projectCapitalInr, 2_500_000);
  assert.equal(large.route.route_id, 'panel-component-plant');
  assert.equal(large.fitVerdict.id, 'actionable');
  assert.equal(large.projectCapitalInr, 1_000_000_000);
  assert.notEqual(low.personalFit, large.personalFit);
  assert.equal(low.countrySignal, large.countrySignal);
});

test('capital and time alone cannot create an actionable verdict when the operator fit is poor', () => {
  const mismatched = {
    ...LOW_CAPITAL_PROFILE,
    entryModel: 'enable',
    strengths: ['distribution'],
    buyerAccess: 'consumer',
    sectors: ['agrifood']
  };
  const opportunity = Model.deriveOpportunity(
    PANEL,
    mismatched,
    Model.createMarketContext([PANEL]),
    [PANEL_ROUTES[0]]
  );
  assert.equal(opportunity.fitTrace.capital, 100);
  assert.equal(opportunity.fitTrace.horizon, 100);
  assert.equal(opportunity.fitVerdict.id, 'stretch');
  assert.match(opportunity.fitVerdict.detail, /buyer access|operating strengths/i);
});

test('a reviewed route cannot be displaced by a research-only variant for the same HS code', () => {
  const reviewed = route('853710', 'reviewed', {
    entry_model: 'make',
    capital_min_inr: 100_000_000,
    months_min: 24,
    required_strengths: ['engineering'],
    buyer_types: ['oems']
  });
  const research = route('853710', 'research', {
    confidence: 'research_only',
    impact_type: 'research',
    required_strengths: ['sourcing'],
    buyer_types: ['factories']
  });
  const opportunity = Model.deriveOpportunity(
    PANEL,
    LOW_CAPITAL_PROFILE,
    Model.createMarketContext([PANEL]),
    [research, reviewed]
  );
  assert.equal(opportunity.route.route_id, 'reviewed');
});

test('capital constraints change ranking instead of merely changing prose', () => {
  const products = [
    product('853710', 'Electrical control panels'),
    product('850300', 'Parts suitable for electric motors or generators')
  ];
  const routes = [
    route('853710', 'small-panel-shop', {
      required_strengths: [],
      buyer_types: [],
      capital_min_inr: 2_000_000,
      capital_max_inr: 5_000_000
    }),
    route('850300', 'motor-component-plant', {
      entry_model: 'make',
      required_strengths: [],
      buyer_types: [],
      capital_min_inr: 100_000_000,
      capital_max_inr: 300_000_000,
      months_min: 18,
      months_max: 30
    })
  ];

  assert.deepEqual(rankedCodes(products, LOW_CAPITAL_PROFILE, 'fit', routes), ['853710', '850300']);
  assert.deepEqual(rankedCodes(products, LARGE_CAPITAL_PROFILE, 'fit', routes), ['850300', '853710']);
});

test('strengths independently change ranking between otherwise equal routes', () => {
  const products = [
    product('853710', 'Electrical control panels'),
    product('850440', 'Static converters')
  ];
  const routes = [
    route('853710', 'engineering-route', { required_strengths: ['engineering'], buyer_types: [] }),
    route('850440', 'distribution-route', { required_strengths: ['distribution'], buyer_types: [] })
  ];
  const base = { ...LOW_CAPITAL_PROFILE, buyerAccess: 'none' };

  assert.deepEqual(
    rankedCodes(products, { ...base, strengths: ['engineering'] }, 'fit', routes),
    ['853710', '850440']
  );
  assert.deepEqual(
    rankedCodes(products, { ...base, strengths: ['distribution'] }, 'fit', routes),
    ['850440', '853710']
  );
});

test('buyer access independently changes ranking between otherwise equal routes', () => {
  const products = [
    product('853710', 'Electrical control panels'),
    product('850440', 'Static converters')
  ];
  const routes = [
    route('853710', 'factory-sales', { required_strengths: [], buyer_types: ['factories'] }),
    route('850440', 'consumer-sales', { required_strengths: [], buyer_types: ['consumer'] })
  ];
  const base = { ...LOW_CAPITAL_PROFILE, strengths: [] };

  assert.deepEqual(rankedCodes(products, { ...base, buyerAccess: 'factories' }, 'fit', routes), ['853710', '850440']);
  assert.deepEqual(rankedCodes(products, { ...base, buyerAccess: 'consumer' }, 'fit', routes), ['850440', '853710']);
});

test('entry model independently changes ranking between otherwise equal routes', () => {
  const products = [
    product('853710', 'Electrical control panels'),
    product('850440', 'Static converters')
  ];
  const routes = [
    route('853710', 'assembly-route', { entry_model: 'assemble', required_strengths: [], buyer_types: [] }),
    route('850440', 'contract-route', { entry_model: 'contract', required_strengths: [], buyer_types: [] })
  ];
  const base = { ...LOW_CAPITAL_PROFILE, strengths: [], buyerAccess: 'none' };

  assert.deepEqual(rankedCodes(products, { ...base, entryModel: 'assemble' }, 'fit', routes), ['853710', '850440']);
  assert.deepEqual(rankedCodes(products, { ...base, entryModel: 'contract' }, 'fit', routes), ['850440', '853710']);
});

test('sector preference independently changes ranking between otherwise equal routes', () => {
  const products = [
    product('853710', 'Electrical control panels'),
    product('940320', 'Metal furniture')
  ];
  const routes = [
    route('853710', 'electronics-route', { required_strengths: [], buyer_types: [] }),
    route('940320', 'consumer-route', { required_strengths: [], buyer_types: [] })
  ];
  const base = { ...LOW_CAPITAL_PROFILE, strengths: [], buyerAccess: 'none' };

  assert.deepEqual(rankedCodes(products, { ...base, sectors: ['electronics'] }, 'fit', routes), ['853710', '940320']);
  assert.deepEqual(rankedCodes(products, { ...base, sectors: ['consumer'] }, 'fit', routes), ['940320', '853710']);
});

test('role independently changes ranking between make and contract routes', () => {
  const products = [
    product('853710', 'Electrical control panels'),
    product('850440', 'Static converters')
  ];
  const routes = [
    route('853710', 'make-route', { entry_model: 'make', required_strengths: [], buyer_types: [] }),
    route('850440', 'contract-route', { entry_model: 'contract', required_strengths: [], buyer_types: [] })
  ];
  const base = {
    ...LOW_CAPITAL_PROFILE,
    entryModel: 'enable',
    strengths: [],
    buyerAccess: 'none'
  };

  assert.deepEqual(rankedCodes(products, { ...base, role: 'build' }, 'fit', routes), ['853710', '850440']);
  assert.deepEqual(rankedCodes(products, { ...base, role: 'back' }, 'fit', routes), ['850440', '853710']);
});

test('personal score uses the documented component weights exactly', () => {
  const opportunity = Model.deriveOpportunity(
    PANEL,
    LOW_CAPITAL_PROFILE,
    Model.createMarketContext([PANEL]),
    PANEL_ROUTES
  );
  const trace = opportunity.fitTrace;
  const expected = (trace.capital * 0.25)
    + (trace.horizon * 0.20)
    + (trace.entryModel * 0.15)
    + (trace.strengths * 0.15)
    + (trace.buyer * 0.10)
    + (trace.sector * 0.10)
    + (trace.role * 0.05);
  assert.ok(Math.abs(opportunity.personalFit - expected) < 1e-9);
  assert.ok(Math.abs(opportunity.combinedFit - ((opportunity.countrySignal * 0.35) + (expected * 0.65))) < 1e-9);
});

test('the country signal is persona-invariant and the national lens ignores personal rank', () => {
  const products = [
    product('853710', 'Electrical control panels', { last_12m_value_usd: 10_000_000 }),
    product('850300', 'Motor parts', { last_12m_value_usd: 1_000_000_000 })
  ];
  const routes = [
    route('853710', 'small-route'),
    route('850300', 'large-route', {
      entry_model: 'make',
      capital_min_inr: 100_000_000,
      capital_max_inr: 300_000_000,
      months_min: 18,
      months_max: 30,
      required_strengths: ['engineering', 'land_power'],
      buyer_types: ['oems']
    })
  ];
  const context = Model.createMarketContext(products);
  const low = Model.deriveOpportunity(products[1], LOW_CAPITAL_PROFILE, context, routes);
  const large = Model.deriveOpportunity(products[1], LARGE_CAPITAL_PROFILE, context, routes);

  assert.equal(low.countrySignal, large.countrySignal);
  assert.deepEqual(rankedCodes(products, LOW_CAPITAL_PROFILE, 'national', routes), ['850300', '853710']);
  assert.deepEqual(rankedCodes(products, LARGE_CAPITAL_PROFILE, 'national', routes), ['850300', '853710']);
});

test('unmapped customs categories remain research-only at every capital level', () => {
  const unmapped = product('840001', 'Machinery; other, n.e.c.');
  const context = Model.createMarketContext([unmapped]);
  const low = Model.deriveOpportunity(unmapped, LOW_CAPITAL_PROFILE, context, []);
  const large = Model.deriveOpportunity(unmapped, LARGE_CAPITAL_PROFILE, context, []);

  for (const opportunity of [low, large]) {
    assert.equal(opportunity.route.confidence, 'research_only');
    assert.equal(opportunity.route.impact_type, 'research');
    assert.equal(opportunity.fitVerdict.id, 'research');
    assert.ok(opportunity.combinedFit <= 49);
    assert.equal(opportunity.evidence.capex, false);
    assert.match(opportunity.route.wedge, /resolve/i);
  }
});

test('fit and surprising-wedge lenses put reviewed actionable routes ahead of giant research signals', () => {
  const products = [
    product('853710', 'Electrical control panels', { last_12m_value_usd: 1_000_000 }),
    product('840001', 'Unmapped machinery', { last_12m_value_usd: 100_000_000_000 })
  ];
  const routes = [route('853710', 'reviewed-panel', {
    required_strengths: ['sourcing', 'workshop'],
    buyer_types: ['factories']
  })];

  assert.deepEqual(rankedCodes(products, LOW_CAPITAL_PROFILE, 'fit', routes), ['853710', '840001']);
  assert.deepEqual(rankedCodes(products, LOW_CAPITAL_PROFILE, 'wedge', routes), ['853710', '840001']);
  assert.deepEqual(rankedCodes(products, LOW_CAPITAL_PROFILE, 'national', routes), ['840001', '853710']);
});

test('fastest-to-test lens uses modeled launch time and leaves unknown time last', () => {
  const products = [
    product('853710', 'Electrical control panels', { last_12m_value_usd: 10 }),
    product('850440', 'Static converters', { last_12m_value_usd: 100 }),
    product('840001', 'Unmapped machinery', { last_12m_value_usd: 1_000 })
  ];
  const routes = [
    route('853710', 'six-month-route', { months_min: 6, months_max: 9 }),
    route('850440', 'eighteen-month-route', { months_min: 18, months_max: 24 })
  ];

  assert.deepEqual(
    rankedCodes(products, LOW_CAPITAL_PROFILE, 'rising', routes),
    ['853710', '850440', '840001']
  );
});

test('controlled, strategic-asset and structural records stay blocked despite a curated route', () => {
  const records = [
    product('930510', 'Parts and accessories of revolvers or pistols'),
    product('880211', 'Helicopters; of an unladen weight not exceeding 2000kg'),
    product('270900', 'Petroleum oils and oils obtained from bituminous minerals, crude')
  ];
  const routes = records.map((record) => route(record.hs_code, `route-${record.hs_code}`, {
    required_strengths: [],
    buyer_types: []
  }));
  const context = Model.createMarketContext(records);

  records.forEach((record) => {
    const opportunity = Model.deriveOpportunity(record, LARGE_CAPITAL_PROFILE, context, routes);
    assert.equal(opportunity.fitVerdict.id, 'blocked');
    assert.equal(opportunity.combinedFit, 0);
    assert.equal(opportunity.wedgeScore, 0);
  });
});

test('special-trade categories cannot become recommendations without a reviewed route', () => {
  const jewellery = product('711319', 'Articles of jewellery and parts thereof, of precious metal');
  const opportunity = Model.deriveOpportunity(
    jewellery,
    LOW_CAPITAL_PROFILE,
    Model.createMarketContext([jewellery]),
    []
  );
  assert.equal(opportunity.classification.id, 'trade_signal');
  assert.equal(opportunity.fitVerdict.id, 'research');
  assert.match(opportunity.route.wedge, /exports and domestic capability/i);
});

test('clear business-facing titles replace opaque HS labels where a reviewed mapping exists', () => {
  assert.equal(
    Model.productShortTitle(PANEL.title, 88, PANEL.hs_code),
    'Low-voltage electrical control panels and switchboards'
  );
  const opportunity = Model.deriveOpportunity(
    PANEL,
    LOW_CAPITAL_PROFILE,
    Model.createMarketContext([PANEL]),
    PANEL_ROUTES
  );
  assert.equal(opportunity.shortTitle, 'Custom low-voltage control-panel integration');
  assert.match(opportunity.customsTitle, /^Boards, panels/);
  assert.notEqual(opportunity.customsTitle, Model.productShortTitle(PANEL.title, 88, PANEL.hs_code));
});

test('strength coverage rewards the full requirement set instead of capping at three matches', () => {
  const demandingRoute = route('853710', 'four-strength-route', {
    required_strengths: ['engineering', 'operations', 'quality', 'workshop']
  });
  const opportunity = Model.deriveOpportunity(
    PANEL,
    { ...LOW_CAPITAL_PROFILE, strengths: ['engineering', 'operations', 'quality'] },
    Model.createMarketContext([PANEL]),
    [demandingRoute]
  );
  assert.ok(opportunity.fitTrace.strengths < 100);
  assert.ok(opportunity.fitTrace.strengths > 75);
});

test('complexity exposes five dimensions and a stable overall label', () => {
  assert.deepEqual(Model.complexitySummary({
    complexity: {
      capital: 1,
      technical: 2,
      regulatory: 3,
      supply_chain: 4,
      buyer_qualification: 5
    }
  }), {
    overall: 3,
    label: 'Moderate build',
    dimensions: {
      capital: 1,
      technical: 2,
      regulatory: 3,
      supply_chain: 4,
      buyer_qualification: 5
    }
  });
});

test('modeled capital and time envelopes are labeled explicitly', () => {
  assert.equal(Model.capitalRangeLabel(route('853710', 'range')), '₹20L–₹50L');
  assert.equal(Model.monthRangeLabel(route('853710', 'range')), '6–12 months');
  assert.equal(Model.capitalRangeLabel({}), 'unverified capital');
  assert.equal(Model.monthRangeLabel({}), 'an unverified timeframe');
});

test('ranking is deterministic when tier, scores and import values tie', () => {
  const products = [
    product('850002', 'Electrical item B', { last_12m_value_usd: 10, reduction_pct: 0 }),
    product('850001', 'Electrical item A', { last_12m_value_usd: 10, reduction_pct: 0 })
  ];
  const routes = [
    route('850002', 'route-b', { required_strengths: [], buyer_types: [] }),
    route('850001', 'route-a', { required_strengths: [], buyer_types: [] })
  ];
  assert.deepEqual(rankedCodes(products, Model.DEFAULT_PROFILE, 'fit', routes), ['850001', '850002']);
});

test('missing trade trend stays missing without changing the observed market signal', () => {
  const unmapped = product('840001', 'Machinery', { last_12m_value_usd: 100, reduction_pct: undefined });
  const opportunity = Model.deriveOpportunity(
    unmapped,
    Model.DEFAULT_PROFILE,
    Model.createMarketContext([unmapped]),
    []
  );
  assert.equal(opportunity.momentum, null);
  assert.equal(opportunity.evidence.trend, false);
  assert.equal(opportunity.countrySignal, opportunity.marketPercentile);
});

test('trend narrative describes import value rather than claiming physical dependency growth', () => {
  assert.match(Model.trendNarrative(-0.2), /^Import value rose 20\.0%/);
  assert.match(Model.trendNarrative(0.2), /^Import value fell 20\.0%/);
});

test('substitution scenarios are bounded by the annual import pool', () => {
  assert.equal(Model.scenarioValue(1_000_000, 1), 10_000);
  assert.equal(Model.scenarioValue(1_000_000, 200), 1_000_000);
  assert.equal(Model.scenarioValue(null, 1), null);
});

test('spreadsheet text neutralizes formula prefixes without changing numeric values', () => {
  assert.equal(Model.spreadsheetSafeText('=2+2'), "'=2+2");
  assert.equal(Model.spreadsheetSafeText(' @SUM(A1:A2)'), "' @SUM(A1:A2)");
  assert.equal(Model.spreadsheetSafeText('-malicious'), "'-malicious");
  assert.equal(Model.spreadsheetSafeText(-4), '-4');
});
