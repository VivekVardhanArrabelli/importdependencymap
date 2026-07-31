'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Model = require('../client/opportunity-model.js');

const PRODUCTS = [
  {
    hs_code: '850760',
    title: 'Electric accumulators; lithium-ion',
    sectors: ['electronics', 'energy'],
    last_12m_value_usd: 5_000_000_000,
    reduction_pct: -0.3
  },
  {
    hs_code: '841370',
    title: 'Pumps; centrifugal',
    sectors: ['industrial'],
    last_12m_value_usd: 500_000_000,
    reduction_pct: -0.1
  },
  {
    hs_code: '270900',
    title: 'Petroleum oils and oils obtained from bituminous minerals, crude',
    sectors: ['industrial'],
    last_12m_value_usd: 100_000_000_000,
    reduction_pct: 0.04
  }
];

test('HS chapter sectors do not inherit substring false positives', () => {
  assert.deepEqual(
    Model.sectorGroups({ hs_code: '080280', title: 'Areca nuts, shelled or peeled' }),
    ['agrifood']
  );
  assert.deepEqual(
    Model.sectorGroups({ hs_code: '720836', title: 'Iron or steel, in coils' }),
    ['metals']
  );
  assert.deepEqual(
    Model.sectorGroups({ hs_code: '850760', title: 'Electric accumulators; lithium-ion batteries' }),
    ['electronics', 'energy']
  );
});

test('country import signal is monotonic with market scale when trend is equal', () => {
  const context = Model.createMarketContext(PRODUCTS);
  const profile = Model.sanitizeProfile({});
  const low = Model.deriveOpportunity(PRODUCTS[1], profile, context);
  const high = Model.deriveOpportunity(PRODUCTS[0], profile, context);
  assert.ok(high.marketPercentile > low.marketPercentile);
  assert.ok(high.countrySignal > low.countrySignal);
});

test('unknown CAPEX keeps capital out of the fit score', () => {
  const context = Model.createMarketContext(PRODUCTS);
  const lowCapital = Model.deriveOpportunity(
    PRODUCTS[0],
    { ...Model.DEFAULT_PROFILE, capitalInr: 2_500_000 },
    context
  );
  const highCapital = Model.deriveOpportunity(
    PRODUCTS[0],
    { ...Model.DEFAULT_PROFILE, capitalInr: 1_000_000_000 },
    context
  );
  assert.equal(lowCapital.combinedFit, highCapital.combinedFit);
  assert.equal(lowCapital.mandateMatch, highCapital.mandateMatch);
  assert.notEqual(lowCapital.route, highCapital.route);
  assert.equal(lowCapital.evidence.capex, false);
});

test('substitution scenarios are bounded by the annual import pool', () => {
  assert.equal(Model.scenarioValue(1_000_000, 1), 10_000);
  assert.equal(Model.scenarioValue(1_000_000, 200), 1_000_000);
  assert.equal(Model.scenarioValue(null, 1), null);
});

test('structural exposure remains nationally visible but is penalized for founder fit', () => {
  const context = Model.createMarketContext(PRODUCTS);
  const profile = Model.sanitizeProfile({
    role: 'build',
    countryWeight: 60,
    horizonMonths: 24,
    risk: 'balanced'
  });
  const crude = Model.deriveOpportunity(PRODUCTS[2], profile, context);
  const battery = Model.deriveOpportunity(PRODUCTS[0], profile, context);
  assert.equal(crude.isStructural, true);
  assert.ok(crude.countrySignal > battery.countrySignal);
  assert.ok(crude.combinedFit < battery.combinedFit);
});

test('special-trade records require a company-formation gate', () => {
  const artwork = {
    hs_code: '970199',
    title: 'Paintings and drawings; other',
    last_12m_value_usd: 500_000_000,
    reduction_pct: -0.5
  };
  const opportunity = Model.deriveOpportunity(
    artwork,
    Model.DEFAULT_PROFILE,
    Model.createMarketContext([artwork])
  );
  assert.equal(opportunity.classification.id, 'trade_signal');
  assert.match(opportunity.reasons[0], /company-formation path unverified/i);
  assert.ok(opportunity.wedgeScore < 50);
});

test('controlled goods are explicitly gated out of ordinary opportunities', () => {
  const firearmParts = {
    hs_code: '930510',
    title: 'Parts and accessories of revolvers or pistols',
    last_12m_value_usd: 1_000_000_000,
    reduction_pct: -0.4
  };
  const opportunity = Model.deriveOpportunity(
    firearmParts,
    Model.DEFAULT_PROFILE,
    Model.createMarketContext([firearmParts])
  );
  assert.deepEqual(Model.sectorGroups(firearmParts), ['controlled']);
  assert.equal(opportunity.classification.id, 'controlled');
  assert.equal(opportunity.combinedFit, 0);
  assert.equal(opportunity.wedgeScore, 0);
  assert.match(opportunity.route, /licensing|end-use controls/i);
});

test('aircraft and vessels carry a strategic-asset penalty for a short founder mandate', () => {
  const products = [
    {
      hs_code: '880211',
      title: 'Helicopters; of an unladen weight not exceeding 2000kg',
      last_12m_value_usd: 10_000_000_000,
      reduction_pct: -0.4
    },
    {
      hs_code: '870899',
      title: 'Motor vehicle components',
      last_12m_value_usd: 500_000_000,
      reduction_pct: -0.1
    }
  ];
  const profile = {
    ...Model.DEFAULT_PROFILE,
    capitalInr: 10_000_000,
    role: 'build',
    horizonMonths: 12,
    risk: 'resilient',
    sectors: ['mobility']
  };
  const context = Model.createMarketContext(products);
  const helicopter = Model.deriveOpportunity(products[0], profile, context);
  const component = Model.deriveOpportunity(products[1], profile, context);
  assert.equal(helicopter.classification.id, 'strategic_asset');
  assert.ok(component.combinedFit > helicopter.combinedFit);
  assert.ok(component.wedgeScore > helicopter.wedgeScore);
  assert.match(helicopter.route, /licensed OEM|strategic consortium/i);
});

test('exclusion clauses do not create false frontier signals', () => {
  assert.notEqual(
    Model.classifyOpportunity({
      hs_code: '700719',
      title: 'Safety glass; not of size and shape suitable for incorporation in vehicles, aircraft'
    }).id,
    'frontier'
  );
  assert.notEqual(
    Model.classifyOpportunity({
      hs_code: '940169',
      title: 'Seats; with wooden frames, not upholstered (excluding medical furniture)'
    }).id,
    'frontier'
  );
});

test('raw agrifood is labelled as primary supply rather than manufacturing', () => {
  const soy = {
    hs_code: '120190',
    title: 'Soya beans; other than seed',
    last_12m_value_usd: 1_000_000_000,
    reduction_pct: -0.2
  };
  assert.equal(Model.classifyOpportunity(soy).id, 'primary');
  assert.match(Model.engagementRoute(soy, Model.DEFAULT_PROFILE), /production|producers/i);
});

test('ranking is deterministic when scores and values tie', () => {
  const tied = [
    { hs_code: '850002', title: 'Electrical item B', last_12m_value_usd: 10, reduction_pct: 0 },
    { hs_code: '850001', title: 'Electrical item A', last_12m_value_usd: 10, reduction_pct: 0 }
  ];
  const ranked = Model.rankOpportunities(tied, Model.DEFAULT_PROFILE, 'fit');
  assert.deepEqual(ranked.map((item) => item.product.hs_code), ['850001', '850002']);
});

test('missing trend is marked missing instead of converted to a zero signal', () => {
  const product = { hs_code: '840001', title: 'Machinery', last_12m_value_usd: 100 };
  const opportunity = Model.deriveOpportunity(
    product,
    Model.DEFAULT_PROFILE,
    Model.createMarketContext([product])
  );
  assert.equal(opportunity.momentum, null);
  assert.equal(opportunity.evidence.trend, false);
  assert.equal(opportunity.countrySignal, opportunity.marketPercentile);
  assert.equal(opportunity.risingScore, -1);
  assert.equal(
    opportunity.wedgeScore,
    ((opportunity.mandateMatch * 0.58) + (opportunity.marketPercentile * 0.2)) / 0.78
  );
});

test('trend narrative describes import value rather than claiming physical dependency growth', () => {
  assert.match(Model.trendNarrative(-0.2), /^Import value rose 20\.0%/);
  assert.match(Model.trendNarrative(0.2), /^Import value fell 20\.0%/);
});

test('the momentum lens always places records with missing trend after measured records', () => {
  const products = [
    { hs_code: '840001', title: 'Large unknown-trend machine', last_12m_value_usd: 1000 },
    { hs_code: '840002', title: 'Measured machine', last_12m_value_usd: 1, reduction_pct: 0 }
  ];
  const ranked = Model.rankOpportunities(products, Model.DEFAULT_PROFILE, 'rising');
  assert.deepEqual(ranked.map((item) => item.product.hs_code), ['840002', '840001']);
});

test('spreadsheet text neutralizes formula prefixes without changing numeric values', () => {
  assert.equal(Model.spreadsheetSafeText('=2+2'), "'=2+2");
  assert.equal(Model.spreadsheetSafeText(' @SUM(A1:A2)'), "' @SUM(A1:A2)");
  assert.equal(Model.spreadsheetSafeText('-malicious'), "'-malicious");
  assert.equal(Model.spreadsheetSafeText(-4), '-4');
});
