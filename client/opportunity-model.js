(function exposeOpportunityModel(root, factory) {
  const model = factory();
  if (typeof module === 'object' && module.exports) module.exports = model;
  if (root) root.OpportunityModel = model;
})(typeof globalThis !== 'undefined' ? globalThis : this, function createOpportunityModel() {
  'use strict';

  const DEFAULT_PROFILE = Object.freeze({
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

  const VALID_ROLES = new Set(['build', 'back', 'partner']);
  const VALID_ENTRY_MODELS = new Set(['make', 'assemble', 'contract', 'enable']);
  const VALID_STRENGTHS = new Set([
    'buyer_access',
    'engineering',
    'operations',
    'distribution',
    'sourcing',
    'quality',
    'workshop',
    'land_power'
  ]);
  const VALID_BUYERS = new Set(['none', 'factories', 'oems', 'government', 'consumer', 'export']);
  const VALID_SECTORS = new Set([
    'agrifood',
    'chemicals',
    'materials',
    'textiles',
    'machinery',
    'electronics',
    'mobility',
    'metals',
    'energy',
    'instruments',
    'consumer',
    'controlled'
  ]);
  const VALID_LENSES = new Set(['fit', 'national', 'rising', 'wedge']);
  const FIT_TIER_RANK = Object.freeze({ blocked: 0, research: 1, partner: 2, stretch: 3, actionable: 4 });

  const STRUCTURAL_PATTERNS = [
    /\bcrude petroleum\b/i,
    /\bpetroleum oils? and oils obtained from bituminous minerals, crude\b/i,
    /\bcoal\b/i,
    /\bnatural gas\b/i,
    /\bgold, non-monetary, unwrought\b/i,
    /\bores? and concentrates?\b/i
  ];

  const FRONTIER_PATTERNS = [
    /\bintegrated circuits?\b/i,
    /\bsemiconductor\b/i,
    /\baircraft\b/i,
    /\bturbo-jet\b/i,
    /\bturbo-propeller\b/i,
    /\bnuclear reactor\b/i,
    /\bpharmaceutical\b/i,
    /\bmedical\b/i,
    /\boptical\b/i,
    /\blithium-ion\b/i
  ];

  const SCALE_PATTERNS = [
    /\bmachin(?:e|ery)\b/i,
    /\bmotor\b/i,
    /\bgenerator\b/i,
    /\btransformer\b/i,
    /\bbatter(?:y|ies)\b/i,
    /\baccumulator\b/i,
    /\bpump\b/i,
    /\bcompressor\b/i,
    /\bvehicle\b/i,
    /\btransmission\b/i,
    /\bchemical\b/i,
    /\bsteel\b/i,
    /\baluminium\b/i,
    /\bcomponent\b/i,
    /\bparts?\b/i
  ];

  const PROCESS_PATTERNS = [
    /\bfood\b/i,
    /\bfruit\b/i,
    /\bvegetable\b/i,
    /\btextile\b/i,
    /\bfabric\b/i,
    /\bgarment\b/i,
    /\bpaper\b/i,
    /\bwood\b/i,
    /\bfurniture\b/i,
    /\bglass\b/i,
    /\bceramic\b/i,
    /\brubber\b/i,
    /\bplastic\b/i,
    /\bpackaging\b/i
  ];

  const CLEAR_TITLES = Object.freeze({
    '700719': 'Tempered architectural and appliance safety glass',
    '847150': 'Computer and server processing units',
    '847149': 'Computer systems and server assemblies',
    '711319': 'Gold and platinum jewellery',
    '853710': 'Low-voltage electrical control panels and switchboards',
    '853690': 'Low-voltage switching and circuit-protection components',
    '200819': 'Prepared or preserved nuts and seeds',
    '152000': 'Crude glycerine and glycerol by-product streams',
    '540247': 'Non-textured polyester filament yarn',
    '540246': 'Partially oriented polyester filament yarn',
    '540233': 'Textured polyester filament yarn',
    '850300': 'Motor and generator components',
    '850440': 'Power converters, inverters and industrial power supplies',
    '850760': 'Lithium-ion cells and accumulators',
    '842139': 'Industrial gas and air filtration equipment',
    '841989': 'Industrial thermal-processing equipment',
    '401693': 'Industrial rubber seals, gaskets and washers',
    '401699': 'Custom industrial rubber components',
    '400510': 'Carbon-black and silica rubber compounds',
    '940320': 'Non-office metal furniture',
    '190590': 'Specialty bakery and shelf-stable baked products',
    '230990': 'Prepared animal-feed ingredients and blends',
    '640411': 'Textile-upper sports footwear',
    '640399': 'Leather-upper footwear',
    '470710': 'Recovered corrugated cardboard and kraft paper',
    '470790': 'Recovered mixed paper and paperboard',
    '440890': 'Veneer and thin wood sheets',
    '392010': 'Polyethylene film and flexible sheet',
    '691490': 'Specialty ceramic components and articles',
    '854370': 'Special-purpose electronic equipment and controllers',
    '843351': 'Combine harvester-threshers',
    '120190': 'Soybeans for processing and food/feed use',
    '080420': 'Fresh and dried figs',
    '080280': 'Areca nuts',
    '151110': 'Crude palm oil',
    '710692': 'Semi-manufactured silver',
    '721891': 'Semi-finished stainless-steel slabs',
    '284390': 'Precious-metal compounds and amalgams',
    '381800': 'Semiconductor-grade wafers and doped elements',
    '520100': 'Raw cotton',
    '760110': 'Unalloyed unwrought aluminium',
    '720310': 'Direct-reduced iron products',
    '250300': 'Industrial sulphur',
    '853400': 'Bare printed circuit boards',
    '852491': 'LCD display modules',
    '852492': 'OLED display modules'
  });

  function finiteNumber(value) {
    if (value == null || value === '') return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function clamp(value, minimum, maximum) {
    return Math.min(Math.max(value, minimum), maximum);
  }

  function uniqueAllowed(value, allowed) {
    return Array.isArray(value) ? [...new Set(value.filter((entry) => allowed.has(entry)))] : [];
  }

  function sanitizeProfile(input) {
    const profile = input && typeof input === 'object' ? input : {};
    const capital = finiteNumber(profile.capitalInr);
    const multiplier = finiteNumber(profile.capitalMultiplier);
    const horizon = finiteNumber(profile.horizonMonths);
    const capturePct = finiteNumber(profile.capturePct);
    const migratedEntryModel = VALID_ENTRY_MODELS.has(profile.entryModel)
      ? profile.entryModel
      : profile.risk === 'frontier'
        ? 'make'
        : profile.risk === 'resilient'
          ? 'enable'
          : DEFAULT_PROFILE.entryModel;

    return {
      capitalInr: capital !== null && capital > 0 ? capital : DEFAULT_PROFILE.capitalInr,
      capitalMultiplier: [1, 3, 10].includes(multiplier) ? multiplier : DEFAULT_PROFILE.capitalMultiplier,
      role: VALID_ROLES.has(profile.role) ? profile.role : DEFAULT_PROFILE.role,
      horizonMonths: [6, 12, 24, 36].includes(horizon) ? horizon : DEFAULT_PROFILE.horizonMonths,
      entryModel: migratedEntryModel,
      strengths: uniqueAllowed(profile.strengths, VALID_STRENGTHS),
      buyerAccess: VALID_BUYERS.has(profile.buyerAccess) ? profile.buyerAccess : DEFAULT_PROFILE.buyerAccess,
      sectors: uniqueAllowed(profile.sectors, VALID_SECTORS),
      capturePct: capturePct === null ? DEFAULT_PROFILE.capturePct : clamp(capturePct, 0.1, 5)
    };
  }

  function profileCompleteness(profileInput) {
    const profile = sanitizeProfile(profileInput);
    const answered = 4
      + Number(profile.strengths.length > 0)
      + Number(profile.buyerAccess !== 'none')
      + Number(profile.sectors.length > 0);
    return { answered, total: 7, percentage: Math.round((answered / 7) * 100) };
  }

  function projectCapital(profileInput) {
    const profile = sanitizeProfile(profileInput);
    return profile.capitalInr * profile.capitalMultiplier;
  }

  function upperBound(sorted, value) {
    let low = 0;
    let high = sorted.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (sorted[middle] <= value) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  function createMarketContext(products) {
    const positiveValues = (Array.isArray(products) ? products : [])
      .map((product) => finiteNumber(product && product.last_12m_value_usd))
      .filter((value) => value !== null && value > 0)
      .sort((left, right) => left - right);
    return { positiveValues };
  }

  function marketPercentile(value, context) {
    const number = finiteNumber(value);
    const sorted = context && Array.isArray(context.positiveValues) ? context.positiveValues : [];
    if (number === null || number <= 0 || !sorted.length) return 0;
    if (sorted.length === 1) return 100;
    const rank = upperBound(sorted, number);
    return clamp(((rank - 1) / (sorted.length - 1)) * 100, 0, 100);
  }

  function importValueMomentum(reductionPct) {
    const reduction = finiteNumber(reductionPct);
    if (reduction === null) return null;
    const importGrowth = -reduction;
    return clamp(50 + clamp(importGrowth, -0.5, 0.5) * 100, 0, 100);
  }

  function isStructuralExposure(product) {
    const title = String(product && product.title ? product.title : '');
    const code = String(product && product.hs_code ? product.hs_code : '');
    if (STRUCTURAL_PATTERNS.some((pattern) => pattern.test(title))) return true;
    return /^(2601|2602|2603|2604|2605|2606|2607|2608|2609|2610|2701|2709|2711|7108)/.test(code);
  }

  function sectorGroups(product) {
    const code = String(product && product.hs_code ? product.hs_code : '').padStart(2, '0');
    const chapter = Number(code.slice(0, 2));
    const title = String(product && product.title ? product.title : '');
    const groups = [];
    if (chapter >= 1 && chapter <= 24) groups.push('agrifood');
    else if (chapter >= 25 && chapter <= 26) groups.push('materials');
    else if (chapter === 27) groups.push('energy');
    else if (chapter >= 28 && chapter <= 38) groups.push('chemicals');
    else if (chapter >= 39 && chapter <= 49) groups.push('materials');
    else if (chapter >= 50 && chapter <= 63) groups.push('textiles');
    else if (chapter >= 64 && chapter <= 67) groups.push('consumer');
    else if (chapter >= 68 && chapter <= 70) groups.push('materials');
    else if (chapter === 71) groups.push(/\b(jewellery|goldsmiths' wares|ornament)\b/i.test(title) ? 'consumer' : 'metals');
    else if (chapter >= 72 && chapter <= 83) groups.push('metals');
    else if (chapter === 84) {
      if (code.startsWith('8471')) groups.push('electronics');
      else {
        groups.push('machinery');
        if (code.startsWith('8486')) groups.push('electronics');
      }
    } else if (chapter === 85) groups.push('electronics');
    else if (chapter >= 86 && chapter <= 89) groups.push('mobility');
    else if (chapter >= 90 && chapter <= 92) groups.push('instruments');
    else if (chapter === 93) groups.push('controlled');
    else if (chapter >= 94 && chapter <= 99) groups.push('consumer');

    if (/\b(batter(?:y|ies)|accumulators?|solar|photovoltaic|wind turbine|power generation)\b/i.test(title) && !groups.includes('energy')) {
      groups.push('energy');
    }
    if (!groups.length) {
      const legacyMap = { industrial: 'machinery', automotive: 'mobility' };
      const supplied = Array.isArray(product && product.sectors) ? product.sectors : [];
      supplied.forEach((sector) => {
        const normalized = legacyMap[sector] || sector;
        if (VALID_SECTORS.has(normalized) && !groups.includes(normalized)) groups.push(normalized);
      });
    }
    return groups.length ? groups : ['materials'];
  }

  function classifyOpportunity(product) {
    const title = String(product && product.title ? product.title : '');
    const positiveSignals = title
      .replace(/\([^)]*\b(?:excluding|other than|not|except)\b[^)]*\)/gi, ' ')
      .replace(/\b(?:excluding|other than|except|not)\b[^;]*$/gi, ' ')
      .replace(/\bnon[- ](?:medical|optical|aircraft|pharmaceutical)\b/gi, ' ');
    const code = String(product && product.hs_code ? product.hs_code : '');
    const chapter = Number(code.slice(0, 2));
    const sectors = sectorGroups(product);
    if (isStructuralExposure(product)) return { id: 'structural', label: 'National-scale exposure', complexity: 5, description: 'Usually a consortium or policy-led challenge rather than a startup.' };
    if (chapter === 93) return { id: 'controlled', label: 'Controlled-sector gate', complexity: 5, description: 'Licensing, end-use controls, procurement rules, and eligibility come first.' };
    if (chapter === 88 || chapter === 89) return { id: 'strategic_asset', label: 'Strategic-asset gate', complexity: 5, description: 'Aircraft and vessel programs require licensed operators and strategic capital.' };
    if (chapter >= 97 || chapter === 71) return { id: 'trade_signal', label: 'Trade signal to resolve', complexity: 3, description: 'Imports are visible, but net trade and a repeatable company wedge are unresolved.' };
    if (chapter >= 1 && chapter <= 14) return { id: 'primary', label: 'Primary-supply challenge', complexity: 2, description: 'Production cycles, aggregation, quality, geography and offtake determine feasibility.' };
    if (chapter >= 15 && chapter <= 24) return { id: 'process', label: 'Food and formulation', complexity: 2, description: 'Contract processing or a focused local line may be testable.' };
    if (chapter >= 25 && chapter <= 26) return { id: 'resource', label: 'Resource-development challenge', complexity: 4, description: 'Feedstock, permitting, infrastructure and offtake are gating factors.' };
    if (FRONTIER_PATTERNS.some((pattern) => pattern.test(positiveSignals)) || sectors.includes('instruments')) {
      return { id: 'frontier', label: 'Frontier capability', complexity: 5, description: 'Technical validation, certification and patient capital are likely.' };
    }
    if (SCALE_PATTERNS.some((pattern) => pattern.test(positiveSignals)) || sectors.some((sector) => ['mobility', 'energy', 'machinery', 'metals', 'chemicals'].includes(sector))) {
      return { id: 'scale', label: 'Scale manufacturing', complexity: 4, description: 'Production engineering, anchor demand and supplier qualification are likely.' };
    }
    if (PROCESS_PATTERNS.some((pattern) => pattern.test(positiveSignals))) return { id: 'process', label: 'Processing opportunity', complexity: 2, description: 'A focused processing or conversion route may be testable.' };
    return { id: 'specialist', label: 'Specialist manufacturing', complexity: 3, description: 'A narrow production or integration wedge may precede a full facility.' };
  }

  function capitalBandLabel(capitalInr) {
    const capital = finiteNumber(capitalInr) || DEFAULT_PROFILE.capitalInr;
    if (capital < 5_000_000) return '₹25–50 lakh cheque';
    if (capital < 25_000_000) return '₹50 lakh–₹2.5 crore cheque';
    if (capital < 100_000_000) return '₹2.5–10 crore cheque';
    if (capital < 500_000_000) return '₹10–50 crore cheque';
    return '₹50 crore+ cheque';
  }

  function productShortTitle(value, maximumLength, hsCode) {
    const code = String(hsCode || '');
    if (CLEAR_TITLES[code]) return CLEAR_TITLES[code];
    const limit = finiteNumber(maximumLength) || 88;
    const source = String(value || 'Untitled customs category').replace(/\s+/g, ' ').trim();
    const semicolonParts = source.split(';').map((part) => part.trim()).filter(Boolean);
    let title = semicolonParts.length > 1 ? `${semicolonParts[0]} — ${semicolonParts[1]}` : source;
    title = title
      .replace(/\bn\.e\.c\./gi, 'other specified')
      .replace(/\bheading no\.\s*\d+(?:\.\d+)?\b/gi, 'the referenced category')
      .replace(/,\s*whether or not.*$/i, '')
      .replace(/,\s*not elsewhere specified.*$/i, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (title.length <= limit) return title;
    return `${title.slice(0, Math.max(1, limit - 1)).trimEnd()}…`;
  }

  function normalizeRoutes(routesInput) {
    const source = routesInput && !Array.isArray(routesInput) && Array.isArray(routesInput.routes)
      ? routesInput.routes
      : Array.isArray(routesInput)
        ? routesInput
        : [];
    return source.filter((route) => route && /^\d{6}$/.test(String(route.hs_code || '')));
  }

  function genericResearchRoute(product, classification) {
    const name = productShortTitle(product && product.title, 88, product && product.hs_code);
    const descriptions = {
      structural: 'Resolve whether policy, feedstock or consortium action—not a startup—is the correct intervention.',
      controlled: 'Clear legal eligibility and end-use controls before considering any commercial route.',
      strategic_asset: 'Resolve the licensed OEM or component pathway before treating the platform as a company opportunity.',
      primary: 'Resolve geography, production cycle, domestic supply and buyer economics before proposing an operating model.',
      trade_signal: 'Resolve product mix, exports and domestic capability before inventing a company around the customs category.',
      resource: 'Resolve feedstock rights, permitting, infrastructure and offtake before proposing a project.',
      frontier: 'Resolve the specific component, technology owner and qualification path before estimating capital.',
      scale: 'Resolve the exact product, process and buyer specification before estimating a plant.',
      process: 'Resolve the product mix and a credible processing route before calling this personally feasible.',
      specialist: 'Resolve the precise sellable product and production route before estimating launch economics.'
    };
    return {
      route_id: `research-${String(product && product.hs_code || 'unknown')}`,
      hs_code: String(product && product.hs_code || ''),
      business_name: name,
      entry_model: classification.id === 'process' ? 'contract' : 'make',
      wedge: descriptions[classification.id] || descriptions.specialist,
      includes: 'The official HS6 customs category shown below.',
      excludes: 'No reviewed business-entry route or launch envelope is available yet.',
      capital_min_inr: null,
      capital_max_inr: null,
      months_min: null,
      months_max: null,
      buyer: 'Buyer segment unresolved',
      first_proof: 'Resolve shipment mix and interview qualified buyers before choosing a business model.',
      critical_unknowns: ['Exact product mix', 'Domestic supply and exports', 'Process and equipment', 'Standards and buyer qualification'],
      required_strengths: [],
      buyer_types: [],
      complexity: { capital: classification.complexity, technical: classification.complexity, regulatory: 3, supply_chain: 4, buyer_qualification: 3 },
      impact_type: 'research',
      confidence: 'research_only',
      source_note: 'Official trade signal only; business model and launch economics are not yet curated.',
      reviewed_at: null
    };
  }

  function capitalFit(route, profile) {
    const available = projectCapital(profile);
    const minimum = finiteNumber(route.capital_min_inr);
    const maximum = finiteNumber(route.capital_max_inr);
    if (minimum === null) return 20;
    if (available < minimum) return clamp((available / minimum) * 100, 0, 95);
    if (maximum === null || available <= maximum * 3) return 100;
    return clamp(100 - Math.log10(available / (maximum * 3)) * 35, 35, 100);
  }

  function horizonFit(route, profile) {
    const minimum = finiteNumber(route.months_min);
    const maximum = finiteNumber(route.months_max);
    if (minimum === null) return 20;
    if (profile.horizonMonths < minimum) return clamp((profile.horizonMonths / minimum) * 100, 0, 95);
    if (maximum === null || profile.horizonMonths <= maximum * 2) return 100;
    return clamp(100 - ((profile.horizonMonths - maximum * 2) * 1.5), 70, 100);
  }

  function entryModelFit(route, profile) {
    if (route.entry_model === profile.entryModel) return 100;
    const nearby = {
      make: new Set(['assemble', 'contract']),
      assemble: new Set(['make', 'contract', 'enable']),
      contract: new Set(['assemble', 'make']),
      enable: new Set(['assemble'])
    };
    return nearby[profile.entryModel] && nearby[profile.entryModel].has(route.entry_model) ? 62 : 28;
  }

  function strengthFit(route, profile) {
    const required = uniqueAllowed(route.required_strengths, VALID_STRENGTHS);
    if (!profile.strengths.length) return 42;
    if (!required.length) return 60;
    const matches = required.filter((strength) => profile.strengths.includes(strength)).length;
    return clamp(30 + (matches / required.length) * 70, 30, 100);
  }

  function buyerFit(route, profile) {
    if (profile.buyerAccess === 'none') return 45;
    const buyers = uniqueAllowed(route.buyer_types, VALID_BUYERS);
    return buyers.includes(profile.buyerAccess) ? 100 : 25;
  }

  function sectorFit(product, profile) {
    if (!profile.sectors.length) return 48;
    const groups = sectorGroups(product);
    return groups.some((sector) => profile.sectors.includes(sector)) ? 100 : 10;
  }

  function roleFit(route, profile) {
    const score = {
      build: { make: 100, assemble: 96, contract: 90, enable: 82 },
      back: { make: 80, assemble: 90, contract: 94, enable: 85 },
      partner: { make: 92, assemble: 96, contract: 78, enable: 84 }
    };
    return score[profile.role][route.entry_model] || 70;
  }

  function scoreRoute(route, product, profile) {
    const trace = {
      capital: capitalFit(route, profile),
      horizon: horizonFit(route, profile),
      entryModel: entryModelFit(route, profile),
      strengths: strengthFit(route, profile),
      buyer: buyerFit(route, profile),
      sector: sectorFit(product, profile),
      role: roleFit(route, profile)
    };
    const total = (trace.capital * 0.25)
      + (trace.horizon * 0.20)
      + (trace.entryModel * 0.15)
      + (trace.strengths * 0.15)
      + (trace.buyer * 0.10)
      + (trace.sector * 0.10)
      + (trace.role * 0.05);
    return { total: clamp(total, 0, 100), trace };
  }

  function fitVerdict(route, classification, profile, personal) {
    if (classification.id === 'controlled') return { id: 'blocked', label: 'Controlled: not a normal founder route', detail: 'Legal eligibility and end-use controls must be cleared first.' };
    if (['strategic_asset', 'structural'].includes(classification.id)) return { id: 'blocked', label: 'National or consortium-scale', detail: 'The import signal is real; this is not presented as a startup fit.' };
    if (route.confidence !== 'curated_model' || route.impact_type === 'research') return { id: 'research', label: 'Resolve the category first', detail: 'Trade demand is observed, but the business route is not reviewed enough to recommend.' };
    const available = projectCapital(profile);
    const minimumCapital = finiteNumber(route.capital_min_inr);
    const minimumMonths = finiteNumber(route.months_min);
    const capitalReady = minimumCapital !== null && available >= minimumCapital;
    const timeReady = minimumMonths !== null && profile.horizonMonths >= minimumMonths;
    const personalScore = personal && finiteNumber(personal.total);
    if (capitalReady && timeReady && personalScore !== null && personalScore >= 68) {
      return { id: 'actionable', label: 'Actionable hypothesis', detail: 'The modeled entry envelope and the route fit your stated capital stack, time window and operating profile.' };
    }
    if (capitalReady && timeReady) {
      return { id: 'stretch', label: 'Route fit needs work', detail: 'Capital and time fit, but your entry model, buyer access or operating strengths do not yet align strongly.' };
    }
    const capitalStretch = minimumCapital !== null && available >= minimumCapital * 0.5;
    const timeStretch = minimumMonths !== null && profile.horizonMonths >= minimumMonths * 0.75;
    if (capitalStretch && timeStretch) return { id: 'stretch', label: 'Stretch, but testable', detail: 'A narrower pilot, vendor credit or co-investor may close the gap.' };
    return { id: 'partner', label: 'Needs a larger stack or operator', detail: 'The country signal may matter, but this route exceeds your current constraint.' };
  }

  function chooseRoute(product, routesInput, profile, classification) {
    const code = String(product && product.hs_code || '');
    const candidates = normalizeRoutes(routesInput).filter((route) => String(route.hs_code) === code);
    if (!candidates.length) return { route: genericResearchRoute(product, classification), alternatives: [] };
    const ranked = candidates
      .map((route) => ({ route, score: scoreRoute(route, product, profile).total }))
      .sort((left, right) => {
        const confidenceDifference = Number(right.route.confidence === 'curated_model') - Number(left.route.confidence === 'curated_model');
        return confidenceDifference
          || right.score - left.score
          || String(left.route.route_id).localeCompare(String(right.route.route_id));
      });
    return { route: ranked[0].route, alternatives: ranked.slice(1).map((entry) => entry.route) };
  }

  function complexitySummary(route) {
    const source = route && route.complexity && typeof route.complexity === 'object' ? route.complexity : {};
    const dimensions = ['capital', 'technical', 'regulatory', 'supply_chain', 'buyer_qualification'];
    const values = dimensions.map((key) => clamp(finiteNumber(source[key]) || 3, 1, 5));
    const overall = Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
    const label = overall <= 2 ? 'Lower-complexity entry' : overall === 3 ? 'Moderate build' : overall === 4 ? 'Complex build' : 'Frontier-scale build';
    return { overall, label, dimensions: Object.fromEntries(dimensions.map((key, index) => [key, values[index]])) };
  }

  function trendNarrative(reductionPct) {
    const reduction = finiteNumber(reductionPct);
    if (reduction === null) return 'Baseline change unavailable';
    const growth = -reduction;
    const magnitude = Math.abs(growth) * 100;
    if (magnitude < 0.05) return 'Import value was broadly flat versus baseline';
    if (growth >= 10) return 'Import value rose more than 10× versus baseline';
    return growth > 0 ? `Import value rose ${magnitude.toFixed(1)}% versus baseline` : `Import value fell ${magnitude.toFixed(1)}% versus baseline`;
  }

  function engagementRoute(product, profileInput, routesInput) {
    const profile = sanitizeProfile(profileInput);
    const classification = classifyOpportunity(product);
    const selected = chooseRoute(product, routesInput, profile, classification).route;
    if (profile.role === 'back') return `Find an operator capable of this route: ${selected.wedge}`;
    if (profile.role === 'partner') return `Form a customer- or supplier-backed venture around this route: ${selected.wedge}`;
    return selected.wedge;
  }

  function deriveOpportunity(product, profileInput, contextInput, routesInput) {
    const profile = sanitizeProfile(profileInput);
    const context = contextInput || createMarketContext([product]);
    const importValue = finiteNumber(product && product.last_12m_value_usd);
    const reduction = finiteNumber(product && product.reduction_pct);
    const market = marketPercentile(importValue, context);
    const momentum = importValueMomentum(reduction);
    const countrySignal = momentum === null ? market : (market * 0.7) + (momentum * 0.3);
    const classification = classifyOpportunity(product);
    const selection = chooseRoute(product, routesInput, profile, classification);
    const route = selection.route;
    const personal = scoreRoute(route, product, profile);
    const verdict = fitVerdict(route, classification, profile, personal);
    const tierRank = FIT_TIER_RANK[verdict.id] || 0;
    let combinedFit = (countrySignal * 0.35) + (personal.total * 0.65);
    if (verdict.id === 'research') combinedFit = Math.min(combinedFit, 49);
    if (verdict.id === 'blocked') combinedFit = 0;
    const minimumMonths = finiteNumber(route.months_min);
    const speedScore = minimumMonths === null ? -1 : clamp(110 - (minimumMonths * 5) + (personal.total * 0.2), 0, 100);
    const directBonus = route.impact_type === 'direct' ? 8 : route.impact_type === 'enabling' ? 2 : -12;
    let wedgeScore = clamp((countrySignal * 0.38) + (personal.total * 0.52) + directBonus, 0, 100);
    if (verdict.id === 'blocked') wedgeScore = 0;
    const scenario = importValue === null ? null : clamp(importValue * (profile.capturePct / 100), 0, Math.max(importValue, 0));
    const sectors = sectorGroups(product);
    const reasons = [];
    if (verdict.id === 'actionable') reasons.push(`Fits a modeled ${capitalRangeLabel(route)} entry envelope`);
    else reasons.push(verdict.label);
    if (minimumMonths !== null) reasons.push(`First paid proof modeled in ${monthRangeLabel(route)}`);
    const matchedStrength = profile.strengths.find((strength) => (route.required_strengths || []).includes(strength));
    if (matchedStrength) reasons.push(`Uses your ${strengthLabel(matchedStrength)} edge`);
    if (reasons.length < 3 && market >= 80) reasons.push(`Top ${Math.max(1, Math.ceil(100 - market))}% of mapped import exposure`);
    if (reasons.length < 3 && momentum !== null && momentum >= 58) reasons.push(trendNarrative(reduction));

    return {
      product,
      sectors,
      importValue,
      reductionPct: reduction,
      marketPercentile: market,
      momentum,
      countrySignal: clamp(countrySignal, 0, 100),
      mandateMatch: personal.total,
      personalFit: personal.total,
      fitTrace: personal.trace,
      combinedFit: clamp(combinedFit, 0, 100),
      risingScore: speedScore,
      wedgeScore,
      fitTierRank: tierRank,
      fitVerdict: verdict,
      classification,
      route,
      alternatives: selection.alternatives,
      routeText: engagementRoute(product, profile, routesInput),
      scenarioValue: scenario,
      reasons: reasons.slice(0, 3),
      evidence: { demand: importValue !== null, trend: reduction !== null, supplier: false, capex: false },
      isStructural: classification.id === 'structural',
      shortTitle: route.business_name || productShortTitle(product && product.title, 88, product && product.hs_code),
      customsTitle: productShortTitle(product && product.title, 130),
      complexity: complexitySummary(route),
      projectCapitalInr: projectCapital(profile)
    };
  }

  function scoreForLens(opportunity, lens) {
    if (lens === 'national') return opportunity.countrySignal;
    if (lens === 'rising') return opportunity.risingScore;
    if (lens === 'wedge') return opportunity.wedgeScore;
    return opportunity.combinedFit;
  }

  function rankOpportunities(products, profileInput, lensInput, contextInput, routesInput) {
    const profile = sanitizeProfile(profileInput);
    const lens = VALID_LENSES.has(lensInput) ? lensInput : 'fit';
    const context = contextInput || createMarketContext(products);
    return (Array.isArray(products) ? products : [])
      .map((product) => deriveOpportunity(product, profile, context, routesInput))
      .sort((left, right) => {
        if (lens === 'fit' && right.fitTierRank !== left.fitTierRank) return right.fitTierRank - left.fitTierRank;
        if (lens === 'rising') {
          const leftMissing = left.risingScore < 0;
          const rightMissing = right.risingScore < 0;
          if (leftMissing !== rightMissing) return leftMissing ? 1 : -1;
        }
        if (lens === 'wedge' && right.fitTierRank !== left.fitTierRank) return right.fitTierRank - left.fitTierRank;
        const scoreDifference = scoreForLens(right, lens) - scoreForLens(left, lens);
        if (Math.abs(scoreDifference) > 1e-9) return scoreDifference;
        const valueDifference = (right.importValue || 0) - (left.importValue || 0);
        if (valueDifference !== 0) return valueDifference;
        return String(left.product && left.product.hs_code || '').localeCompare(String(right.product && right.product.hs_code || ''));
      });
  }

  function scenarioValue(importValueInput, capturePctInput) {
    const importValue = finiteNumber(importValueInput);
    const capturePct = finiteNumber(capturePctInput);
    if (importValue === null || capturePct === null) return null;
    return clamp(importValue * (clamp(capturePct, 0, 100) / 100), 0, Math.max(importValue, 0));
  }

  function capitalRangeLabel(route) {
    const minimum = finiteNumber(route && route.capital_min_inr);
    const maximum = finiteNumber(route && route.capital_max_inr);
    if (minimum === null && maximum === null) return 'unverified capital';
    if (minimum === null) return `up to ${compactInr(maximum)}`;
    if (maximum === null) return `${compactInr(minimum)}+`;
    return `${compactInr(minimum)}–${compactInr(maximum)}`;
  }

  function monthRangeLabel(route) {
    const minimum = finiteNumber(route && route.months_min);
    const maximum = finiteNumber(route && route.months_max);
    if (minimum === null && maximum === null) return 'an unverified timeframe';
    if (minimum === null) return `up to ${maximum} months`;
    if (maximum === null || minimum === maximum) return `${minimum} months`;
    return `${minimum}–${maximum} months`;
  }

  function compactInr(value) {
    const number = finiteNumber(value);
    if (number === null) return 'unavailable';
    if (number >= 10_000_000) return `₹${trimNumber(number / 10_000_000)}Cr`;
    if (number >= 100_000) return `₹${trimNumber(number / 100_000)}L`;
    return `₹${Math.round(number).toLocaleString('en-IN')}`;
  }

  function trimNumber(value) {
    return Number(value).toFixed(2).replace(/\.00$/, '').replace(/(\.\d)0$/, '$1');
  }

  function strengthLabel(value) {
    return {
      buyer_access: 'buyer access',
      engineering: 'engineering',
      operations: 'operations',
      distribution: 'distribution',
      sourcing: 'sourcing',
      quality: 'quality and compliance',
      workshop: 'workshop',
      land_power: 'industrial infrastructure'
    }[value] || String(value || '').replaceAll('_', ' ');
  }

  function spreadsheetSafeText(value) {
    const text = value == null ? '' : String(value);
    return typeof value === 'string' && /^\s*[=+\-@\t\r]/.test(text) ? `'${text}` : text;
  }

  return {
    DEFAULT_PROFILE,
    sanitizeProfile,
    profileCompleteness,
    projectCapital,
    finiteNumber,
    clamp,
    createMarketContext,
    marketPercentile,
    importValueMomentum,
    isStructuralExposure,
    sectorGroups,
    classifyOpportunity,
    capitalBandLabel,
    engagementRoute,
    productShortTitle,
    trendNarrative,
    normalizeRoutes,
    genericResearchRoute,
    complexitySummary,
    capitalRangeLabel,
    monthRangeLabel,
    strengthLabel,
    deriveOpportunity,
    rankOpportunities,
    scoreForLens,
    scenarioValue,
    spreadsheetSafeText
  };
});
