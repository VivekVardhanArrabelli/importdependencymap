(function exposeOpportunityModel(root, factory) {
  const model = factory();
  if (typeof module === 'object' && module.exports) module.exports = model;
  if (root) root.OpportunityModel = model;
})(typeof globalThis !== 'undefined' ? globalThis : this, function createOpportunityModel() {
  'use strict';

  const DEFAULT_PROFILE = Object.freeze({
    capitalInr: 50_000_000,
    role: 'build',
    horizonMonths: 24,
    risk: 'balanced',
    sectors: [],
    countryWeight: 60,
    capturePct: 1
  });

  const VALID_ROLES = new Set(['build', 'back', 'partner']);
  const VALID_RISKS = new Set(['resilient', 'balanced', 'frontier']);
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

  function finiteNumber(value) {
    if (value == null || value === '') return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function clamp(value, minimum, maximum) {
    return Math.min(Math.max(value, minimum), maximum);
  }

  function sanitizeProfile(input) {
    const profile = input && typeof input === 'object' ? input : {};
    const capital = finiteNumber(profile.capitalInr);
    const horizon = finiteNumber(profile.horizonMonths);
    const countryWeight = finiteNumber(profile.countryWeight);
    const capturePct = finiteNumber(profile.capturePct);
    const sectors = Array.isArray(profile.sectors)
      ? [...new Set(profile.sectors.filter((sector) => VALID_SECTORS.has(sector)))]
      : [];

    return {
      capitalInr: capital !== null && capital > 0 ? capital : DEFAULT_PROFILE.capitalInr,
      role: VALID_ROLES.has(profile.role) ? profile.role : DEFAULT_PROFILE.role,
      horizonMonths: [12, 24, 36].includes(horizon) ? horizon : DEFAULT_PROFILE.horizonMonths,
      risk: VALID_RISKS.has(profile.risk) ? profile.risk : DEFAULT_PROFILE.risk,
      sectors,
      countryWeight: countryWeight === null
        ? DEFAULT_PROFILE.countryWeight
        : clamp(countryWeight, 0, 100),
      capturePct: capturePct === null
        ? DEFAULT_PROFILE.capturePct
        : clamp(capturePct, 0.1, 5)
    };
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
    const sorted = context && Array.isArray(context.positiveValues)
      ? context.positiveValues
      : [];
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
    else if (chapter === 71) {
      groups.push(/\b(jewellery|goldsmiths' wares|silversmiths' wares|ornament)\b/i.test(title)
        ? 'consumer'
        : 'metals');
    }
    else if (chapter >= 72 && chapter <= 83) groups.push('metals');
    else if (chapter === 84) {
      if (code.startsWith('8471')) groups.push('electronics');
      else {
        groups.push('machinery');
        if (code.startsWith('8486')) groups.push('electronics');
      }
    }
    else if (chapter === 85) groups.push('electronics');
    else if (chapter >= 86 && chapter <= 89) groups.push('mobility');
    else if (chapter >= 90 && chapter <= 92) groups.push('instruments');
    else if (chapter === 93) groups.push('controlled');
    else if (chapter >= 94 && chapter <= 99) groups.push('consumer');

    if (
      /\b(batter(?:y|ies)|accumulators?|solar|photovoltaic|wind turbine|power generation)\b/i.test(title)
      && !groups.includes('energy')
    ) {
      groups.push('energy');
    }

    if (!groups.length) {
      const legacyMap = {
        industrial: 'machinery',
        automotive: 'mobility'
      };
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
    if (isStructuralExposure(product)) {
      return {
        id: 'structural',
        label: 'Structural exposure',
        complexity: 5,
        description: 'National-scale import exposure; usually a consortium or policy-led challenge.'
      };
    }
    if (chapter === 93) {
      return {
        id: 'controlled',
        label: 'Controlled-sector gate',
        complexity: 5,
        description: 'Licensing, end-use controls, procurement rules, and regulatory eligibility must be cleared first.'
      };
    }
    if (chapter === 88 || chapter === 89) {
      return {
        id: 'strategic_asset',
        label: 'Strategic asset gate',
        complexity: 5,
        description: 'Aircraft and vessel programs require licensed operators, long qualification cycles, and strategic-scale capital.'
      };
    }
    if (chapter >= 97) {
      return {
        id: 'trade_signal',
        label: 'Formation gate required',
        complexity: 3,
        description: 'Import demand is visible, but a repeatable domestic company-formation path is not established.'
      };
    }
    if (chapter >= 1 && chapter <= 14) {
      return {
        id: 'primary',
        label: 'Primary supply',
        complexity: 2,
        description: 'Domestic production, aggregation, quality, seasonality, and offtake need validation.'
      };
    }
    if (chapter >= 15 && chapter <= 24) {
      return {
        id: 'process',
        label: 'Food processing',
        complexity: 2,
        description: 'A focused processing, sourcing, or regional supply-chain wedge may be testable.'
      };
    }
    if (chapter >= 25 && chapter <= 26) {
      return {
        id: 'resource',
        label: 'Resource development',
        complexity: 4,
        description: 'Feedstock, permitting, beneficiation, infrastructure, and offtake are likely gating factors.'
      };
    }
    if (
      FRONTIER_PATTERNS.some((pattern) => pattern.test(positiveSignals))
      || sectors.includes('instruments')
    ) {
      return {
        id: 'frontier',
        label: 'Frontier capability',
        complexity: 5,
        description: 'Deep technical validation, certification, and patient capital are likely.'
      };
    }
    if (
      SCALE_PATTERNS.some((pattern) => pattern.test(positiveSignals))
      || sectors.includes('mobility')
      || sectors.includes('energy')
      || sectors.includes('machinery')
      || sectors.includes('metals')
      || sectors.includes('chemicals')
    ) {
      return {
        id: 'scale',
        label: 'Scale manufacturing',
        complexity: 4,
        description: 'A production system, anchor demand, and supplier qualification are likely.'
      };
    }
    if (PROCESS_PATTERNS.some((pattern) => pattern.test(positiveSignals))) {
      return {
        id: 'process',
        label: 'Processing opportunity',
        complexity: 2,
        description: 'A focused process, regional supply chain, or specialist line may be testable.'
      };
    }
    return {
      id: 'specialist',
      label: 'Specialist manufacturing',
      complexity: 3,
      description: 'A narrow production wedge can be investigated before a full-scale facility.'
    };
  }

  function capitalBandLabel(capitalInr) {
    const capital = finiteNumber(capitalInr) || DEFAULT_PROFILE.capitalInr;
    if (capital < 5_000_000) return '₹25–50 lakh mandate';
    if (capital < 25_000_000) return '₹50 lakh–₹2.5 crore mandate';
    if (capital < 100_000_000) return '₹2.5–10 crore mandate';
    if (capital < 500_000_000) return '₹10–50 crore mandate';
    return '₹50 crore+ mandate';
  }

  function engagementRoute(product, profileInput) {
    const profile = sanitizeProfile(profileInput);
    const classification = classifyOpportunity(product);
    const capital = profile.capitalInr;
    let action;

    if (capital < 5_000_000) {
      action = 'validate demand through samples, tooling, services, or a narrow assembly wedge';
    } else if (capital < 25_000_000) {
      action = 'fund customer discovery, certification, and a pilot production cell';
    } else if (capital < 100_000_000) {
      action = 'validate a specialist production line with an anchor customer';
    } else if (capital < 500_000_000) {
      action = 'structure a commercial facility or operator-led joint venture';
    } else {
      action = 'structure a platform-scale plant, joint venture, or supplier cluster';
    }

    if (classification.id === 'structural') {
      action = 'approach through a consortium, strategic offtake, or policy-led program';
    } else if (classification.id === 'controlled') {
      action = 'first clear licensing, end-use controls, procurement eligibility, and a compliant operator pathway';
    } else if (classification.id === 'strategic_asset') {
      action = 'work through a licensed OEM, strategic consortium, or component-level program before considering a platform build';
    } else if (classification.id === 'trade_signal') {
      action = 'first prove that this trade category represents a repeatable domestic capability gap';
    } else if (classification.id === 'resource') {
      action = 'validate feedstock, beneficiation, permitting, infrastructure, and offtake with a domain operator';
    } else if (classification.id === 'primary') {
      action = capital < 25_000_000
        ? 'validate growers or producers, quality, aggregation, and offtake through an asset-light pilot'
        : 'structure production, aggregation, quality systems, and anchor offtake before scaling assets';
    }

    if (profile.role === 'back') {
      return `Find a technically credible operator, then use the mandate to ${action}.`;
    }
    if (profile.role === 'partner') {
      return `Form a customer-backed venture to ${action}.`;
    }
    return `${action.charAt(0).toUpperCase()}${action.slice(1)}.`;
  }

  function sectorAlignment(product, profile) {
    if (!profile.sectors.length) return 65;
    const productSectors = sectorGroups(product);
    return productSectors.some((sector) => profile.sectors.includes(sector)) ? 100 : 15;
  }

  function executionAlignment(classification, profile) {
    const horizonTarget = profile.horizonMonths === 12
      ? 2
      : profile.horizonMonths === 24
        ? 3.2
        : 4.3;
    const riskTarget = profile.risk === 'resilient'
      ? 2
      : profile.risk === 'balanced'
        ? 3.2
        : 4.6;
    const roleTarget = profile.role === 'build' ? 3 : profile.role === 'back' ? 3.5 : 4.2;
    const distance = (
      Math.abs(classification.complexity - horizonTarget) * 11
      + Math.abs(classification.complexity - riskTarget) * 9
      + Math.abs(classification.complexity - roleTarget) * 7
    );
    return clamp(100 - distance, 0, 100);
  }

  function productShortTitle(value, maximumLength) {
    const limit = finiteNumber(maximumLength) || 88;
    const source = String(value || 'Untitled opportunity').replace(/\s+/g, ' ').trim();
    const semicolonParts = source.split(';').map((part) => part.trim()).filter(Boolean);
    let title = semicolonParts.length > 1
      ? `${semicolonParts[0]} — ${semicolonParts[1]}`
      : source;
    title = title.replace(/,\s*whether or not.*$/i, '').replace(/,\s*not elsewhere specified.*$/i, '');
    if (title.length <= limit) return title;
    return `${title.slice(0, Math.max(1, limit - 1)).trimEnd()}…`;
  }

  function trendNarrative(reductionPct) {
    const reduction = finiteNumber(reductionPct);
    if (reduction === null) return 'Baseline change unavailable';
    const growth = -reduction;
    const magnitude = Math.abs(growth) * 100;
    if (magnitude < 0.05) return 'Import value was broadly flat versus baseline';
    if (growth >= 10) return 'Import value rose more than 10× versus baseline';
    return growth > 0
      ? `Import value rose ${magnitude.toFixed(1)}% versus baseline`
      : `Import value fell ${magnitude.toFixed(1)}% versus baseline`;
  }

  function deriveOpportunity(product, profileInput, contextInput) {
    const profile = sanitizeProfile(profileInput);
    const context = contextInput || createMarketContext([product]);
    const importValue = finiteNumber(product && product.last_12m_value_usd);
    const reduction = finiteNumber(product && product.reduction_pct);
    const market = marketPercentile(importValue, context);
    const momentum = importValueMomentum(reduction);
    const countrySignal = momentum === null
      ? market
      : (market * 0.7) + (momentum * 0.3);
    const classification = classifyOpportunity(product);
    const sectorMatch = sectorAlignment(product, profile);
    const executionMatch = executionAlignment(classification, profile);
    const mandateMatch = (sectorMatch * 0.55) + (executionMatch * 0.45);
    const countryShare = profile.countryWeight / 100;
    const formationPenalty = classification.id === 'structural'
      ? (profile.role === 'partner' ? 12 : 26)
      : classification.id === 'controlled'
        ? 100
        : classification.id === 'strategic_asset'
          ? (profile.role === 'partner' ? 22 : 48)
      : classification.id === 'trade_signal'
        ? 35
        : 0;
    const combinedFit = clamp(
      (countrySignal * countryShare)
      + (mandateMatch * (1 - countryShare))
      - formationPenalty,
      0,
      100
    );
    const risingScore = momentum === null
      ? -1
      : clamp((market * 0.45) + (momentum * 0.55), 0, 100);
    const wedgeBase = momentum === null
      ? ((mandateMatch * 0.58) + (market * 0.2)) / 0.78
      : (mandateMatch * 0.58) + (market * 0.2) + (momentum * 0.22);
    const wedgePenalty = classification.id === 'structural'
      ? 32
      : classification.id === 'controlled'
        ? 100
        : classification.id === 'strategic_asset'
          ? 62
      : classification.id === 'trade_signal'
        ? 48
        : 0;
    const wedgeScore = clamp(wedgeBase - wedgePenalty, 0, 100);
    const scenario = importValue === null
      ? null
      : clamp(importValue * (profile.capturePct / 100), 0, Math.max(importValue, 0));
    const productSectors = sectorGroups(product);
    const selectedSectorMatch = profile.sectors.find((sector) => productSectors.includes(sector));
    const reasons = [];

    if (classification.id === 'structural') {
      reasons.push('National-scale exposure, not a typical startup wedge');
    } else if (classification.id === 'controlled') {
      reasons.push('Controlled sector; licensing and eligibility come first');
    } else if (classification.id === 'strategic_asset') {
      reasons.push('Strategic asset; licensed operator or component wedge required');
    } else if (classification.id === 'trade_signal') {
      reasons.push('Import signal; company-formation path unverified');
    } else if (classification.id === 'primary') {
      reasons.push('Primary production and supply-chain opportunity');
    } else if (classification.id === 'resource') {
      reasons.push('Resource and processing opportunity');
    } else if (market >= 80) {
      const topShare = Math.max(1, Math.ceil(100 - market));
      reasons.push(`Top ${topShare}% of mapped import exposure`);
    }
    if (momentum !== null && momentum >= 58) {
      reasons.push(trendNarrative(reduction));
    }
    if (selectedSectorMatch) reasons.push(`Matches your ${selectedSectorMatch} edge`);
    if (!reasons.length) reasons.push(`${classification.label} aligned to your mandate`);
    if (reasons.length < 2) reasons.push(`Screened for a ${profile.horizonMonths}-month operating milestone`);

    return {
      product,
      sectors: productSectors,
      importValue,
      reductionPct: reduction,
      marketPercentile: market,
      momentum,
      countrySignal: clamp(countrySignal, 0, 100),
      mandateMatch: clamp(mandateMatch, 0, 100),
      combinedFit,
      risingScore,
      wedgeScore,
      classification,
      route: engagementRoute(product, profile),
      scenarioValue: scenario,
      reasons: reasons.slice(0, 2),
      evidence: {
        demand: importValue !== null,
        trend: reduction !== null,
        supplier: false,
        capex: false
      },
      isStructural: classification.id === 'structural',
      shortTitle: productShortTitle(product && product.title)
    };
  }

  function scoreForLens(opportunity, lens) {
    if (lens === 'national') return opportunity.countrySignal;
    if (lens === 'rising') return opportunity.risingScore;
    if (lens === 'wedge') return opportunity.wedgeScore;
    return opportunity.combinedFit;
  }

  function rankOpportunities(products, profileInput, lensInput, contextInput) {
    const profile = sanitizeProfile(profileInput);
    const lens = VALID_LENSES.has(lensInput) ? lensInput : 'fit';
    const context = contextInput || createMarketContext(products);
    return (Array.isArray(products) ? products : [])
      .map((product) => deriveOpportunity(product, profile, context))
      .sort((left, right) => {
        const scoreDifference = scoreForLens(right, lens) - scoreForLens(left, lens);
        if (Math.abs(scoreDifference) > 1e-9) return scoreDifference;
        const valueDifference = (right.importValue || 0) - (left.importValue || 0);
        if (valueDifference !== 0) return valueDifference;
        return String(left.product && left.product.hs_code || '')
          .localeCompare(String(right.product && right.product.hs_code || ''));
      });
  }

  function scenarioValue(importValueInput, capturePctInput) {
    const importValue = finiteNumber(importValueInput);
    const capturePct = finiteNumber(capturePctInput);
    if (importValue === null || capturePct === null) return null;
    return clamp(importValue * (clamp(capturePct, 0, 100) / 100), 0, Math.max(importValue, 0));
  }

  function spreadsheetSafeText(value) {
    const text = value == null ? '' : String(value);
    return typeof value === 'string' && /^\s*[=+\-@\t\r]/.test(text)
      ? `'${text}`
      : text;
  }

  return {
    DEFAULT_PROFILE,
    sanitizeProfile,
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
    deriveOpportunity,
    rankOpportunities,
    scoreForLens,
    scenarioValue,
    spreadsheetSafeText
  };
});
