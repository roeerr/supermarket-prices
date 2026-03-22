const express = require('express');
const path = require('path');
const axios = require('axios');
const { fetchAllChains, buildRefreshPlan, CHAINS, fetchFromUrl } = require('./fetcher');
const { loadCachedStore, saveChainCache, saveRefreshResults } = require('./chain-cache');
const { getProductComparisonKey, dedupeProducts } = require('./product-utils');
const { hasBrokenText, normalizeDisplayText, normalizeOptionalText } = require('./text-normalizer');

const app = express();
const PORT = process.env.PORT || 3000;

if (process.platform === 'win32') {
  try { process.stdout.setDefaultEncoding('utf8'); } catch (error) {}
  try { process.stderr.setDefaultEncoding('utf8'); } catch (error) {}
}

app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

let store = { products: [], lastFetch: null, chainStats: {} };
let isFetching = false;

for (const chain of CHAINS) {
  if (chain.name) chain.name = normalizeDisplayText(chain.name);
  if (chain.storeName) chain.storeName = normalizeDisplayText(chain.storeName);
  if (chain.reason) chain.reason = normalizeDisplayText(chain.reason);
}

const chainMetaById = new Map(CHAINS.map(chain => [chain.id, chain]));
const PRODUCT_IMAGE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const PRODUCT_IMAGE_NEGATIVE_TTL_MS = 30 * 60 * 1000;
const productImageCache = new Map();
const marketValueCache = { signature: '', groups: null };
const inferredCompareCache = { signature: '', byProductId: new Map() };

function loadFromDisk() {
  try {
    const cached = loadCachedStore(CHAINS);
    store.products = (cached.products || []).map(normalizeStoredProduct);
    store.lastFetch = cached.lastFetch || null;
    store.chainStats = cached.chainStats || {};
    console.log(`[CACHE] Loaded ${store.products.length} products`);
  } catch (error) {
    console.error('[CACHE] Cache load error:', error.message);
  }
}

function getCanonicalChainName(chainId, fallback = '') {
  return normalizeDisplayText(chainMetaById.get(chainId)?.name || fallback || chainId || '');
}

function getCanonicalStoreName(product) {
  const meta = chainMetaById.get(product.chain);
  const fallback = normalizeDisplayText(meta?.storeName || meta?.name || '');
  if (!product.storeName || normalizeDisplayText(product.storeName) === normalizeDisplayText(product.chainName) || hasBrokenText(product.storeName)) {
    return fallback;
  }
  return normalizeDisplayText(product.storeName);
}

function normalizeStoredProduct(product) {
  if (!product || typeof product !== 'object') return product;
  return {
    ...product,
    name: normalizeDisplayText(product.name),
    unitQty: normalizeOptionalText(product.unitQty),
    manufacturer: normalizeOptionalText(product.manufacturer),
    country: normalizeOptionalText(product.country),
    chainName: getCanonicalChainName(product.chain, product.chainName),
    storeName: getCanonicalStoreName(product),
  };
}

function normalizeMatchText(value = '') {
  return normalizeDisplayText(value)
    .toLowerCase()
    .replace(/[\"'`\u05f3\u05f4,.;:!?()[\]{}\/\\+-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeMatchName(value = '') {
  return normalizeMatchText(value)
    .replace(/\b\u05db\u05e9\u05dc[\"\u201d]?\u05e4\b/g, ' ')
    .replace(/\b\u05db\u05e9\u05e8\s+\u05dc\u05e4\u05e1\u05d7\b/g, ' ')
    .replace(/\bחדש\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeMatchName(value = '') {
  return [...new Set(normalizeMatchName(value).split(' ').filter(token => token && token.length > 1))];
}

function getUniqueBarcode(candidates = []) {
  const barcodes = [...new Set(candidates.map(item => String(item.barcode || '').trim()).filter(Boolean))];
  return barcodes.length === 1 ? barcodes[0] : '';
}

function getProductCompareResolution(product) {
  const normalized = normalizeStoredProduct(product);
  const directBarcode = String(normalized.barcode || '').trim();
  if (/^\d{4,14}$/.test(directBarcode)) {
    return {
      compareBarcode: directBarcode,
      compareConfidence: 'barcode',
      compareSource: 'barcode',
      compareLabel: '',
      matchedName: normalized.name,
      matchedChain: normalized.chain,
    };
  }

  const signature = getMarketValueSignature();
  if (inferredCompareCache.signature !== signature || !(inferredCompareCache.byProductId instanceof Map)) {
    inferredCompareCache.signature = signature;
    inferredCompareCache.byProductId = buildInferredCompareIndex();
  }

  return inferredCompareCache.byProductId.get(normalized.id) || {
    compareBarcode: '',
    compareConfidence: '',
    compareSource: '',
    compareLabel: '',
    matchedName: '',
    matchedChain: '',
  };
}

function getMarketComparisonKey(product) {
  const normalized = normalizeStoredProduct(product);
  const compare = getProductCompareResolution(normalized);
  const compareBarcode = String(compare.compareBarcode || '').trim();
  if (/^\d{4,14}$/.test(compareBarcode)) return `barcode:${compareBarcode}`;
  return getProductComparisonKey(normalized);
}

function buildInferredCompareIndex() {
  const sourceProducts = store.products.map(normalizeStoredProduct);
  const withBarcode = sourceProducts
    .filter(product => /^\d{4,14}$/.test(String(product.barcode || '').trim()))
    .map(product => ({
      product,
      barcode: String(product.barcode || '').trim(),
      nameKey: normalizeMatchName(product.name),
      brandKey: normalizeMatchText(product.manufacturer),
      tokens: tokenizeMatchName(product.name),
    }));

  const byNameBrand = new Map();
  const byName = new Map();
  const byBrand = new Map();
  const byToken = new Map();

  for (const entry of withBarcode) {
    const nameBrandKey = `${entry.nameKey}||${entry.brandKey}`;
    if (!byNameBrand.has(nameBrandKey)) byNameBrand.set(nameBrandKey, []);
    byNameBrand.get(nameBrandKey).push(entry);

    if (!byName.has(entry.nameKey)) byName.set(entry.nameKey, []);
    byName.get(entry.nameKey).push(entry);

    if (entry.brandKey) {
      if (!byBrand.has(entry.brandKey)) byBrand.set(entry.brandKey, []);
      byBrand.get(entry.brandKey).push(entry);
    }

    for (const token of entry.tokens) {
      if (!byToken.has(token)) byToken.set(token, []);
      byToken.get(token).push(entry);
    }
  }

  const result = new Map();

  for (const product of sourceProducts) {
    const barcode = String(product.barcode || '').trim();
    if (/^\d{4,14}$/.test(barcode)) continue;

    const nameKey = normalizeMatchName(product.name);
    const brandKey = normalizeMatchText(product.manufacturer);
    if (!nameKey) continue;

    const exactNameBrand = byNameBrand.get(`${nameKey}||${brandKey}`) || [];
    const exactNameBrandBarcode = getUniqueBarcode(exactNameBrand);
    if (exactNameBrandBarcode) {
      const match = exactNameBrand[0].product;
      result.set(product.id, {
        compareBarcode: exactNameBrandBarcode,
        compareConfidence: 'high',
        compareSource: 'exact_name_brand',
        compareLabel: 'התאמה חכמה גבוהה',
        matchedName: match.name,
        matchedChain: match.chain,
      });
      continue;
    }

    const exactName = byName.get(nameKey) || [];
    const exactNameBarcode = getUniqueBarcode(exactName);
    if (exactNameBarcode) {
      const match = exactName[0].product;
      const allBrandsMissing = exactName.every(item => !item.brandKey);
      if (!brandKey || allBrandsMissing) {
        result.set(product.id, {
          compareBarcode: exactNameBarcode,
          compareConfidence: 'medium',
          compareSource: 'exact_name',
          compareLabel: 'התאמה חכמה',
          matchedName: match.name,
          matchedChain: match.chain,
        });
        continue;
      }
    }

    const tokens = tokenizeMatchName(product.name);
    const candidateMap = new Map();
    const preferredPool = brandKey && byBrand.has(brandKey) ? byBrand.get(brandKey) : [];
    const candidates = preferredPool.length
      ? preferredPool
      : tokens.flatMap(token => byToken.get(token) || []);

    for (const candidate of candidates) {
      candidateMap.set(`${candidate.barcode}|${candidate.product.chain}|${candidate.product.id}`, candidate);
    }

    let best = null;
    let bestScore = 0;
    let secondBest = 0;
    const tokenSet = new Set(tokens);
    for (const candidate of candidateMap.values()) {
      if (!candidate.tokens.length || !tokenSet.size) continue;
      let intersection = 0;
      for (const token of candidate.tokens) if (tokenSet.has(token)) intersection++;
      const union = new Set([...tokens, ...candidate.tokens]).size;
      const score = union ? intersection / union : 0;
      if (score > bestScore) {
        secondBest = bestScore;
        bestScore = score;
        best = candidate;
      } else if (score > secondBest) {
        secondBest = score;
      }
    }

    if (best && bestScore >= 0.78 && (bestScore - secondBest) >= 0.18 && (!brandKey || best.brandKey === brandKey)) {
      result.set(product.id, {
        compareBarcode: best.barcode,
        compareConfidence: 'medium',
        compareSource: 'fuzzy',
        compareLabel: 'התאמה חכמה',
        matchedName: best.product.name,
        matchedChain: best.product.chain,
      });
    }
  }

  return result;
}

function toApiProduct(product) {
  const normalized = normalizeStoredProduct(product);
  const compare = getProductCompareResolution(normalized);
  return {
    id: normalized.id,
    barcode: normalized.barcode,
    name: normalized.name,
    price: normalized.price,
    unit_price: normalized.unitPrice,
    unit_qty: normalized.unitQty,
    manufacturer: normalized.manufacturer,
    country: normalized.country,
    chain: normalized.chain,
    chain_name: normalized.chainName,
    store_id: normalized.storeId,
    store_name: normalized.storeName,
    compare_barcode: compare.compareBarcode,
    compare_confidence: compare.compareConfidence,
    compare_source: compare.compareSource,
    compare_label: compare.compareLabel,
    compare_match_name: compare.matchedName,
    compare_match_chain: compare.matchedChain,
  };
}

function dedupeCatalogProducts(products) {
  return dedupeProducts(products, { scope: 'global' }).products;
}

function dedupeCompareProducts(products) {
  return dedupeProducts(products, { scope: 'chain' }).products;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function getMarketValueSignature() {
  return `${store.lastFetch || 0}:${store.products.length}`;
}

function getMedianFromSortedPrices(prices) {
  if (!Array.isArray(prices) || prices.length === 0) return 0;
  const mid = Math.floor(prices.length / 2);
  return prices.length % 2 === 1
    ? prices[mid]
    : (prices[mid - 1] + prices[mid]) / 2;
}

function getMarketValueIndex() {
  const signature = getMarketValueSignature();
  if (marketValueCache.signature === signature && marketValueCache.groups) {
    return marketValueCache.groups;
  }

  const groups = new Map();
  for (const rawProduct of store.products) {
    const product = normalizeStoredProduct(rawProduct);
    const key = getMarketComparisonKey(product);
    const price = Number(product.price || 0);
    if (!key || !Number.isFinite(price) || price <= 0) continue;

    let group = groups.get(key);
    if (!group) {
      group = {
        key,
        barcode: String(product.barcode || '').trim(),
        name: product.name || '',
        manufacturer: product.manufacturer || '',
        chainPrices: new Map(),
      };
      groups.set(key, group);
    }

    const current = group.chainPrices.get(product.chain);
    if (!current || price < current.price) {
      group.chainPrices.set(product.chain, {
        price,
        storeName: product.storeName || '',
        storeId: product.storeId || '',
      });
    }
  }

  for (const group of groups.values()) {
    const prices = [...group.chainPrices.values()]
      .map(entry => entry.price)
      .filter(price => Number.isFinite(price) && price > 0)
      .sort((a, b) => a - b);

    group.sortedPrices = prices;
    group.chainCount = prices.length;
    group.marketMinPrice = prices[0] || 0;
    group.marketMaxPrice = prices.length ? prices[prices.length - 1] : 0;
    group.marketAvgPrice = prices.length
      ? prices.reduce((sum, price) => sum + price, 0) / prices.length
      : 0;
    group.marketMedianPrice = getMedianFromSortedPrices(prices);
  }

  marketValueCache.signature = signature;
  marketValueCache.groups = groups;
  return groups;
}

function pickBestChainProducts(products) {
  const bestByKey = new Map();
  for (const rawProduct of Array.isArray(products) ? products : []) {
    const product = normalizeStoredProduct(rawProduct);
    const key = getMarketComparisonKey(product);
    const price = Number(product.price || 0);
    if (!key || !Number.isFinite(price) || price <= 0) continue;

    const current = bestByKey.get(key);
    if (
      !current ||
      price < current.price ||
      (price === current.price && String(product.storeName || '') < String(current.storeName || ''))
    ) {
      bestByKey.set(key, product);
    }
  }

  return [...bestByKey.values()];
}

function describeValueBucket(score, diffToBestPct, diffToMedianPct) {
  if (score >= 90) {
    return {
      label: 'מציאה',
      tone: 'best',
      reason: diffToBestPct <= 1
        ? 'מהזולים בשוק'
        : 'כמעט הכי זול בשוק',
    };
  }

  if (score >= 75) {
    return {
      label: 'כדאי',
      tone: 'good',
      reason: diffToMedianPct < 0
        ? `זול בכ-${Math.abs(diffToMedianPct).toFixed(0)}% מהחציון`
        : 'תחרותי מול המתחרים',
    };
  }

  if (score >= 60) {
    return {
      label: 'סביר',
      tone: 'fair',
      reason: 'קרוב למחיר השוק',
    };
  }

  if (score >= 40) {
    return {
      label: 'יקר',
      tone: 'bad',
      reason: diffToMedianPct > 0
        ? `יקר בכ-${Math.abs(diffToMedianPct).toFixed(0)}% מהחציון`
        : 'יש חלופות טובות יותר',
    };
  }

  return {
    label: 'לא כדאי',
    tone: 'worst',
    reason: diffToBestPct > 0
      ? `יקר בכ-${Math.abs(diffToBestPct).toFixed(0)}% מהמחיר הטוב בשוק`
      : 'נמוך בכיסוי השוואה',
  };
}

function calculateValueMetrics(product, group) {
  const price = Number(product.price || 0);
  const prices = Array.isArray(group && group.sortedPrices) ? group.sortedPrices : [];
  const minPrice = Number(group && group.marketMinPrice || 0);
  const maxPrice = Number(group && group.marketMaxPrice || 0);
  const medianPrice = Number(group && group.marketMedianPrice || 0);
  const avgPrice = Number(group && group.marketAvgPrice || 0);
  const chainCount = Number(group && group.chainCount || 0);
  const epsilon = 0.009;

  const cheaperChains = prices.filter(item => item < price - epsilon).length;
  const rank = cheaperChains + 1;
  const diffToBest = price - minPrice;
  const diffToMedian = price - medianPrice;
  const diffToBestPct = minPrice > 0 ? (diffToBest / minPrice) * 100 : 0;
  const diffToMedianPct = medianPrice > 0 ? (diffToMedian / medianPrice) * 100 : 0;
  const savingsVsMedian = Math.max(0, diffToMedian * -1);
  const savingsVsMedianPct = medianPrice > 0 ? (savingsVsMedian / medianPrice) * 100 : 0;

  let score;
  if (price <= minPrice * 1.01) score = 96;
  else if (price <= medianPrice * 0.97) score = 86;
  else if (price <= medianPrice * 1.03) score = 72;
  else if (price <= medianPrice * 1.10) score = 54;
  else score = 28;

  score += Math.min(6, Math.max(0, chainCount - 3) * 1.5);
  score -= Math.min(18, cheaperChains * 5);
  if (diffToBest <= 0.01) score = Math.max(score, 97);
  score = Math.round(clamp(score, 1, 99));

  const bucket = describeValueBucket(score, diffToBestPct, diffToMedianPct);

  return {
    value_score: score,
    value_label: bucket.label,
    value_tone: bucket.tone,
    value_reason: bucket.reason,
    buy_here: score >= 60,
    market_rank: rank,
    market_chain_count: chainCount,
    market_min_price: minPrice,
    market_max_price: maxPrice,
    market_median_price: medianPrice,
    market_avg_price: avgPrice,
    gap_from_best: Math.max(0, diffToBest),
    gap_from_best_pct: Math.max(0, diffToBestPct),
    savings_vs_median: savingsVsMedian,
    savings_vs_median_pct: Math.max(0, savingsVsMedianPct),
    diff_from_median: diffToMedian,
    diff_from_median_pct: diffToMedianPct,
  };
}

function getValueProducts(opts) {
  const {
    chain = '',
    q = '',
    manufacturer = '',
    minPrice = 0,
    maxPrice = 999999,
    sortBy = 'value_score',
    sortDir = 'desc',
    page = 1,
    limit = 50,
    scope = 'smart',
  } = opts;

  const chainIds = String(chain || '').split(',').filter(Boolean);
  if (chainIds.length !== 1) {
    return {
      error: 'יש לבחור רשת אחת כדי להציג כדאיות מול מתחרים',
      total: 0,
      page: 1,
      limit: Math.min(200, Math.max(1, parseInt(limit, 10) || 50)),
      totalPages: 0,
      products: [],
      summary: null,
    };
  }

  const selectedChainId = chainIds[0];
  const qLow = String(q || '').trim().toLowerCase();
  const manufacturerLow = String(manufacturer || '').trim().toLowerCase();
  const minP = parseFloat(minPrice) || 0;
  const maxP = parseFloat(maxPrice) || 999999;

  let selectedProducts = store.products.filter(product => product.chain === selectedChainId);

  if (qLow) {
    selectedProducts = selectedProducts.filter(product =>
      product.name.toLowerCase().includes(qLow) ||
      (product.manufacturer || '').toLowerCase().includes(qLow) ||
      (product.barcode || '').includes(qLow)
    );
  }

  if (manufacturerLow) {
    selectedProducts = selectedProducts.filter(product =>
      (product.manufacturer || '').toLowerCase().includes(manufacturerLow)
    );
  }

  if (minP > 0 || maxP < 999999) {
    selectedProducts = selectedProducts.filter(product =>
      product.price >= minP && product.price <= maxP
    );
  }

  const bestProducts = pickBestChainProducts(selectedProducts);
  const marketIndex = getMarketValueIndex();
  const allCompared = [];

  for (const product of bestProducts) {
    const key = getMarketComparisonKey(product);
    const group = marketIndex.get(key);
    if (!group || group.chainCount < 2) continue;

    const metrics = calculateValueMetrics(product, group);
    allCompared.push({ product, metrics });
  }

  const filtered = scope === 'all'
    ? allCompared
    : allCompared.filter(item => item.metrics.buy_here);

  const dir = sortDir === 'asc' ? 1 : -1;
  const sortMap = {
    value_score: (a, b) => (a.metrics.value_score - b.metrics.value_score) * dir,
    price: (a, b) => (a.product.price - b.product.price) * dir,
    name: (a, b) => a.product.name.localeCompare(b.product.name, 'he') * dir,
    manufacturer: (a, b) => (a.product.manufacturer || '').localeCompare(b.product.manufacturer || '', 'he') * dir,
    unit_price: (a, b) => ((a.product.unitPrice || a.product.price) - (b.product.unitPrice || b.product.price)) * dir,
  };

  const sorted = [...filtered].sort(
    sortMap[sortBy] || ((a, b) =>
      (b.metrics.value_score - a.metrics.value_score) ||
      (b.metrics.savings_vs_median - a.metrics.savings_vs_median))
  );

  const pageNum = Math.max(1, parseInt(page, 10) || 1);
  const limitNum = Math.min(200, Math.max(1, parseInt(limit, 10) || 50));
  const offset = (pageNum - 1) * limitNum;

  const summary = {
    compared_products: allCompared.length,
    recommended_count: allCompared.filter(item => item.metrics.value_score >= 75).length,
    fair_count: allCompared.filter(item => item.metrics.value_score >= 60 && item.metrics.value_score < 75).length,
    expensive_count: allCompared.filter(item => item.metrics.value_score < 60).length,
    avg_score: allCompared.length
      ? Math.round(allCompared.reduce((sum, item) => sum + item.metrics.value_score, 0) / allCompared.length)
      : 0,
    avg_savings: allCompared.length
      ? Number((allCompared.reduce((sum, item) => sum + item.metrics.savings_vs_median, 0) / allCompared.length).toFixed(2))
      : 0,
  };

  return {
    chain: selectedChainId,
    chain_name: getCanonicalChainName(selectedChainId),
    scope,
    total: sorted.length,
    page: pageNum,
    limit: limitNum,
    totalPages: Math.ceil(sorted.length / limitNum),
    summary,
    products: sorted.slice(offset, offset + limitNum).map(item => ({
      ...toApiProduct(item.product),
      ...item.metrics,
    })),
  };
}

function getCachedProductImage(cacheKey) {
  const cached = productImageCache.get(cacheKey);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    productImageCache.delete(cacheKey);
    return null;
  }
  return { ...cached.value, cached: true };
}

function setCachedProductImage(cacheKey, value) {
  const ttl = value && value.imageUrl ? PRODUCT_IMAGE_CACHE_TTL_MS : PRODUCT_IMAGE_NEGATIVE_TTL_MS;
  productImageCache.set(cacheKey, {
    expiresAt: Date.now() + ttl,
    value: { ...value, cached: false },
  });
}

function buildImageSearchUrl({ barcode = '', name = '', manufacturer = '' }) {
  const query = [name, manufacturer, barcode].map(value => String(value || '').trim()).filter(Boolean).join(' ');
  if (!query) return '';
  return `https://www.google.com/search?tbm=isch&q=${encodeURIComponent(query)}`;
}

function normalizeImageQueryText(value = '') {
  return normalizeDisplayText(value)
    .replace(/^[^\p{L}\p{N}]+/u, '')
    .replace(/[?!.,"'`~:;()[\]{}]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isUsefulManufacturer(value = '') {
  const normalized = normalizeImageQueryText(value).toLowerCase();
  if (!normalized) return false;
  return !['undefined', '\u05DC\u05D0 \u05D9\u05D3\u05D5\u05E2', 'unknown', 'n/a', 'na'].includes(normalized);
}

function buildImageLookupQueries({ barcode = '', name = '', manufacturer = '' }) {
  const cleanBarcode = String(barcode || '').trim();
  const cleanName = normalizeImageQueryText(name);
  const cleanManufacturer = isUsefulManufacturer(manufacturer) ? normalizeImageQueryText(manufacturer) : '';
  const queries = [];

  if (/^\d{8,14}$/.test(cleanBarcode)) queries.push(cleanBarcode);
  if (cleanName && cleanBarcode) queries.push(`${cleanName} ${cleanBarcode}`);
  if (cleanName && cleanManufacturer) queries.push(`${cleanName} ${cleanManufacturer}`);
  if (cleanName) queries.push(cleanName);

  return [...new Set(queries.filter(Boolean))];
}

function extractOpenFoodFactsImage(product) {
  if (!product || typeof product !== 'object') return '';
  const display = product.selected_images && product.selected_images.front && product.selected_images.front.display;
  return (
    product.image_front_url ||
    product.image_url ||
    (display && (display.he || display.en || display.fr || Object.values(display).find(Boolean))) ||
    ''
  );
}

async function lookupOpenFoodFactsImage(barcode) {
  const code = String(barcode || '').trim();
  if (!/^\d{8,14}$/.test(code)) return null;

  const { data } = await axios.get(`https://world.openfoodfacts.net/api/v2/product/${encodeURIComponent(code)}.json`, {
    timeout: 8000,
    headers: {
      'User-Agent': 'supermarket-prices/1.0 (image lookup)',
    },
    params: {
      fields: 'code,product_name,image_front_url,image_url,selected_images',
    },
    validateStatus: () => true,
  });

  if (!data || data.status !== 1 || !data.product) return null;
  const imageUrl = extractOpenFoodFactsImage(data.product);
  if (!imageUrl) return null;

  return {
    imageUrl,
    providerProductName: data.product.product_name || '',
    source: 'openfoodfacts',
  };
}

function extractDuckDuckGoToken(html = '') {
  const text = String(html || '');
  const match =
    text.match(/vqd='([^']+)'/) ||
    text.match(/vqd=\"([^\"]+)\"/) ||
    text.match(/"vqd":"([^"]+)"/);
  return match ? match[1] : '';
}

function pickDuckDuckGoImage(results, context) {
  const cleanBarcode = String(context.barcode || '').trim();
  const nameTokens = normalizeImageQueryText(context.name)
    .toLowerCase()
    .split(' ')
    .filter(token => token.length >= 3);
  const manufacturerTokens = normalizeImageQueryText(context.manufacturer)
    .toLowerCase()
    .split(' ')
    .filter(token => token.length >= 3);

  let best = null;
  let bestScore = -Infinity;

  for (const item of Array.isArray(results) ? results : []) {
    const imageUrl = String(item && item.image || '').trim();
    if (!/^https?:\/\//i.test(imageUrl)) continue;

    const haystack = `${item.title || ''} ${item.url || ''} ${imageUrl}`.toLowerCase();
    let score = 0;

    if (cleanBarcode && haystack.includes(cleanBarcode)) score += 12;
    for (const token of nameTokens) {
      if (haystack.includes(token)) score += 3;
    }
    for (const token of manufacturerTokens) {
      if (haystack.includes(token)) score += 2;
    }
    if (/pricez|superpharm|shufersal|rami-levy|victory|ksp|bigstore|danone|actimel|nutella|cars/i.test(haystack)) score += 2;
    if (/\.(jpg|jpeg|png|webp)(\?|$)/i.test(imageUrl)) score += 1;

    if (score > bestScore) {
      best = item;
      bestScore = score;
    }
  }

  if (!best || bestScore < 1) return null;
  return {
    imageUrl: best.image,
    providerProductName: best.title || '',
    providerPageUrl: best.url || '',
    source: 'duckduckgo',
  };
}

async function lookupDuckDuckGoImage(context) {
  const queries = buildImageLookupQueries(context);
  for (const query of queries) {
    const page = await axios.get('https://duckduckgo.com/', {
      timeout: 8000,
      params: { q: query, iax: 'images', ia: 'images' },
      headers: { 'User-Agent': 'Mozilla/5.0' },
      validateStatus: () => true,
    });

    if (page.status !== 200) continue;
    const vqd = extractDuckDuckGoToken(page.data);
    if (!vqd) continue;

    const res = await axios.get('https://duckduckgo.com/i.js', {
      timeout: 8000,
      params: { l: 'us-en', o: 'json', q: query, vqd, f: ',,,,' },
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Referer': `https://duckduckgo.com/?q=${encodeURIComponent(query)}&iax=images&ia=images`,
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
      },
      validateStatus: () => true,
    });

    if (res.status !== 200 || !res.data) continue;
    const picked = pickDuckDuckGoImage(res.data.results, context);
    if (picked && picked.imageUrl) return picked;
  }

  return null;
}

async function getProductImageData({ barcode = '', name = '', manufacturer = '' }) {
  const cacheKey = String(barcode || `${name}|${manufacturer}`).trim().toLowerCase();
  if (!cacheKey) {
    return {
      ok: false,
      imageUrl: '',
      source: 'none',
      searchUrl: '',
      cached: false,
    };
  }

  const cached = getCachedProductImage(cacheKey);
  if (cached) return cached;

  const searchUrl = buildImageSearchUrl({ barcode, name, manufacturer });
  let result = {
    ok: false,
    imageUrl: '',
    source: 'none',
    providerProductName: '',
    searchUrl,
  };

  try {
    const openFoodFacts = await lookupOpenFoodFactsImage(barcode);
    if (openFoodFacts && openFoodFacts.imageUrl) {
      result = {
        ok: true,
        imageUrl: openFoodFacts.imageUrl,
        source: openFoodFacts.source,
        providerProductName: openFoodFacts.providerProductName || '',
        searchUrl,
      };
    }
  } catch (error) {}

  if (!result.imageUrl) {
    try {
      const duckDuckGo = await lookupDuckDuckGoImage({ barcode, name, manufacturer });
      if (duckDuckGo && duckDuckGo.imageUrl) {
        result = {
          ok: true,
          imageUrl: duckDuckGo.imageUrl,
          source: duckDuckGo.source,
          providerProductName: duckDuckGo.providerProductName || '',
          providerPageUrl: duckDuckGo.providerPageUrl || '',
          searchUrl,
        };
      }
    } catch (error) {}
  }

  setCachedProductImage(cacheKey, result);
  return { ...result, cached: false };
}

function buildChainStats() {
  store.chainStats = {};
  for (const product of store.products) {
    if (!store.chainStats[product.chain]) {
      store.chainStats[product.chain] = { name: getCanonicalChainName(product.chain, product.chainName), count: 0 };
    }
    store.chainStats[product.chain].count++;
  }
}

function isChainLoaded(chainId, expectedCount = null) {
  if (Number.isFinite(expectedCount) && expectedCount === 0) return true;
  const stats = store.chainStats[chainId];
  if (!stats) return false;
  const count = Number(stats.count || 0);
  if (Number.isFinite(expectedCount) && expectedCount >= 0) return count === expectedCount;
  return count > 0;
}

function mergeChainResults(chainResults = {}) {
  const refreshedChainIds = Object.keys(chainResults);
  if (!refreshedChainIds.length) return store.products.length;

  const refreshedSet = new Set(refreshedChainIds);
  const nextProducts = store.products.filter(product => !refreshedSet.has(product.chain));

  for (const chain of CHAINS) {
    const entry = chainResults[chain.id];
    if (!entry) continue;
    const normalized = (entry.products || []).map(normalizeStoredProduct);
    nextProducts.push(...normalized);
  }

  store.products = dedupeCompareProducts(nextProducts);
  buildChainStats();
  return store.products.length;
}

const sseClients = new Set();

function broadcastLog(type, msg, data = {}) {
  const normalizedMsg = normalizeDisplayText(msg);
  const payload = JSON.stringify({ type, msg: normalizedMsg, data, ts: Date.now() });
  const line = `data: ${payload}\n\n`;
  const prefix = {
    start: '[START]',
    info: '[INFO]',
    ok: '[OK]',
    warn: '[WARN]',
    error: '[ERROR]',
    progress: '[PROGRESS]',
    chain: '[CHAIN]',
    done: '[DONE]',
  }[type] || '[LOG]';

  console.log(`${prefix} ${normalizedMsg}`);
  for (const res of sseClients) {
    try { res.write(line); } catch (error) {}
  }
}

app.get('/api/log-stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  res.write(`data: ${JSON.stringify({ type: 'connected', msg: '\u05DE\u05D7\u05D5\u05D1\u05E8 \u05DC\u05D6\u05E8\u05DD \u05DC\u05D5\u05D2\u05D9\u05DD', ts: Date.now() })}\n\n`);
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

function searchProducts(opts) {
  const {
    q = '',
    chain = '',
    manufacturer = '',
    minPrice = 0,
    maxPrice = 999999,
    sortBy = 'name',
    sortDir = 'asc',
    page = 1,
    limit = 50,
    barcode = '',
  } = opts;

  let results = store.products;
  const qLow = String(q || '').trim().toLowerCase();

  if (qLow) {
    results = results.filter(product =>
      product.name.toLowerCase().includes(qLow) ||
      (product.manufacturer || '').toLowerCase().includes(qLow) ||
      (product.barcode || '').includes(qLow)
    );
  }

  if (barcode) results = results.filter(product => product.barcode === barcode);

  if (chain) {
    const chains = chain.split(',').filter(Boolean);
    if (chains.length) results = results.filter(product => chains.includes(product.chain));
  }

  if (manufacturer) {
    const needle = manufacturer.toLowerCase();
    results = results.filter(product => (product.manufacturer || '').toLowerCase().includes(needle));
  }

  const minP = parseFloat(minPrice) || 0;
  const maxP = parseFloat(maxPrice) || 999999;
  if (minP > 0 || maxP < 999999) {
    results = results.filter(product => product.price >= minP && product.price <= maxP);
  }

  results = dedupeCatalogProducts(results);

  const dir = sortDir === 'desc' ? -1 : 1;
  const sortMap = {
    name: (a, b) => a.name.localeCompare(b.name, 'he') * dir,
    price: (a, b) => (a.price - b.price) * dir,
    unit_price: (a, b) => ((a.unitPrice || a.price) - (b.unitPrice || b.price)) * dir,
    manufacturer: (a, b) => (a.manufacturer || '').localeCompare(b.manufacturer || '', 'he') * dir,
    chain: (a, b) => (a.chainName || '').localeCompare(b.chainName || '', 'he') * dir,
  };

  if (sortMap[sortBy]) results = [...results].sort(sortMap[sortBy]);

  const total = results.length;
  const pageNum = Math.max(1, parseInt(page, 10));
  const limitNum = Math.min(200, Math.max(1, parseInt(limit, 10)));
  const offset = (pageNum - 1) * limitNum;

  return {
    total,
    page: pageNum,
    limit: limitNum,
    totalPages: Math.ceil(total / limitNum),
    products: results.slice(offset, offset + limitNum).map(toApiProduct),
  };
}

app.get('/api/status', (req, res) => {
  const chainList = CHAINS.map(chain => {
    const stats = store.chainStats[chain.id] || { name: chain.name, count: 0 };
    return {
      chain: chain.id,
      chain_name: stats.name || chain.name,
      count: stats.count || 0,
    };
  });

  res.json({
    totalProducts: store.products.length,
    chains: chainList,
    lastFetch: store.lastFetch,
    isFetching,
    hasData: store.products.length > 0,
    availableChains: CHAINS.map(chain => ({
      id: chain.id,
      name: chain.name,
      color: chain.color,
      platform: chain.platform,
    })),
  });
});

app.post('/api/refresh', (req, res) => {
  if (isFetching) return res.json({ success: false, message: 'Refresh is already running' });

  const forceLive = !!(req.body && (req.body.force || req.body.mode === 'full'));
  res.json({ success: true, message: forceLive ? 'Full refresh started' : 'Smart refresh started' });
  (async () => {
    isFetching = true;
    const refreshPlan = buildRefreshPlan({ forceLive });
    const chainsToRefresh = refreshPlan
      .filter(item => forceLive || item.action === 'live' || !isChainLoaded(item.chain.id, item.cachedCount))
      .map(item => item.chain.id);
    const skippedCount = refreshPlan.length - chainsToRefresh.length;

    broadcastLog('start', '=== Starting smart refresh for ' + CHAINS.length + ' chains ===', {
      totalChains: CHAINS.length,
      refreshingChains: chainsToRefresh.length,
      skippedChains: skippedCount,
      forceLive,
    });

    try {
      if (skippedCount > 0) {
        broadcastLog('info', skippedCount + ' chains are still fresh in local cache - skipping them');
      }

      if (chainsToRefresh.length === 0) {
        broadcastLog('done', '=== Everything is still fresh - nothing to refresh right now ===', {
          total: store.products.length,
          savedAt: store.lastFetch,
          skippedChains: skippedCount,
        });
        return;
      }

      broadcastLog('info', 'Refreshing only ' + chainsToRefresh.length + ' chains that actually need an update');
      const { chainResults } = await fetchAllChains(broadcastLog, {
        chainIds: chainsToRefresh,
        plan: refreshPlan.filter(item => chainsToRefresh.includes(item.chain.id)),
        forceLive,
      });

      if (Object.keys(chainResults).length > 0) {
        const totalProducts = mergeChainResults(chainResults);
        store.lastFetch = Date.now();
        saveRefreshResults(chainResults, store.lastFetch);
        broadcastLog('done', '=== Saved ' + totalProducts.toLocaleString('he-IL') + ' products ===', {
          total: totalProducts,
          savedAt: store.lastFetch,
          refreshedChains: Object.keys(chainResults).length,
          skippedChains: skippedCount,
        });
      } else {
        broadcastLog('warn', '=== No products were refreshed - check the log for details ===');
      }
    } catch (error) {
      broadcastLog('error', 'General refresh error: ' + error.message);
    } finally {
      isFetching = false;
    }
  })();
});

app.post('/api/add-url', async (req, res) => {
  const { url, chainId, chainName } = req.body;
  if (!url) return res.status(400).json({ error: '\u05D7\u05E1\u05E8 URL' });

  const chain = CHAINS.find(item => item.id === chainId) || {
    id: chainId || 'custom',
    name: normalizeDisplayText(chainName || '\u05DE\u05D5\u05EA\u05D0\u05DD'),
    color: '#888',
    platform: 'direct',
  };

  try {
    const products = await fetchFromUrl(url, chain, broadcastLog);
    if (products.length > 0) {
      const normalizedProducts = products.map(normalizeStoredProduct);
      store.products = dedupeCompareProducts([
        ...store.products.filter(product => product.chain !== chain.id),
        ...normalizedProducts,
      ]);
      store.lastFetch = Date.now();
      buildChainStats();
      saveChainCache(chain, normalizedProducts, {
        updatedAt: store.lastFetch,
        checkedAt: store.lastFetch,
        sourceKey: `manual:${url}`,
        sourceInfo: { url },
        lastFetch: store.lastFetch,
      });
      return res.json({ success: true, count: normalizedProducts.length });
    }

    return res.json({ success: false, message: '\u05DC\u05D0 \u05E0\u05DE\u05E6\u05D0\u05D5 \u05DE\u05D5\u05E6\u05E8\u05D9\u05DD' });
  } catch (error) {
    return res.json({ success: false, message: error.message });
  }
});

app.get('/api/products', (req, res) => res.json(searchProducts(req.query)));

app.get('/api/value-products', (req, res) => {
  const data = getValueProducts(req.query);
  if (data && data.error) {
    return res.status(400).json(data);
  }
  return res.json(data);
});

app.get('/api/product-image', async (req, res) => {
  const { barcode = '', name = '', manufacturer = '' } = req.query;
  if (!barcode && !name && !manufacturer) {
    return res.status(400).json({ ok: false, imageUrl: '', source: 'none', searchUrl: '' });
  }

  try {
    const result = await getProductImageData({ barcode, name, manufacturer });
    return res.json(result);
  } catch (error) {
    return res.json({
      ok: false,
      imageUrl: '',
      source: 'none',
      searchUrl: buildImageSearchUrl({ barcode, name, manufacturer }),
      error: error.message,
      cached: false,
    });
  }
});

app.get('/api/compare/:barcode', (req, res) => {
  const results = dedupeCompareProducts(
    store.products.filter(product => product.barcode === req.params.barcode)
  )
    .sort((a, b) => a.price - b.price)
    .map(toApiProduct);

  res.json({ barcode: req.params.barcode, results });
});

app.get('/api/chains', (req, res) => {
  const chainMap = {};
  for (const product of store.products) {
    if (!chainMap[product.chain]) {
      chainMap[product.chain] = {
        chain: product.chain,
        chain_name: getCanonicalChainName(product.chain, product.chainName),
        count: 0,
        min_price: Infinity,
        max_price: -Infinity,
        total_price: 0,
      };
    }

    const entry = chainMap[product.chain];
    const price = Number(product.price) || 0;
    entry.count++;
    entry.total_price += price;
    if (price < entry.min_price) entry.min_price = price;
    if (price > entry.max_price) entry.max_price = price;
  }

  const result = CHAINS.map(meta => {
    const entry = chainMap[meta.id] || {
      chain: meta.id,
      chain_name: meta.name,
      count: 0,
      min_price: Infinity,
      max_price: -Infinity,
      total_price: 0,
    };

    return {
      chain: entry.chain,
      chain_name: entry.chain_name,
      count: entry.count,
      min_price: Number.isFinite(entry.min_price) ? entry.min_price : 0,
      max_price: Number.isFinite(entry.max_price) ? entry.max_price : 0,
      avg_price: entry.count > 0 ? entry.total_price / entry.count : 0,
      color: meta.color || '#666',
      platform: meta.platform || '',
    };
  });

  res.json(result);
});

loadFromDisk();
app.listen(PORT, () => {
  console.log(`\n[SERVER] Price server: http://localhost:${PORT}`);
  console.log(`[DATA] ${store.products.length > 0 ? `${store.products.length} products in memory` : 'empty'}\n`);
});
