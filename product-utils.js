function normalizeComparisonKeyPart(value = '') {
  return String(value || '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function getProductComparisonKey(product) {
  const barcode = String(product && product.barcode || '').trim();
  if (/^\d{8,14}$/.test(barcode)) return `barcode:${barcode}`;

  const name = normalizeComparisonKeyPart(product && product.name);
  const manufacturer = normalizeComparisonKeyPart(product && product.manufacturer);
  const unitQty = normalizeComparisonKeyPart(product && product.unitQty);
  if (!name) return '';
  return `text:${name}|${manufacturer}|${unitQty}`;
}

function getProductDedupKey(product, scope = 'chain') {
  const comparisonKey = getProductComparisonKey(product);
  const fallbackKey = comparisonKey || `id:${String(product && product.id || '').trim()}`;
  if (!fallbackKey || fallbackKey === 'id:') return '';

  if (scope === 'global') return fallbackKey;

  const chainId = String(product && product.chain || '').trim();
  return chainId ? `${chainId}|${fallbackKey}` : fallbackKey;
}

function toComparablePrice(product) {
  const price = Number(product && product.price);
  return Number.isFinite(price) && price > 0 ? price : Number.POSITIVE_INFINITY;
}

function toComparableUnitPrice(product) {
  const unitPrice = Number(product && (product.unitPrice || product.unit_price));
  return Number.isFinite(unitPrice) && unitPrice > 0 ? unitPrice : Number.POSITIVE_INFINITY;
}

function toTieBreaker(product) {
  return [
    String(product && product.storeName || ''),
    String(product && product.storeId || ''),
    String(product && product.chainName || ''),
    String(product && product.id || ''),
  ].join('\u0000');
}

function pickPreferredProduct(current, candidate) {
  if (!current) return candidate;
  if (!candidate) return current;

  const currentPrice = toComparablePrice(current);
  const candidatePrice = toComparablePrice(candidate);
  if (candidatePrice < currentPrice) return candidate;
  if (candidatePrice > currentPrice) return current;

  const currentUnitPrice = toComparableUnitPrice(current);
  const candidateUnitPrice = toComparableUnitPrice(candidate);
  if (candidateUnitPrice < currentUnitPrice) return candidate;
  if (candidateUnitPrice > currentUnitPrice) return current;

  return toTieBreaker(candidate).localeCompare(toTieBreaker(current), 'he') < 0
    ? candidate
    : current;
}

function dedupeProducts(products, options = {}) {
  const scope = options.scope === 'global' ? 'global' : 'chain';
  const bestByKey = new Map();
  let removed = 0;

  for (const product of Array.isArray(products) ? products : []) {
    if (!product || typeof product !== 'object') continue;
    const key = getProductDedupKey(product, scope);
    if (!key) continue;

    const current = bestByKey.get(key);
    if (!current) {
      bestByKey.set(key, product);
      continue;
    }

    bestByKey.set(key, pickPreferredProduct(current, product));
    removed += 1;
  }

  return {
    products: [...bestByKey.values()],
    removed,
  };
}

module.exports = {
  normalizeComparisonKeyPart,
  getProductComparisonKey,
  getProductDedupKey,
  pickPreferredProduct,
  dedupeProducts,
};
