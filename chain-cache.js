const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { dedupeProducts } = require('./product-utils');

const CACHE_VERSION = 1;
const CACHE_DIR = path.join(__dirname, 'cache');
const CHAINS_DIR = path.join(CACHE_DIR, 'chains');
const MANIFEST_FILE = path.join(CACHE_DIR, 'manifest.json');
const LEGACY_DB_FILE = path.join(__dirname, 'prices-data.json');

function ensureCacheDirs() {
  if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });
  if (!fs.existsSync(CHAINS_DIR)) fs.mkdirSync(CHAINS_DIR, { recursive: true });
}

function getChainFileName(chainId) {
  return `${chainId}.json.gz`;
}

function getChainFilePath(chainId) {
  return path.join(CHAINS_DIR, getChainFileName(chainId));
}

function defaultManifest() {
  return {
    version: CACHE_VERSION,
    lastFetch: null,
    chains: {},
  };
}

function readJsonFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJsonFile(filePath, value) {
  const tempPath = `${filePath}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(value, null, 2), 'utf8');
  fs.renameSync(tempPath, filePath);
}

function readGzipJson(filePath) {
  const buf = fs.readFileSync(filePath);
  const json = zlib.gunzipSync(buf).toString('utf8');
  return JSON.parse(json);
}

function writeGzipJson(filePath, value) {
  const tempPath = `${filePath}.tmp`;
  const json = JSON.stringify(value);
  const gz = zlib.gzipSync(Buffer.from(json, 'utf8'));
  fs.writeFileSync(tempPath, gz);
  fs.renameSync(tempPath, filePath);
}

function loadManifest() {
  ensureCacheDirs();
  if (!fs.existsSync(MANIFEST_FILE)) {
    const manifest = defaultManifest();
    writeJsonFile(MANIFEST_FILE, manifest);
    return manifest;
  }

  try {
    const manifest = readJsonFile(MANIFEST_FILE);
    return {
      version: manifest.version || CACHE_VERSION,
      lastFetch: manifest.lastFetch || null,
      chains: manifest.chains && typeof manifest.chains === 'object' ? manifest.chains : {},
    };
  } catch (error) {
    return defaultManifest();
  }
}

function saveManifest(manifest) {
  ensureCacheDirs();
  writeJsonFile(MANIFEST_FILE, {
    version: CACHE_VERSION,
    lastFetch: manifest.lastFetch || null,
    chains: manifest.chains && typeof manifest.chains === 'object' ? manifest.chains : {},
  });
}

function loadChainCache(chainId) {
  const manifest = loadManifest();
  const meta = manifest.chains[chainId];
  if (!meta) return null;

  const filePath = getChainFilePath(chainId);
  if (!fs.existsSync(filePath)) return null;

  try {
    const products = normalizeProducts(readGzipJson(filePath));
    return {
      meta,
      products,
    };
  } catch (error) {
    return null;
  }
}

function normalizeProducts(products) {
  return dedupeProducts(
    (Array.isArray(products) ? products : []).filter(product => product && product.id),
    { scope: 'chain' }
  ).products;
}

function buildChainStats(products, fallbackName) {
  return {
    name: fallbackName || (products[0] && products[0].chainName) || '',
    count: Array.isArray(products) ? products.length : 0,
  };
}

function saveChainCache(chain, products, meta = {}) {
  ensureCacheDirs();
  const manifest = loadManifest();
  const normalizedProducts = normalizeProducts(products);
  const fileName = getChainFileName(chain.id);
  const filePath = getChainFilePath(chain.id);
  writeGzipJson(filePath, normalizedProducts);

  const previous = manifest.chains[chain.id] || {};
  manifest.chains[chain.id] = {
    ...previous,
    id: chain.id,
    name: chain.name,
    platform: chain.platform || previous.platform || '',
    file: fileName,
    count: normalizedProducts.length,
    updatedAt: meta.updatedAt || previous.updatedAt || Date.now(),
    checkedAt: meta.checkedAt || Date.now(),
    sourceKey: meta.sourceKey || previous.sourceKey || '',
    sourceInfo: meta.sourceInfo || previous.sourceInfo || null,
    cacheMaxAgeMs: Number.isFinite(meta.cacheMaxAgeMs) ? meta.cacheMaxAgeMs : (previous.cacheMaxAgeMs || null),
    fromCache: !!meta.fromCache,
    staleFallback: !!meta.staleFallback,
  };

  if (meta.lastFetch) manifest.lastFetch = meta.lastFetch;
  saveManifest(manifest);
  return manifest.chains[chain.id];
}

function saveRefreshResults(chainResults, lastFetch) {
  const entries = Object.values(chainResults || {});
  for (const entry of entries) {
    if (!entry || !entry.chain) continue;
    saveChainCache(entry.chain, entry.products || [], {
      ...(entry.meta || {}),
      lastFetch,
    });
  }

  const manifest = loadManifest();
  manifest.lastFetch = lastFetch || manifest.lastFetch || Date.now();
  saveManifest(manifest);
  return manifest;
}

function migrateLegacyFile(chainDefinitions = []) {
  if (!fs.existsSync(LEGACY_DB_FILE)) return null;

  const manifest = loadManifest();
  if (Object.keys(manifest.chains).length > 0) return manifest;

  try {
    const data = readJsonFile(LEGACY_DB_FILE);
    const products = Array.isArray(data.products) ? data.products : [];
    const byChain = new Map();
    for (const product of products) {
      const chainId = product && product.chain;
      if (!chainId) continue;
      if (!byChain.has(chainId)) byChain.set(chainId, []);
      byChain.get(chainId).push(product);
    }

    const chainMap = new Map((chainDefinitions || []).map(chain => [chain.id, chain]));
    for (const [chainId, chainProducts] of byChain.entries()) {
      const chain = chainMap.get(chainId) || {
        id: chainId,
        name: (chainProducts[0] && chainProducts[0].chainName) || chainId,
        platform: '',
      };
      saveChainCache(chain, chainProducts, {
        updatedAt: data.lastFetch || Date.now(),
        checkedAt: data.lastFetch || Date.now(),
        lastFetch: data.lastFetch || null,
      });
    }

    const nextManifest = loadManifest();
    nextManifest.lastFetch = data.lastFetch || nextManifest.lastFetch || null;
    saveManifest(nextManifest);
    return nextManifest;
  } catch (error) {
    return null;
  }
}

function loadCachedStore(chainDefinitions = []) {
  ensureCacheDirs();
  let manifest = loadManifest();
  if (Object.keys(manifest.chains).length === 0) {
    const migrated = migrateLegacyFile(chainDefinitions);
    if (migrated) manifest = migrated;
  }

  const products = [];
  const chainStats = {};
  let manifestUpdated = false;

  for (const chain of chainDefinitions) {
    const cached = loadChainCache(chain.id);
    if (!cached) continue;
    const chainProducts = normalizeProducts(cached.products);
    const meta = manifest.chains[chain.id];
    if (meta && meta.count !== chainProducts.length) {
      meta.count = chainProducts.length;
      manifestUpdated = true;
    }
    if (!chainProducts.length) continue;
    for (const product of chainProducts) products.push(product);
    chainStats[chain.id] = buildChainStats(chainProducts, chain.name);
  }

  if (manifestUpdated) saveManifest(manifest);

  return {
    products,
    lastFetch: manifest.lastFetch || null,
    chainStats,
    manifest,
  };
}

module.exports = {
  CACHE_DIR,
  CHAINS_DIR,
  MANIFEST_FILE,
  LEGACY_DB_FILE,
  loadManifest,
  loadCachedStore,
  loadChainCache,
  saveChainCache,
  saveRefreshResults,
};
