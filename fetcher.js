/**
 * fetcher.js  â€“  ×©×œ×™×¤×ª ×ž×—×™×¨×™ ×¡×•×¤×¨×ž×¨×§×˜×™ï¿½? ×™×©×¨ï¿½?×œ×™ï¿½?
 *
 * ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
 * 3 ×¤×œ×˜×¤×•×¨×ž×•×ª (×ž×§×•×¨: il-supermarket-scraper PyPI):
 *
 *  1. CERBERUS FTP  url.retail.publishedprices.co.il
 *     ×¨×ž×™ ×œ×•×™ Â· ×™×•×—× × ×•×£ Â· ï¿½?×•×©×¨ ×¢×“ Â· ×§×©×ª Â· ×“×•×¨×œ×•×Ÿ
 *     ×˜×™×‘ ×˜×¢ï¿½? Â· ×¤×¨×© ×ž×¨×§×˜+×“×•×©
 *
 *  2. LAIBCATALOG   laibcatalog.co.il  (Nibit/Matrix ×”×—×“×©)
 *     ×•×™×§×˜×•×¨×™ Â· ×ž×—×¡× ×™ ×”×©×•×§ Â· ×ž×—×¡× ×™ ×œ×”×‘
 *
 *  3. WEB SCRAPING  (×©×•×¤×¨×¡×œ, ×—×¦×™ ×—×™× ï¿½?, ×•×¢×•×“)
 *
 * ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
 */

const axios  = require('axios');
const xml2js = require('xml2js');
const zlib   = require('zlib');
const iconv  = require('iconv-lite');
const AdmZip = require('adm-zip');
const ftp    = require('basic-ftp');
const { Writable } = require('stream');
const { fetchAlma } = require('./alma-fetcher');
const { fetchWolt, inspectWoltSource } = require('./wolt-fetcher');
const { loadChainCache, loadManifest } = require('./chain-cache');
const { dedupeProducts: dedupeProductSet } = require('./product-utils');
const { normalizeDisplayText, normalizeOptionalText } = require('./text-normalizer');

// â”€â”€ ×¨×©×ª×•×ª â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const CHAINS = [
  // == Alma (browser-assisted, specific branch) ==
  { id:'alma_rishon', name:'××œ×ž×” ×ž×¨×§×˜ (×¨××©×•×Ÿ ×œ×¦×™×•×Ÿ)', color:'#207d3f', platform:'alma', retailerId:'1467', branchId:'3462', storeName:'×¨××©×•×Ÿ ×œ×¦×™×•×Ÿ × ×¨×§×™×¡×™×', homeUrl:'https://www.alma-market.co.il/' },
  { id:'city_market_narkisim', name:'\u05E1\u05D9\u05D8\u05D9 \u05DE\u05E8\u05E7\u05D8 (\u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD)', color:'#d97706', platform:'wolt', venueSlug:'city-market-narkisim-rishon-lezion', venueUrl:'https://wolt.com/he/isr/rishon-lezion-hashfela-area/venue/city-market-narkisim-rishon-lezion', storeName:'\u05E8\u05D0\u05E9\u05D5\u05DF \u05DC\u05E6\u05D9\u05D5\u05DF \u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD' },
  { id:'ampm_narkisim', name:'AM PM (\u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD)', color:'#111827', platform:'wolt', venueSlug:'am-pm-rishon-lezion', venueUrl:'https://wolt.com/he/isr/rishon-lezion-hashfela-area/venue/am-pm-rishon-lezion', storeName:'\u05E8\u05D0\u05E9\u05D5\u05DF \u05DC\u05E6\u05D9\u05D5\u05DF \u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD' },

  // == Cerberus FTP ==
  { id:'rami_levy',    name:'×¨×ž×™ ×œ×•×™',       color:'#f7941d', platform:'cerberus', ftpUser:'RamiLevi',    chainId:'7290058140886' },
  { id:'yochananof',   name:'×™×•×—× × ×•×£',        color:'#8b1a1a', platform:'cerberus', ftpUser:'yohananof',   chainId:'7290803800003' },
  { id:'osher_ad',     name:'××•×©×¨ ×¢×“',         color:'#6a0dad', platform:'cerberus', ftpUser:'osherad',     chainId:'7290103152017' },
  { id:'keshet',       name:'×§×©×ª ×˜×¢×ž×™×',       color:'#009688', platform:'cerberus', ftpUser:'Keshet',      chainId:'7290785400000' },
  { id:'doralon',      name:'×“×•×¨×œ×•×Ÿ',          color:'#795548', platform:'cerberus', ftpUser:'doralon',     chainId:'7290492000005', ftpSecure:true, ftpSecureOptions:{ rejectUnauthorized:false } },
  { id:'tiv_taam',     name:'×˜×™×‘ ×˜×¢×',         color:'#2e7d32', platform:'cerberus', ftpUser:'TivTaam',     chainId:'7290873255550' },
  { id:'fresh_dosh',   name:'×¤×¨×© ×ž×¨×§×˜+×“×•×©',   color:'#ff5722', platform:'cerberus', ftpUser:'freshmarket', chainId:'7290876100000' },

  // == Laibcatalog (Nibit ×”×—×“×©) ==
  { id:'victory',         name:'×•×™×§×˜×•×¨×™',        color:'#004b99', platform:'laib', chainId:['7290696200003','7290058103393'] },
  { id:'mahsanei_hashuk', name:'×ž×—×¡× ×™ ×”×©×•×§',     color:'#333333', platform:'laib', chainId:['7290661400001','7290633800006'] },

  // == ×©×•×¤×¨×¡×œ (MultiPageWeb) ==
  { id:'shufersal', name:'×©×•×¤×¨×¡×œ', color:'#e31f26', platform:'shufersal' },

  // == ??? ???? (web, ???) ==
  { id:'hazi_hinam', name:'×—×¦×™ ×—×™× ×', color:'#cc0000', platform:'hazihinam' },

  // == ?????? ????? ???? ?? ?????? ==
  { id:'coop',      name:'×§×•××•×¤',    color:'#1565c0', platform:'inactive', reason:'Legacy Coop source is unavailable' },
  { id:'eden_teva', name:'×¢×“×Ÿ ×˜×‘×¢',  color:'#558b2f', platform:'inactive', reason:'Legacy Eden Teva source is unavailable' },
];

const CANONICAL_CHAIN_TEXT = {
  alma_rishon: {
    name: '\u05D0\u05DC\u05DE\u05D4 \u05DE\u05E8\u05E7\u05D8 (\u05E8\u05D0\u05E9\u05D5\u05DF \u05DC\u05E6\u05D9\u05D5\u05DF)',
    storeName: '\u05E8\u05D0\u05E9\u05D5\u05DF \u05DC\u05E6\u05D9\u05D5\u05DF \u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD',
  },
  city_market_narkisim: {
    name: '\u05E1\u05D9\u05D8\u05D9 \u05DE\u05E8\u05E7\u05D8 (\u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD)',
    storeName: '\u05E8\u05D0\u05E9\u05D5\u05DF \u05DC\u05E6\u05D9\u05D5\u05DF \u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD',
  },
  ampm_narkisim: {
    name: 'AM PM (\u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD)',
    storeName: '\u05E8\u05D0\u05E9\u05D5\u05DF \u05DC\u05E6\u05D9\u05D5\u05DF \u05E0\u05E8\u05E7\u05D9\u05E1\u05D9\u05DD',
  },
  rami_levy: { name: '\u05E8\u05DE\u05D9 \u05DC\u05D5\u05D9' },
  yochananof: { name: '\u05D9\u05D5\u05D7\u05E0\u05E0\u05D5\u05E3' },
  osher_ad: { name: '\u05D0\u05D5\u05E9\u05E8 \u05E2\u05D3' },
  keshet: { name: '\u05E7\u05E9\u05EA \u05D8\u05E2\u05DE\u05D9\u05DD' },
  doralon: { name: '\u05D3\u05D5\u05E8\u05DC\u05D5\u05DF' },
  tiv_taam: { name: '\u05D8\u05D9\u05D1 \u05D8\u05E2\u05DD' },
  fresh_dosh: { name: '\u05E4\u05E8\u05E9 \u05DE\u05E8\u05E7\u05D8+\u05D3\u05D5\u05E9' },
  victory: { name: '\u05D5\u05D9\u05E7\u05D8\u05D5\u05E8\u05D9' },
  mahsanei_hashuk: { name: '\u05DE\u05D7\u05E1\u05E0\u05D9 \u05D4\u05E9\u05D5\u05E7' },
  shufersal: { name: '\u05E9\u05D5\u05E4\u05E8\u05E1\u05DC' },
  hazi_hinam: { name: '\u05D7\u05E6\u05D9 \u05D7\u05D9\u05E0\u05DD' },
  coop: { name: '\u05E7\u05D5\u05D0\u05D5\u05E4' },
  eden_teva: { name: '\u05E2\u05D3\u05DF \u05D8\u05D1\u05E2' },
};

// â”€â”€ XML / Buffer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const parser = new xml2js.Parser({ explicitArray:false, ignoreAttrs:true, trim:true, emptyTag:'' });

async function decodeBuffer(buf) {
  try { buf = zlib.gunzipSync(buf); } catch(e){}
  let txt = buf.toString('utf8');
  if (txt.includes('\ufffd')) txt = iconv.decode(buf, 'windows-1255');
  return txt.replace(/^\uFEFF/, '').replace(/xmlns[^"]*"[^"]*"/g, '');
}

async function parseXml(buf) {
  try { return await parser.parseStringPromise(await decodeBuffer(buf)); }
  catch(e) { return null; }
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  return value ? [value] : [];
}

function firstDefined(...values) {
  return values.find(v => v !== undefined && v !== null && v !== '');
}

function getErrorMessage(err) {
  if (!err) return 'Unknown error';
  if (typeof err === 'string') return err;
  if (typeof err.message === 'string' && err.message) return err.message;
  if (typeof err.code === 'string' && err.code) return err.code;
  return String(err);
}

function extractFilename(value = '') {
  const raw = String(value || '');
  try {
    const url = new URL(raw);
    const byQuery = url.searchParams.get('fileNm') || url.searchParams.get('filename') || url.searchParams.get('file');
    if (byQuery) return decodeURIComponent(byQuery);
    return decodeURIComponent(url.pathname.split('/').pop() || raw);
  } catch(e) {
    return decodeURIComponent(raw.split('/').pop().split('?')[0] || raw);
  }
}

function getFileMeta(value) {
  const fileName = extractFilename(value);
  const baseName = fileName.replace(/\.(gz|xml|zip)$/i, '');
  const withoutPrefix = baseName.replace(/^PriceFull/i, '');
  const parts = withoutPrefix.split('-').filter(Boolean);
  if (parts.length < 2) {
    return { fileName, storeKey: baseName, timestamp: baseName };
  }

  let tsIndex = parts.findIndex((part, index) => index > 0 && /^\d{8,14}$/.test(part));
  if (tsIndex === -1) tsIndex = Math.max(1, parts.length - 1);

  return {
    fileName,
    storeKey: parts.slice(1, tsIndex).join('-') || parts[1] || baseName,
    timestamp: parts.slice(tsIndex).join('') || baseName,
  };
}

function selectLatestEntries(entries, getValue = entry => entry?.name || entry?.fileName || entry?.url || '') {
  const selected = new Map();

  for (const entry of entries) {
    const value = getValue(entry);
    const meta = getFileMeta(value);
    const current = selected.get(meta.storeKey);
    if (!current || meta.timestamp > current.meta.timestamp || (meta.timestamp === current.meta.timestamp && meta.fileName > current.meta.fileName)) {
      selected.set(meta.storeKey, { entry, meta });
    }
  }

  return [...selected.values()]
    .sort((a, b) => b.meta.timestamp.localeCompare(a.meta.timestamp) || a.meta.storeKey.localeCompare(b.meta.storeKey))
    .map(item => item.entry);
}

function hasBrokenText(value = '') {
  return /[ï¿½Ã—Ã¢]/.test(String(value || ''));
}

function normalizeChainProducts(products, chain) {
  return asArray(products)
    .filter(product => product && typeof product === 'object')
    .map(product => {
      const next = {
        ...product,
        chain: chain.id,
        chainName: normalizeDisplayText(chain.name),
        name: normalizeDisplayText(product.name),
        storeName: normalizeDisplayText(product.storeName),
        manufacturer: normalizeOptionalText(product.manufacturer),
        country: normalizeOptionalText(product.country),
        unitQty: normalizeOptionalText(product.unitQty),
      };
      const preferredStoreName = normalizeDisplayText(chain.storeName || chain.name);

      if (!next.storeName || next.storeName === normalizeDisplayText(product.chainName) || hasBrokenText(next.storeName)) {
        next.storeName = preferredStoreName;
      }

      return next;
    });
}

for (const chain of CHAINS) {
  const canonical = CANONICAL_CHAIN_TEXT[chain.id] || {};
  chain.name = canonical.name || normalizeDisplayText(chain.name);
  if (chain.storeName || canonical.storeName) {
    chain.storeName = canonical.storeName || normalizeDisplayText(chain.storeName);
  }
  if (chain.reason) {
    chain.reason = normalizeDisplayText(chain.reason);
  }
}

function appendAll(target, items) {
  for (const item of items) target.push(item);
}

function buildSourceKey(values = []) {
  return asArray(values)
    .map(value => extractFilename(value))
    .filter(Boolean)
    .join('|');
}

function getDefaultCacheMaxAgeMs(chain) {
  switch (chain.platform) {
    case 'alma':
      return 4 * 60 * 60 * 1000;
    case 'wolt':
      return 3 * 60 * 60 * 1000;
    case 'inactive':
      return 7 * 24 * 60 * 60 * 1000;
    default:
      return 6 * 60 * 60 * 1000;
  }
}

function getDefaultProbeIntervalMs(chain) {
  switch (chain.platform) {
    case 'alma':
      return 0;
    case 'wolt':
      return 75 * 60 * 1000;
    case 'inactive':
      return 12 * 60 * 60 * 1000;
    default:
      return 90 * 60 * 1000;
  }
}

function isTimestampFresh(timestamp, maxAgeMs) {
  if (!timestamp || !Number.isFinite(maxAgeMs) || maxAgeMs <= 0) return false;
  return (Date.now() - timestamp) <= maxAgeMs;
}

function isCacheFresh(meta, maxAgeMs) {
  return isTimestampFresh(meta && meta.updatedAt, maxAgeMs);
}

function hasUsableCacheMeta(chain, meta) {
  if (!meta || typeof meta !== 'object') return false;
  return Number(meta.count || 0) > 0 || chain.platform === 'inactive';
}

function formatAgeMs(ageMs) {
  const minutes = Math.max(1, Math.round(ageMs / 60000));
  if (minutes < 60) return `${minutes} ×“×§'`;
  const hours = Math.floor(minutes / 60);
  const restMinutes = minutes % 60;
  if (restMinutes === 0) return `${hours} ×©'`;
  return `${hours} ×©' ${restMinutes} ×“×§'`;
}

function extractProducts(xml, chain, storeId, storeName) {
  const root = xml?.Root || xml?.Prices || xml?.root || Object.values(xml||{})[0];
  if (!root) return [];
  const sid   = String(firstDefined(root?.StoreId, root?.StoreID, root?.storeid, root?.Storeid, storeId, '0'));
  const sname = String(firstDefined(root?.StoreName, root?.storeName, root?.storename, storeName, chain.name));
  const arr   = [
    ...asArray(root?.Products?.Product),
    ...asArray(root?.Products?.Item),
    ...asArray(root?.products?.product),
    ...asArray(root?.products?.item),
    ...asArray(root?.Items?.Item),
    ...asArray(root?.items?.item),
    ...asArray(root?.Item),
  ];
  return arr.map(p => {
    const price = parseFloat(firstDefined(
      p.ItemPrice, p.itemprice, p.Price, p.price,
      p.UnitOfMeasurePrice, p.unitofmeasureprice, 0
    ));
    const name  = String(firstDefined(
      p.ItemName, p.itemname,
      p.ManufacturerItemDescription, p.manufactureritemdescription,
      p.ItemNm, p.itemnm, ''
    )).trim();
    const code  = firstDefined(
      p.ItemCode, p.itemcode,
      p.ItemBarcode, p.itembarcode,
      p.Barcode, p.barcode, ''
    );
    if (!name || price <= 0) return null;
    const qty   = parseFloat(firstDefined(p.Quantity, p.quantity, p.QuantityInPackage, p.quantityinpackage, 1));
    const uMeas = String(firstDefined(p.UnitOfMeasure, p.unitofmeasure, p.MeasureUnit, p.measureunit, '')).trim();
    const uP    = parseFloat(firstDefined(p.UnitOfMeasurePrice, p.unitofmeasureprice, 0));
    return {
      id: `${chain.id}_${sid}_${code}`,
      barcode: String(code), name, price,
      unitPrice: uP || (qty > 0 ? price/qty : price),
      unitQty: `${qty} ${uMeas}`.trim(),
      manufacturer: String(firstDefined(
        p.ManufacturerName, p.manufacturername,
        p.ManufactureName, p.manufacturename, ''
      )).trim(),
      country: String(firstDefined(
        p.ManufactureCountry, p.manufacturecountry,
        p.CountryOfOrigin, p.countryoforigin, ''
      )),
      chain: chain.id, chainName: chain.name,
      storeId: String(sid), storeName: String(sname)
    };
  }).filter(Boolean);
}

// â”€â”€ HTTP helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';

async function httpGet(url, responseType='arraybuffer', timeout=25000) {
  return axios.get(url, { timeout, responseType,
    headers: { 'User-Agent':UA, 'Accept':'*/*', 'Accept-Language':'he-IL,he;q=0.9' } });
}

async function fetchGzUrl(url, chain, emit, storeId, storeName) {
  const short = url.length > 80 ? '...' + url.slice(-70) : url;
  emit('info', `×ž×•×¨×™×“: ${short}`);
  try {
    const res = await httpGet(url);
    let buf   = Buffer.from(res.data);
    const kb  = Math.round(buf.length/1024);
    emit('info', `×”×ª×§×‘×œ ${kb} KB â€” ×ž×¤×¨×¡×¨...`);

    if (url.toLowerCase().includes('.zip')) {
      try { const z = new AdmZip(buf); const e=z.getEntries(); if(e.length) buf=e[0].getData(); } catch(e){}
    }

    const xml = await parseXml(buf);
    if (!xml) { emit('warn', 'Failed to parse XML'); return []; }
    const prods = extractProducts(xml, chain, storeId, storeName);
    if (prods.length) emit('ok', `Found ${prods.length.toLocaleString('he-IL')} products`);
    else emit('warn', 'File is empty - no products');
    return prods;
  } catch(e) {
    const msg = e.response ? `HTTP ${e.response.status}` : getErrorMessage(e);
    emit('error', `Error: ${msg} - ${short}`);
    return [];
  }
}

// â”€â”€ URL extractor (with HTML entity decoding) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function extractFileLinks(html, baseUrl) {
  if (!html || typeof html !== 'string') return [];
  // ×¤×¢× ×•×— HTML entities ×œ×¤× ×™ ×—×™×¤×•×© URLs
  const decoded = html
    .replace(/&amp;/g,  '&')
    .replace(/&lt;/g,   '<')
    .replace(/&gt;/g,   '>')
    .replace(/&quot;/g, '"');

  const links = new Set();
  let base;
  try { base = new URL(baseUrl.startsWith('http') ? baseUrl : 'http://'+baseUrl); } catch(e) { return []; }
  const hrefRe = /href\s*=\s*["']([^"']+\.(?:xml|gz|zip)[^"']*)["']/gi;
  const textRe = /https?:\/\/[^\s"'<>]+\.(?:xml|gz|zip)/gi;
  let m;
  while ((m = hrefRe.exec(decoded)) !== null) { try { links.add(new URL(m[1], base).href); } catch(e){} }
  while ((m = textRe.exec(decoded)) !== null) { links.add(m[0]); }
  return [...links];
}

async function inspectCerberusSource(chain) {
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;

  try {
    await client.access({
      host: CERBERUS_FTP_HOST,
      user: chain.ftpUser,
      password: '',
      secure: !!chain.ftpSecure,
      secureOptions: chain.ftpSecureOptions,
    });

    const list = await client.list('/');
    let files = selectLatestEntries(
      list.filter(file => /PriceFull/i.test(file.name) && /\.(gz|xml|zip)$/i.test(file.name)),
      file => file.name
    );

    if (!files.length) {
      files = selectLatestEntries(
        list.filter(file => /\.(gz|xml|zip)$/i.test(file.name)),
        file => file.name
      ).slice(0, 3);
    }

    return {
      sourceKey: `cerberus:${buildSourceKey(files.map(file => file.name))}`,
      sourceInfo: {
        files: files.map(file => file.name),
      },
      cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
    };
  } finally {
    client.close();
  }
}

async function inspectMatrixSource(chain) {
  const chainIds = Array.isArray(chain.chainId) ? chain.chainId : [chain.chainId];
  const res = await httpGet(`${MATRIX_BASE}/NBCompetitionRegulations.aspx`, 'text', 20000);
  const links = selectLatestEntries(
    extractFileLinks(res.data, MATRIX_BASE)
      .filter(link => /PriceFull/i.test(link))
      .filter(link => chainIds.some(cid => extractFilename(link).includes(cid))),
    link => link
  );

  return {
    sourceKey: `matrix:${buildSourceKey(links)}`,
    sourceInfo: {
      files: links.map(link => extractFilename(link)),
    },
    cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
  };
}

async function inspectLaibSource(chain) {
  const chainIds = Array.isArray(chain.chainId) ? chain.chainId : [chain.chainId];
  const parts = [];
  const files = [];

  for (const cid of chainIds) {
    const brRes = await httpGet(`${LAIB_BASE}/webapi/api/getbranches?edi=${cid}`, 'json', 20000);
    const branches = Array.isArray(brRes.data) ? brRes.data : [];

    if (!branches.length) {
      const matrixInfo = await inspectMatrixSource({ ...chain, chainId: cid });
      parts.push(`${cid}:${matrixInfo.sourceKey}`);
      appendAll(files, matrixInfo.sourceInfo.files || []);
      continue;
    }

    const uniqueFiles = new Set();
    for (const branch of branches) {
      const bnum = branch.number || branch.branchNumber || '';
      const filesRes = await httpGet(
        `${LAIB_BASE}/webapi/api/getfiles?edi=${cid}&branchNumber=${bnum}`,
        'json',
        15000
      );
      const branchFiles = Array.isArray(filesRes.data) ? filesRes.data : [];
      const priceFiles = selectLatestEntries(
        branchFiles.filter(file => /pricefull/i.test(file.fileType || '') || /PriceFull/i.test(file.fileName || '')),
        file => file.fileName
      );

      for (const file of priceFiles) uniqueFiles.add(file.fileName);
    }

    const names = [...uniqueFiles].sort();
    parts.push(`${cid}:laib:${names.join('|')}`);
    appendAll(files, names);
  }

  return {
    sourceKey: parts.join('||'),
    sourceInfo: { files },
    cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
  };
}

async function inspectShufersalSource(chain) {
  const indexUrl = `${SHUFERSAL_BASE}/FileObject/UpdateCategory?catID=2&storeId=0&page=1`;
  const res = await httpGet(indexUrl, 'text', 25000);
  let fileLinks = [];

  try {
    const xml = await parser.parseStringPromise(res.data);
    const flatten = (obj) => {
      if (!obj || typeof obj !== 'object') return;
      for (const [key, value] of Object.entries(obj)) {
        if (key === 'FileNm' || key === 'FileName') {
          const fileName = String(value);
          if (/PriceFull/i.test(fileName)) {
            fileLinks.push(`${SHUFERSAL_BASE}/FileObject/UpdateCategory?catID=2&storeId=0&fileNm=${encodeURIComponent(fileName)}`);
          }
        } else {
          flatten(value);
        }
      }
    };
    flatten(xml);
  } catch (error) {}

  if (!fileLinks.length) {
    fileLinks = extractFileLinks(res.data, SHUFERSAL_BASE).filter(link => /PriceFull/i.test(link));
  }

  const limited = selectLatestEntries(fileLinks, link => link);
  return {
    sourceKey: `shufersal:${buildSourceKey(limited)}`,
    sourceInfo: {
      files: limited.map(link => extractFilename(link)),
    },
    cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
  };
}

async function inspectHaziHinamSource(chain) {
  const baseUrl = 'https://shop.hazi-hinam.co.il/Prices';
  const today = new Date().toISOString().slice(0, 10);
  const res = await httpGet(`${baseUrl}?d=${today}&t=null&f=null`, 'text', 20000);
  const links = selectLatestEntries(
    extractFileLinks(res.data, 'https://shop.hazi-hinam.co.il').filter(link => /PriceFull/i.test(link)),
    link => link
  );

  return {
    sourceKey: `hazihinam:${buildSourceKey(links)}`,
    sourceInfo: {
      files: links.map(link => extractFilename(link)),
    },
    cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
  };
}

async function inspectDirectSource(chain) {
  const res = await httpGet(chain.indexUrl, 'text', 20000);
  const links = selectLatestEntries(
    extractFileLinks(res.data, chain.indexUrl).filter(link => /PriceFull|\.xml|\.gz/i.test(link)),
    link => link
  );

  return {
    sourceKey: `direct:${buildSourceKey(links) || chain.indexUrl}`,
    sourceInfo: {
      files: links.map(link => extractFilename(link)),
      url: chain.indexUrl,
    },
    cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
  };
}

async function inspectChainSource(chain) {
  switch (chain.platform) {
    case 'cerberus':
      return inspectCerberusSource(chain);
    case 'laib':
      return inspectLaibSource(chain);
    case 'wolt':
      return inspectWoltSource(chain);
    case 'shufersal':
      return inspectShufersalSource(chain);
    case 'hazihinam':
      return inspectHaziHinamSource(chain);
    case 'direct':
      return inspectDirectSource(chain);
    case 'alma':
      return {
        sourceKey: '',
        sourceInfo: {
          strategy: 'ttl',
          homeUrl: chain.homeUrl || '',
        },
        cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
      };
    case 'inactive':
      return {
        sourceKey: `inactive:${chain.id}`,
        sourceInfo: {
          reason: chain.reason || '',
        },
        cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
      };
    default:
      return {
        sourceKey: '',
        sourceInfo: null,
        cacheMaxAgeMs: getDefaultCacheMaxAgeMs(chain),
      };
  }
}

// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
// 1. CERBERUS â€” FTP
// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
const CERBERUS_FTP_HOST = 'url.retail.publishedprices.co.il';

async function fetchCerberus(chain, emit) {
  emit('start', `×ž×ª×—×‘×¨ ×œ-Cerberus FTP ×¢×‘×•×¨ ${chain.name}`, { platform:'Cerberus FTP' });
  emit('info', `FTP host: ${CERBERUS_FTP_HOST} | user: ${chain.ftpUser}`);

  const client = new ftp.Client(30000);
  client.ftp.verbose = false;

  const allProducts = [];

  try {
    await client.access({
      host: CERBERUS_FTP_HOST,
      user: chain.ftpUser,
      password: '',
      secure: !!chain.ftpSecure,
      secureOptions: chain.ftpSecureOptions,
    });
    emit('ok', `FTP ×ž×—×•×‘×¨ ×‘×”×¦×œ×—×”`);

    // ×¨×©×™×ž×ª ×§×‘×¦×™ï¿½?
    const list = await client.list('/');
    const priceFiles = selectLatestEntries(
      list.filter(f => /PriceFull/i.test(f.name) && /\.(gz|xml|zip)$/.test(f.name)),
      f => f.name
    );

    if (priceFiles.length === 0) {
      emit('warn', `No PriceFull files found on FTP (${list.length} files available)`);
      // × ×¡×” ×œ×”×•×¨×™×“ ×›×œ×©×”×•
      const anyFiles = selectLatestEntries(list.filter(f => /\.(gz|xml|zip)$/i.test(f.name)), f => f.name).slice(0, 3);
      if (anyFiles.length) {
        emit('info', `Trying fallback files: ${anyFiles.map(f=>f.name).join(', ')}`);
        priceFiles.push(...anyFiles);
      }
    } else {
      emit('info', `Found ${priceFiles.length} PriceFull files (out of ${list.length} files)`);
    }

    for (const [i, file] of priceFiles.entries()) {
      emit('progress', `×§×•×‘×¥ ${i+1}/${priceFiles.length}: ${file.name}`);
      try {
        // FTP binary download to buffer
        const bufParts = [];
        const writable = new Writable({ write(chunk,_,cb){ bufParts.push(chunk); cb(); } });
        await client.downloadTo(writable, file.name);
        const buf = Buffer.concat(bufParts);
        emit('info', `×”×•×¨×“: ${file.name} (${Math.round(buf.length/1024)} KB)`);
        const xml = await parseXml(buf);
        if (!xml) { emit('warn', `Could not parse: ${file.name}`); continue; }
        const prods = extractProducts(xml, chain);
        if (prods.length) {
          emit('ok', `${file.name}: ${prods.length.toLocaleString('he-IL')} products`);
          appendAll(allProducts, prods);
        } else {
          emit('warn', `${file.name}: ×¨×™×§`);
        }
      } catch(e) {
        emit('error', `Error in ${file.name}: ${getErrorMessage(e)}`);
      }
    }
  } catch(e) {
    const msg = getErrorMessage(e);
    emit('error', `FTP error: ${msg}`);
    if (typeof msg === 'string' && (msg.includes('ECONNREFUSED') || msg.includes('ENOTFOUND'))) {
      emit('info', 'FTP access is blocked from the current network - run from Israel');
    }
  } finally {
    client.close();
  }

  return allProducts;
}

async function fetchLaibSmart(chain, emit) {
  emit('start', `×ž×ª×—×‘×¨ ×œ-Laibcatalog ×¢×‘×•×¨ ${chain.name}`, { platform:'Laibcatalog' });
  const chainIds = Array.isArray(chain.chainId) ? chain.chainId : [chain.chainId];
  const allProducts = [];

  for (const cid of chainIds) {
    emit('info', `×‘×•×“×§ chain ID: ${cid}`);
    try {
      const brRes = await httpGet(`${LAIB_BASE}/webapi/api/getbranches?edi=${cid}`, 'json', 20000);
      const branches = Array.isArray(brRes.data) ? brRes.data : [];
      emit('info', `chain ${cid}: ${branches.length} branches`);

      if (branches.length === 0) {
        appendAll(allProducts, await fetchMatrixFallback({ ...chain, chainId: cid }, emit));
        continue;
      }

      const uniqueFiles = new Map();
      for (const branch of branches) {
        try {
          const bnum = branch.number || branch.branchNumber || '';
          const filesRes = await httpGet(
            `${LAIB_BASE}/webapi/api/getfiles?edi=${cid}&branchNumber=${bnum}`,
            'json',
            15000
          );
          const files = Array.isArray(filesRes.data) ? filesRes.data : [];
          const priceFiles = selectLatestEntries(
            files.filter(file => /pricefull/i.test(file.fileType || '') || /PriceFull/i.test(file.fileName || '')),
            file => file.fileName
          );

          if (priceFiles.length === 0 && files.length > 0) {
            emit('info', `Branch ${bnum}: ${files.length} files (not PriceFull)`);
            continue;
          }

          for (const file of priceFiles) {
            uniqueFiles.set(file.fileName, file);
          }
        } catch (error) {
          emit('warn', `Branch error: ${getErrorMessage(error)}`);
        }
      }

      const filesToFetch = [...uniqueFiles.values()];
      emit('info', `Found ${filesToFetch.length} unique PriceFull files`);
      for (const [index, file] of filesToFetch.entries()) {
        emit('progress', `×§×•×‘×¥ ${index + 1}/${filesToFetch.length}: ${file.fileName}`);
        const dlUrl = `${LAIB_BASE}/webapi/${cid}/${file.fileName}`;
        appendAll(allProducts, await fetchGzUrl(dlUrl, chain, emit));
      }
    } catch (error) {
      const msg = error.response ? `HTTP ${error.response.status}` : getErrorMessage(error);
      emit('error', `Laibcatalog error for ${cid}: ${msg}`);
    }
  }

  return allProducts;
}
const LAIB_BASE = 'https://laibcatalog.co.il';
const MATRIX_BASE = 'http://matrixcatalog.co.il';

async function fetchMatrixFallback(chain, emit) {
  const chainIds = Array.isArray(chain.chainId) ? chain.chainId : [chain.chainId];
  emit('info', `×ž× ×¡×” fallback ×œ-Matrix ×¢×‘×•×¨ ${chain.name}`);

  try {
    const res = await httpGet(`${MATRIX_BASE}/NBCompetitionRegulations.aspx`, 'text', 20000);
    const links = selectLatestEntries(
      extractFileLinks(res.data, MATRIX_BASE)
        .filter(link => /PriceFull/i.test(link))
        .filter(link => chainIds.some(cid => extractFilename(link).includes(cid))),
      link => link
    );

    if (!links.length) {
      emit('warn', 'No matching Matrix files were found');
      return [];
    }

    emit('info', `Found ${links.length} matching Matrix files`);
    const products = [];
    for (const [i, link] of links.entries()) {
      emit('progress', `Matrix ×§×•×‘×¥ ${i+1}/${links.length}: ${extractFilename(link)}`);
      appendAll(products, await fetchGzUrl(link, chain, emit));
    }
    return products;
  } catch (e) {
    emit('warn', `Matrix fallback × ×›×©×œ: ${getErrorMessage(e)}`);
    return [];
  }
}

async function fetchLaib(chain, emit) {
  return fetchLaibSmart(chain, emit);
}

// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
// 3. ×©×•×¤×¨×¡×œ (MultiPageWeb, HTTPS)
// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
const SHUFERSAL_BASE = 'https://prices.shufersal.co.il';

async function fetchShufersal(chain, emit) {
  emit('start', `×ž×ª×—×‘×¨ ×œ×©×•×¤×¨×¡×œ`, { platform:'Shufersal Web' });
  const indexUrl = `${SHUFERSAL_BASE}/FileObject/UpdateCategory?catID=2&storeId=0&page=1`;
  emit('info', `×›×ª×•×‘×ª: ${indexUrl}`);

  try {
    const res = await httpGet(indexUrl, 'text', 25000);
    emit('ok', `×”×ª×§×‘×œ×” ×ª×’×•×‘×”`);

    let fileLinks = [];

    // × ×™×¡×™×•×Ÿ XML
    try {
      const xml = await parser.parseStringPromise(res.data);
      const flatten = (obj) => {
        if (!obj || typeof obj !== 'object') return;
        for (const [k,v] of Object.entries(obj)) {
          if (k==='FileNm'||k==='FileName') {
            const n=String(v);
            if (/PriceFull/i.test(n))
              fileLinks.push(`${SHUFERSAL_BASE}/FileObject/UpdateCategory?catID=2&storeId=0&fileNm=${encodeURIComponent(n)}`);
          } else flatten(v);
        }
      };
      flatten(xml);
    } catch(e){}

    // ×’×™×‘×•×™ HTML (×¢ï¿½? decode entities)
    if (fileLinks.length === 0) {
      fileLinks = extractFileLinks(res.data, SHUFERSAL_BASE).filter(l => /PriceFull/i.test(l));
    }

    emit('info', `Found ${fileLinks.length} PriceFull files`);
    if (!fileLinks.length) { emit('warn', 'No files were found'); return []; }

    const limited = selectLatestEntries(fileLinks, link => link);
    const products = [];
    for (const [i,url] of limited.entries()) {
      emit('progress', `×§×•×‘×¥ ${i+1}/${limited.length}`);
      appendAll(products, await fetchGzUrl(url, chain, emit));
    }
    return products;
  } catch(e) {
    const msg = e.response ? `HTTP ${e.response.status}` : getErrorMessage(e);
    emit('error', `Shufersal error: ${msg}`);
    return [];
  }
}

// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
// 4. ×—×¦×™ ×—×™× ï¿½? (web ×—×“×©)
// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
async function fetchHaziHinam(chain, emit) {
  emit('start', 'Connecting to Hazi Hinam', { platform:'HaziHinam Web' });
  const BASE = 'https://shop.hazi-hinam.co.il/Prices';
  emit('info', `×›×ª×•×‘×ª: ${BASE}`);

  try {
    const today   = new Date().toISOString().slice(0,10);
    const res     = await httpGet(`${BASE}?d=${today}&t=null&f=null`, 'text', 20000);
    emit('ok', `HTTP ${res.status}`);

    // ×—×™×¤×•×© ×œ×™× ×§×™ï¿½? ×œ×§×‘×¦×™ï¿½?
    const links = selectLatestEntries(
      extractFileLinks(res.data, 'https://shop.hazi-hinam.co.il').filter(l => /PriceFull/i.test(l)),
      link => link
    );

    emit('info', `Found ${links.length} files`);
    if (!links.length) { emit('warn', 'No files were found - the page structure may have changed'); return []; }

    const products = [];
    for (const [i,url] of links.entries()) {
      emit('progress', `×§×•×‘×¥ ${i+1}/${links.length}`);
      appendAll(products, await fetchGzUrl(url, chain, emit));
    }
    return products;
  } catch(e) {
    emit('error', `Hazi Hinam error: ${getErrorMessage(e)}`);
    return [];
  }
}

// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
// 5. Direct (×§×•ï¿½?×•×¤, ×¢×“×Ÿ ×˜×‘×¢)
// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
async function fetchDirect(chain, emit) {
  emit('start', `×ž×ª×—×‘×¨ ×œ-${chain.name}`, { platform:'Direct' });
  emit('info', `×›×ª×•×‘×ª: ${chain.indexUrl}`);
  try {
    const res = await httpGet(chain.indexUrl, 'text', 20000);
    const ct  = (res.headers['content-type']||'').toLowerCase();
    emit('ok', `HTTP ${res.status} (${ct.split(';')[0]})`);

    if (ct.includes('xml') || (typeof res.data==='string' && res.data.trim().startsWith('<'))) {
      const xml = await parseXml(Buffer.from(res.data));
      if (xml) { const p=extractProducts(xml,chain); if(p.length){emit('ok',`${p.length} products`);return p;} }
    }
    const links = selectLatestEntries(
      extractFileLinks(res.data, chain.indexUrl).filter(l => /PriceFull|\.xml|\.gz/i.test(l)),
      link => link
    );
    emit('info', `Found ${links.length} links`);
    const prods = [];
    for (const [i,url] of links.entries()) {
      emit('progress', `×§×•×‘×¥ ${i+1}/${links.length}`);
      appendAll(prods, await fetchGzUrl(url, chain, emit));
    }
    return prods;
  } catch(e) {
    emit('error', `${chain.name}: ${getErrorMessage(e)}`);
    return [];
  }
}

async function fetchInactive(chain, emit) {
  emit('start', `×ž×“×œ×’ ×¢×œ ${chain.name}`, { platform:'Inactive' });
  emit('warn', chain.reason || 'No automatic data source is currently available for this chain');
  return [];
}

// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
// ????
// ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?ï¿½?
function buildRefreshPlan(options = {}) {
  const selectedChains = Array.isArray(options.chainIds) && options.chainIds.length
    ? CHAINS.filter(chain => options.chainIds.includes(chain.id))
    : CHAINS;
  const manifest = loadManifest();
  const manifestChains = manifest && manifest.chains && typeof manifest.chains === 'object'
    ? manifest.chains
    : {};

  return selectedChains.map(chain => {
    const meta = manifestChains[chain.id] || null;
    const cachedCount = Number(meta && meta.count || 0);
    const cacheMaxAgeMs = Number.isFinite(meta && meta.cacheMaxAgeMs)
      ? meta.cacheMaxAgeMs
      : getDefaultCacheMaxAgeMs(chain);
    const probeIntervalMs = getDefaultProbeIntervalMs(chain);
    const hasUsableCache = hasUsableCacheMeta(chain, meta);

    let action = 'live';
    let reason = hasUsableCache ? 'stale-cache' : 'missing-cache';

    if (!options.forceLive && hasUsableCache && isCacheFresh(meta, cacheMaxAgeMs)) {
      action = 'cache_fresh';
      reason = 'fresh-cache';
    } else if (
      !options.forceLive &&
      hasUsableCache &&
      meta &&
      meta.sourceKey &&
      isTimestampFresh(meta.checkedAt, probeIntervalMs)
    ) {
      action = 'cache_recent_check';
      reason = 'recent-check';
    }

    return {
      chain,
      meta,
      cachedCount,
      cacheMaxAgeMs,
      probeIntervalMs,
      hasUsableCache,
      action,
      reason,
    };
  });
}

async function fetchAllChains(emit = ()=>{}, options = {}) {
  const selectedChains = Array.isArray(options.chainIds) && options.chainIds.length
    ? CHAINS.filter(chain => options.chainIds.includes(chain.id))
    : CHAINS;
  const refreshPlan = Array.isArray(options.plan) && options.plan.length
    ? options.plan
    : buildRefreshPlan(options);
  const planByChainId = new Map(refreshPlan.map(item => [item.chain.id, item]));
  const safeEmit = (type, msg, data = {}) => emit(type, normalizeDisplayText(msg), data);

  safeEmit('start', `×ž×ª×—×™×œ ×©×œ×™×¤×” ×ž-${selectedChains.length} ×¨×©×ª×•×ª`, { total:selectedChains.length });
  const allProducts = [];
  const status = {};
  const chainResults = {};

  for (const [idx, chain] of selectedChains.entries()) {
    safeEmit('chain', `[${idx+1}/${selectedChains.length}] ${chain.name}`, {
      chainId: chain.id,
      chainName: chain.name,
      color: chain.color,
      platform: chain.platform,
      index: idx + 1,
      total: selectedChains.length
    });

    let products = [];
    let fromCache = false;
    let staleFallback = false;
    const t0 = Date.now();
    const cEmit = (type, msg, data) => safeEmit(type, msg, { chainId: chain.id, ...data });
    const plan = planByChainId.get(chain.id) || buildRefreshPlan({ ...options, chainIds: [chain.id] })[0];
    const cachedMeta = plan && plan.meta ? plan.meta : null;
    const cachedCount = plan && Number.isFinite(plan.cachedCount) ? plan.cachedCount : 0;
    const cacheMaxAgeMs = plan && Number.isFinite(plan.cacheMaxAgeMs)
      ? plan.cacheMaxAgeMs
      : getDefaultCacheMaxAgeMs(chain);
    const allowEmptyCache = chain.platform === 'inactive';
    let cached = null;
    let sourceProbe = null;
    const ensureCached = () => {
      if (cached !== null) return cached;
      cached = loadChainCache(chain.id);
      return cached;
    };
    const useCachedProducts = (message, type = 'ok') => {
      const cachedEntry = ensureCached();
      const cachedProducts = Array.isArray(cachedEntry && cachedEntry.products) ? cachedEntry.products : [];
      if (!cachedEntry || (!cachedProducts.length && !allowEmptyCache)) return false;
      products = cachedProducts;
      fromCache = true;
      cEmit(type, message);
      return true;
    };

    if (plan && plan.hasUsableCache) {
      cEmit('info', `Local cache found: ${cachedCount.toLocaleString('he-IL')} products`);
    }

    if (plan && plan.action === 'cache_fresh') {
      if (!useCachedProducts(`Cache is still fresh (${formatAgeMs(Date.now() - cachedMeta.updatedAt)}) - skipping remote check`)) {
        cEmit('warn', 'Cache metadata exists but the local file is missing - switching to live fetch');
      }
    } else if (plan && plan.action === 'cache_recent_check') {
      if (!useCachedProducts(`Source was checked recently (${formatAgeMs(Date.now() - cachedMeta.checkedAt)}) - using local cache`)) {
        cEmit('warn', 'Cache metadata exists but the local file is missing - switching to live fetch');
      }
    }

    if (!fromCache) {
      try {
        sourceProbe = await inspectChainSource(chain);
      } catch (error) {
        cEmit('warn', 'Update check failed: ' + getErrorMessage(error));
      }

      const cachedEntry = plan && plan.hasUsableCache ? ensureCached() : null;
      const cachedProducts = Array.isArray(cachedEntry && cachedEntry.products) ? cachedEntry.products : [];
      const cacheMatchesSource = !!(
        cachedEntry &&
        (cachedProducts.length > 0 || allowEmptyCache) &&
        sourceProbe &&
        sourceProbe.sourceKey &&
        cachedMeta &&
        cachedMeta.sourceKey === sourceProbe.sourceKey
      );

      if (cacheMatchesSource) {
        products = cachedProducts;
        fromCache = true;
        cEmit('ok', 'Source files are unchanged - using local cache');
      }
    }


    if (!fromCache) {
      try {
        switch(chain.platform) {
          case 'cerberus':  products = await fetchCerberus(chain, cEmit);  break;
          case 'laib':      products = await fetchLaibSmart(chain, cEmit); break;
          case 'wolt':      products = await fetchWolt(chain, cEmit);      break;
          case 'shufersal': products = await fetchShufersal(chain, cEmit); break;
          case 'hazihinam': products = await fetchHaziHinam(chain, cEmit); break;
          case 'alma':      products = await fetchAlma(chain, cEmit);      break;
          case 'direct':    products = await fetchDirect(chain, cEmit);    break;
          case 'inactive':  products = await fetchInactive(chain, cEmit);  break;
        }
      } catch (error) {
        safeEmit('error', `General error in ${chain.name}: ${getErrorMessage(error)}`, { chainId: chain.id });
      }

      if ((!products || !products.length) && (cachedCount > 0 || allowEmptyCache)) {
        const cachedEntry = ensureCached();
        const cachedProducts = Array.isArray(cachedEntry && cachedEntry.products) ? cachedEntry.products : [];
        if (cachedEntry && (cachedProducts.length > 0 || allowEmptyCache)) {
          products = cachedProducts;
          fromCache = true;
          staleFallback = true;
          const fallbackAge = cachedMeta && cachedMeta.updatedAt
            ? formatAgeMs(Date.now() - cachedMeta.updatedAt)
            : 'previous cache';
          cEmit('warn', `Live fetch failed - using last cache (${fallbackAge})`);
        }
      }
    }

    products = normalizeChainProducts(products, chain);
    const uniqueProducts = dedupeProductSet(products, { scope: 'chain' }).products;
    if (uniqueProducts.length !== products.length) {
      cEmit('info', `×”×•×¡×¨×• ${products.length - uniqueProducts.length} ×›×¤×™×œ×•×™×•×ª`, {
        before: products.length,
        after: uniqueProducts.length
      });
    }
    products = uniqueProducts;

    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    status[chain.id] = {
      name: chain.name,
      platform: chain.platform,
      count: products.length,
      success: products.length > 0,
      elapsed,
      fromCache,
      staleFallback
    };

    chainResults[chain.id] = {
      chain,
      products,
      meta: {
        sourceKey: sourceProbe?.sourceKey || cachedMeta?.sourceKey || '',
        sourceInfo: sourceProbe?.sourceInfo || cachedMeta?.sourceInfo || null,
        cacheMaxAgeMs,
        updatedAt: fromCache ? (cachedMeta?.updatedAt || Date.now()) : Date.now(),
        checkedAt: !fromCache || sourceProbe
          ? Date.now()
          : (cachedMeta?.checkedAt || cachedMeta?.updatedAt || Date.now()),
        fromCache,
        staleFallback
      }
    };

    if (products.length > 0) {
      safeEmit('ok', `${chain.name}: ${products.length.toLocaleString('he-IL')} products (${elapsed}s${fromCache ? ', cache' : ''})`, {
        chainId: chain.id,
        count: products.length,
        fromCache,
        staleFallback
      });
    } else {
      safeEmit('warn', `${chain.name}: no products were loaded (${elapsed}s)`, { chainId: chain.id });
    }

    appendAll(allProducts, products);
  }

  const uniqueAllProducts = dedupeProductSet(allProducts, { scope: 'chain' }).products;
  safeEmit('done', `Done: ${uniqueAllProducts.length.toLocaleString('he-IL')} products`, {
    total: uniqueAllProducts.length,
    chains: Object.values(status).map(item => ({
      name: item.name,
      count: item.count,
      success: item.success,
      fromCache: item.fromCache
    }))
  });

  return { products: uniqueAllProducts, status, chainResults };
}

// URL ×™×©×™×¨ ×ž×”×ž×ž×©×§
async function fetchFromUrl(url, chain, emit=()=>{}) {
  const safeEmit = (type, msg, data = {}) => emit(type, normalizeDisplayText(msg), data);
  safeEmit('start', `×˜×•×¢×Ÿ ×ž-URL ×™×©×™×¨: ${url}`, { platform:'direct' });
  return await fetchGzUrl(url, chain, safeEmit, '0', chain.name);
}

module.exports = { fetchAllChains, buildRefreshPlan, CHAINS, fetchFromUrl };
