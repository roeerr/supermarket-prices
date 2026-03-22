const axios = require('axios');

const WOLT_API_BASE = 'https://consumer-api.wolt.com';
const WOLT_HTML_LANGUAGE = 'he';
const WOLT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36';
const WOLT_REQUEST_DELAY_MS = 700;
const WOLT_HTML_DELAY_MS = 450;
const WOLT_MAX_RETRIES = 2;
const WOLT_BACKOFF_MS = 4000;

let nextWoltRequestAt = 0;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getErrorMessage(err) {
  if (!err) return 'Unknown error';
  if (typeof err === 'string') return err;
  if (typeof err.message === 'string' && err.message) return err.message;
  if (typeof err.code === 'string' && err.code) return err.code;
  return String(err);
}

function shortText(text, max = 80) {
  const value = String(text || '').trim();
  if (value.length <= max) return value;
  return value.slice(0, max - 3) + '...';
}

function firstText() {
  for (const value of arguments) {
    if (typeof value === 'string' && value.trim()) return value.trim();
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  }
  return '';
}

function asArray(value) {
  return Array.isArray(value) ? value : (value ? [value] : []);
}

function toPrice(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num / 100 : 0;
}

function normalizeUnitName(unit = '') {
  const normalized = String(unit || '').trim().toLowerCase();
  switch (normalized) {
    case 'millilitre':
      return 'ml';
    case 'litre':
      return 'l';
    case 'gram':
      return 'g';
    case 'kilogram':
      return 'kg';
    default:
      return normalized;
  }
}

function getRetryAfterMs(error) {
  const raw = error && error.response && error.response.headers
    ? error.response.headers['retry-after']
    : null;

  if (!raw) return 0;

  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds > 0) {
    return Math.round(seconds * 1000);
  }

  const dateMs = Date.parse(raw);
  if (Number.isFinite(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }

  return 0;
}

function isRetriableError(error) {
  const status = error && error.response && error.response.status;
  return status === 429 || status === 408 || status === 425 || status === 502 || status === 503 || status === 504;
}

function isRateLimitError(error) {
  return !!(error && error.response && error.response.status === 429);
}

async function waitForWoltSlot() {
  const waitMs = nextWoltRequestAt - Date.now();
  if (waitMs > 0) await sleep(waitMs);
}

function reserveWoltSlot(delayMs) {
  nextWoltRequestAt = Math.max(nextWoltRequestAt, Date.now() + Math.max(0, delayMs));
}

function buildHeaders(extra = {}) {
  return {
    'User-Agent': WOLT_UA,
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'he-IL,he;q=0.9,en;q=0.8',
    'App-Language': WOLT_HTML_LANGUAGE,
    'App-Locale': WOLT_HTML_LANGUAGE,
    ...extra,
  };
}

async function requestWithThrottle(url, config = {}, options = {}) {
  const normalDelayMs = Number.isFinite(options.normalDelayMs) ? options.normalDelayMs : WOLT_REQUEST_DELAY_MS;
  const retries = Number.isFinite(options.retries) ? options.retries : WOLT_MAX_RETRIES;
  const baseBackoffMs = Number.isFinite(options.baseBackoffMs) ? options.baseBackoffMs : WOLT_BACKOFF_MS;

  for (let attempt = 0; attempt <= retries; attempt++) {
    await waitForWoltSlot();

    try {
      const response = await axios.get(url, {
        timeout: 30000,
        ...config,
        headers: buildHeaders(config.headers),
      });
      reserveWoltSlot(normalDelayMs);
      return response.data;
    } catch (error) {
      const retryAfterMs = getRetryAfterMs(error);
      const backoffMs = Math.max(retryAfterMs, baseBackoffMs * (attempt + 1));
      reserveWoltSlot(backoffMs);

      if (attempt < retries && isRetriableError(error)) {
        continue;
      }

      throw error;
    }
  }

  throw new Error('Wolt request failed');
}

function extractQueryState(html) {
  const match = String(html || '').match(/<script type="application\/json" class="query-state">([\s\S]*?)<\/script>/i);
  if (!match) return null;

  try {
    return JSON.parse(match[1]);
  } catch (error) {
    return null;
  }
}

function findQuery(queryState, predicate) {
  const queries = queryState && Array.isArray(queryState.queries) ? queryState.queries : [];
  return queries.find(query => predicate(query && query.queryKey, query && query.state));
}

function getListingFromQueryState(queryState, venueSlug) {
  const query = findQuery(queryState, queryKey =>
    Array.isArray(queryKey) &&
    queryKey[0] === 'venue-assortment' &&
    queryKey[1] === 'category-listing' &&
    queryKey[2] === venueSlug
  );

  return query && query.state ? query.state.data : null;
}

function getCategoryFromQueryState(queryState, venueSlug, categorySlug) {
  const query = findQuery(queryState, queryKey =>
    Array.isArray(queryKey) &&
    queryKey[0] === 'venue-assortment' &&
    queryKey[1] === 'category' &&
    queryKey[2] === venueSlug &&
    queryKey[3] === categorySlug
  );

  const pages = query && query.state && query.state.data && Array.isArray(query.state.data.pages)
    ? query.state.data.pages
    : [];

  return pages[0] || null;
}

function buildCategoryUrl(chain, categorySlug) {
  return `${chain.venueUrl.replace(/\/$/, '')}/items/${encodeURIComponent(categorySlug)}`;
}

async function fetchListingFromApi(chain) {
  return requestWithThrottle(
    `${WOLT_API_BASE}/consumer-api/consumer-assortment/v1/venues/slug/${chain.venueSlug}/assortment`,
    {
      params: {
        language: WOLT_HTML_LANGUAGE,
      },
    },
    {
      normalDelayMs: WOLT_REQUEST_DELAY_MS,
    }
  );
}

async function fetchListingFromHtml(chain) {
  const html = await requestWithThrottle(
    chain.venueUrl,
    {
      responseType: 'text',
      headers: {
        Accept: 'text/html,application/xhtml+xml',
      },
    },
    {
      normalDelayMs: WOLT_HTML_DELAY_MS,
      retries: 1,
      baseBackoffMs: 3000,
    }
  );

  const queryState = extractQueryState(html);
  const listing = getListingFromQueryState(queryState, chain.venueSlug);
  if (!listing) {
    throw new Error('Wolt root page did not include a category listing');
  }

  return listing;
}

async function fetchCategoryPageFromApi(chain, categorySlug, pageToken) {
  return requestWithThrottle(
    `${WOLT_API_BASE}/consumer-api/consumer-assortment/v1/venues/slug/${chain.venueSlug}/assortment/categories/slug/${categorySlug}`,
    {
      params: {
        language: WOLT_HTML_LANGUAGE,
        ...(pageToken ? { page_token: pageToken } : {}),
      },
    },
    {
      normalDelayMs: WOLT_REQUEST_DELAY_MS,
    }
  );
}

async function fetchCategoryPageFromHtml(chain, categorySlug) {
  const html = await requestWithThrottle(
    buildCategoryUrl(chain, categorySlug),
    {
      responseType: 'text',
      headers: {
        Accept: 'text/html,application/xhtml+xml',
      },
    },
    {
      normalDelayMs: WOLT_HTML_DELAY_MS,
      retries: 1,
      baseBackoffMs: 3000,
    }
  );

  const queryState = extractQueryState(html);
  const page = getCategoryFromQueryState(queryState, chain.venueSlug, categorySlug);
  if (!page) {
    throw new Error(`Wolt category page was missing dehydrated data for ${categorySlug}`);
  }

  return page;
}

function collectLeafCategories(categories, parentPath = '', target = []) {
  for (const category of asArray(categories)) {
    if (!category || !category.slug) continue;

    const nextPath = [parentPath, firstText(category.name, category.slug)].filter(Boolean).join(' > ');
    const subcategories = asArray(category.subcategories);

    if (subcategories.length > 0) {
      collectLeafCategories(subcategories, nextPath, target);
      continue;
    }

    target.push({
      slug: category.slug,
      name: firstText(category.name, category.slug),
      path: nextPath,
    });
  }

  return target;
}

function getSourceInfo(chain, listing) {
  const categories = collectLeafCategories(listing && listing.categories);
  return {
    sourceKey: listing && listing.assortment_id
      ? `wolt:${chain.venueSlug}:${listing.assortment_id}`
      : `wolt:${chain.venueSlug}`,
    sourceInfo: {
      venueSlug: chain.venueSlug,
      venueUrl: chain.venueUrl,
      assortmentId: listing && listing.assortment_id ? listing.assortment_id : '',
      categories: categories.length,
    },
  };
}

function getImageUrl(item) {
  const images = asArray(item && item.images);
  for (const image of images) {
    if (image && typeof image.url === 'string' && image.url.trim()) {
      return image.url.trim();
    }
  }
  return '';
}

function mapWoltItem(item, chain, category, index) {
  if (!item || typeof item !== 'object') return null;

  const name = firstText(item.name);
  const price = toPrice(item.price);
  if (!name || !Number.isFinite(price) || price <= 0) return null;

  const barcode = firstText(item.barcode_gtin, item.barcode);
  const unitPrice = toPrice(item && item.unit_price && item.unit_price.price);
  const derivedUnitQty = [
    item && item.unit_price && item.unit_price.base ? String(item.unit_price.base) : '',
    normalizeUnitName(item && item.unit_price && item.unit_price.unit),
  ].filter(Boolean).join(' ');

  return {
    id: `${chain.id}_${chain.venueSlug}_${barcode || item.id || `${category.slug}_${index}`}`,
    barcode: barcode || '',
    name,
    price,
    unitPrice: unitPrice > 0 ? unitPrice : price,
    unitQty: firstText(item.unit_info, derivedUnitQty),
    manufacturer: firstText(item.brand, item.brand_name, item.manufacturer, item.producer),
    country: firstText(item.country_of_origin, item.origin_country, item.origin),
    storeId: chain.venueSlug,
    storeName: chain.storeName || chain.name,
    chain: chain.id,
    chainName: chain.name,
    imageUrl: getImageUrl(item),
  };
}

function appendMappedItems(target, items, chain, category) {
  let added = 0;
  const list = asArray(items);

  for (let i = 0; i < list.length; i++) {
    const mapped = mapWoltItem(list[i], chain, category, i);
    if (!mapped) continue;
    target.push(mapped);
    added += 1;
  }

  return added;
}

async function inspectWoltSource(chain) {
  try {
    const listing = await fetchListingFromApi(chain);
    return getSourceInfo(chain, listing);
  } catch (apiError) {
    const listing = await fetchListingFromHtml(chain);
    return getSourceInfo(chain, listing);
  }
}

async function fetchWolt(chain, emit) {
  emit('start', `Connecting to Wolt assortment for ${chain.name}`, { platform: 'Wolt Assortment API' });

  let listing = null;
  let htmlFallbackOnly = false;

  try {
    listing = await fetchListingFromApi(chain);
  } catch (apiError) {
    emit('warn', `Wolt API listing failed: ${getErrorMessage(apiError)} - falling back to the venue page`);
    listing = await fetchListingFromHtml(chain);
    htmlFallbackOnly = true;
  }

  const categories = collectLeafCategories(listing && listing.categories);
  if (!categories.length) {
    emit('warn', 'Wolt listing returned no leaf categories');
    return [];
  }

  emit('info', `Wolt branch: ${chain.storeName || chain.name} | ${categories.length} leaf categories`);

  const products = [];
  let fallbackAnnounced = htmlFallbackOnly;
  let partialCategories = 0;

  for (let i = 0; i < categories.length; i++) {
    const category = categories[i];
    const label = shortText(category.path || category.name || category.slug, 90);

    if (i === 0 || i === categories.length - 1 || i % 10 === 0) {
      emit('progress', `Category ${i + 1}/${categories.length}: ${label}`);
    }

    let firstPage = null;
    let usedHtmlFallback = htmlFallbackOnly;

    try {
      firstPage = htmlFallbackOnly
        ? await fetchCategoryPageFromHtml(chain, category.slug)
        : await fetchCategoryPageFromApi(chain, category.slug, null);
    } catch (error) {
      if (!htmlFallbackOnly && isRateLimitError(error)) {
        htmlFallbackOnly = true;
        usedHtmlFallback = true;

        if (!fallbackAnnounced) {
          emit('warn', 'Wolt API is rate-limiting - switching to safe HTML fallback for the remaining categories');
          fallbackAnnounced = true;
        }

        try {
          firstPage = await fetchCategoryPageFromHtml(chain, category.slug);
        } catch (fallbackError) {
          emit('warn', `Wolt category ${category.slug} failed: ${getErrorMessage(fallbackError)}`);
          continue;
        }
      } else {
        emit('warn', `Wolt category ${category.slug} failed: ${getErrorMessage(error)}`);
        continue;
      }
    }

    const categoryProducts = appendMappedItems(products, firstPage && firstPage.items, chain, category);
    let nextPageToken = firstPage && firstPage.metadata ? firstPage.metadata.next_page_token : null;

    if (usedHtmlFallback && nextPageToken) {
      partialCategories += 1;
    }

    if (!usedHtmlFallback) {
      let loadedPages = 1;

      while (nextPageToken) {
        try {
          const nextPage = await fetchCategoryPageFromApi(chain, category.slug, nextPageToken);
          appendMappedItems(products, nextPage && nextPage.items, chain, category);
          nextPageToken = nextPage && nextPage.metadata ? nextPage.metadata.next_page_token : null;
          loadedPages += 1;
        } catch (error) {
          if (isRateLimitError(error)) {
            htmlFallbackOnly = true;

            if (!fallbackAnnounced) {
              emit('warn', 'Wolt API is rate-limiting - switching to safe HTML fallback for the remaining categories');
              fallbackAnnounced = true;
            }

            emit('warn', `Wolt category ${category.slug}: pagination stopped after ${loadedPages} pages`);
            partialCategories += 1;
          } else {
            emit('warn', `Wolt category ${category.slug} page ${loadedPages + 1} failed: ${getErrorMessage(error)}`);
          }
          break;
        }
      }
    }

    if (categoryProducts > 0 && (i === 0 || i === categories.length - 1 || i % 10 === 0 || categoryProducts >= 40)) {
      emit('info', `${label}: ${categoryProducts.toLocaleString('he-IL')} products`);
    }
  }

  if (partialCategories > 0) {
    emit('warn', `Wolt safe mode: ${partialCategories} categories were loaded from the first page only`);
  }

  return products;
}

module.exports = {
  fetchWolt,
  inspectWoltSource,
};
