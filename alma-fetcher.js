const fs = require('fs');
const path = require('path');
const net = require('net');
const vm = require('vm');
const axios = require('axios');
const { spawn } = require('child_process');
const WebSocket = require('./vendor/ws');

const ALMA_BASE = 'https://www.alma-market.co.il';
const ALMA_APP_ID = 4;
const ALMA_PAGE_SIZE = 200;
const CHROME_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseNumber(value, fallback = 0) {
  const num = parseFloat(value);
  return Number.isFinite(num) ? num : fallback;
}

function firstText() {
  for (const value of arguments) {
    if (typeof value === 'string' && value.trim()) return value.trim();
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    if (value && typeof value === 'object' && typeof value.name === 'string' && value.name.trim()) {
      return value.name.trim();
    }
  }
  return '';
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

function getChromeExecutable() {
  const candidates = [
    process.env.CHROME_PATH,
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }

  return null;
}

function removeDirSafe(dirPath) {
  if (!dirPath || !fs.existsSync(dirPath)) return;
  try {
    if (typeof fs.rmSync === 'function') {
      fs.rmSync(dirPath, { recursive: true, force: true });
    } else {
      fs.rmdirSync(dirPath, { recursive: true });
    }
  } catch (e) {}
}

function terminateProcess(child) {
  return new Promise(resolve => {
    if (!child || !child.pid) return resolve();
    if (child.exitCode !== null || child.killed) return resolve();

    child.once('exit', () => resolve());

    try {
      if (process.platform === 'win32') {
        const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], {
          stdio: 'ignore',
          windowsHide: true,
        });
        killer.once('exit', () => resolve());
        killer.once('error', () => {
          try { child.kill(); } catch (e) {}
          setTimeout(resolve, 500);
        });
      } else {
        child.kill('SIGTERM');
        setTimeout(resolve, 500);
      }
    } catch (e) {
      resolve();
    }
  });
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = address && address.port;
      server.close(err => err ? reject(err) : resolve(port));
    });
  });
}

async function getDebuggerPageUrl(port, timeoutMs = 15000) {
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    try {
      const res = await axios.get(`http://127.0.0.1:${port}/json/list`, { timeout: 1000 });
      const page = (Array.isArray(res.data) ? res.data : []).find(target => target.type === 'page' && target.webSocketDebuggerUrl);
      if (page) return page.webSocketDebuggerUrl;
    } catch (e) {}

    await sleep(250);
  }

  throw new Error('Failed to connect to Chrome DevTools');
}

class CdpSession {
  constructor(webSocket) {
    this.webSocket = webSocket;
    this.nextId = 1;
    this.pending = new Map();
    this.waiters = [];
    this.closed = false;

    webSocket.on('message', data => this.handleMessage(data));
    webSocket.on('error', err => this.handleClose(err));
    webSocket.on('close', () => this.handleClose(new Error('Chrome DevTools connection closed')));
  }

  handleMessage(raw) {
    let message;
    try {
      message = JSON.parse(String(raw));
    } catch (e) {
      return;
    }

    if (message.id) {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(message.error.message || 'CDP error'));
      else pending.resolve(message.result || {});
      return;
    }

    if (!message.method) return;
    const nextWaiters = [];
    for (const waiter of this.waiters) {
      if (waiter.method === message.method) {
        let matched = false;
        try {
          matched = waiter.predicate(message.params || {});
        } catch (e) {
          waiter.reject(e);
          continue;
        }

        if (matched) {
          clearTimeout(waiter.timer);
          waiter.resolve(message.params || {});
          continue;
        }
      }
      nextWaiters.push(waiter);
    }
    this.waiters = nextWaiters;
  }

  handleClose(error) {
    if (this.closed) return;
    this.closed = true;

    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();

    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.waiters = [];
  }

  send(method, params) {
    if (this.closed) return Promise.reject(new Error('CDP session is closed'));

    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      this.pending.set(id, { resolve, reject });
      this.webSocket.send(JSON.stringify({
        id,
        method,
        params: params || {},
      }), err => {
        if (!err) return;
        this.pending.delete(id);
        reject(err);
      });
    });
  }

  waitFor(method, predicate, timeoutMs) {
    const timeout = timeoutMs || 15000;
    return new Promise((resolve, reject) => {
      const waiter = {
        method,
        predicate: typeof predicate === 'function' ? predicate : () => true,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters = this.waiters.filter(item => item !== waiter);
          reject(new Error(`Timeout waiting for ${method}`));
        }, timeout),
      };
      this.waiters.push(waiter);
    });
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try { this.webSocket.close(); } catch (e) {}
  }
}

async function cdpEvaluate(session, expression) {
  const result = await session.send('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue: true,
  });

  if (result.exceptionDetails) {
    const description = result.result && result.result.description;
    throw new Error(description || 'Failed to evaluate Chrome expression');
  }

  return result.result ? result.result.value : undefined;
}

async function waitForAlmaReady(session, emit) {
  let lastState = null;
  let challengeLogged = false;

  for (let i = 0; i < 80; i++) {
    lastState = await cdpEvaluate(session, `(() => ({
      href: location.href,
      title: document.title || '',
      readyState: document.readyState,
      hasFrontendData: !!window.frontendData,
      hasDataScript: Array.from(document.scripts || []).some(script => /\\/data\\.js/i.test(script.src || '')),
      bodyText: document.body ? document.body.innerText.slice(0, 200) : ''
    }))()`);

    if (lastState && (lastState.hasFrontendData || lastState.hasDataScript)) return lastState;

    if (!challengeLogged && lastState && /just a moment/i.test(`${lastState.title} ${lastState.bodyText}`)) {
      emit('info', 'Alma Cloudflare challenge in progress...');
      challengeLogged = true;
    }

    await sleep(500);
  }

  throw new Error(`Alma page did not become ready (${shortText(JSON.stringify(lastState || {}), 140)})`);
}

async function openAlmaBrowser(chain, emit) {
  const chromePath = getChromeExecutable();
  if (!chromePath) throw new Error('Chrome or Edge was not found on this machine');

  const port = await getFreePort();
  const profileDir = fs.mkdtempSync(path.join(__dirname, '.alma-chrome-'));
  const args = [
    `--remote-debugging-port=${port}`,
    '--headless=new',
    '--disable-gpu',
    '--disable-blink-features=AutomationControlled',
    '--window-size=1366,768',
    '--lang=he-IL',
    '--no-first-run',
    '--no-default-browser-check',
    `--user-data-dir=${profileDir}`,
    'about:blank',
  ];

  emit('info', `Launching browser: ${path.basename(chromePath)}`);
  const chrome = spawn(chromePath, args, {
    stdio: 'ignore',
    windowsHide: true,
  });

  try {
    const debuggerUrl = await getDebuggerPageUrl(port, 20000);
    const session = await new Promise((resolve, reject) => {
      const ws = new WebSocket(debuggerUrl);
      ws.once('open', () => resolve(new CdpSession(ws)));
      ws.once('error', reject);
    });

    await session.send('Page.enable');
    await session.send('Runtime.enable');
    await session.send('Network.enable');
    await session.send('Network.setCacheDisabled', { cacheDisabled: true });
    await session.send('Network.setUserAgentOverride', {
      userAgent: CHROME_UA,
      acceptLanguage: 'he-IL,he;q=0.9',
      platform: 'Windows',
    });
    await session.send('Page.addScriptToEvaluateOnNewDocument', {
      source: `
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = window.chrome || { runtime: {} };
      `,
    });

    await session.send('Page.navigate', { url: chain.homeUrl || ALMA_BASE });
    try {
      await session.waitFor('Page.loadEventFired', null, 20000);
    } catch (e) {}

    await waitForAlmaReady(session, emit);
    return { chrome, session, profileDir };
  } catch (error) {
    await terminateProcess(chrome);
    removeDirSafe(profileDir);
    throw error;
  }
}

async function closeAlmaBrowser(browser) {
  if (!browser) return;
  try {
    if (browser.session) browser.session.close();
  } catch (e) {}
  await terminateProcess(browser.chrome);
  removeDirSafe(browser.profileDir);
}

function getNamesText(value) {
  if (!value) return '';
  if (typeof value === 'string') return value.trim();
  if (Array.isArray(value)) {
    for (const item of value) {
      const text = getNamesText(item);
      if (text) return text;
    }
    return '';
  }
  if (typeof value.name === 'string' && value.name.trim()) return value.name.trim();

  const preferredKeys = ['1', 1, 'he', 'he-IL', '2', 2];
  for (const key of preferredKeys) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
    const text = getNamesText(value[key]);
    if (text) return text;
  }

  for (const item of Object.values(value)) {
    const text = getNamesText(item);
    if (text) return text;
  }

  return '';
}

function parseFrontendDataScript(scriptText) {
  const sandbox = { window: {} };
  vm.runInNewContext(scriptText, sandbox);
  const frontendData =
    sandbox.window &&
    sandbox.window.sp &&
    sandbox.window.sp.frontendData;

  if (!frontendData) throw new Error('Alma frontendData was not found inside data.js');
  return frontendData;
}

function getAlmaNodeChildren(node) {
  const keys = ['children', 'categories', 'subCategories', 'subcategories', 'items'];
  for (let i = 0; i < keys.length; i++) {
    const value = node && node[keys[i]];
    if (Array.isArray(value) && value.length) return value;
  }
  return [];
}

function hasVisibleProducts(node, branchId) {
  if (!node || !node.branches) return true;
  const meta = node.branches[branchId] || node.branches[String(branchId)];
  if (!meta || typeof meta.hasVisibleProducts !== 'boolean') return true;
  return meta.hasVisibleProducts;
}

function collectLeafCategories(frontendData, branchId) {
  const seen = new Set();
  const categories = [];
  const retailer = frontendData && frontendData.retailer || {};
  const rootNodes =
    Array.isArray(frontendData && frontendData.tree) ? frontendData.tree :
    Array.isArray(frontendData && frontendData.tree && frontendData.tree.categories) ? frontendData.tree.categories :
    Array.isArray(frontendData && frontendData.tree && frontendData.tree.children) ? frontendData.tree.children :
    Array.isArray(retailer.categories) ? retailer.categories :
    [];

  function walk(nodes, parents) {
    if (!Array.isArray(nodes)) return;
    for (let i = 0; i < nodes.length; i++) {
      const node = nodes[i];
      if (!node || typeof node !== 'object') continue;

      const id = Number(node.id);
      const name = getNamesText(node.names || node.name);
      const children = getAlmaNodeChildren(node);
      const nextParents = name ? parents.concat([name]) : parents;

      if (Number.isFinite(id) && hasVisibleProducts(node, branchId) && children.length === 0 && !seen.has(id)) {
        seen.add(id);
        categories.push({
          id,
          name: name || String(id),
          path: nextParents.join(' > ') || String(id),
        });
      }

      if (children.length) walk(children, nextParents);
    }
  }

  walk(rootNodes, []);
  return categories;
}

function getBranchInfo(frontendData, branchId) {
  const retailer = frontendData && frontendData.retailer || {};
  const branches = Array.isArray(retailer.branches) ? retailer.branches : [];
  const branch = branches.find(item => String(item && item.id) === String(branchId));
  if (!branch) return null;

  return {
    id: branch.id,
    name: branch.name || '',
    city: branch.city || '',
    location: branch.location || '',
  };
}

function getAlmaUnitQty(product) {
  const unitResolution = parseNumber(product.unitResolution, 0);
  const weight = parseNumber(product.weight, 0);
  const numberOfItems = parseNumber(product.numberOfItems, 0);
  const unitLabel = firstText(product.unitOfMeasure, product.unitOfNormalization, product.soldBy);

  if (unitResolution > 0) return `${unitResolution} ${unitLabel}`.trim();
  if (weight > 0) return `${weight} ${unitLabel}`.trim();
  if (numberOfItems > 1) return `${numberOfItems}`;
  return '';
}

function mapAlmaProduct(product, chain, branchInfo) {
  if (!product || typeof product !== 'object') return null;

  const branch = product.branch || {};
  const price = parseNumber(firstText(branch.salePrice, branch.regularPrice), 0);
  const regularPrice = parseNumber(firstText(branch.regularPrice, branch.salePrice), 0);
  const finalPrice = price > 0 ? price : regularPrice;
  if (finalPrice <= 0) return null;

  const stableId = firstText(product.id, product.productId, branch.branchProductId);
  const name = firstText(
    getNamesText(product.names),
    product.localName
  );

  if (!stableId || !name) return null;

  return {
    id: `${chain.id}_${branchInfo.id}_${stableId}`,
    barcode: '',
    name,
    price: finalPrice,
    unitPrice: parseNumber(firstText(product.unitPrice, product.pricePerUnit), finalPrice) || finalPrice,
    unitQty: getAlmaUnitQty(product),
    manufacturer: firstText(getNamesText(product.brand && product.brand.names), product.brand && product.brand.name),
    country: firstText(product.countryOfOrigin, product.country, product.origin),
    chain: chain.id,
    chainName: chain.name,
    storeId: String(branchInfo.id),
    storeName: String(branchInfo.name || chain.name),
  };
}

async function fetchInPage(session, url, pathname) {
  const payload = await cdpEvaluate(session, `(async () => {
    const targetPath = ${JSON.stringify(pathname)};
    history.replaceState({}, '', targetPath);
    const response = await fetch(${JSON.stringify(url)}, {
      credentials: 'include',
      headers: {
        'Accept': 'application/json, text/plain, */*',
        'Pathname': targetPath
      }
    });
    const text = await response.text();
    return {
      ok: response.ok,
      status: response.status,
      text
    };
  })()`);

  if (!payload || !payload.ok) {
    const preview = payload && payload.text ? shortText(payload.text, 160) : 'empty response';
    throw new Error(`HTTP ${payload ? payload.status : 'n/a'}: ${preview}`);
  }

  try {
    return JSON.parse(payload.text);
  } catch (e) {
    throw new Error(`Invalid Alma JSON: ${shortText(payload && payload.text, 160)}`);
  }
}

async function fetchAlmaJson(session, url, pathname) {
  let lastError = null;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      return await fetchInPage(session, url, pathname);
    } catch (error) {
      lastError = error;
      if (attempt < 3) await sleep(600 * attempt);
    }
  }

  throw lastError;
}

async function getAlmaBootstrap(session, chain) {
  const scriptUrls = await cdpEvaluate(session, `(() => Array.from(document.scripts || []).map(script => script.src).filter(Boolean))()`);
  const dataScriptUrl = (Array.isArray(scriptUrls) ? scriptUrls : []).find(url => /\/data\.js/i.test(url));

  if (!dataScriptUrl) {
    throw new Error('Alma data.js script was not found in the page DOM');
  }

  const res = await axios.get(dataScriptUrl, {
    timeout: 30000,
    responseType: 'text',
    headers: {
      'User-Agent': CHROME_UA,
      'Accept': '*/*',
      'Accept-Language': 'he-IL,he;q=0.9',
    },
  });

  const frontendData = parseFrontendDataScript(res.data);
  return {
    branch: getBranchInfo(frontendData, chain.branchId),
    categories: collectLeafCategories(frontendData, chain.branchId),
  };
}

function buildCategoryUrl(chain, categoryId, from, size) {
  const params = new URLSearchParams({
    appId: String(ALMA_APP_ID),
    from: String(from),
    size: String(size),
    languageId: '1',
    minScore: '0',
  });

  return `${ALMA_BASE}/v2/retailers/${chain.retailerId}/branches/${chain.branchId}/categories/${categoryId}/products?${params.toString()}`;
}

async function fetchCategoryProducts(session, chain, branchInfo, category) {
  const pathname = `/categories/${category.id}/products`;
  const firstPage = await fetchAlmaJson(session, buildCategoryUrl(chain, category.id, 0, ALMA_PAGE_SIZE), pathname);
  const firstItems = Array.isArray(firstPage.products) ? firstPage.products : [];
  const total = Number.isFinite(firstPage.total) ? firstPage.total : firstItems.length;
  const products = [];

  for (let i = 0; i < firstItems.length; i++) {
    const mapped = mapAlmaProduct(firstItems[i], chain, branchInfo);
    if (mapped) products.push(mapped);
  }

  for (let offset = firstItems.length; offset < total; offset += ALMA_PAGE_SIZE) {
    const page = await fetchAlmaJson(session, buildCategoryUrl(chain, category.id, offset, ALMA_PAGE_SIZE), pathname);
    const pageItems = Array.isArray(page.products) ? page.products : [];
    for (let i = 0; i < pageItems.length; i++) {
      const mapped = mapAlmaProduct(pageItems[i], chain, branchInfo);
      if (mapped) products.push(mapped);
    }
  }

  return products;
}

async function fetchAlma(chain, emit) {
  emit('start', `Connecting to Alma for ${chain.name}`, { platform: 'Alma Web' });

  let browser = null;
  try {
    browser = await openAlmaBrowser(chain, emit);
    const bootstrap = await getAlmaBootstrap(browser.session, chain);

    if (!bootstrap.branch) {
      emit('warn', `Alma branch ${chain.branchId} was not found`);
      return [];
    }

    const branchInfo = {
      id: bootstrap.branch.id,
      name: bootstrap.branch.name || chain.storeName || chain.name,
    };

    emit('info', `Alma branch: ${branchInfo.name} | ${bootstrap.categories.length} leaf categories`);
    if (!bootstrap.categories.length) {
      emit('warn', 'No Alma categories were discovered for this branch');
      return [];
    }

    const products = [];
    for (let i = 0; i < bootstrap.categories.length; i++) {
      const category = bootstrap.categories[i];
      const label = shortText(category.path || category.name || String(category.id), 90);
      if (i === 0 || i === bootstrap.categories.length - 1 || i % 10 === 0) {
        emit('progress', `Category ${i + 1}/${bootstrap.categories.length}: ${label}`);
      }

      try {
        const categoryProducts = await fetchCategoryProducts(browser.session, chain, branchInfo, category);
        if (categoryProducts.length) {
          emit('info', `${label}: ${categoryProducts.length.toLocaleString('he-IL')} products`);
          for (let j = 0; j < categoryProducts.length; j++) products.push(categoryProducts[j]);
        }
      } catch (error) {
        emit('warn', `Alma category ${category.id} failed: ${getErrorMessage(error)}`);
      }
    }

    return products;
  } catch (error) {
    emit('error', `Alma error: ${getErrorMessage(error)}`);
    return [];
  } finally {
    await closeAlmaBrowser(browser);
  }
}

module.exports = { fetchAlma };
