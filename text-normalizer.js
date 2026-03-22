const BIDI_CONTROL_RE = /[\u200e\u200f\u202a-\u202e]/g;
const BROKEN_TEXT_RE = /[ï¿½Ã×â]/;
const UNKNOWN_TEXTS = new Set([
  'undefined',
  'null',
  'unknown',
  'n/a',
  'na',
  '\u05DC\u05D0 \u05D9\u05D3\u05D5\u05E2',
]);

function hasBrokenText(value = '') {
  return BROKEN_TEXT_RE.test(String(value || ''));
}

function repairBrokenText(value = '') {
  if (value === null || value === undefined) return '';
  if (typeof value !== 'string') return value;

  let next = value;
  for (let i = 0; i < 2 && hasBrokenText(next); i++) {
    try {
      const repaired = Buffer.from(next, 'latin1').toString('utf8');
      if (!repaired || repaired === next) break;
      next = repaired;
    } catch (error) {
      break;
    }
  }

  return next;
}

function stripDirectionalMarks(value = '') {
  return String(value || '').replace(BIDI_CONTROL_RE, '');
}

function normalizeDisplayText(value = '') {
  return stripDirectionalMarks(repairBrokenText(String(value || '')))
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeOptionalText(value = '') {
  const normalized = normalizeDisplayText(value);
  if (!normalized) return '';
  return UNKNOWN_TEXTS.has(normalized.toLowerCase()) ? '' : normalized;
}

module.exports = {
  hasBrokenText,
  repairBrokenText,
  stripDirectionalMarks,
  normalizeDisplayText,
  normalizeOptionalText,
};
