const parsePair = (paramId, hexValue) => ({
  paramId: parseInt(paramId),
  value: parseInt(hexValue, 16)
});

const probeIndex = (paramId) => Math.floor((paramId - 2020) / 4) + 1;
const fieldType = (paramId) => (paramId - 2020) % 4;

const FIELD_LABELS = { 0: 'level', 1: 'volume', 2: 'temperature', 3: 'percentage' };
const DIVIDE_BY_TEN = new Set([0, 1]);

const decodeFuelData = (payload) => {
  if (!payload || typeof payload !== 'string') return null;

  const parts = payload.split(',');
  if (parts.length < 2) return null;

  const messageType = parseInt(parts[1]);
  if (!Number.isFinite(messageType)) return null;

  if (parts.length < 4) return { messageType };

  const tanks = {};

  for (let i = 3; i + 1 < parts.length; i += 2) {
    const { paramId, value } = parsePair(parts[i], parts[i + 1]);
    if (!Number.isFinite(paramId) || !Number.isFinite(value)) continue;

    const probe = probeIndex(paramId);
    const field = fieldType(paramId);
    if (probe < 1 || !(field in FIELD_LABELS)) continue;

    const key = `tank${probe}`;
    if (!tanks[key]) tanks[key] = {};

    tanks[key][FIELD_LABELS[field]] = DIVIDE_BY_TEN.has(field) ? value / 10 : value;
  }

  return { messageType, ...tanks };
};

const hasFuelData = (decoded) => {
  if (!decoded) return false;
  const t1 = decoded.tank1;
  const t2 = decoded.tank2;
  return (t1 && t1.volume > 0) || (t2 && t2.volume > 0);
};

module.exports = { decodeFuelData, hasFuelData };
