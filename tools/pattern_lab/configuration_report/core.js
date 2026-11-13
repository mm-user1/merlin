/* Native chunk decoding and metadata helpers. Also executed unchanged by Node tests. */
(() => {
  'use strict';
  const check = (ok, message) => { if (!ok) throw new Error(message); };
  const key = value => value === undefined ? 'missing' : JSON.stringify(value);
  const scalarPaths = (value, path = '', out = {}) => {
    if (value !== null && typeof value === 'object') {
      for (const [k, v] of Object.entries(value)) scalarPaths(v, path + '/' + k.replace(/~/g, '~0').replace(/\//g, '~1'), out);
    } else out[path] = value;
    return out;
  };
  const digest = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(b => b.toString(16).padStart(2, '0')).join('');
  function capabilities() {
    check(typeof DecompressionStream === 'function' && globalThis.crypto?.subtle,
      'Open this standalone report in an installed Chrome or Edge with Web Crypto and DecompressionStream enabled.');
  }
  const numericArray = (a, nullable = false) => Array.isArray(a) && a.every(x => nullable && x === null || Number.isSafeInteger(x));
  function validate(value, entry, configurations) {
    check(value?.schema === 'pattern-lab.configuration-chunk.v1' && Array.isArray(value.records), 'Native chunk schema');
    const rows = value.records;
    check(rows.length === (entry.kind === 'price' ? 1 : entry.resultIds.length), 'Native record count');
    if (entry.kind === 'curves') check(Array.isArray(value.timeGrids) && value.timeGrids.every(g => numericArray(g) && g.every((t, i) => !i || t > g[i - 1])), 'Native time grids');
    rows.forEach((r, i) => {
      check(r.stageId === entry.stageId, 'Native stage identity');
      if (entry.kind === 'price') {
        const p = r.price;
        check(r.instrumentId === entry.instrumentId && p?.curveVersion === 'configuration-curves.1' && p.valueScale === 100 &&
          numericArray(p.timeMs) && p.timeMs.every((t,j)=>!j||t>p.timeMs[j-1]) && numericArray(p.priceQ, true) && p.timeMs.length === p.priceQ.length && Array.isArray(p.coverageGaps) && ['available','noObservedBars'].includes(p.status), 'Native price shape');
      } else {
        const c = configurations.get(r.configurationId);
        check(c && c.instrumentId === entry.instrumentId && c.timeframeMinutes === entry.timeframeMinutes && r.resultId === entry.resultIds[i], 'Native result identity');
        if (configurations.byResult) check(configurations.byResult.get(r.resultId) === r.configurationId, 'Native configuration/result binding');
        if (entry.kind === 'metrics') {
          check(r.status === 'complete' && r.metrics?.metricsVersion === 'configuration-metrics.1' && r.execution && typeof r.execution === 'object', 'Native metrics shape');
          for (const name of ['returnPct', 'annualizedProfitPct', 'mtmMaxDdPct', 'balanceMaxDdPct', 'dailySharpe', 'trades', 'tradesPerMonth', 'winRatePct', 'profitFactor', 'timeInMarketPct', 'maxEntryLeverage']) {
            check(r.metrics[name] === null || typeof r.metrics[name] === 'number' && Number.isFinite(r.metrics[name]), 'Native metric value');
          }
        } else {
          const c = r.curve, times = value.timeGrids[c?.gridId];
          check(c?.curveVersion === 'configuration-curves.1' && c.valueScale === 100 && Number.isInteger(c.gridId) && times &&
            numericArray(c.equityQ) && numericArray(c.balanceQ) && times.length > 0 && times.length === c.equityQ.length && times.length === c.balanceQ.length && Array.isArray(c.cashGaps) && ['available','noObservedBars'].includes(c.status), 'Native curve shape');
        }
      }
    });
    return value;
  }
  async function decode(encoded, entry, configurations) {
    capabilities();
    check(Number.isSafeInteger(entry.decodedBytes) && entry.decodedBytes > 0 && entry.decodedBytes <= 8 * 1024 * 1024, 'Decoded byte bound');
    check(typeof encoded === 'string' && encoded.length === 4 * Math.ceil(entry.bytes / 3), 'Encoded byte bound');
    const bytes = Uint8Array.from(atob(encoded), c => c.charCodeAt(0));
    check(bytes.length === entry.bytes && await digest(bytes) === entry.sha256, 'Compressed integrity');
    const reader = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();
    const raw = new Uint8Array(entry.decodedBytes);
    let count = 0;
    try {
      while (true) {
        const {done, value} = await reader.read();
        if (done) break;
        check(count + value.length <= raw.length, 'Decoded size overflow');
        raw.set(value, count); count += value.length;
      }
    } catch (error) { await reader.cancel().catch(() => {}); throw error; }
    check(count === raw.length && await digest(raw) === entry.decodedSha256, 'Decoded integrity');
    return validate(JSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(raw)), entry, configurations);
  }
  class Loader {
    constructor(manifest, block) {
      this.entries = manifest.index.members;
      this.configurations = new Map(manifest.metadata.configurations.map(c => [c.configurationId, c]));
      this.configurations.byResult = new Map(manifest.rows.filter(r=>r.status==='complete').map(r=>[r.resultId,r.configurationId]));
      this.block = block; this.cache = new Map(); this.bytes = 0; this.peak = 0; this.running = 0;
      this.decoded = {IS:0, HO1:0, HO2:0}; this.limit = 32 * 1024 * 1024; this.needed = new Set();
    }
    retain(indices) {
      const wanted = new Set(indices);
      check([...wanted].reduce((n, i) => n + this.entries[i].decodedBytes, 0) <= this.limit, 'Selection exceeds decoded cache budget');
      this.needed = wanted;
      for (const [i, item] of this.cache) if (!wanted.has(i) && !item.pending) this.drop(i);
    }
    drop(i) {
      const item = this.cache.get(i);
      if (item && !item.pending) { this.bytes -= this.entries[i].decodedBytes; this.cache.delete(i); }
    }
    hide() {
      for (const i of this.needed) if (this.entries[i].stageId === 'HO2') this.needed.delete(i);
      for (const [i, item] of this.cache) if (this.entries[i].stageId === 'HO2') { item.discard = true; this.drop(i); }
    }
    async get(i, cache = true) {
      if (this.cache.has(i)) return this.cache.get(i).promise;
      check(this.running < 2, 'Decoder concurrency bound');
      const entry = this.entries[i];
      for (const [j, item] of this.cache) if (this.bytes + entry.decodedBytes > this.limit && !this.needed.has(j) && !item.pending) this.drop(j);
      check(this.bytes + entry.decodedBytes <= this.limit, 'Decoded cache resource bound');
      this.bytes += entry.decodedBytes; this.peak = Math.max(this.peak, this.bytes); this.running++;
      const item = {pending:true, discard:!cache};
      const promise = decode(this.block(i), entry, this.configurations).then(value => {
        this.decoded[entry.stageId]++; return value;
      }).catch(error => { item.discard = true; throw error; }).finally(() => {
        this.running--; item.pending = false;
        if (item.discard || !this.needed.has(i)) this.drop(i);
      });
      item.promise = promise; this.cache.set(i, item);
      return promise;
    }
  }
  globalThis.ConfigurationReportCore = {check, key, scalarPaths, digest, capabilities, validate, decode, Loader};
})();
