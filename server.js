// We Meet - signaling + matchmaking server. Zero dependencies (Node 16+).
// Video/audio flows peer-to-peer via WebRTC; this server only pairs people and relays setup messages.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const REPORT_LOG = path.join(__dirname, 'reports.log');
const BAN_AFTER_REPORTS = 3;                 // reports against one IP before a temporary ban
const BAN_MS = 24 * 60 * 60 * 1000;          // 24 hours
const RATE_LIMIT = { windowMs: 10000, max: 200 };
const PAGE_LOCATIONS = ['public', 'Public', '.'].map(d => path.join(__dirname, d, 'index.html'));

const clients = new Map();   // id -> { id, res, ip, partner, interests, blocked:Set, hits:[] }
const reportsByIp = new Map();
const bans = new Map();      // ip -> until timestamp
let queue = [];

const getIp = req => (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const isBanned = ip => (bans.get(ip) || 0) > Date.now();
const send = (c, obj) => { if (c && !c.res.writableEnded) c.res.write('data: ' + JSON.stringify(obj) + '\n\n'); };

function readBody(req) {
  return new Promise((resolve, reject) => {
    let d = '';
    req.on('data', ch => { d += ch; if (d.length > 20000) { reject(new Error('too large')); req.destroy(); } });
    req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
    req.on('error', reject);
  });
}

function leave(c) {
  if (c.partner) {
    const p = clients.get(c.partner);
    c.partner = null;
    if (p) { p.partner = null; send(p, { type: 'partner-left' }); }
  }
  queue = queue.filter(i => i !== c.id);
}

function enqueue(c, interests, country, want) {
  leave(c);
  c.interests = interests; c.country = country; c.want = want;
  const wants = (a, b) => a.want === 'ALL' || a.want === b.country;   // does a accept b's country?
  const ok = o => o && o.id !== c.id && !c.blocked.has(o.id) && !o.blocked.has(c.id) && wants(c, o) && wants(o, c);
  const candidates = queue.map(i => clients.get(i)).filter(ok);
  const best = candidates.find(o => o.interests.some(t => interests.includes(t))) || candidates[0];
  if (best) {
    queue = queue.filter(i => i !== best.id);
    c.partner = best.id; best.partner = c.id;
    send(best, { type: 'matched', initiator: true });
    send(c, { type: 'matched', initiator: false });
  } else {
    queue.push(c.id);
    send(c, { type: 'waiting' });
  }
}

function logReport(reporter, reported, reason) {
  const line = JSON.stringify({ time: new Date().toISOString(), reporter: reporter.id, reported: reported.id, reason: String(reason || '').slice(0, 200) });
  fs.appendFile(REPORT_LOG, line + '\n', () => {});
  const n = (reportsByIp.get(reported.ip) || 0) + 1;
  reportsByIp.set(reported.ip, n);
  if (n >= BAN_AFTER_REPORTS) {
    bans.set(reported.ip, Date.now() + BAN_MS);
    reportsByIp.delete(reported.ip);
    send(reported, { type: 'banned' });
    if (!reported.res.writableEnded) reported.res.end();
  }
}

const cleanInterests = arr => (Array.isArray(arr) ? arr : []).slice(0, 5)
  .map(s => String(s).toLowerCase().trim().slice(0, 20)).filter(Boolean);

const server = http.createServer(async (req, res) => {
  const ip = getIp(req);

  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    const file = PAGE_LOCATIONS.find(f => fs.existsSync(f));
    if (!file) {
      res.writeHead(500);
      return res.end('index.html not found. Put it next to server.js or inside a folder named public.');
    }
    return fs.readFile(file, (e, buf) => {
      if (e) { res.writeHead(500); return res.end('Could not read index.html'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(buf);
    });
  }

  if (req.method === 'GET' && req.url === '/config') {
    let iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
    try { if (process.env.ICE_SERVERS) iceServers = JSON.parse(process.env.ICE_SERVERS); } catch {}
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers }));
  }

  if (req.method === 'GET' && req.url === '/events') {
    if (isBanned(ip)) { res.writeHead(403); return res.end('Temporarily banned'); }
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    const id = crypto.randomBytes(8).toString('hex');
    const c = { id, res, ip, partner: null, interests: [], country: null, want: 'ALL', blocked: new Set(), hits: [] };
    clients.set(id, c);
    send(c, { type: 'hello', id });
    const ka = setInterval(() => { if (!res.writableEnded) res.write(': ping\n\n'); }, 20000);
    req.on('close', () => { clearInterval(ka); leave(c); clients.delete(id); });
    return;
  }

  if (req.method === 'POST') {
    let body;
    try { body = await readBody(req); } catch { res.writeHead(413); return res.end(); }
    const c = clients.get(body.id);
    if (!c) { res.writeHead(404); return res.end(); }
    const now = Date.now();
    c.hits = c.hits.filter(t => now - t < RATE_LIMIT.windowMs); c.hits.push(now);
    if (c.hits.length > RATE_LIMIT.max) { res.writeHead(429); return res.end(); }

    const partner = c.partner ? clients.get(c.partner) : null;
    switch (req.url) {
      case '/find': {
        const code = v => (/^[A-Z]{2}$/.test(v) ? v : null);
        enqueue(c, cleanInterests(body.interests), code(body.country) || c.country, code(body.want) || 'ALL');
        break;
      }
      case '/signal': if (partner && body.data) send(partner, { type: 'signal', data: body.data }); break;
      case '/chat': { const t = String(body.text || '').trim().slice(0, 500); if (partner && t) send(partner, { type: 'chat', text: t }); break; }
      case '/block': if (partner) { c.blocked.add(partner.id); leave(c); } break;
      case '/report': if (partner) { logReport(c, partner, body.reason); c.blocked.add(partner.id); leave(c); } break;
      case '/stop': leave(c); break;
      default: res.writeHead(404); return res.end();
    }
    res.writeHead(204); return res.end();
  }

  res.writeHead(404); res.end();
});

// Live stats: users online, and how many per country
setInterval(() => {
  const countries = {};
  clients.forEach(c => { if (c.country) countries[c.country] = (countries[c.country] || 0) + 1; });
  clients.forEach(c => send(c, { type: 'stats', total: clients.size, countries }));
}, 3000);

server.listen(PORT, () => console.log('We Meet running on http://localhost:' + PORT));
