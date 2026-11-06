// Duotalk - signaling + matchmaking server. Zero dependencies (Node 16+).
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
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || null;   // from Google Cloud Console (OAuth client ID)
const ADMIN_KEY = process.env.ADMIN_KEY || null;                  // secret to view /admin/users?key=...
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'users.json');
const sessions = new Map();                                       // token -> { email }

// Simple user store (JSON file). On free Render hosting the disk resets on restart: use a hosted database for permanence.
let users = {};
try { users = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch {}
let saveTimer;
function touchUser(email, patch = {}) {
  const u = users[email] || (users[email] = { email, firstSeen: new Date().toISOString() });
  Object.assign(u, { lastSeen: new Date().toISOString() }, patch);
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(DB_FILE, JSON.stringify(users), () => {}), 500);
}

const clients = new Map();   // id -> { id, res, ip, partner, interests, blocked:Set, hits:[] }
const reportsByIp = new Map();
const bans = new Map();      // ip -> until timestamp
let queue = [];

const geoCache = new Map();
async function lookupGeo(ip) {
  ip = ip.replace('::ffff:', '');
  if (geoCache.has(ip)) return geoCache.get(ip);
  try {
    const r = await fetch('https://ipwho.is/' + encodeURIComponent(ip) + '?fields=success,country_code', { signal: AbortSignal.timeout(4000) });
    const j = await r.json();
    const g = j.success && /^[A-Z]{2}$/.test(j.country_code) ? { code: j.country_code } : null;
    geoCache.set(ip, g);
    return g;
  } catch { return null; }
}

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
    send(best, { type: 'matched', initiator: true, peer: c.country });
    send(c, { type: 'matched', initiator: false, peer: best.country });
  } else {
    queue.push(c.id);
    send(c, { type: 'waiting' });
  }
}

function logReport(reporter, reported, reason) {
  const line = JSON.stringify({ time: new Date().toISOString(), reporter: reporter.id, reported: reported.id, reason: String(reason || '').slice(0, 200) });
  fs.appendFile(REPORT_LOG, line + '\n', () => {});
  const n = (reportsByIp.get(reported.email) || 0) + 1;
  reportsByIp.set(reported.email, n);
  if (n >= BAN_AFTER_REPORTS) {
    bans.set(reported.email, Date.now() + BAN_MS);
    reportsByIp.delete(reported.email);
    send(reported, { type: 'banned' });
    if (!reported.res.writableEnded) reported.res.end();
  }
}

const cleanInterests = arr => (Array.isArray(arr) ? arr : []).slice(0, 5)
  .map(s => String(s).toLowerCase().trim().slice(0, 20)).filter(Boolean);

const server = http.createServer(async (req, res) => {
  const ip = getIp(req);
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
    // Look in public/ first, then next to server.js (in case the folder structure was flattened on upload)
    const file = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find(f => fs.existsSync(f));
    return fs.readFile(file || '', (e, buf) => {
      if (e) { res.writeHead(500); return res.end('index.html not found next to server.js or in a public folder'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(buf);
    });
  }

  if (req.method === 'GET' && pathname === '/config') {
    let iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
    try { if (process.env.ICE_SERVERS) iceServers = JSON.parse(process.env.ICE_SERVERS); } catch {}
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers, googleClientId: GOOGLE_CLIENT_ID, allowGuest: process.env.ALLOW_GUEST === '1' }));
  }

  if (req.method === 'GET' && pathname === '/admin/users') {
    if (!ADMIN_KEY || url.searchParams.get('key') !== ADMIN_KEY) { res.writeHead(403); return res.end('Forbidden'); }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(Object.values(users), null, 2));
  }

  if (req.method === 'GET' && pathname === '/events') {
    const sess = sessions.get(url.searchParams.get('t'));
    if (!sess) { res.writeHead(401); return res.end('Please sign in'); }
    if (isBanned(sess.email)) { res.writeHead(403); return res.end('Temporarily banned'); }
    // one account = one live connection: a newer tab or device replaces the older one
    for (const old of clients.values()) if (old.email === sess.email) { send(old, { type: 'kicked' }); old.res.end(); }
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    const id = crypto.randomBytes(8).toString('hex');
    const c = { id, res, ip, email: sess.email, gender: users[sess.email] && users[sess.email].gender, partner: null, interests: [], country: null, want: 'ALL', blocked: new Set(), hits: [] };
    clients.set(id, c);
    send(c, { type: 'hello', id });
    lookupGeo(ip).then(g => { if (g && clients.has(id)) { c.geo = g; c.country = g.code; touchUser(c.email, { country: g.code }); send(c, { type: 'geo', code: g.code }); } });
    const ka = setInterval(() => { if (!res.writableEnded) res.write(': ping\n\n'); }, 20000);
    req.on('close', () => { clearInterval(ka); leave(c); clients.delete(id); });
    return;
  }

  if (req.method === 'POST') {
    let body;
    try { body = await readBody(req); } catch { res.writeHead(413); return res.end(); }
    if (pathname === '/auth') {
      let email = null;
      if (body.guest && process.env.ALLOW_GUEST === '1') email = 'guest-' + crypto.randomBytes(4).toString('hex') + '@guest.local';   // testing only
      else if (body.credential && GOOGLE_CLIENT_ID) {
        try {
          const r = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(body.credential), { signal: AbortSignal.timeout(5000) });
          const j = await r.json();
          if (r.ok && j.aud === GOOGLE_CLIENT_ID && String(j.email_verified) === 'true' && j.email) email = j.email.toLowerCase();
        } catch {}
      }
      if (!email) { res.writeHead(401); return res.end(); }
      if (isBanned(email)) { res.writeHead(403); return res.end(); }
      const token = crypto.randomBytes(24).toString('hex');
      sessions.set(token, { email });
      touchUser(email);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ token, email, gender: users[email].gender || null }));
    }
    if (pathname === '/me') {
      const sess = sessions.get(body.token);
      if (!sess) { res.writeHead(401); return res.end(); }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ email: sess.email, gender: (users[sess.email] || {}).gender || null }));
    }
    const c = clients.get(body.id);
    if (!c) { res.writeHead(404); return res.end(); }
    const now = Date.now();
    c.hits = c.hits.filter(t => now - t < RATE_LIMIT.windowMs); c.hits.push(now);
    if (c.hits.length > RATE_LIMIT.max) { res.writeHead(429); return res.end(); }

    const partner = c.partner ? clients.get(c.partner) : null;
    switch (pathname) {
      case '/find': {
        const code = v => (/^[A-Z]{2}$/.test(v) ? v : null);
        enqueue(c, cleanInterests(body.interests), (c.geo ? c.geo.code : (code(body.country) || c.country)), code(body.want) || 'ALL');
        break;
      }
      case '/signal': if (partner && body.data) send(partner, { type: 'signal', data: body.data }); break;
      case '/chat': { const t = String(body.text || '').trim().slice(0, 500); if (partner && t) send(partner, { type: 'chat', text: t }); break; }
      case '/block': if (partner) { c.blocked.add(partner.id); leave(c); } break;
      case '/report': if (partner) { logReport(c, partner, body.reason); c.blocked.add(partner.id); leave(c); } break;
      case '/stop': leave(c); break;
      case '/gender': if (['male', 'female', 'couple'].includes(body.gender)) { c.gender = body.gender; touchUser(c.email, { gender: body.gender, country: c.country }); } break;
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

server.listen(PORT, () => console.log('Duotalk running on http://localhost:' + PORT));
